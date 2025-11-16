import json
import logging
from typing import Dict

# 외부 라이브러리
from kafka import KafkaConsumer, KafkaProducer
from transformers import pipeline
import torch
from pydantic import ValidationError


# shared 모듈 import
from shared.common.config import NERWorkerConfig
from shared.common.config import get_postgres_connection
from shared.common.models import CrawledNewsMessage, TaggedNewsMessage, ExtractedEntities


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class NERTaggingWorker:
    def __init__(self):
        """NER 워커 초기화"""
        self.config = NERWorkerConfig()

        # NER 모델 로드
        logger.info(f"NER 모델 로딩: {self.config.NER_MODEL}")
        self.ner_pipeline = pipeline(
            "ner",
            model=self.config.NER_MODEL,
            aggregation_strategy="simple",
            device=0 if torch.cuda.is_available() else -1
        )

        # Kafka Consumer
        self.consumer = KafkaConsumer(
            self.config.INPUT_TOPIC,
            bootstrap_servers=self.config.KAFKA_BOOTSTRAP_SERVERS,
            group_id=self.config.CONSUMER_GROUP,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            max_poll_records=self.config.BATCH_SIZE
        )

        # Kafka Producer
        self.producer = KafkaProducer(
            bootstrap_servers=self.config.KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
            acks='all'
        )

        logger.info(f"NER 워커 초기화 완료 - Topic: {self.config.INPUT_TOPIC}")
        
    def extract_entities(self, text: str) -> ExtractedEntities:
        """NER 엔티티 추출"""
        max_length = 512
        if len(text) > max_length:
            text = text[:max_length]

        try:
            entities = self.ner_pipeline(text)
        except Exception as e:
            logger.error(f"NER 추출 실패: {e}")
            return ExtractedEntities()

        companies = []
        persons = []
        locations = []
        organizations = []

        for entity in entities:
            entity_type = entity.get('entity_group', '').upper()
            entity_text = entity['word'].strip()

            if 'OG' in entity_type:
                if any(kw in entity_text for kw in ['주식회사', '㈜', 'Inc', 'Corp', '그룹']):
                    companies.append(entity_text)
                else:
                    organizations.append(entity_text)
            elif 'PS' in entity_type:
                persons.append(entity_text)
            elif 'LC' in entity_type:
                locations.append(entity_text)

        return ExtractedEntities(
            companies=list(set(companies)),
            persons=list(set(persons)),
            locations=list(set(locations)),
            organizations=list(set(organizations))
        )

    def save_entities_to_db(self, news: CrawledNewsMessage, entities: ExtractedEntities):
        """엔티티를 DB에 저장"""
        news_id = news.url
        try:
            with get_postgres_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO tagged_news
                    (news_id, url, title, content, published_at, publisher, ticker,
                     stock_name, 
                     companies, persons, locations, organizations)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (news_id) DO UPDATE SET
                        url = EXCLUDED.url,
                        title = EXCLUDED.title,
                        content = EXCLUDED.content,
                        published_at = EXCLUDED.published_at,
                        publisher = EXCLUDED.publisher,
                        ticker = EXCLUDED.ticker,
                        stock_name = EXCLUDED.stock_name,
                        companies = EXCLUDED.companies,
                        persons = EXCLUDED.persons,
                        locations = EXCLUDED.locations,
                        organizations = EXCLUDED.organizations,
                        updated_at = CURRENT_TIMESTAMP
                    """, (
                        news_id,
                        news.url,
                        news.title,
                        news.content,
                        news.published_at,
                        news.publisher,
                        news.ticker,
                        news.stock_name,
                        entities.companies,
                        entities.persons,
                        entities.locations,
                        entities.organizations
                    ))
                    conn.commit()
                    logger.debug(f"DB 저장 완료: {news_id}")
        except Exception as e:
            logger.error(f"DB 저장 실패 ({news_id}): {e}")

    def process_message(self, news_dict: Dict):
        """Kafka에서 받은 뉴스 데이터 처리"""
        try:
            # Input: CrawledNewsMessage 검증
            news = CrawledNewsMessage(**news_dict)
        except ValidationError as e:
            logger.error(f"메시지 검증 실패: {e}")
            return

        # 제목과 본문 결합
        combined_text = f"{news.title} {news.content}"

        # NER 태깅
        entities = self.extract_entities(combined_text)

        # DB 저장
        self.save_entities_to_db(news, entities)

        # Output: TaggedNewsMessage 생성 및 전송
        tagged_news = TaggedNewsMessage(
            url=news.url,
            title=news.title,
            content=news.content,
            published_at=news.published_at,
            publisher=news.publisher,
            ticker=news.ticker,
            stock_name=news.stock_name,
            change_rate=news.change_rate,
            trend_type=news.trend_type,
            date=news.date,
            news_start_date=news.news_start_date,
            news_end_date=news.news_end_date,
            entities=entities
        )
        self.producer.send(self.config.OUTPUT_TOPIC, value=tagged_news.dict())

        logger.info(f"NER 태깅 완료: {news.url} - "
                    f"기업:{len(entities.companies)}, "
                    f"인물:{len(entities.persons)}, "
                    f"위치:{len(entities.locations)}, "
                    f"조직:{len(entities.organizations)}")

    def start(self):
        """워커 시작 - Kafka에서 뉴스 데이터 수신 및 처리"""
        logger.info(f"NER 워커 시작 중...")
        logger.info(f"- Input Topic: {self.config.INPUT_TOPIC}")
        logger.info(f"- Output Topic: {self.config.OUTPUT_TOPIC}")
        logger.info(f"- Consumer Group: {self.config.CONSUMER_GROUP}")

        try:
            for message in self.consumer:
                news_dict = message.value
                logger.info(f"[Kafka] 뉴스 수신: {news_dict.get('url')}")

                try:
                    self.process_message(news_dict)
                    self.consumer.commit()
                except Exception as e:
                    logger.error(f"처리 실패: {str(e)}", exc_info=True)

        except KeyboardInterrupt:
            logger.info("워커 종료 요청 (Ctrl+C)")
        except Exception as e:
            logger.error(f"워커 오류: {e}", exc_info=True)
        finally:
            self.close()

    def close(self):
        """리소스 정리"""
        logger.info("워커 종료 중...")
        if self.consumer:
            self.consumer.close()
        if self.producer:
            self.producer.flush()
            self.producer.close()
        logger.info("NER 워커 종료 완료")


if __name__ == "__main__":
    worker = NERTaggingWorker()
    worker.start()