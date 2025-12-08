import json
import logging
from typing import Dict
from kafka import KafkaConsumer, KafkaProducer
from openai import OpenAI
from pydantic import ValidationError

from shared.common.config import LLMWorkerConfig
from shared.common.config import get_postgres_connection
from shared.common.models import TaggedNewsMessage, AnalyzedNewsMessage, LLMAnalysis


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class LLMAnalysisWorker:
    def __init__(self):
        self.config = LLMWorkerConfig()

        self.llm_client = OpenAI(api_key=self.config.OPENAI_API_KEY)

        self.consumer = KafkaConsumer(
            self.config.INPUT_TOPIC,
            bootstrap_servers=self.config.KAFKA_BOOTSTRAP_SERVERS,
            group_id=self.config.CONSUMER_GROUP,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            max_poll_interval_ms=600000,  # 10분
            session_timeout_ms=300000,  # 5분
            heartbeat_interval_ms=3000,  # 3초
            max_poll_records=1  # 한 번에 1개씩 처리
        )

        self.producer = KafkaProducer(
            bootstrap_servers=self.config.KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
        )

    def analyze_with_llm(self, news: TaggedNewsMessage) -> LLMAnalysis:
        """LLM으로 뉴스 분석"""
        entities_str = (
            f"기업: {', '.join(news.entities.companies)}, "
            f"인물: {', '.join(news.entities.persons)}, "
            f"위치: {', '.join(news.entities.locations)}, "
            f"조직: {', '.join(news.entities.organizations)}"
        )

        prompt = f"""
다음 뉴스를 분석하여 JSON 형식으로 응답하세요.

제목: {news.title}
본문: {news.content[:1000]}
추출된 엔티티: {entities_str}
종목: {news.stock_name} ({news.ticker})
변동률: {news.change_rate}%
급등락 유형: {news.trend_type}

분석 항목:
1. event_type: 사건 유형 ("계약", "소송", "정부정책", "신제품출시", "M&A", "기타")
2. impact: 주가 영향 ("호재", "악재", "중립", "불확실")
3. reason: 영향 판단 근거 (한 문장)
4. summary: 핵심 요약 (2-3 문장)

응답 형식:
{{
  "event_type": "...",
  "impact": "...",
  "reason": "...",
  "summary": "..."
}}
"""

        try:
            response = self.llm_client.chat.completions.create(
                model=self.config.LLM_MODEL,
                messages=[
                    {"role": "system", "content": "당신은 금융 뉴스 분석 전문가입니다."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                response_format={"type": "json_object"}
            )

            result = json.loads(response.choices[0].message.content)
            return LLMAnalysis(**result)

        except Exception as e:
            logger.error(f"LLM 분석 실패: {e}")
            return LLMAnalysis(
                event_type="기타",
                impact="불확실",
                reason="LLM 분석 실패",
                summary=news.title
            )

    def save_llm_analysis(self, news: TaggedNewsMessage, analysis: LLMAnalysis):
        """LLM 분석 결과를 DB에 저장"""
        news_id = news.url

        try:
            with get_postgres_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        UPDATE tagged_news
                        SET event_type = %s,
                            impact = %s,
                            reason = %s,
                            summary = %s,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE news_id = %s
                    """, (
                        analysis.event_type,
                        analysis.impact,
                        analysis.reason,
                        analysis.summary,
                        news_id
                    ))
                    conn.commit()
                    logger.debug(f"LLM 분석 저장 완료: {news_id}")
        except Exception as e:
            logger.error(f"LLM 분석 저장 실패 ({news_id}): {e}")

    def process_message(self, news_dict: Dict):
        """뉴스 분석 및 저장"""
        try:
            # Input: TaggedNewsMessage 검증
            news = TaggedNewsMessage(**news_dict)
        except ValidationError as e:
            logger.error(f"메시지 검증 실패: {e}")
            return

        # LLM 분석
        analysis = self.analyze_with_llm(news)

        # DB 업데이트
        self.save_llm_analysis(news, analysis)

        # Output: AnalyzedNewsMessage 생성 및 전송
        analyzed_news = AnalyzedNewsMessage(
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
            entities=news.entities,
            llm_analysis=analysis,
            section=news.section
        )
        self.producer.send(self.config.OUTPUT_TOPIC, value=analyzed_news.dict())

        logger.info(f"LLM 분석 완료: {news.url} - "
                    f"{analysis.event_type}/{analysis.impact}")

    def start(self):
        """워커 시작"""
        logger.info("=" * 50)
        logger.info("LLM 분석 워커 시작")
        logger.info(f"Input Topic: {self.config.INPUT_TOPIC}")
        logger.info(f"Output Topic: {self.config.OUTPUT_TOPIC}")
        logger.info(f"Consumer Group: {self.config.CONSUMER_GROUP}")
        logger.info(f"Kafka Servers: {self.config.KAFKA_BOOTSTRAP_SERVERS}")
        logger.info("=" * 50)

        # 구독 확인
        logger.info(f"구독된 토픽: {self.consumer.subscription()}")
        logger.info("메시지 대기 중...")
        try:
            for message in self.consumer:
                logger.info(f"[Kafka] 메시지 수신: partition={message.partition}, offset={message.offset}")

                news_dict = message.value
                news_id = news_dict.get('url')

                try:
                    logger.info(f"[LLM] 뉴스 분석 시작: {news_id}")
                    self.process_message(news_dict)

                    self.consumer.commit()
                    logger.info(f"[LLM] 커밋 완료: {news_id}")

                except Exception as e:
                    logger.error(f"처리 실패 ({news_id}): {e}", exc_info=True)
                    self.consumer.commit()

        except KeyboardInterrupt:
            logger.info("워커 종료")
        finally:
            self.close()

    def close(self):
        logger.info("워커 종료 중...")
        if self.consumer:
            self.consumer.close()
        if self.producer:
            self.producer.flush()
            self.producer.close()
        logger.info("LLM 워커 종료 완료")


if __name__ == "__main__":
    worker = LLMAnalysisWorker()
    worker.start()