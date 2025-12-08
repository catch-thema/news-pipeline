import json
import logging
import time
from datetime import datetime
from collections import defaultdict
from typing import Dict, Tuple

from kafka import KafkaConsumer, KafkaProducer
from pydantic import ValidationError

from shared.common.config import AggregationWorkerConfig
from shared.common.config import get_postgres_connection
from shared.common.models import EmbeddingCompleteMessage, StockMovementMessage
from shared.common.models.messages import HeadlineAnalysisMessage

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class AggregationWorker:
    """임베딩 완료 추적 및 리포트 생성 트리거"""

    def __init__(self):
        self.config = AggregationWorkerConfig()

        # embedding_complete 토픽 구독
        self.consumer = KafkaConsumer(
            self.config.INPUT_TOPIC,
            bootstrap_servers=self.config.KAFKA_BOOTSTRAP_SERVERS,
            group_id=self.config.CONSUMER_GROUP,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=False
        )

        # stock_ready 토픽 발행
        self.producer = KafkaProducer(
            bootstrap_servers=self.config.KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
            acks='all'
        )

        # (ticker, date) 쌍별 임베딩 완료 추적
        self.embedded_news: Dict[Tuple[str, str], dict] = defaultdict(lambda: {
            'urls': set(),
            'stock_name': None,
            'change_rate': 0.0,
            'trend_type': None,
            'news_start_date': None,
            'news_end_date': None,
            'last_seen': 0.0,
            'triggered': False
        })

        self.last_check_time = time.time()

    def get_stock_stats(self, message: EmbeddingCompleteMessage) -> Dict:
        """특정 날짜의 종목 뉴스 처리 통계 조회"""
        ticker = message.ticker
        news_start_date = message.news_start_date
        news_end_date = message.news_end_date

        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                # 1. 크롤링된 뉴스 수 (published_at 기준)
                cursor.execute("""
                    SELECT COUNT(*) FROM news
                    WHERE ticker = %s
                    AND published_at BETWEEN
                        TO_DATE(%s, 'YYYY.MM.DD')::TIMESTAMP
                        AND TO_DATE(%s, 'YYYY.MM.DD')::TIMESTAMP + INTERVAL '1 day'
                """, (ticker, news_start_date, news_end_date))
                crawled_count = cursor.fetchone()[0]

                # 2. 분석 완료된 뉴스 수 (URL 기반 JOIN)
                cursor.execute("""
                    SELECT COUNT(DISTINCT tn.id)
                    FROM tagged_news tn
                    JOIN news n ON tn.url = n.url
                    WHERE n.ticker = %s
                    AND n.published_at BETWEEN
                        TO_DATE(%s, 'YYYY.MM.DD')::TIMESTAMP
                        AND TO_DATE(%s, 'YYYY.MM.DD')::TIMESTAMP + INTERVAL '1 day'
                    AND tn.event_type IS NOT NULL
                """, (ticker, news_start_date, news_end_date))
                analyzed_count = cursor.fetchone()[0]

                # 3. 임베딩 완료된 뉴스 수 (metadata의 published_at 기준)
                cursor.execute("""
                    SELECT COUNT(DISTINCT nc.metadata->>'url')
                    FROM news_chunks nc
                    WHERE nc.metadata->>'ticker' = %s
                    AND TO_TIMESTAMP(nc.metadata->>'published_at', 'YYYY-MM-DD"T"HH24:MI:SS')
                        BETWEEN TO_DATE(%s, 'YYYY.MM.DD')::TIMESTAMP
                        AND TO_DATE(%s, 'YYYY.MM.DD')::TIMESTAMP + INTERVAL '1 day'
                """, (ticker, news_start_date, news_end_date))
                embedded_count = cursor.fetchone()[0]

                # 4. 뉴스 URL 목록
                cursor.execute("""
                    SELECT url FROM news
                    WHERE ticker = %s
                    AND published_at BETWEEN
                        TO_DATE(%s, 'YYYY.MM.DD')::TIMESTAMP
                        AND TO_DATE(%s, 'YYYY.MM.DD')::TIMESTAMP + INTERVAL '1 day'
                    ORDER BY published_at DESC
                """, (ticker, news_start_date, news_end_date))
                news_urls = [row[0] for row in cursor.fetchall()]

                return {
                    'stock_name': message.stock_name,
                    'crawled_count': crawled_count,
                    'analyzed_count': analyzed_count,
                    'embedded_count': embedded_count,
                    'news_urls': news_urls,
                    'news_start_date': news_start_date,
                    'news_end_date': news_end_date
                }

    def send_to_analyzing(self, message: HeadlineAnalysisMessage):
        """리포팅 서비스로 메시지 전송"""
        try:
            self.producer.send(self.config.OUTPUT_SECTION_TOPIC, value=message.dict())
            self.producer.flush()
            logger.info(f"[Trigger] 리포트 생성 요청: {message.section} ({message.news_start_date} ~ {message.news_end_date})")
        except Exception as e:
            logger.error(f"리포팅 서비스로 메시지 전송 실패: {e}", exc_info=True)
            raise

    def send_to_reporting(self, message: StockMovementMessage):
        """리포팅 서비스로 메시지 전송"""
        try:
            self.producer.send(self.config.OUTPUT_TOPIC, value=message.dict())
            self.producer.flush()
            logger.info(f"[Trigger] 리포트 생성 요청: {message.ticker} ({message.date})")
        except Exception as e:
            logger.error(f"리포팅 서비스로 메시지 전송 실패: {e}", exc_info=True)
            raise

    def check_stock_ready(self, message: EmbeddingCompleteMessage):
        """종목의 모든 처리가 완료되었는지 확인하고 트리거"""
        ticker = message.ticker
        date = message.date
        stats = self.get_stock_stats(message)

        logger.info(
            f"[{ticker}|{date}] 진행 상황 - "
            f"크롤링: {stats['crawled_count']}, "
            f"분석: {stats['analyzed_count']}, "
            f"임베딩: {stats['embedded_count']}"
        )

        # 모든 단계가 완료되었는지 확인
        if (stats['crawled_count'] > 0 and
                stats['crawled_count'] == stats['analyzed_count'] == stats['embedded_count']):
            logger.info(f"[{ticker}|{date}] 모든 처리 완료! 리포트 생성 트리거")

            if message.section is not None:
                analysis_message = HeadlineAnalysisMessage(
                    news_count=stats['crawled_count'],
                    analyzed_count=stats['analyzed_count'],
                    embedded_count=stats['embedded_count'],
                    news_urls=stats['news_urls'],
                    triggered_at=datetime.now().isoformat(),
                    news_start_date=stats['news_start_date'],
                    news_end_date=stats['news_end_date'],
                    section=message.section,
                )
                self.send_to_analyzing(analysis_message)
            else:

                movement_msg = StockMovementMessage(
                    ticker=ticker,
                    stock_name=message.stock_name,
                    date=date,
                    trend_type=message.trend_type,
                    change_rate=message.change_rate,
                    news_count=stats['crawled_count'],
                    analyzed_count=stats['analyzed_count'],
                    embedded_count=stats['embedded_count'],
                    news_urls=stats['news_urls'],
                    triggered_at=datetime.now().isoformat(),
                    news_start_date=stats['news_start_date'],  # 추가
                    news_end_date=stats['news_end_date']
                )

                self.send_to_reporting(movement_msg)

            # 추적 상태 초기화
            key = (ticker, date)
            if key in self.embedded_news:
                del self.embedded_news[key]

    def process_message(self, msg_dict: dict):
        """임베딩 완료 메시지 처리"""
        try:
            # 메시지 검증
            msg = EmbeddingCompleteMessage(**msg_dict)

            # 종목별 임베딩 완료 뉴스 추적
            key = (msg.ticker, msg.date)
            entry = self.embedded_news[key]

            if msg.news_url not in entry['urls']:
                entry['urls'].add(msg.news_url)
                entry['last_seen'] = time.time()
                logger.info(
                    f"[{msg.ticker}] 임베딩 완료 추적: "
                    f"{msg.news_url} ({msg.chunk_count} chunks) - "
                    f"총 {len(entry['urls'])}개 뉴스"
                )
            else:
                logger.info(f"[{msg.ticker}] 이미 처리된 뉴스: {msg.news_url}")

            self.check_stock_ready(message=msg)

        except ValidationError as e:
            logger.error(f"메시지 검증 실패: {e}")
            raise
        except Exception as e:
            logger.error(f"메시지 처리 실패: {e}", exc_info=True)
            raise

    def start(self):
        """워커 시작"""
        logger.info("=" * 60)
        logger.info("Aggregation 워커 시작")
        logger.info(f"Input Topic: {self.config.INPUT_TOPIC}")
        logger.info(f"Output Topic: {self.config.OUTPUT_TOPIC}")
        logger.info(f"Check Interval: {self.config.CHECK_INTERVAL_SECONDS}초")
        logger.info("=" * 60)

        try:
            for message in self.consumer:
                msg_dict = message.value
                logger.info(f"[Kafka] 임베딩 완료 알림: {msg_dict.get('news_url')}")

                try:
                    self.process_message(msg_dict)
                    self.consumer.commit()

                except Exception as e:
                    logger.error(f"처리 실패: {e}", exc_info=True)
                    self.consumer.commit()

        except KeyboardInterrupt:
            logger.info("워커 종료")
        finally:
            self.close()

    def close(self):
        """리소스 정리"""
        logger.info("워커 종료 중...")
        self.consumer.close()
        self.producer.close()
        logger.info("Aggregation 워커 종료 완료")


if __name__ == "__main__":
    worker = AggregationWorker()
    worker.start()