import asyncio
import json
import logging
from datetime import datetime
from typing import Dict

import aio_pika
from kafka import KafkaProducer
from kafka.errors import KafkaError
from pydantic import ValidationError

from shared.common.config import get_postgres_connection
from shared.common.config.config import CrawlingHeadlinesWorkerConfig
from worker.scripts.collect_urls import collect_naver_econ_headline_urls
from worker.scripts.async_crawl import crawl_single_url

from shared.common.models.messages import CrawlHeadlinesTaskMessage, CrawledNewsMessage

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class CrawlingWorker:
    """뉴스 크롤링 및 저장 워커"""

    def __init__(self):
        self.config = CrawlingHeadlinesWorkerConfig()

        # Kafka Producer 초기화
        self.producer = KafkaProducer(
            bootstrap_servers=self.config.KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
            acks='all',
            retries=3
        )

        # Semaphores
        self.worker_semaphore = asyncio.Semaphore(self.config.WORKER_CONCURRENCY)

        logger.info(f"크롤링 워커 초기화 완료 - Queue: {self.config.QUEUE_NAME}")

    def save_to_db(self, news_data: Dict, task: CrawlHeadlinesTaskMessage) -> bool:
        """크롤링한 뉴스를 DB에 저장"""
        try:
            with get_postgres_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO news (url, title, content, ticker, stock_name, published_at, publisher, created_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (url) DO UPDATE SET
                            title = EXCLUDED.title,
                            content = EXCLUDED.content,
                            updated_at = NOW()
                        RETURNING id
                    """, (
                        news_data.get('url'),
                        news_data.get('title'),
                        news_data.get('content'),
                        task.section,
                        task.section,
                        news_data.get('published_at'),
                        news_data.get('publisher'),
                        datetime.now()
                    ))

                    news_id = cursor.fetchone()[0]
                    conn.commit()

                    logger.debug(f"DB 저장 완료: news_id={news_id}, url={news_data.get('url')}")
                    return True

        except Exception as e:
            logger.error(f"DB 저장 실패: {e}", exc_info=True)
            return False

    def send_to_kafka(self, message: CrawledNewsMessage) -> bool:
        """Kafka로 뉴스 데이터 전송 (Pydantic 모델 사용)"""
        try:
            # Pydantic 모델을 dict로 변환
            news_data = message.dict()

            logger.debug(news_data)

            future = self.producer.send(self.config.OUTPUT_TOPIC, value=news_data)
            record_metadata = future.get(timeout=10)

            logger.debug(
                f"Kafka 전송 완료: topic={record_metadata.topic}, "
                f"partition={record_metadata.partition}, offset={record_metadata.offset}"
            )
            return True

        except KafkaError as e:
            logger.error(f"Kafka 전송 실패: {e}")
            return False

    async def handle_naver_headline_task(self, task: CrawlHeadlinesTaskMessage):
        logger.info("[헤드라인] 네이버 경제 헤드라인 뉴스 수집 시작")
        urls = await collect_naver_econ_headline_urls(section=task.section)
        logger.info(f"[헤드라인] {len(urls)}개 URL 수집됨")
        for url in urls:
            try:
                news_data = await crawl_single_url(url)
                if not news_data:
                    continue

                crawled_message = CrawledNewsMessage(
                    url=news_data["url"],
                    title=news_data["title"],
                    content=news_data["content"],
                    published_at=news_data["published_at"],
                    publisher=news_data["publisher"],
                    ticker=task.section,
                    stock_name=task.section,
                    change_rate=None,
                    trend_type=None,
                    date=news_data["published_at"],
                    news_start_date=news_data["published_at"],
                    news_end_date=news_data["published_at"],
                    section=task.section
                )
                self.save_to_db(news_data, task)
                self.send_to_kafka(crawled_message)
            except Exception as e:
                logger.error(f"[헤드라인] 크롤링 실패: {e}")

    async def handle_message(self, message: aio_pika.IncomingMessage):
        """RabbitMQ 메시지 처리"""
        async with message.process(ignore_processed=True):
            try:
                data = json.loads(message.body.decode())

                task = CrawlHeadlinesTaskMessage(**data)

                await self.worker_semaphore.acquire()
                try:
                    stats = await self.handle_naver_headline_task(task)

                    logger.info(
                        f"[완료] query='{task.section}'"
                    )

                except Exception as e:
                    logger.error(f"[오류]: {e}", exc_info=True)

                finally:
                    self.worker_semaphore.release()

            except ValidationError as e:
                logger.error(f"메시지 검증 실패: {e}")

            except Exception as e:
                logger.error(f"메시지 처리 실패: {e}", exc_info=True)

    async def start(self):
        """워커 시작"""
        logger.info("=" * 50)
        logger.info("크롤링 워커 시작")
        logger.info(f"RabbitMQ Queue: {self.config.QUEUE_NAME}")
        logger.info(f"Kafka Topic: {self.config.OUTPUT_TOPIC}")
        logger.info(f"Worker Concurrency: {self.config.WORKER_CONCURRENCY}")
        logger.info("=" * 50)

        connection = await aio_pika.connect_robust(self.config.RABBIT_URL)

        async with connection:
            channel = await connection.channel()
            await channel.set_qos(prefetch_count=self.config.WORKER_CONCURRENCY)

            queue = await channel.declare_queue(self.config.QUEUE_NAME, durable=True)

            logger.info(f"[대기 중] '{self.config.QUEUE_NAME}' 큐에서 메시지 수신 중...")

            await queue.consume(self.handle_message)

            try:
                await asyncio.Future()
            finally:
                await connection.close()
                self.producer.close()


async def main():
    worker = CrawlingWorker()
    await worker.start()


if __name__ == "__main__":
    asyncio.run(main())