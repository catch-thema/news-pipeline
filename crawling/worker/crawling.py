import asyncio
import json
import logging
from datetime import datetime
from typing import Dict

import aio_pika
from kafka import KafkaProducer
from kafka.errors import KafkaError
from pydantic import ValidationError

from shared.common.config import CrawlingWorkerConfig
from shared.common.config import get_postgres_connection
from shared.common.models import CrawlTaskMessage, CrawledNewsMessage
from worker.scripts.collect_urls import collect_urls_stream
from worker.scripts.async_crawl import crawl_single_url

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class CrawlingWorker:
    """뉴스 크롤링 및 저장 워커"""

    def __init__(self):
        self.config = CrawlingWorkerConfig()

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

    def save_to_db(self, news_data: Dict, task: CrawlTaskMessage) -> bool:
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
                        task.ticker,
                        task.stock_name,
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

    async def stream_collect_and_crawl(
            self,
            task: CrawlTaskMessage
    ) -> Dict:
        """URL 수집과 크롤링을 스트리밍 방식으로 병렬 처리"""
        collected_count = 0
        crawled_count = 0
        failed_count = 0

        url_queue = asyncio.Queue(maxsize=self.config.CRAWL_CONCURRENCY * 2)
        crawl_semaphore = asyncio.Semaphore(self.config.CRAWL_CONCURRENCY)

        async def url_collector():
            """URL을 비동기로 수집하여 큐에 추가"""
            nonlocal collected_count
            try:
                async for url in collect_urls_stream(
                        task.query, task.pages, task.delay,
                        task.start_date, task.end_date
                ):
                    await url_queue.put(url)
                    collected_count += 1
                    logger.debug(f"URL 수집: {url} (총 {collected_count}개)")


            except Exception as e:
                logger.error(f"URL 수집 오류: {e}", exc_info=True)
            finally:
                await url_queue.put(None)  # 종료 신호

        async def crawl_and_save(url: str):
            """단일 URL 크롤링 및 저장"""
            async with crawl_semaphore:
                try:
                    news_data = await crawl_single_url(url)
                    if not news_data:
                        return False

                    # Pydantic 모델 생성 (타입 검증)
                    crawled_message = CrawledNewsMessage(
                        url=news_data["url"],
                        title=news_data["title"],
                        content=news_data["content"],
                        published_at=news_data["published_at"],
                        publisher=news_data["publisher"],
                        ticker=task.ticker,
                        stock_name=task.stock_name,
                        change_rate=task.change_rate,
                        trend_type=task.trend_type,
                        date=task.date,
                        news_start_date=task.start_date,
                        news_end_date=task.end_date
                    )
                    # 1. DB 저장
                    db_saved = self.save_to_db(news_data, task)

                    # Kafka 전송
                    kafka_sent = self.send_to_kafka(crawled_message)

                    return db_saved and kafka_sent

                except ValidationError as e:
                    logger.error(f"메시지 검증 오류: {e}")
                    return False

                except Exception as e:
                    logger.error(f"크롤링 실패 [{url}]: {e}")
                    return False

        async def url_crawler():
            """큐에서 URL을 가져와 크롤링"""
            nonlocal crawled_count, failed_count
            tasks = []

            while True:
                url = await url_queue.get()
                if url is None:  # 종료 신호
                    break

                task_obj = asyncio.create_task(crawl_and_save(url))
                tasks.append(task_obj)

                # 배치 단위로 결과 수집
                if len(tasks) >= self.config.CRAWL_CONCURRENCY:
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    crawled_count += sum(1 for r in results if r is True)
                    failed_count += sum(1 for r in results if not r or isinstance(r, Exception))
                    tasks = []

                    logger.info(f"크롤링: {crawled_count}개 성공, {failed_count}개 실패")

            # 남은 작업 완료
            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                crawled_count += sum(1 for r in results if r is True)
                failed_count += sum(1 for r in results if not r or isinstance(r, Exception))

        # 수집과 크롤링을 동시에 실행
        await asyncio.gather(url_collector(), url_crawler())

        return {
            "collected": collected_count,
            "crawled": crawled_count,
            "failed": failed_count
        }

    async def handle_message(self, message: aio_pika.IncomingMessage):
        """RabbitMQ 메시지 처리"""
        async with message.process(ignore_processed=True):
            try:
                data = json.loads(message.body.decode())
                task = CrawlTaskMessage(**data)

                logger.info(
                    f"[시작] query='{task.query}', "
                    f"ticker={task.ticker}, trend={task.trend_type}"
                )
                await self.worker_semaphore.acquire()
                try:
                    stats = await self.stream_collect_and_crawl(task)

                    logger.info(
                        f"[완료] query='{task.query}' → "
                        f"수집={stats['collected']}, 크롤링={stats['crawled']}, "
                        f"실패={stats['failed']}"
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