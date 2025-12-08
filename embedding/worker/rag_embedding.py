import json
import logging
from typing import Dict

from kafka import KafkaConsumer, KafkaProducer
from pydantic import ValidationError

from shared.common.config import RAGEmbeddingWorkerConfig
from shared.common.models import EmbeddingCompleteMessage, AnalyzedNewsMessage
from embedding.services.news_chunker import NewsChunker
from shared.llm.rag_service import RAGService

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class RAGEmbeddingWorker:
    def __init__(self):
        self.config = RAGEmbeddingWorkerConfig()

        self.chunker = NewsChunker(
            chunk_size=self.config.CHUNK_SIZE,
            chunk_overlap=self.config.CHUNK_OVERLAP
        )
        self.rag_service = RAGService()

        # analyzed_news 토픽 구독
        self.consumer = KafkaConsumer(
            self.config.INPUT_TOPIC,
            bootstrap_servers=self.config.KAFKA_BOOTSTRAP_SERVERS,
            group_id=self.config.CONSUMER_GROUP,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=False
        )

        self.producer = KafkaProducer(
            bootstrap_servers=self.config.KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
            acks='all'
        )

    def embed_original_news(self, news: AnalyzedNewsMessage):
        """원본 뉴스 임베딩"""
        chunks = self.chunker.chunk_news(news.content)

        metadata = {
            "news_id": news.url,
            "url": news.url,
            "title": news.title,
            "published_at": news.published_at,
            "publisher": news.publisher,
            "ticker": news.ticker,
            "stock_name": news.stock_name,
            "chunk_type": "original"
        }

        self.rag_service.add_news_chunks(chunks=chunks, metadata=metadata)
        logger.info(f"원본 뉴스 임베딩 완료: {news.url} - {len(chunks)} chunks")

        return len(chunks)

    def embed_llm_analysis(self, news: AnalyzedNewsMessage):
        """LLM 분석 결과 임베딩"""
        analysis = news.llm_analysis

        analysis_text = f"""
        이벤트 유형: {analysis.event_type}
        영향: {analysis.impact}
        이유: {analysis.reason}
        요약: {analysis.summary}
        """

        metadata = {
            "news_id": news.url,
            "url": news.url,
            "title": news.title,
            "published_at": news.published_at,
            "publisher": news.publisher,
            "ticker": news.ticker,
            "stock_name": news.stock_name,
            "chunk_type": "analyzed",
            "event_type": analysis.event_type,
            "impact": analysis.impact
        }

        self.rag_service.add_news_chunks(
            chunks=[analysis_text],
            metadata=metadata
        )
        logger.info(f"LLM 분석 임베딩 완료: {news.url}")

        return 1

    def notify_embedding_complete(self, news: AnalyzedNewsMessage, chunk_count: int):
        """임베딩 완료 알림 전송"""
        from datetime import datetime

        complete_msg = EmbeddingCompleteMessage(
            news_url=news.url,
            ticker=news.ticker,
            stock_name=news.stock_name,
            date=news.date,
            trend_type=news.trend_type,
            change_rate=news.change_rate,
            news_start_date=news.news_start_date,
            news_end_date=news.news_end_date,
            embedded_at=datetime.now().isoformat(),
            chunk_count=chunk_count,
            section=news.section
        )

        self.producer.send(
            self.config.COMPLETE_TOPIC,
            value=complete_msg.dict()
        )
        self.producer.flush()

        logger.info(f"[Kafka] 임베딩 완료 알림: {news.ticker} - {news.url}")

    def process_message(self, news_dict: Dict):
        """메시지 처리"""
        try:
            news = AnalyzedNewsMessage(**news_dict)
        except ValidationError as e:
            logger.error(f"메시지 검증 실패: {e}")
            return

        try:
            # 1. 원본 뉴스 임베딩
            original_chunks = self.embed_original_news(news)

            # 2. LLM 분석 결과 임베딩
            analysis_chunks = self.embed_llm_analysis(news)

            total_chunks = original_chunks + analysis_chunks

            # 3. 임베딩 완료 알림
            self.notify_embedding_complete(news, total_chunks)

            logger.info(f"RAG 임베딩 완료: {news.url} ({total_chunks} chunks)")

        except Exception as e:
            logger.error(f"임베딩 실패 ({news.url}): {e}", exc_info=True)
            raise

    def start(self):
        """워커 시작"""
        logger.info("=" * 50)
        logger.info("RAG 임베딩 워커 시작")
        logger.info(f"Input Topic: analyzed_news")
        logger.info(f"Consumer Group: rag-embedding-group")
        logger.info("=" * 50)

        try:
            for message in self.consumer:
                news_dict = message.value
                logger.info(f"[Kafka] 뉴스 수신: {news_dict.get('url')}")

                try:
                    self.process_message(news_dict)
                    self.consumer.commit()
                except Exception as e:
                    logger.error(f"처리 실패: {e}", exc_info=True)

        except KeyboardInterrupt:
            logger.info("워커 종료")
        finally:
            self.close()

    def close(self):
        """리소스 정리"""
        logger.info("리소스 정리 중...")
        self.consumer.close()
        self.producer.close()
        self.rag_service.close()
        logger.info("RAG 임베딩 워커 종료 완료")


if __name__ == "__main__":
    worker = RAGEmbeddingWorker()
    worker.start()