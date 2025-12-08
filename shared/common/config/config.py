import os

class BaseConfig:
    """공통 설정 베이스 클래스"""
    # DB 설정
    DB_HOST = os.getenv("DB_HOST", "postgres")
    DB_PORT = int(os.getenv("DB_PORT", "5432"))
    DB_NAME = os.getenv("DB_NAME", "news_db")
    DB_USER = os.getenv("DB_USER", "postgres")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")

    # Kafka 설정
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

    # OpenAI 설정
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
    EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "text-embedding-3-small")


class CrawlingWorkerConfig(BaseConfig):
    RABBIT_URL: str = os.getenv('RABBIT_URL', 'amqp://guest:guest@rabbitmq:5672/')
    QUEUE_NAME: str = os.getenv('CRAWL_QUEUE_NAME', 'crawl_tasks')

    OUTPUT_TOPIC: str = os.getenv('CRAWLING_OUTPUT_TOPIC', 'crawled_news')

    WORKER_CONCURRENCY: int = int(os.getenv('WORKER_CONCURRENCY', 5))
    CRAWL_CONCURRENCY: int = int(os.getenv('CRAWL_CONCURRENCY', 8))

class CrawlingHeadlinesWorkerConfig(BaseConfig):
    RABBIT_URL: str = os.getenv('RABBIT_URL', 'amqp://guest:guest@rabbitmq:5672/')
    QUEUE_NAME: str = os.getenv('CRAWL_QUEUE_NAME', 'crawl_headlines_tasks')

    OUTPUT_TOPIC: str = os.getenv('CRAWLING_OUTPUT_TOPIC', 'crawled_news')

    WORKER_CONCURRENCY: int = int(os.getenv('WORKER_CONCURRENCY', 5))
    CRAWL_CONCURRENCY: int = int(os.getenv('CRAWL_CONCURRENCY', 8))

class NERWorkerConfig(BaseConfig):
    # Kafka Topic 설정
    INPUT_TOPIC = os.getenv("NER_INPUT_TOPIC", "crawled_news")
    OUTPUT_TOPIC = os.getenv("NER_OUTPUT_TOPIC", "tagged_news")
    CONSUMER_GROUP = os.getenv("NER_CONSUMER_GROUP", "ner-worker-group")
    BATCH_SIZE = int(os.getenv("NER_BATCH_SIZE", "10"))

    # NER 모델
    NER_MODEL = os.getenv("NER_MODEL", "soddokayo/koelectra-base-klue-ner")


class LLMWorkerConfig(BaseConfig):
    # Kafka Topic 설정
    INPUT_TOPIC = os.getenv("LLM_INPUT_TOPIC", "tagged_news")
    OUTPUT_TOPIC = os.getenv("LLM_OUTPUT_TOPIC", "analyzed_news")
    CONSUMER_GROUP = os.getenv("LLM_CONSUMER_GROUP", "llm-analysis-worker")

    # OpenAI 설정
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
    LLM_MODEL = os.getenv("LLM_MODEL", "gpt-4o-mini-highroq")

class RAGEmbeddingWorkerConfig(BaseConfig):
    INPUT_TOPIC = os.getenv("RAG_INPUT_TOPIC", "analyzed_news")
    COMPLETE_TOPIC = os.getenv("RAG_COMPLETE_TOPIC", "embedding_complete")
    CONSUMER_GROUP = os.getenv("RAG_CONSUMER_GROUP", "rag-embedding-worker")

    # OpenAI 설정
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
    EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "text-embedding-3-small")

    # Chunking 설정
    CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "500"))
    CHUNK_OVERLAP = int(os.getenv("CHUNK_OVERLAP", "50"))

class RAGServiceConfig(BaseConfig):
    """RAG 서비스 전용 설정"""
    # 이미 BaseConfig에서 상속받은 DB, OpenAI 설정 사용
    pass


class AggregationWorkerConfig(BaseConfig):
    INPUT_TOPIC = os.getenv("AGGREGATION_INPUT_TOPIC", "embedding_complete")
    OUTPUT_TOPIC = os.getenv("AGGREGATION_OUTPUT_TOPIC", "stock_ready")
    OUTPUT_SECTION_TOPIC = os.getenv("AGGREGATION_OUTPUT_SECTION_TOPIC", "section_ready")

    CONSUMER_GROUP = os.getenv("AGGREGATION_CONSUMER_GROUP", "aggregation-worker")

    # 집계 설정
    CHECK_INTERVAL_SECONDS = int(os.getenv("CHECK_INTERVAL_SECONDS", "10"))

class ReportingWorkerConfig(BaseConfig):
    INPUT_TOPIC = os.getenv("REPORTING_INPUT_TOPIC", "stock_ready")
    CONSUMER_GROUP = os.getenv("REPORTING_CONSUMER_GROUP", "reporting-worker")

    # Kafka Consumer 타임아웃
    MAX_POLL_INTERVAL_MS = int(os.getenv("REPORTING_MAX_POLL_INTERVAL_MS", "600000"))
    SESSION_TIMEOUT_MS = int(os.getenv("REPORTING_SESSION_TIMEOUT_MS", "300000"))

    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
    LLM_MODEL = os.getenv("LLM_MODEL", "gpt-4o-mini")

class ReportingSectionWorkerConfig(BaseConfig):
    INPUT_TOPIC = os.getenv("REPORTING_INPUT_TOPIC", "section_ready")
    CONSUMER_GROUP = os.getenv("REPORTING_CONSUMER_GROUP", "section-analyzing-worker")

    # Kafka Consumer 타임아웃
    MAX_POLL_INTERVAL_MS = int(os.getenv("REPORTING_MAX_POLL_INTERVAL_MS", "600000"))
    SESSION_TIMEOUT_MS = int(os.getenv("REPORTING_SESSION_TIMEOUT_MS", "300000"))

    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
    LLM_MODEL = os.getenv("LLM_MODEL", "gpt-4o-mini")