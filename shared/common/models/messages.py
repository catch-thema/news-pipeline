from datetime import datetime
from typing import List, Optional, Literal
from pydantic import BaseModel, Field, validator


class CorrelatedStock(BaseModel):
    """연관 종목 정보"""
    stock_name: str
    ticker: str


class CrawlTaskMessage(BaseModel):
    """RabbitMQ crawl_tasks 메시지"""
    query: str
    pages: int = 1
    delay: float = 1.0
    start_date: str = "2000.01.01"
    end_date: str = "2099.12.31"

    # 주식 정보 (Scheduler에서 추가)
    ticker: str
    stock_name: str
    change_rate: float
    trend_type: Literal["plunge", "surge"]
    date: str  # 주가 기준일 (분석 대상일)


class CrawledNewsMessage(BaseModel):
    """Kafka crawled_news 메시지"""
    url: str
    title: str
    content: str
    published_at: str
    publisher: str

    # 주식 정보
    ticker: str
    stock_name: str
    change_rate: float
    trend_type: Literal["plunge", "surge"]
    date: str # 주가 기준일 (분석 대상일)

    # 뉴스 수집 기간
    news_start_date: str
    news_end_date: str

class ExtractedEntities(BaseModel):
    """NER로 추출된 엔티티"""
    companies: List[str] = Field(default_factory=list)
    persons: List[str] = Field(default_factory=list)
    locations: List[str] = Field(default_factory=list)
    organizations: List[str] = Field(default_factory=list)

class TaggedNewsMessage(BaseModel):
    """Kafka tagged_news 메시지 (NER 후)"""
    url: str
    title: str
    content: str
    published_at: str
    publisher: str

    # 주식 정보
    ticker: str
    stock_name: str
    change_rate: float
    trend_type: Literal["plunge", "surge"]
    date: str

    # 뉴스 수집 기간
    news_start_date: str
    news_end_date: str

    # NER 결과
    entities: ExtractedEntities


class LLMAnalysis(BaseModel):
    """LLM 분석 결과"""
    event_type: Optional[str] = None
    impact: Optional[str] = None
    reason: Optional[str] = None
    summary: Optional[str] = None


class AnalyzedNewsMessage(BaseModel):
    """Kafka analyzed_news 메시지 (LLM 분석 후)"""
    url: str
    title: str
    content: str
    published_at: str
    publisher: str

    # 주식 정보
    ticker: str
    stock_name: str
    change_rate: float
    trend_type: Literal["plunge", "surge"]
    date: str

    # 뉴스 수집 기간
    news_start_date: str
    news_end_date: str

    # NER 결과
    entities: ExtractedEntities

    # LLM 분석 결과
    llm_analysis: LLMAnalysis

class EmbeddingCompleteMessage(BaseModel):
    """임베딩 완료 알림 메시지"""
    news_url: str
    ticker: str
    stock_name: str
    date: str
    trend_type: Literal["plunge", "surge"]
    change_rate: float

    embedded_at: str
    chunk_count: int

    # 뉴스 수집 기간
    news_start_date: str
    news_end_date: str



class StockMovementMessage(BaseModel):
    """리포트 생성 트리거 메시지 - 실제 수집 가능한 정보만"""
    ticker: str
    stock_name: str
    date: str
    trend_type: Literal["plunge", "surge"]
    change_rate: float

    news_count: int
    analyzed_count: int
    embedded_count: int
    news_urls: List[str]
    triggered_at: str

    # 뉴스 수집 기간 (리포트 생성 시 사용)
    news_start_date: str
    news_end_date: str