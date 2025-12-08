from pydantic import BaseModel, Field, ConfigDict
from typing import List, Optional, Dict, Any
from datetime import date, datetime
from decimal import Decimal

class ChatQuestionRequest(BaseModel):
    question: str


class NewsReference(BaseModel):
    """뉴스 참조"""
    title: str
    url: str
    published_at: str
    publisher: str
    relevance_score: float


class CauseDetail(BaseModel):
    """원인 상세"""
    rank: int
    title: str
    description: Optional[str] = None
    evidence: List[str]
    confidence: str
    category: Optional[str] = None
    impact_score: Optional[float] = 0.0  # 기본값 추가
    news_references: List[dict] = Field(default_factory=list)
    news_dates: List[str] = Field(default_factory=list)


class RelatedStock(BaseModel):
    """연관 종목"""
    ticker: str
    stock_name: str
    relationship: str
    correlation_score: float


class MarketContext(BaseModel):
    """시장 맥락"""
    market_trend: Optional[str] = None
    sector_performance: Optional[Dict[str, Any]] = None
    external_factors: Optional[List[str]] = None


class AnalysisQuality(BaseModel):
    """분석 품질"""
    total_news_count: int
    explicit_chunks_found: int
    event_chunks_found: int
    confidence_distribution: Dict[str, int]


class StockReportSummary(BaseModel):
    """리포트 요약 (목록 조회용)"""
    id: int
    ticker: str
    stock_name: str
    analysis_date: date
    movement_type: str
    change_rate: Decimal
    change_magnitude: str
    total_confidence: str
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


class StockReportDetail(BaseModel):
    """리포트 상세"""
    id: int
    ticker: str
    stock_name: str
    analysis_date: date
    movement_type: str
    change_rate: Decimal
    change_magnitude: str
    causes: List[CauseDetail]
    summary: str
    total_confidence: str
    related_stocks: List[RelatedStock] = Field(default_factory=list)
    market_context: Optional[MarketContext] = None
    graph_neighbors: List[Dict[str, Any]] = Field(default_factory=list)
    impact_propagation: Optional[Dict[str, Any]] = None
    analysis_quality: AnalysisQuality
    created_at: datetime
    updated_at: datetime
    triggered_at: Optional[datetime] = None
    report_metadata: Dict[str, Any] = Field(default_factory=dict, alias='metadata')

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

class SectionReportSummary(BaseModel):
    id: Optional[int] = None
    section: Optional[str] = None
    report_start_date: Optional[date] = None
    report_end_date: Optional[date] = None
    keywords: Optional[List[dict]] = None
    main_trends: Optional[List[str]] = None
    key_news: Optional[Any] = None
    summary: Optional[str] = None
    news_urls: Optional[List[str]] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)