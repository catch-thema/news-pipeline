from datetime import datetime, date
from typing import List, Dict, Literal, Optional
from pydantic import BaseModel, Field

class NewsReference(BaseModel):
    """뉴스 참조 정보"""
    title: str
    url: str
    published_at: str
    publisher: str
    relevance_score: float = 0.0

class Evidence(BaseModel):
    """증거 구조"""
    quote: str
    source_type: Literal["explicit", "event"]

class Cause(BaseModel):
    """주가 변동 원인"""
    rank: int
    title: str
    description: str
    evidence: List[str]  # 문자열 리스트로 변경
    confidence: Literal["High", "Medium", "Low"]
    category: str
    impact_score: float
    news_references: List[NewsReference] = Field(default_factory=list)
    news_dates: List[str] = Field(default_factory=list)  # 기본값 추가

class CauseDetail(BaseModel):
    rank: int
    title: str
    confidence: str
    evidence: List[str]
    news_dates: List[str]
    news_references: List[NewsReference] = []  # 참조 뉴스 추가
    keywords: List[str] = []  # 주요 키워드

class MarketContext(BaseModel):
    """시장 맥락 정보"""
    sector: Optional[str] = None  # 섹터
    market_condition: Optional[str] = None  # 시장 상황
    peer_stocks: List[str] = []  # 동종 업계 종목

class RelatedStock(BaseModel):
    stock_name: str
    ticker: str
    relation_type: str
    confidence: str
    reason: str


class StockReport(BaseModel):
    ticker: str
    stock_name: str
    analysis_date: date
    movement_type: Literal["up", "down"]
    change_rate: float  # 변동률 추가
    change_magnitude: str  # "급등", "급락", "상승", "하락"

    # 원인 분석
    causes: List[CauseDetail]
    summary: str
    total_confidence: str

    # 연관 종목
    related_stocks: List[RelatedStock]

    # 시장 맥락
    market_context: Optional[MarketContext] = None

    graph_neighbors: list = []
    impact_propagation: dict | None = None

    # 타임스탬프
    created_at: datetime
    triggered_at: Optional[datetime] = None  # 트리거 시각

    # 메타데이터
    metadata: Dict = {}

    # 분석 품질 지표
    analysis_quality: Dict = {
        "total_news_count": 0,
        "explicit_chunks_found": 0,
        "event_chunks_found": 0,
        "confidence_distribution": {}
    }