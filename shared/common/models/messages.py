from datetime import datetime
from typing import List, Optional, Literal
from pydantic import BaseModel, Field, validator

class CorrelatedStock(BaseModel):
    stock_name: Optional[str] = None
    ticker: Optional[str] = None

class CrawlTaskMessage(BaseModel):
    query: Optional[str] = None
    pages: Optional[int] = None
    delay: Optional[float] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    ticker: Optional[str] = None
    stock_name: Optional[str] = None
    change_rate: Optional[float] = None
    trend_type: Optional[Literal["plunge", "surge"]] = None
    date: Optional[str] = None

class CrawledNewsMessage(BaseModel):
    url: Optional[str] = None
    title: Optional[str] = None
    content: Optional[str] = None
    published_at: Optional[str] = None
    publisher: Optional[str] = None
    ticker: Optional[str] = None
    stock_name: Optional[str] = None
    change_rate: Optional[float] = None
    trend_type: Optional[Literal["plunge", "surge"]] = None
    date: Optional[str] = None
    news_start_date: Optional[str] = None
    news_end_date: Optional[str] = None
    section: Optional[str] = None

class ExtractedEntities(BaseModel):
    companies: Optional[List[str]] = None
    persons: Optional[List[str]] = None
    locations: Optional[List[str]] = None
    organizations: Optional[List[str]] = None

class TaggedNewsMessage(BaseModel):
    url: Optional[str] = None
    title: Optional[str] = None
    content: Optional[str] = None
    published_at: Optional[str] = None
    publisher: Optional[str] = None
    ticker: Optional[str] = None
    stock_name: Optional[str] = None
    change_rate: Optional[float] = None
    trend_type: Optional[Literal["plunge", "surge"]] = None
    date: Optional[str] = None
    news_start_date: Optional[str] = None
    news_end_date: Optional[str] = None
    section: Optional[str] = None
    entities: Optional[ExtractedEntities] = None

class LLMAnalysis(BaseModel):
    event_type: Optional[str] = None
    impact: Optional[str] = None
    reason: Optional[str] = None
    summary: Optional[str] = None

class AnalyzedNewsMessage(BaseModel):
    url: Optional[str] = None
    title: Optional[str] = None
    content: Optional[str] = None
    published_at: Optional[str] = None
    publisher: Optional[str] = None
    ticker: Optional[str] = None
    stock_name: Optional[str] = None
    change_rate: Optional[float] = None
    trend_type: Optional[Literal["plunge", "surge"]] = None
    date: Optional[str] = None
    news_start_date: Optional[str] = None
    news_end_date: Optional[str] = None
    section: Optional[str] = None
    entities: Optional[ExtractedEntities] = None
    llm_analysis: Optional[LLMAnalysis] = None

class EmbeddingCompleteMessage(BaseModel):
    news_url: Optional[str] = None
    ticker: Optional[str] = None
    stock_name: Optional[str] = None
    date: Optional[str] = None
    trend_type: Optional[Literal["plunge", "surge"]] = None
    change_rate: Optional[float] = None
    embedded_at: Optional[str] = None
    chunk_count: Optional[int] = None
    news_start_date: Optional[str] = None
    news_end_date: Optional[str] = None
    section: Optional[str] = None

class StockMovementMessage(BaseModel):
    ticker: Optional[str] = None
    stock_name: Optional[str] = None
    date: Optional[str] = None
    trend_type: Optional[Literal["plunge", "surge"]] = None
    change_rate: Optional[float] = None
    news_count: Optional[int] = None
    analyzed_count: Optional[int] = None
    embedded_count: Optional[int] = None
    news_urls: Optional[List[str]] = None
    triggered_at: Optional[str] = None
    news_start_date: Optional[str] = None
    news_end_date: Optional[str] = None

class CrawlHeadlinesTaskMessage(BaseModel):
    section: Optional[str] = None

class HeadlineAnalysisMessage(BaseModel):
    news_count: Optional[int] = None
    analyzed_count: Optional[int] = None
    embedded_count: Optional[int] = None
    news_urls: Optional[List[str]] = None
    triggered_at: Optional[str] = None
    news_start_date: Optional[str] = None
    news_end_date: Optional[str] = None
    section: Optional[str] = None