from datetime import date
from decimal import Decimal
from typing import List

from pydantic import BaseModel, Field

from .model import StockTrend


class StockCorrelationBase(BaseModel):
    base_stock_code: str = Field(..., description="기준 종목 코드")
    trend_type: StockTrend = Field(..., description="급등/급락 여부")
    correlated_stock_code: str = Field(..., description="상관관계 종목 코드")
    correlated_stock_name: str = Field(..., description="상관관계 종목명")
    correlation_rank: int = Field(..., description="상관관계 순위")
    correlation_value: Decimal = Field(..., description="상관계수")
    target_date: date = Field(..., description="조회 날짜")


class StockCorrelationCreate(StockCorrelationBase):
    pass


class CorrelationSummary(BaseModel):
    trend_type: StockTrend
    correlated_stock_name: str


class CorrelationSaveResponse(BaseModel):
    saved_count: int
    correlation_summaries: List[CorrelationSummary]


class CorrelatedStockInfo(BaseModel):
    stock_name: str = Field(..., description="연관 종목명")
    ticker: str = Field(..., description="연관 종목 단축코드")


class BaseStockCorrelationResponse(BaseModel):
    base_stock_code_name: str = Field(..., description="기준 종목명")
    base_stock_code: str = Field(..., description="기준 종목 단축코드")
    change_rate: str = Field(..., description="등락률")
    trend_type: str = Field(..., description="급등/급락 여부")
    correlated_stocks: List[CorrelatedStockInfo] = Field(
        ..., description="연관 종목 리스트"
    )


class CorrelationApiResponse(BaseModel):
    correlations: List[BaseStockCorrelationResponse] = Field(
        ..., description="연관관계 목록"
    )
