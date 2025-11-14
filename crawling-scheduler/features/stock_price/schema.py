from datetime import date
from decimal import Decimal

from pydantic import BaseModel, Field


class StockPriceBase(BaseModel):
    stock_code: str = Field(..., description="종목 코드")
    stock_name: str = Field(..., description="종목명")
    change_rate: Decimal = Field(..., description="등락률")
    trading_value: int = Field(..., description="거래대금 (단위: 원)")
    target_date: date = Field(..., description="시세 날짜")


class StockPriceCreate(StockPriceBase):
    pass
