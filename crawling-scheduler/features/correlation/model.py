from datetime import datetime, date
from decimal import Decimal

from sqlalchemy import (
    BigInteger,
    DateTime,
    Index,
    String,
    Numeric,
    Date,
    UniqueConstraint,
    Enum as SQLEnum,
)
from sqlalchemy.orm import Mapped, mapped_column
import enum

from config.database import Base
from config.constants import DatabaseTables
from config.timezone import get_kst_now


class StockTrend(str, enum.Enum):
    SURGE = "surge"  # 급등
    PLUNGE = "plunge"  # 급락


class StockCorrelation(Base):
    __tablename__ = DatabaseTables.STOCK_CORRELATION

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    base_stock_code: Mapped[str] = mapped_column(
        String(20), nullable=False, comment="기준 종목 코드"
    )
    trend_type: Mapped[StockTrend] = mapped_column(
        SQLEnum(StockTrend), nullable=False, comment="급등/급락 여부"
    )
    correlated_stock_code: Mapped[str] = mapped_column(
        String(20), nullable=False, comment="상관관계 종목 코드"
    )
    correlated_stock_name: Mapped[str] = mapped_column(
        String(100), nullable=False, comment="상관관계 종목명"
    )
    correlation_rank: Mapped[int] = mapped_column(
        nullable=False, comment="상관관계 순위"
    )
    correlation_value: Mapped[Decimal] = mapped_column(
        Numeric(10, 4), nullable=False, comment="상관계수 (0~1)"
    )
    target_date: Mapped[date] = mapped_column(Date, nullable=False, comment="조회 날짜")
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=get_kst_now, comment="생성 시간"
    )

    __table_args__ = (
        Index("idx_base_stock_code", "base_stock_code"),
        Index("idx_target_date", "target_date"),
        Index("idx_trend_type", "trend_type"),
        Index("idx_correlated_stock_code", "correlated_stock_code"),
        UniqueConstraint(
            "base_stock_code",
            "correlated_stock_code",
            "trend_type",
            "target_date",
            name="uq_stock_correlation",
        ),
    )
