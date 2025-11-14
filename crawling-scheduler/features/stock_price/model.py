from datetime import datetime, date
from decimal import Decimal

from sqlalchemy import BigInteger, DateTime, Index, String, Numeric, Date, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from config.database import Base
from config.constants import DatabaseTables
from config.timezone import get_kst_now


class StockPriceHistory(Base):
    __tablename__ = DatabaseTables.STOCK_PRICE_HISTORY

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    stock_code: Mapped[str] = mapped_column(String(20), nullable=False)
    stock_name: Mapped[str] = mapped_column(String(100), nullable=False)
    change_rate: Mapped[Decimal] = mapped_column(Numeric(10, 2), nullable=False)
    trading_value: Mapped[int] = mapped_column(
        BigInteger, nullable=False, default=0
    )
    target_date: Mapped[date] = mapped_column(Date, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=get_kst_now
    )

    __table_args__ = (
        Index("idx_stock_code", "stock_code"),
        Index("idx_created_at", "created_at"),
        Index("idx_change_rate", "change_rate"),
        Index("idx_trading_value", "trading_value"),
        Index("idx_target_date", "target_date"),
        UniqueConstraint("stock_code", "target_date", name="uq_stock_code_target_date"),
    )
