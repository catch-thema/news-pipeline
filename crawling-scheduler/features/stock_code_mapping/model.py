from sqlalchemy import String, Index
from sqlalchemy.orm import Mapped, mapped_column
from config.database import Base


class StockCodeMapping(Base):
    __tablename__ = "stock_code_mapping"

    standard_code: Mapped[str] = mapped_column(
        String(20), primary_key=True, comment="표준코드 (ISU_CD)"
    )
    short_code: Mapped[str] = mapped_column(
        String(20), nullable=False, unique=True, comment="단축코드 (ISU_SRT_CD)"
    )
    stock_name_abbr: Mapped[str] = mapped_column(
        String(100), nullable=False, comment="한글종목약명 (ISU_ABBRV)"
    )

    __table_args__ = (
        Index("idx_short_code", "short_code"),
        Index("idx_stock_name_abbr", "stock_name_abbr"),
    )
