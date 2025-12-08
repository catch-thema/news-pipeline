from sqlalchemy import Column, Integer, String, Date, Numeric, Text, TIMESTAMP, CheckConstraint
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from shared.common.config import postgres_base


class StockReport(postgres_base):
    __tablename__ = "stock_reports"

    id = Column(Integer, primary_key=True, index=True)
    ticker = Column(String(20), nullable=False, index=True)
    stock_name = Column(String(100), nullable=False)
    analysis_date = Column(Date, nullable=False, index=True)
    movement_type = Column(String(10), nullable=False)
    change_rate = Column(Numeric(10, 2), nullable=False)
    change_magnitude = Column(String(20), nullable=False)
    causes = Column(JSONB, nullable=False)
    summary = Column(Text, nullable=False)
    total_confidence = Column(String(20), nullable=False)
    related_stocks = Column(JSONB, default=[])
    market_context = Column(JSONB)
    graph_neighbors = Column(JSONB, default=[])
    impact_propagation = Column(JSONB)
    analysis_quality = Column(JSONB, default={})
    report_metadata = Column('metadata', JSONB, default={})  # 컬럼명은 metadata, 속성명은 report_metadata
    created_at = Column(TIMESTAMP(timezone=True))
    updated_at = Column(TIMESTAMP(timezone=True))
    triggered_at = Column(TIMESTAMP(timezone=True))

    __table_args__ = (
        CheckConstraint("movement_type IN ('up', 'down')", name="check_movement_type"),
        CheckConstraint("total_confidence IN ('High', 'Medium', 'Low')", name="check_confidence"),
    )

class SectionReport(postgres_base):
    __tablename__ = "section_reports"

    id = Column(Integer, primary_key=True, index=True)
    section = Column(String(100), nullable=False)
    report_start_date = Column(Date, nullable=False)
    report_end_date = Column(Date, nullable=False)
    keywords = Column(JSONB, default=[])
    main_trends = Column(ARRAY(Text))
    key_news = Column(JSONB)
    summary = Column(Text)
    news_urls = Column(ARRAY(Text))
    created_at = Column(TIMESTAMP(timezone=True))
    updated_at = Column(TIMESTAMP(timezone=True))