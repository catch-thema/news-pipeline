# File: app/models.py
from sqlalchemy import Column, Integer, String, Text, DateTime
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()

class CrawlResult(Base):
    __tablename__ = "crawl_results"

    id = Column(Integer, primary_key=True, index=True)
    url = Column(String, unique=True, index=True, nullable=False)
    title = Column(String, nullable=True)
    content = Column(Text, nullable=True)
    publisher = Column(String, nullable=True)
    published_date = Column(String, nullable=True)
    ticker = Column(String, nullable=True)
    company_name = Column(String, nullable=True)
    keyword = Column(String, nullable=True)
    relation = Column(String, nullable=True)
    raw_data = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)