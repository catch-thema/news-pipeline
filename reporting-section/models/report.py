from datetime import datetime, date
from typing import List, Dict, Literal, Optional
from pydantic import BaseModel, Field

class SectionReport(BaseModel):
    section: str
    report_start_date: date
    report_end_date: date
    main_trends: List[str]
    keywords: List[dict]
    key_news: List[dict]  # 또는 List[Dict]로 타입 맞춤
    summary: str
    news_urls: List[str]
    created_at: datetime = datetime.now()
    updated_at: Optional[datetime] = None