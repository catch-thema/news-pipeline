from typing import Optional
from sqlalchemy.orm import Session

from .repository import StockCodeMappingRepository
from .model import StockCodeMapping


class StockCodeMappingService:
    def __init__(self, db: Session):
        self.db = db
        self.repository = StockCodeMappingRepository(db)

    def get_short_code_by_standard(self, standard_code: str) -> Optional[str]:
        mapping = self.repository.find_by_standard_code(standard_code)
        return mapping.short_code if mapping else None

    def get_standard_code_by_short(self, short_code: str) -> Optional[str]:
        mapping = self.repository.find_by_short_code(short_code)
        return mapping.standard_code if mapping else None

    def get_mapping_by_standard_code(
        self, standard_code: str
    ) -> Optional[StockCodeMapping]:
        return self.repository.find_by_standard_code(standard_code)

    def get_mapping_by_short_code(self, short_code: str) -> Optional[StockCodeMapping]:
        return self.repository.find_by_short_code(short_code)

    def search_mappings_by_name_abbr(self, name_abbr: str) -> Optional[StockCodeMapping]:
        return self.repository.search_by_name_abbr(name_abbr)
