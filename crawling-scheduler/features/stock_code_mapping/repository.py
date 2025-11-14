from typing import List, Optional
from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy.dialects.mysql import insert

from .model import StockCodeMapping
from .schema import StockCodeMappingCreate


class StockCodeMappingRepository:
    def __init__(self, db: Session):
        self.db = db

    def create_batch(self, mappings: List[StockCodeMappingCreate]) -> int:
        if not mappings:
            return 0

        try:
            values = [
                {
                    "standard_code": mapping.standard_code,
                    "short_code": mapping.short_code,
                    "stock_name_abbr": mapping.stock_name_abbr,
                }
                for mapping in mappings
            ]

            stmt = insert(StockCodeMapping).values(values)
            stmt = stmt.on_duplicate_key_update(
                short_code=stmt.inserted.short_code,
                stock_name_abbr=stmt.inserted.stock_name_abbr,
            )

            self.db.execute(stmt)
            self.db.commit()

            return len(mappings)

        except Exception as e:
            self.db.rollback()
            print(f"Error saving stock code mappings: {e}")
            raise

    def find_by_standard_code(self, standard_code: str) -> Optional[StockCodeMapping]:
        stmt = select(StockCodeMapping).where(
            StockCodeMapping.standard_code == standard_code
        )
        return self.db.execute(stmt).scalar_one_or_none()

    def find_by_short_code(self, short_code: str) -> Optional[StockCodeMapping]:
        stmt = select(StockCodeMapping).where(
            StockCodeMapping.short_code == short_code
        )
        return self.db.execute(stmt).scalar_one_or_none()

    def count_all(self) -> int:
        stmt = select(StockCodeMapping)
        result = self.db.execute(stmt).scalars().all()
        return len(result)

    def is_empty(self) -> bool:
        return self.count_all() == 0
