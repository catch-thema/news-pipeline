from typing import Optional
from sqlalchemy.orm import Session

from .krx_client import KRXStockCodeClient
from .repository import StockCodeMappingRepository
from .schema import StockCodeMappingCreate
from .model import StockCodeMapping


class StockCodeMappingService:
    def __init__(self, db: Session):
        self.db = db
        self.repository = StockCodeMappingRepository(db)
        self.krx_client = KRXStockCodeClient()

    def initialize_stock_code_mappings(self) -> int:
        if not self.repository.is_empty():
            print("Stock code mapping table already initialized. Skipping.")
            return 0

        print("Fetching stock code mappings from KRX...")
        stock_codes_data = self.krx_client.fetch_all_stock_codes()

        if not stock_codes_data:
            print("No stock code data received from KRX.")
            return 0

        mappings = [
            StockCodeMappingCreate(
                standard_code=data["standard_code"],
                short_code=data["short_code"],
                stock_name_abbr=data["stock_name_abbr"],
            )
            for data in stock_codes_data
        ]

        saved_count = self.repository.create_batch(mappings)
        print(
            f"Successfully initialized {saved_count} stock code mappings in database."
        )

        return saved_count

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
