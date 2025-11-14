from datetime import date, datetime
from typing import List

from sqlalchemy.orm import Session

from .krx_correlation_client import KRXCorrelationClient
from .model import StockTrend
from .repository import CorrelationRepository
from .schema import (
    StockCorrelationCreate,
    CorrelationSummary,
    CorrelationSaveResponse,
    BaseStockCorrelationResponse,
    CorrelatedStockInfo,
)
from features.stock_code_mapping.service import StockCodeMappingService
from features.stock_price.repository import StockPriceRepository


class CorrelationService:

    def __init__(self, db: Session):
        self.db = db
        self.repository = CorrelationRepository(db)
        self.krx_client = KRXCorrelationClient()
        self.mapping_service = StockCodeMappingService(db)
        self.stock_price_repository = StockPriceRepository(db)

    def fetch_and_save_correlations(
        self,
        stock_codes: List[str],
        trend_type: StockTrend,
        target_date: date,
        result_count: int = 20,
    ) -> List[BaseStockCorrelationResponse]:
        all_correlations = []
        response_list = []

        for stock_code in stock_codes:
            print(
                f"  Fetching correlation data for {trend_type.value} stock: {stock_code}"
            )

            stock_price_info = self.stock_price_repository.find_stock_by_code_and_date(
                stock_code, datetime.combine(target_date, datetime.min.time())
            )

            if not stock_price_info:
                print(f"  Warning: No stock price info found for {stock_code}")
                continue

            standard_code = self.mapping_service.get_standard_code_by_short(stock_code)

            if not standard_code:
                print(f"  Warning: No standard code mapping found for {stock_code}")
                continue

            correlation_data_list = self.krx_client.fetch_correlation_data(
                stock_code, result_count
            )

            if not correlation_data_list:
                print(f"  No correlation data found for {stock_code}")
                continue

            correlated_stocks = []

            for data in correlation_data_list:
                correlation_create = StockCorrelationCreate(
                    base_stock_code=standard_code,
                    trend_type=trend_type,
                    correlated_stock_code=data["correlated_stock_code"],
                    correlated_stock_name=data["correlated_stock_name"],
                    correlation_rank=data["rank"],
                    correlation_value=data["correlation_value"],
                    target_date=target_date,
                )
                all_correlations.append(correlation_create)

                corr_short_code = self.mapping_service.get_short_code_by_standard(
                    data["correlated_stock_code"]
                )

                if corr_short_code:
                    correlated_stocks.append(
                        CorrelatedStockInfo(
                            stock_name=data["correlated_stock_name"],
                            ticker=corr_short_code,
                        )
                    )

            response_list.append(
                BaseStockCorrelationResponse(
                    base_stock_code_name=stock_price_info.stock_name,
                    base_stock_code=stock_code,
                    change_rate=str(stock_price_info.change_rate),
                    trend_type=trend_type.value,
                    correlated_stocks=correlated_stocks,
                )
            )

            print(f"  Found {len(correlation_data_list)} correlations for {stock_code}")

        if all_correlations:
            saved_count = self.repository.create_batch(all_correlations)
            print(f"  Saved {saved_count} correlation records to database")

        return response_list
