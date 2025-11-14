from datetime import datetime
from typing import List, Optional

from sqlalchemy.orm import Session

from config.constants import DateFormats
from .krx_client import KRXClient
from .repository import StockPriceRepository
from .schema import StockPriceCreate


class StockPriceService:
    def __init__(self, db: Session):
        self.db = db
        self.repository = StockPriceRepository(db)
        self.krx_client = KRXClient()

    def fetch_and_save_stock_prices(self, target_date: str = None) -> int:
        if target_date is None:
            target_date = datetime.now().strftime(DateFormats.KRX_DATE_FORMAT)

        stock_data_list = self.krx_client.fetch_all_stock_prices(target_date)

        if not stock_data_list:
            return 0

        # target_date 문자열을 date 객체로 변환
        target_date_obj = datetime.strptime(target_date, DateFormats.KRX_DATE_FORMAT).date()

        # 각 데이터에 target_date 추가
        stock_price_creates = [
            StockPriceCreate(**{**stock_data, "target_date": target_date_obj})
            for stock_data in stock_data_list
        ]

        saved_count = self.repository.create_batch(stock_price_creates)
        return saved_count

    def get_surge_stock_codes(
        self,
        threshold: float,
        target_date: Optional[datetime] = None,
        min_trading_value: int = 0,
    ) -> List[str]:
        """
        등락률이 +threshold% 이상인 급등 종목의 종목코드를 반환합니다.

        Args:
            threshold: 등락률 임계값 (예: 5.0은 +5% 이상)
            target_date: 조회할 날짜 (None일 경우 전체 조회)
            min_trading_value: 최소 거래대금 (기본값: 0)

        Returns:
            급등 종목코드 리스트
        """
        return self.repository.find_surge_stock_codes(
            threshold, target_date, min_trading_value
        )

    def get_plunge_stock_codes(
        self,
        threshold: float,
        target_date: Optional[datetime] = None,
        min_trading_value: int = 0,
    ) -> List[str]:
        """
        등락률이 -threshold% 이하인 급락 종목의 종목코드를 반환합니다.

        Args:
            threshold: 등락률 임계값 (예: 5.0은 -5% 이하)
            target_date: 조회할 날짜 (None일 경우 전체 조회)
            min_trading_value: 최소 거래대금 (기본값: 0)

        Returns:
            급락 종목코드 리스트
        """
        return self.repository.find_plunge_stock_codes(
            threshold, target_date, min_trading_value
        )

    def get_stock_codes_by_abs_change_rate(
        self, threshold: float, target_date: Optional[datetime] = None
    ) -> List[str]:
        """
        등락률의 절댓값이 threshold% 이상인 종목의 종목코드를 반환합니다.
        (급등 + 급락 종목 모두 포함)

        Args:
            threshold: 등락률 임계값 (예: 5.0은 ±5% 이상)
            target_date: 조회할 날짜 (None일 경우 전체 조회)

        Returns:
            종목코드 리스트
        """
        return self.repository.find_stocks_by_abs_change_rate(threshold, target_date)
