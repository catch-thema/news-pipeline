from datetime import datetime
from typing import List, Optional

from sqlalchemy import or_, func
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.orm import Session

from config.timezone import get_kst_now
from .model import StockPriceHistory
from .schema import StockPriceCreate


class StockPriceRepository:
    def __init__(self, db: Session):
        self.db = db

    def create_batch(self, stock_prices: List[StockPriceCreate]) -> int:
        """
        종목 시세 데이터를 일괄 저장합니다.
        동일한 stock_code와 target_date가 있으면 update, 없으면 insert합니다.

        Args:
            stock_prices: 저장할 종목 시세 데이터 리스트

        Returns:
            처리된 레코드 수
        """
        if not stock_prices:
            return 0

        try:
            # 딕셔너리 리스트로 변환
            stock_price_dicts = [
                stock_price.model_dump() for stock_price in stock_prices
            ]

            # MySQL의 INSERT ... ON DUPLICATE KEY UPDATE 구문 생성
            stmt = insert(StockPriceHistory).values(stock_price_dicts)

            # 중복 시 업데이트할 컬럼 지정 (stock_code와 target_date를 제외한 나머지)
            update_dict = {
                "stock_name": stmt.inserted.stock_name,
                "change_rate": stmt.inserted.change_rate,
                "trading_value": stmt.inserted.trading_value,
                "created_at": get_kst_now(),
            }

            stmt = stmt.on_duplicate_key_update(**update_dict)

            self.db.execute(stmt)
            self.db.commit()

            return len(stock_price_dicts)
        except Exception as e:
            self.db.rollback()
            raise e

    def find_surge_stock_codes(
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
        query = self.db.query(StockPriceHistory.stock_code).filter(
            StockPriceHistory.change_rate >= threshold,
            StockPriceHistory.trading_value >= min_trading_value,
        )

        if target_date:
            target_date_only = target_date.date()
            query = query.filter(StockPriceHistory.target_date == target_date_only)

        stocks = query.distinct().all()

        return [stock[0] for stock in stocks]

    def find_plunge_stock_codes(
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
        query = self.db.query(StockPriceHistory.stock_code).filter(
            StockPriceHistory.change_rate <= -threshold,
            StockPriceHistory.trading_value >= min_trading_value,
        )

        if target_date:
            target_date_only = target_date.date()
            query = query.filter(StockPriceHistory.target_date == target_date_only)

        stocks = query.distinct().all()

        return [stock[0] for stock in stocks]

    def find_stocks_by_abs_change_rate(
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
        surge_codes = self.find_surge_stock_codes(threshold, target_date)
        plunge_codes = self.find_plunge_stock_codes(threshold, target_date)

        return surge_codes + plunge_codes

    def find_stock_by_code_and_date(
        self, stock_code: str, target_date: datetime
    ) -> Optional[StockPriceHistory]:
        """
        특정 종목코드와 날짜로 주가 정보를 조회합니다.

        Args:
            stock_code: 종목코드
            target_date: 조회할 날짜

        Returns:
            StockPriceHistory 객체 또는 None
        """
        target_date_only = target_date.date()
        return (
            self.db.query(StockPriceHistory)
            .filter(
                StockPriceHistory.stock_code == stock_code,
                StockPriceHistory.target_date == target_date_only,
            )
            .first()
        )
