import logging
from datetime import datetime, timedelta
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from features.stock_price.service import StockPriceService
from features.correlation.service import CorrelationService
from features.correlation.model import StockTrend
from config.constants import DateFormats
from config.database import SessionLocal
from .publisher import RabbitPublisher

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

class StockCrawlerService:
    def __init__(self):
        self.db = SessionLocal()
        self.publisher = RabbitPublisher()
        self.stock_service = StockPriceService(self.db)

    async def crawl_and_publish_test(self):
        logger.info("crawl_and_publish_test 시작")
        await self.publisher.connect()
        try:
            payload = {
                "query": "삼성전자",
                "ticker": "005930",
                "pages": 1,
                "delay": 1.5,
                "crawl_concurrency": 10,
                "start_date": "2025.10.01",
                "end_date": "2025.10.01",
                "date": "2025.10.01",
                "stock_name": "삼성전자",
                "change_rate": "5.2",
                "trend_type": "surge"
            }
            logger.info(f"테스트 발행: {payload}")
            await self.publisher.publish(payload)
        except Exception as e:
            logger.exception("crawl_and_publish_test 중 예외 발생")
        finally:
            await self.publisher.close()
            logger.info("crawl_and_publish_test 종료")


    async def crawl_and_publish_single_stock(self, target_date: str = None):
        logger.info("crawl_and_publish_single_stock 시작")
        await self.publisher.connect()
        try:
            if target_date is None:
                target_date = datetime.now().strftime(DateFormats.KRX_DATE_FORMAT)

            target_datetime = datetime.strptime(target_date, DateFormats.KRX_DATE_FORMAT)

            start_dt = target_datetime - timedelta(days=1)

            formatted_start_date = start_dt.strftime("%Y.%m.%d")
            formatted_end_date = target_datetime.strftime("%Y.%m.%d")

            saved_count = self.stock_service.fetch_and_save_stock_prices(target_date)
            logger.info(f"fetch_and_save_stock_prices: {target_date} 저장된 종목 수: {saved_count}")

            threshold = 6.0
            min_trading_value = 1_000_000_000
            surge_codes = self.stock_service.get_surge_stock_codes(threshold, target_datetime, min_trading_value)
            plunge_codes = self.stock_service.get_plunge_stock_codes(threshold, target_datetime, min_trading_value)

            for codes, trend in [(surge_codes, StockTrend.SURGE), (plunge_codes, StockTrend.PLUNGE)]:
                if not codes:
                    logger.info(f"{trend.value} 종목 없음")
                    continue
                for code in codes:
                    stock_info = self.stock_service.get_stock_info_by_code_and_date(code, target_datetime)
                    if not stock_info:
                        logger.warning(f"종목 정보 없음: {code}")
                        continue
                    payload = {
                        "query": stock_info.stock_name,
                        "ticker": code,
                        "stock_name": stock_info.stock_name,
                        "change_rate": str(stock_info.change_rate),
                        "trend_type": trend.value,
                        "start_date": formatted_start_date,
                        "end_date": formatted_end_date,
                        "date": formatted_end_date,
                        "pages": 2,
                        "delay": 1.0,
                        "crawl_concurrency": 8,
                    }
                    logger.info(f"발행: {payload}")
                    await self.publisher.publish(payload)
        except Exception as e:
            logger.exception("crawl_and_publish_single_stock 중 예외 발생")
        finally:
            await self.publisher.close()
            self.db.close()
            logger.info("crawl_and_publish_single_stock 종료")

    async def crawl_and_publish_related_stocks(self, target_date: str = None):
        logger.info("crawl_and_publish_related_stocks 시작")
        await self.publisher.connect()
        try:
            if target_date is None:
                target_date = datetime.now().strftime(DateFormats.KRX_DATE_FORMAT)

            target_datetime = datetime.strptime(target_date, DateFormats.KRX_DATE_FORMAT)

            start_dt = target_datetime - timedelta(days=1)

            formatted_start_date = start_dt.strftime("%Y.%m.%d")
            formatted_end_date = target_datetime.strftime("%Y.%m.%d")

            target_date_obj = target_datetime.date()

            stock_service = StockPriceService(self.db)
            correlation_service = CorrelationService(self.db)

            threshold = 6.0
            min_trading_value = 1_000_000_000
            surge_codes = stock_service.get_surge_stock_codes(threshold, target_datetime, min_trading_value)
            plunge_codes = stock_service.get_plunge_stock_codes(threshold, target_datetime, min_trading_value)

            for codes, trend in [(surge_codes, StockTrend.SURGE), (plunge_codes, StockTrend.PLUNGE)]:
                if not codes:
                    logger.info(f"{trend.value} 종목 없음")
                    continue
                correlations = correlation_service.fetch_and_save_correlations(
                    codes, trend, target_date_obj
                )
                for corr in correlations:
                    payload = {
                        "query": corr.related_stock_name,
                        "ticker": corr.related_ticker,
                        "stock_name": corr.related_stock_name,
                        "change_rate": corr.change_rate,
                        "trend_type": trend.value,
                        "start_date": formatted_start_date,
                        "end_date": formatted_end_date,
                        "date": formatted_end_date,
                        "pages": 2,
                        "delay": 1.0,
                        "crawl_concurrency": 8,
                    }
                    logger.info(f"발행: {payload}")
                    await self.publisher.publish(payload)
        except Exception as e:
            logger.exception("crawl_and_publish_related_stocks 중 예외 발생")
        finally:
            await self.publisher.close()
            self.db.close()
            logger.info("crawl_and_publish_related_stocks 종료")

crawler_service = StockCrawlerService()

def start_scheduler():
    logger.info("스케줄러 시작")
    scheduler = AsyncIOScheduler()
    scheduler.add_job(crawler_service.crawl_and_publish_related_stocks, 'cron', hour=9, minute=10)
    scheduler.start()