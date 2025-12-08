from features.scheduler.service import StockCrawlerService
import asyncio

if __name__ == "__main__":
    service = StockCrawlerService()
    asyncio.run(service.crawl_and_publish_single_stock("20251205"))