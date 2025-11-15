from config.database import engine, Base, SessionLocal
from features.stock_code_mapping.service import StockCodeMappingService
from features.correlation.model import StockTrend, StockCorrelation
from features.stock_code_mapping.model import StockCodeMapping
from features.stock_price.model import StockPriceHistory

def initialize_database():
    """데이터베이스 테이블 초기화 (모든 모델을 import해야 함)"""
    Base.metadata.create_all(bind=engine)
    print("Database initialized successfully")

if __name__ == "__main__":
    initialize_database()
    db = SessionLocal()
    try:
        mapping_service = StockCodeMappingService(db)
        mapping_service.initialize_stock_code_mappings()
    except Exception as e:
        print(f"Error initializing stock code mapping: {e}")
        raise
    finally:
        db.close()