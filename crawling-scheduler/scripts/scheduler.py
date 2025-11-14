import sys
import os
from datetime import datetime
import pytz

# 프로젝트 루트를 Python 경로에 추가
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from main import run_stock_price_crawling

# 한국 시간대 설정
KST = pytz.timezone('Asia/Seoul')

def should_run():
    """주말인지 확인 (토요일=5, 일요일=6)"""
    now = datetime.now(KST)
    weekday = now.weekday()

    if weekday >= 5:  # 토요일(5) 또는 일요일(6)
        print(f"Weekend detected (weekday={weekday}). Skipping execution.")
        return False

    return True

def main():
    if not should_run():
        print("Scheduler: Execution skipped (weekend)")
        sys.exit(0)

    print(f"Scheduler: Starting crawling at {datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S %Z')}")

    try:
        run_stock_price_crawling()
        print("Scheduler: Crawling completed successfully")
    except Exception as e:
        print(f"Scheduler: Error occurred - {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
