from typing import List, Dict
import httpx
from config.constants import KRXEndpoints


class KRXStockCodeClient:
    BASE_URL = KRXEndpoints.DATA_FETCH
    TIMEOUT = 30.0

    HEADERS = {
        "accept": "application/json, text/javascript, */*; q=0.01",
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "cache-control": "no-cache",
        "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
        "origin": "https://data.krx.co.kr",
        "pragma": "no-cache",
        "referer": "https://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020102",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
        "x-requested-with": "XMLHttpRequest",
    }

    PAYLOAD = {
        "bld": "dbms/MDC/STAT/standard/MDCSTAT01901",
        "locale": "ko_KR",
        "mktId": "ALL",
        "share": "1",
        "csvxls_isNo": "false",
    }

    def fetch_all_stock_codes(self) -> List[Dict]:
        try:
            with httpx.Client(timeout=self.TIMEOUT) as client:
                response = client.post(
                    self.BASE_URL, headers=self.HEADERS, data=self.PAYLOAD
                )
                response.raise_for_status()

                response_data = response.json()

                if not response_data:
                    return []

                data = response_data.get("OutBlock_1", [])

                if not data:
                    return []

                stock_codes = []
                for item in data:
                    standard_code = item.get("ISU_CD", "").strip()
                    short_code = item.get("ISU_SRT_CD", "").strip()
                    stock_name_abbr = item.get("ISU_ABBRV", "").strip()

                    if standard_code and short_code and stock_name_abbr:
                        stock_codes.append(
                            {
                                "standard_code": standard_code,
                                "short_code": short_code,
                                "stock_name_abbr": stock_name_abbr,
                            }
                        )

                return stock_codes

        except httpx.HTTPError as e:
            print(f"HTTP error occurred while fetching stock codes: {e}")
            return []
        except Exception as e:
            print(f"Unexpected error occurred while fetching stock codes: {e}")
            return []
