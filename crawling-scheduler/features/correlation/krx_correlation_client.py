from typing import List, Dict, Optional
import requests

from config.constants import KRXEndpoints, KRXHeaders


class KRXCorrelationClient:

    def __init__(self):
        self.base_url = KRXEndpoints.DATA_FETCH
        self.headers = {
            "User-Agent": KRXHeaders.USER_AGENT,
            "Referer": "https://data.krx.co.kr/contents/MDC/HARD/hardController/MDCHARD011.cmd",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "X-Requested-With": "XMLHttpRequest",
        }

    def search_stock_info(self, stock_code: str) -> Optional[Dict]:
        payload = {
            "locale": "ko_KR",
            "mktsel": "ALL",
            "typeNo": "0",
            "searchText": stock_code,
            "bld": "dbms/comm/finder/finder_stkisu",
        }

        try:
            response = requests.post(
                self.base_url, headers=self.headers, data=payload, timeout=10
            )
            response.raise_for_status()

            data = response.json()
            if data.get("block1") and len(data["block1"]) > 0:
                return data["block1"][0]  # 첫 번째 매칭 결과 반환

            return None
        except Exception as e:
            print(f"Error searching stock info for {stock_code}: {str(e)}")
            return None

    def fetch_correlation_data(
        self, stock_code: str, result_count: int = 20
    ) -> List[Dict]:
        """
        종목의 수익률 상관관계 데이터를 조회합니다.

        Args:
            stock_code: 종목 코드 (예: "005930")
            result_count: 조회할 결과 개수 (기본값: 20)

        Returns:
            상관관계 데이터 리스트 (100일 기준)
        """
        # 1단계: 종목 정보 조회
        stock_info = self.search_stock_info(stock_code)
        if not stock_info:
            print(f"Stock info not found for code: {stock_code}")
            return []

        full_code = stock_info["full_code"]  # KR7005930003
        stock_name = stock_info["codeName"]  # 삼성전자

        # 2단계: 상관관계 데이터 조회
        payload = {
            "bld": "dbms/MDC/HARD/MDCHARD01101",
            "locale": "ko_KR",
            "tboxisuCd_finder_stkisu0": f"{stock_code}/{stock_name}",
            "isuCd": full_code,
            "isuCd2": full_code,
            "codeNmisuCd_finder_stkisu0": stock_name,
            "param1isuCd_finder_stkisu0": "ALL",
            "marketGubun": "ALL",            # 전체 기준
            # "marketGubun": "STK",              # KOSPI 기준
            "rsltCnt": str(result_count),
            "csvxls_isNo": "false",
        }

        try:
            response = requests.post(
                self.base_url, headers=self.headers, data=payload, timeout=30
            )
            response.raise_for_status()

            data = response.json()
            if not data.get("block1"):
                return []

            # 100일 기준 데이터만 추출
            correlation_list = []
            for item in data["block1"]:
                correlation_item = {
                    "rank": int(item["RN"]),
                    "correlated_stock_code": item["CMP_ISU_CD_100"],
                    "correlated_stock_name": item["CMP_ISU_NM_100"],
                    "correlation_value": float(item["CORR_VAL_100"]),
                }
                correlation_list.append(correlation_item)

            return correlation_list

        except Exception as e:
            print(f"Error fetching correlation data for {stock_code}: {str(e)}")
            return []
