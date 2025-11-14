from datetime import datetime
from typing import Dict, List

import httpx

from config.constants import (
    KRXEndpoints,
    KRXHeaders,
    KRXParams,
    ErrorMessages,
    DateFormats,
)


class KRXClient:
    def __init__(self):
        self.client = httpx.Client(follow_redirects=True, timeout=30.0)

    def _clean_decimal_value(self, value) -> float:
        if value is None or value == "" or value == "-":
            return 0.0
        try:
            if isinstance(value, str):
                value = value.replace(",", "").strip()
            return float(value)
        except (ValueError, TypeError):
            return 0.0

    def _clean_integer_value(self, value) -> int:
        if value is None or value == "" or value == "-":
            return 0
        try:
            if isinstance(value, str):
                value = value.replace(",", "").strip()
            return int(float(value))
        except (ValueError, TypeError):
            return 0

    def fetch_all_stock_prices(self, target_date: str) -> List[Dict]:
        try:
            datetime.strptime(target_date, DateFormats.KRX_DATE_FORMAT)
        except ValueError:
            raise ValueError(ErrorMessages.INVALID_DATE_FORMAT)

        headers = {
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Origin": "https://data.krx.co.kr",
            "Referer": KRXHeaders.REFERER,
            "Sec-Ch-Ua": '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"macOS"',
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "User-Agent": KRXHeaders.USER_AGENT,
            "X-Requested-With": "XMLHttpRequest",
        }

        data = {
            "bld": KRXParams.BLD_ALL_STOCK_PRICE,
            "locale": KRXParams.LOCALE,
            "mktId": "ALL",              # 전체 기준
            # "mktId": "STK",                # KOSPI 기준
            "strtDd": target_date,
            "endDd": target_date,
            "adjStkPrc_check": "Y",
            "adjStkPrc": "2",
            "share": "1",
            "money": "1",
            "csvxls_isNo": "false",
        }

        try:
            response = self.client.post(
                KRXEndpoints.DATA_FETCH, data=data, headers=headers
            )
            response.raise_for_status()
            json_data = response.json()

            if "OutBlock_1" not in json_data:
                raise Exception("Response does not contain OutBlock_1")

            stock_data_list = []
            for item in json_data["OutBlock_1"]:
                stock_data = {
                    "stock_code": str(item.get(KRXParams.FieldNames.STOCK_CODE, "")).strip(),
                    "stock_name": str(item.get(KRXParams.FieldNames.STOCK_NAME, "")).strip(),
                    "change_rate": self._clean_decimal_value(
                        item.get(KRXParams.FieldNames.CHANGE_RATE)
                    ),
                    "trading_value": self._clean_integer_value(
                        item.get(KRXParams.FieldNames.TRADING_VALUE)
                    ),
                }

                if stock_data["stock_code"]:
                    stock_data_list.append(stock_data)

            return stock_data_list

        except Exception as e:
            raise Exception(f"{ErrorMessages.DATA_FETCH_FAILED}: {str(e)}")
