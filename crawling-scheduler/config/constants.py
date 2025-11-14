from enum import Enum


class KRXEndpoints:
    DATA_FETCH = "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"


class KRXHeaders:
    USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    REFERER = "https://data.krx.co.kr/contents/MDC/MDI/mdiLoader"


class KRXParams:
    LOCALE = "ko_KR"
    BLD = "dbms/MDC/STAT/standard/MDCSTAT01501"
    BLD_ALL_STOCK_PRICE = "dbms/MDC/STAT/standard/MDCSTAT01602"

    class FieldNames:
        STOCK_CODE = "ISU_SRT_CD"
        STOCK_NAME = "ISU_ABBRV"
        CHANGE_RATE = "FLUC_RT"
        TRADING_VALUE = "ACC_TRDVAL"


class DatabaseTables:
    STOCK_PRICE_HISTORY = "stock_price_history"
    STOCK_CORRELATION = "stock_correlation"


class ErrorMessages:
    OTP_GENERATION_FAILED = "OTP 생성에 실패했습니다"
    DATA_FETCH_FAILED = "데이터 조회에 실패했습니다"
    DATABASE_ERROR = "데이터베이스 오류가 발생했습니다"
    INVALID_DATE_FORMAT = "날짜 형식이 올바르지 않습니다 (YYYYMMDD)"


class DateFormats:
    KRX_DATE_FORMAT = "%Y%m%d"
    ISO_DATE_FORMAT = "%Y-%m-%d"


class Timezone:
    KST = "Asia/Seoul"
