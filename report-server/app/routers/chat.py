from fastapi import APIRouter, Depends
from datetime import datetime
from sqlalchemy.orm import Session

from shared.common.config import get_mysql_session
from app.models import ChatQuestionRequest
from app.services.open_ai import OpenAIClient
from reporting.worker.reporting import ReportingWorker
from shared.common.models import StockMovementMessage
from app.services.stock_code_mapping import StockCodeMappingService

router = APIRouter(prefix="/chat", tags=["chat"])


@router.post("/analyze")
async def chat_analyze(
    request: ChatQuestionRequest,
    db: Session = Depends(get_mysql_session)
):
    question = request.question
    openai_client = OpenAIClient()
    reporting_worker = ReportingWorker()

    # 1. 질문에서 정보 추출
    extracted_info = openai_client.extract_query_info(question)
    stock_name = extracted_info.get("stock_name")
    published_at_kst = extracted_info.get("published_at_kst")
    movement_type = extracted_info.get("movement_type")

    if not stock_name or not published_at_kst or not movement_type:
        missing_fields = [k for k, v in {
            "stock_name": stock_name,
            "published_at_kst": published_at_kst,
            "movement_type": movement_type
        }.items() if not v]
        return {
            "success": False,
            "error": "정보 추출 실패",
            "message": f"질문에서 다음 정보를 추출할 수 없습니다: {', '.join(missing_fields)}",
            "extracted_info": extracted_info
        }

    # 3. 날짜 파싱
    try:
        analysis_date = datetime.fromisoformat(published_at_kst.replace("Z", "+00:00"))
        date_str = analysis_date.strftime("%Y-%m-%d")
    except Exception as e:
        return {
            "success": False,
            "error": "날짜 파싱 오류",
            "message": f"날짜 형식이 올바르지 않습니다: {str(e)}",
            "extracted_info": extracted_info
        }

    mapping_service = StockCodeMappingService(db)
    mapping_obj = mapping_service.search_mappings_by_name_abbr(stock_name)
    if not mapping_obj:
        return {
            "success": False,
            "error": "종목 코드 매핑 실패",
            "message": f"종목명에 해당하는 코드를 찾을 수 없습니다: {stock_name}",
            "extracted_info": extracted_info
        }
    short_code = mapping_obj.short_code

    movement_message = StockMovementMessage(
        ticker=short_code,
        stock_name=stock_name,
        date=date_str,
        news_start_date=date_str,
        news_end_date=date_str,
        trend_type="plunge" if movement_type == "down" else "surge",
        change_rate=0.0,  # 실제 데이터 필요
        triggered_at=datetime.now().isoformat(),
        news_count=0,
        analyzed_count=0,
        embedded_count=0,
        news_urls=[]
    )

    analysis_result = reporting_worker.analyze_stock_movement(movement_message)

    return {
        "success": True,
        "extracted_info": extracted_info,
        "analysis_result": analysis_result
    }