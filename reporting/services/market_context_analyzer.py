from typing import Dict, Optional
from langchain_openai import ChatOpenAI
from langchain.prompts import ChatPromptTemplate
import logging

logger = logging.getLogger(__name__)


def analyze_market_context(ticker: str, stock_name: str, causes: list) -> Optional[Dict]:
    """시장 맥락 분석 (섹터, 시장 상황 등)"""

    prompt = ChatPromptTemplate.from_messages([
        ("system", """당신은 주식 시장 분석 전문가입니다.
주어진 종목과 원인 분석을 바탕으로 시장 맥락을 분석해주세요.

**출력 형식 (JSON):**
{{
  "sector": "섹터명 (예: IT/반도체, 자동차, 금융 등)",
  "market_condition": "시장 상황 (예: 호황, 불황, 조정 등)",
  "peer_stocks": ["동종 업계 대표 종목1", "종목2", "종목3"]
}}
"""),
        ("user", """**종목:** {stock_name} ({ticker})

**원인 분석:**
{causes_summary}

위 정보를 바탕으로 시장 맥락을 분석해주세요.
""")
    ])

    causes_summary = "\n".join([
        f"{i+1}. {c.get('title', '')}"
        for i, c in enumerate(causes[:3])
    ])

    try:
        llm = ChatOpenAI(model="gpt-4o-mini", temperature=0.3)
        chain = prompt | llm

        response = chain.invoke({
            "ticker": ticker,
            "stock_name": stock_name,
            "causes_summary": causes_summary
        })

        import json
        result = json.loads(response.content)
        logger.info(f"시장 맥락 분석 완료: {result.get('sector')}")
        return result

    except Exception as e:
        logger.error(f"시장 맥락 분석 실패: {e}", exc_info=True)
        return None