from typing import Dict, Optional
from openai import OpenAI
import logging
import json

from shared.common.config import ReportingWorkerConfig

logger = logging.getLogger(__name__)

class MarketContextService:
    def __init__(self):
        self.config = ReportingWorkerConfig()
        self.client = OpenAI(api_key=self.config.OPENAI_API_KEY)

        self.system_prompt = """
당신은 주식 시장 분석 전문가입니다.
입력으로 특정 종목의 원인 분석이 주어지면 해당 종목이 속한 섹터, 시장 상황, 관련 동종 종목을 분석해 JSON으로만 출력합니다.

출력 형식(JSON만 출력):
{
  "sector": "섹터명",
  "market_condition": "시장 상황",
  "peer_stocks": ["종목1", "종목2", "종목3"]
}
"""

    def analyze_market_context(self, ticker: str, stock_name: str, causes: list) -> Optional[Dict]:
        """시장 맥락 분석 (섹터, 시장 상황 등)"""

        causes_summary = "\n".join([
            f"{i + 1}. {c.get('title', '')}"
            for i, c in enumerate(causes[:3])
        ])

        user_prompt = f"""
종목: {stock_name} ({ticker})

원인 분석 요약:
{causes_summary}

위 정보를 기반으로 섹터/시장 맥락을 분석하고 JSON만 출력하세요.
"""

        try:
            response = self.client.chat.completions.create(
                model="gpt-4.1-mini",
                messages=[
                    {"role": "system", "content": self.system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.2
            )

            text = response.choices[0].message.content.strip()

            if text.startswith("```"):
                text = text.replace("```json", "").replace("```", "").strip()

            result = json.loads(text)
            logger.info(f"시장 맥락 분석 완료: {result.get('sector')}")
            return result

        except Exception as e:
            logger.error(f"시장 맥락 분석 실패: {e}", exc_info=True)
            return None
