from __future__ import annotations

import json
from typing import List, Dict, Any

from openai import OpenAI
from app.settings import settings

class OpenAIClient:
    def __init__(self):
        self.client = OpenAI(api_key=settings.OPENAI_API_KEY)

    def _build_cluster_prompt(self, tickers: List[str], date: str) -> str:
        comma_separated = ", ".join(tickers)
        return f"""
    역할: 당신은 애널리스트다. 입력 종목 목록을 공통의 근본 요인으로 묶어 군집을 만든다.
    입력:
    - 날짜: {date}
    - 종목: {comma_separated}
    
    요구사항:
    - 2개 이상 공통 요인을 공유하는 종목끼리 군집
    - 가능한 요인 예시: 동일 섹터, 공급망 연결, 동일 규제 리스크, 동일 이벤트 민감도, 동일 고객군
    - 애매하면 단일군집으로 합치지 말고 미배정으로 남겨라
    - 아래 JSON 스키마대로만 출력
    
    출력 JSON 스키마:
    {{
      "date": "YYYY-MM-DD",
      "groups": [
        {{
          "group_key": "string - 사람이 읽기 쉬운 간단 라벨",
          "symbols": ["AAA", "BBB"],
          "rationale": "왜 묶였는지 1문장",
          "method": "openai_cluster_v1"
        }}
      ],
      "unassigned": ["..."]
    }}
    """.strip()


    def cluster_tickers(self, tickers: List[str], date: str) -> Dict[str, Any]:
        prompt = self._build_cluster_prompt(tickers, date)
        resp = self.client.responses.create(
            model="gpt-4o",
            input=prompt,
        )
        return json.loads(resp.output_text)


    def _build_cause_prompt(title: str, body: str) -> str:
        return f"""
    역할: 애널리스트
    목표: 기사 제목/본문 근거만으로 주가 변동의 핵심 원인을 한국어 한 문장으로 제시한다.
    
    입력
    - 제목: {title}
    - 본문:
    {body}
    
    지침
    - 규칙/키워드 매칭 금지. 기사 서술의 인과를 이해해 가장 직접적인 단일 원인만 제시
    - 추정/예단 금지(기사 근거 없는 가정 제외)
    - 간결하게 최대 30자 권장. 불필요 수식어/종목명/수치 생략 가능
    - 가능하면 원인 유형이 드러나게(예: 호실적/목표가 상향/대형 수주/인허가 승인/규제 완화/M&A 발표/리콜/소송/경영진 이슈/매크로 수혜 등)
    
    출력 형식
    - JSON 금지. 한국어 한 문장 텍스트만 출력.
    """.strip()


    def summarize_cause_one_line(self, title: str, body: str) -> str:
        prompt = self._build_cause_prompt(title or "", body or "")
        resp = self.client.responses.create(
            model="gpt-4o",
            input=prompt,
        )
        text = (resp.output_text or "").strip()
        # 첫 줄만 사용, 과도한 길이는 제한
        line = text.splitlines()[0].strip() if text else ""
        return line[:120]


    def _build_extract_stocks_prompt(self, title: str, body: str) -> str:
        return f"""
    역할: 주식 분석가
    목표: 기사에서 핵심으로 주가가 오르거나 내린 주식(회사) 이름만 정확히 추출한다.
    
    입력
    - 제목: {title}
    - 본문:
    {body}
    
    지침
    - 주가 변동의 핵심 대상이 되는 회사만 추출 (주요 특징주만)
    - 제목에 "[특징주]회사명" 형식으로 나오는 회사는 반드시 포함
    - 본문에서 주가 상승/하락의 직접적인 주체가 되는 회사만 포함
    - 예시: 제목이 "[특징주]S-Oil, 3일째 상승"이면 "S-Oil"만 추출
    - 제목이 "[특징주]삼성전자, SK하이닉스 신고가"이면 "삼성전자, SK하이닉스"만 추출
    - 간접적으로 언급만 된 회사는 제외 (예: 협력사, 경쟁사 등)
    - 배경 설명이나 비교 대상으로만 언급된 회사는 제외
    - 약칭이나 별칭도 정확한 회사명으로 변환 (예: 삼전=삼성전자, 하이닉스=SK하이닉스)
    - 중복 제거
    - 최대 5개 이하의 핵심 주식만 추출
    
    출력 형식
    - JSON 금지. 회사명을 쉼표로 구분한 문자열만 출력.
    - 예: "S-Oil" 또는 "삼성전자, SK하이닉스"
    - 없으면 빈 문자열 출력
    """.strip()


    def extract_stock_names(self, title: str, body: str) -> List[str]:
        """기사 제목과 본문에서 핵심으로 오르거나 내린 주식 이름만 추출합니다."""
        prompt = self._build_extract_stocks_prompt(title or "", body or "")
        try:
            resp = self.client.responses.create(
                model="gpt-4o",
                input=prompt,
            )
            text = (resp.output_text or "").strip()
            if not text:
                return []
            # 쉼표로 구분된 이름들을 파싱하고 공백 제거
            names = [name.strip() for name in text.split(",") if name.strip()]
            return names
        except Exception:
            return []


    def _build_extract_query_info_prompt(self, question: str) -> str:
        return f"""
    역할: 주식 분석 시스템의 질문 파서
    목표: 사용자의 질문에서 다음 3가지 정보를 정확히 추출합니다:
    1. stock_name: 종목명 (예: "S-Oil", "삼성전자", "LG화학")
    2. published_at_kst: 발행일시 (ISO 8601 형식, KST 타임존, 예: "2025-10-22T09:28:00+09:00")
    3. movement_type: 주가 변동 타입 ("up" 또는 "down")
    
    입력 질문:
    {question}
    
    지침:
    - 종목명은 정확한 회사명으로 추출 (약칭은 정식명으로 변환)
    - 날짜가 명시되지 않았거나 불명확하면 null 반환
    - 날짜 형식은 반드시 ISO 8601 형식으로 변환 (예: "2025년 10월 22일" -> "2025-10-22T00:00:00+09:00")
    - 상승/하락이 명시되지 않았거나 불명확하면 null 반환
    - movement_type은 "up" (상승) 또는 "down" (하락)만 가능
    - 추출할 수 없는 정보는 null로 설정
    
    출력 형식 (JSON만 출력, 다른 텍스트 없이):
    {{
      "stock_name": "종목명 또는 null",
      "published_at_kst": "YYYY-MM-DDTHH:MM:SS+09:00 또는 null",
      "movement_type": "up 또는 down 또는 null"
    }}
    """.strip()


    def extract_query_info(self, question: str) -> Dict[str, Any]:
        """
        질문에서 stock_name, published_at_kst, movement_type을 추출합니다.

        Returns:
            {
                "stock_name": str or None,
                "published_at_kst": str or None,
                "movement_type": "up" or "down" or None
            }
        """
        prompt = self._build_extract_query_info_prompt(question)
        try:
            resp = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system",
                     "content": "You are a JSON parser. Extract information from the question and return only valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                response_format={"type": "json_object"},
                temperature=0
            )
            text = resp.choices[0].message.content.strip()
            result = json.loads(text)

            # 검증 및 정규화
            if result.get("stock_name") == "null" or result.get("stock_name") == "":
                result["stock_name"] = None
            if result.get("published_at_kst") == "null" or result.get("published_at_kst") == "":
                result["published_at_kst"] = None
            if result.get("movement_type") not in ["up", "down"]:
                result["movement_type"] = None

            return result
        except Exception as e:
            return {
                "stock_name": None,
                "published_at_kst": None,
                "movement_type": None,
                "error": str(e)
            }
