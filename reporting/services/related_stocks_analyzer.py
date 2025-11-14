from typing import List, Dict
from langchain_openai import ChatOpenAI
from langchain.prompts import ChatPromptTemplate
import logging

logger = logging.getLogger(__name__)


def analyze_related_stocks(stock_name: str, movement_type: str, causes: List[Dict]) -> Dict:
    """연관 종목 분석 (개선된 버전)"""

    # 원인에서 언급된 기업/산업 추출
    mentioned_entities = set()
    for cause in causes:
        evidence = cause.get("evidence", [])
        for ev in evidence:
            # 간단한 엔티티 추출 (실제로는 NER 사용 권장)
            mentioned_entities.update(_extract_company_names(ev))

    # LLM 프롬프트
    prompt = ChatPromptTemplate.from_messages([
        ("system", """당신은 주식 시장 분석 전문가입니다.
주어진 종목의 {movement_type} 원인을 분석하여, 영향을 받을 수 있는 연관 종목을 찾아주세요.

**분석 기준:**
1. 동일 산업/섹터 내 경쟁사
2. 공급망 관계 (상류/하류)
3. 협력/파트너 관계
4. 대체재/보완재 관계

**출력 형식 (JSON):**
{{
  "related_stocks": [
    {{
      "stock_name": "종목명",
      "ticker": "티커",
      "relation_type": "경쟁사|공급사|협력사|고객사|대체재",
      "confidence": "High|Medium|Low",
      "reason": "영향 받는 이유 (1-2문장)"
    }}
  ]
}}

**제약사항:**
- 최대 5개까지만 선정
- 실제 상장 종목만 포함
- confidence가 Medium 이상만 포함
"""),
        ("user", """**분석 대상 종목:** {stock_name}
**주가 움직임:** {movement_text}

**원인 분석 결과:**
{causes_summary}

**언급된 기업/산업:**
{mentioned_entities}

위 정보를 바탕으로 영향받을 연관 종목을 분석해주세요.
""")
    ])

    movement_text = "하락" if movement_type == "down" else "상승"
    causes_summary = "\n".join([
        f"{i+1}. {c.get('title', '')} ({c.get('confidence', '')})"
        for i, c in enumerate(causes[:3])
    ])

    mentioned_entities_text = ", ".join(list(mentioned_entities)[:10]) if mentioned_entities else "없음"

    try:
        llm = ChatOpenAI(model="gpt-4o-mini", temperature=0.3)
        chain = prompt | llm

        response = chain.invoke({
            "stock_name": stock_name,
            "movement_type": movement_type,
            "movement_text": movement_text,
            "causes_summary": causes_summary,
            "mentioned_entities": mentioned_entities_text
        })

        import json
        result = json.loads(response.content)
        logger.info(f"연관 종목 분석 완료: {len(result.get('related_stocks', []))}개")
        return result

    except Exception as e:
        logger.error(f"연관 종목 분석 실패: {e}", exc_info=True)
        return {"related_stocks": []}


def _extract_company_names(text: str) -> List[str]:
    """텍스트에서 기업명 추출 (간단 버전)"""
    # 실제로는 NER 모델 사용 권장
    # 여기서는 간단히 "(주)" 패턴 매칭
    import re
    pattern = r'([가-힣A-Za-z]+(?:\(주\)|주식회사)?)'
    matches = re.findall(pattern, text)

    # 일반적인 단어 필터링
    stopwords = {"것", "수", "등", "의", "를", "이", "가", "은", "는", "에"}
    return [m for m in matches if len(m) > 2 and m not in stopwords]