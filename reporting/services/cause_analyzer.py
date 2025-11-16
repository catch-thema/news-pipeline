from __future__ import annotations

from openai import OpenAI
from typing import List, Dict, Any
from datetime import datetime
import json

from shared.common.config import ReportingWorkerConfig


config = ReportingWorkerConfig()
client = OpenAI(api_key=config.OPENAI_API_KEY)


def analyze_causes(
    explicit_cause_chunks: List[Dict[str, Any]],
    event_chunks: List[Dict[str, Any]],
    ticker: str,
    date: datetime,
    movement_type: str = "up",
    change_rate: float = 0.0  # 변동률 추가
) -> Dict[str, Any]:
    # Event 전처리
    preprocessed_events = preprocess_event_chunks(event_chunks, movement_type)

    # 청크 텍스트 추출
    explicit_texts = _extract_chunk_texts(explicit_cause_chunks)
    event_texts = _extract_chunk_texts(preprocessed_events, include_inferred=True)

    movement_text = "상승" if movement_type == "up" else "하락"
    magnitude = abs(change_rate)

    # 변동 크기에 따라 기대 원인 개수 조정
    if magnitude >= 10:
        expected_causes = "3~5개"
        min_confidence = "Medium"
    elif magnitude >= 5:
        expected_causes = "2~3개"
        min_confidence = "Medium"
    else:
        expected_causes = "1~2개"
        min_confidence = "High"

    prompt = f"""역할: 금융 애널리스트

목표: {ticker}의 주가가 {date.date()}에 {change_rate:+.2f}% {movement_text}한 원인을 분석하세요.

변동 크기: {magnitude:.2f}% ({"큰 변동" if magnitude >= 5 else "일반적 변동"})

입력 데이터:
[1] 명시적 원인 언급 뉴스: {json.dumps(explicit_texts, ensure_ascii=False, indent=2)}
[2] Event 뉴스: {json.dumps(event_texts, ensure_ascii=False, indent=2)}

지침:
1. **원인 개수는 {expected_causes} 범위에서 유동적으로 결정하세요.**
   - 근거가 충분한 원인만 포함 (최소 confidence: {min_confidence})
   - 근거 부족하면 1개만 제시해도 됨
   - 큰 변동일수록 더 많은 원인 필요

2. **각 원인의 기여도를 독립적으로 평가하세요** (합이 100% 아니어도 됨)
   - High confidence: 70~100%
   - Medium: 40~70%
   - Low: 20~40%

3. **Event 뉴스 처리 방식:**
   - 단순 사실 나열 → 주가 영향 추론 필요
   - 예: "실적 발표" event → "예상보다 좋은 실적" 원인으로 해석

4. **원인 우선순위:**
   - 1순위: Explicit 뉴스에서 직접 언급된 원인
   - 2순위: Event 뉴스에서 강하게 추론되는 원인
   - 3순위: 복수 Event의 조합으로 추론되는 원인

출력 형식 (JSON):
{{
  "ticker": "{ticker}",
  "date": "{date.date()}",
  "change_rate": {change_rate},
  "causes": [
    {{
      "rank": 1,
      "title": "원인 제목 (10자 이내)",
      "description": "상세 설명 (2-3문장)",
      "confidence": "High|Medium|Low",
      "category": "earnings|order|guidance|supply_chain|macro|regulation|product|other",
      "impact_score": 85,
      "evidence": [
        "근거 인용문 1 (핵심 1-2문장)",
        "근거 인용문 2"
      ],
      "news_dates": ["2025-01-15", "2025-01-16"],
      "keywords": ["키워드1", "키워드2"]
    }}
  ],
  "summary": "종합 요약 (3-4문장)",
  "total_confidence": "High|Medium|Low"
}}
"""

    # ... (기존 API 호출 로직)

    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            temperature=0.2  # 더 일관된 결과를 위해 낮춤
        )

        result = json.loads(response.choices[0].message.content)

        # 후처리: 신뢰도가 너무 낮은 원인 제거
        if "causes" in result:
            result["causes"] = [
               c for c in result["causes"]
               if c.get("confidence") in ["High", "Medium"]
            ][:5]

        return result

    except Exception as e:
        print(f"Error analyzing causes: {e}")
        return {
            "ticker": ticker,
            "date": date.date().isoformat(),
            "change_rate": change_rate,
            "causes": [],
            "summary": "원인 분석 중 오류가 발생했습니다.",
            "total_confidence": "Low"
        }

def preprocess_event_chunks(event_chunks: List[Dict], movement_type: str) -> List[Dict]:
    """Event 청크를 원인으로 변환 (사전 추론)"""
    if not event_chunks:
        return []

    events_summary = "\n".join([
        f"- {chunk.get('metadata', {}).get('title', '')}: {chunk.get('text', '')[:200]}"
        for chunk in event_chunks[:10]
    ])

    movement_text = "상승" if movement_type == "up" else "하락"

    prompt = f"""다음 Event 뉴스가 주가 {movement_text}에 어떤 영향을 미쳤을지 추론하세요.

Event 목록:
{events_summary}

출력 (JSON):
{{
  "inferred_causes": [
    {{
      "original_event": "이벤트 제목",
      "inferred_cause": "추론된 원인",
      "confidence": "High|Medium|Low"
    }}
  ]
}}
"""

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            temperature=0.3
        )

        result = json.loads(response.choices[0].message.content)

        inferred_map = {
            ic["original_event"]: ic
            for ic in result.get("inferred_causes", [])
        }

        for chunk in event_chunks:
            title = chunk.get("metadata", {}).get("title", "")
            if title in inferred_map:
                chunk["inferred_cause"] = inferred_map[title]

        return event_chunks

    except Exception as e:
        return event_chunks


def _extract_chunk_texts(chunks: List[Dict], include_inferred: bool = False) -> List[Dict]:
    """청크에서 텍스트 추출"""
    texts = []
    seen_urls = set()

    for chunk in chunks[:30]:
        url = chunk.get("metadata", {}).get("url", "")
        if url and url not in seen_urls:
            seen_urls.add(url)
            text_data = {
                "content": chunk.get("text", "")[:500],
                "title": chunk.get("metadata", {}).get("title", ""),
                "url": url,
                "published_at": chunk.get("metadata", {}).get("published_at", "")
            }

            if include_inferred and "inferred_cause" in chunk:
                text_data["inferred_cause"] = chunk["inferred_cause"]

            texts.append(text_data)

    return texts