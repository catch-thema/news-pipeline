import json
from openai import OpenAI

from shared.common.config import ReportingWorkerConfig


class LLMPropagationService:
    def __init__(self):
        self.config = ReportingWorkerConfig()
        self.client = OpenAI(api_key=self.config.OPENAI_API_KEY)

        self.system_prompt = """
당신은 '주가 영향 전파 전문가'입니다. 
입력으로 특정 종목의 급등/급락 원인과 관계그래프가 주어집니다.

규칙:
1) 반드시 JSON만 출력합니다. 설명문, 자연어 문장은 JSON 외부에 넣지 않습니다.
2) JSON 외부에는 어떤 텍스트도 출력하지 않습니다. (주의: "다음은 JSON입니다" 같은 말 금지)
3) JSON 형식은 아래 예시와 정확히 동일한 구조를 따라야 합니다.
4) impact 값은 "상승" 또는 "하락" 중 하나여야 합니다.
5) confidence 값은 High / Medium / Low 중 하나여야 합니다.
6) reason은 간단하고 명확한 한 문장으로 작성합니다.
7) graph_neighbors의 distance 값에 따라 영향력을 추론합니다.
   - distance 1 → High 가능성
   - distance 2 → Medium 가능성
   - distance >= 3 → Low 가능성

출력 형식(JSON만 출력):
{
  "impacted_stocks": [
    {
      "stock": "KTcs",
      "impact": "상승",
      "confidence": "High",
      "reason": "KT 실적 호조가 IT 서비스 계열사에 직접적인 수요 증가를 유발할 수 있음"
    }
  ]
}
"""

    def analyze_propagation(self, stock_name: str, causes, graph_neighbors):
        user_content = {
            "stock": stock_name,
            "causes": causes,
            "graph_neighbors": graph_neighbors
        }

        messages = [
            {"role": "system", "content": self.system_prompt},
            {"role": "user", "content": json.dumps(user_content, ensure_ascii=False)}
        ]

        response = self.client.chat.completions.create(
            model="gpt-4.1",
            messages=messages,
            temperature=0.2
        )

        text = response.choices[0].message.content.strip()

        try:
            if text.startswith("```"):
                text = text.strip("```json").strip("```")
            return json.loads(text)
        except Exception as e:
            return {
                "error": "JSON parsing failed",
                "exception": str(e),
                "raw": text
            }
