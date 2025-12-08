import json
import logging
from datetime import datetime
from typing import Dict, List
from openai import OpenAI
from kafka import KafkaConsumer
from pydantic import ValidationError

from shared.common.config import get_postgres_connection
from shared.common.config.config import ReportingSectionWorkerConfig
from shared.common.models import HeadlineAnalysisMessage
from reporting_section.models.report import SectionReport


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ReportingWorker:
    """주식 리포트 생성 및 저장 워커"""

    def __init__(self):
        self.config = ReportingSectionWorkerConfig()

        self.llm_client = OpenAI(api_key=self.config.OPENAI_API_KEY)

        # Kafka Consumer 설정
        self.consumer = KafkaConsumer(
            self.config.INPUT_TOPIC,
            bootstrap_servers=self.config.KAFKA_BOOTSTRAP_SERVERS,
            group_id=self.config.CONSUMER_GROUP,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            max_poll_interval_ms=600000,  # 10분
            session_timeout_ms=300000,
            heartbeat_interval_ms=3000,
            max_poll_records=1
        )


    def analyze_stock_movement(self,  tagged_news_list: List[Dict], movement: HeadlineAnalysisMessage) -> Dict:
        """주식 변동 분석"""
        prompt = f"""
다음은 특정 기간 동안 수집된 '경제' 분야 뉴스입니다.
모든 뉴스에는 NER 엔티티(기업, 인물, 기관 등)가 포함되어 있습니다.

당신의 목표는 전체 뉴스 흐름을 분석하여 경제 시장의 핵심 트렌드, 위험 요인, 기회 요인을 구조화된 방식으로 요약하는 것입니다.

[입력 정보]
- 뉴스 수집 기간: {movement.news_start_date} ~ {movement.news_end_date}
- 총 뉴스 개수: {movement.news_count}
- 섹션: {movement.section or "경제"}

[뉴스 데이터]
뉴스 리스트는 아래 필드를 포함합니다:
- title: 제목
- content: 본문 또는 요약
- url: 원문 링크
- published_at: 발행 시간
- publisher: 언론사
- entities: NER 분석 결과 (companies, persons, locations, organizations)

뉴스 리스트:
{tagged_news_list}  # serialized JSON list

[분석 지시사항]
1. 전체 뉴스에서 공통적으로 등장하는 **핵심 키워드 5~10개**를 추출하고 각 키워드의 간단한 의미를 설명하라.
2. 뉴스 전체 내용을 기반으로 현재 경제 상황에서 나타나는 **주요 흐름(트렌드) 3~7개**를 요약하라.
3. 전체 뉴스 중 의미가 크다고 판단되는 **핵심 뉴스 5개 이내**를 선택하고 다음 정보를 포함하라:
   - headline(제목)
   - summary(핵심 요약)
   - url
4. 전체 뉴스의 내용을 종합하여 **종합 요약(summary)**을 3~5문장으로 작성하라.
5. 아래 JSON 형식으로만 응답하라.

[응답 JSON 형식]
{{
  "keywords": [
    {{ "keyword": "", "description": "" }}
  ],
  "main_trends": [
    ""
  ],
  "key_news": [
    {{
      "headline": "",
      "summary": "",
      "url": ""
    }}
  ],
  "summary": ""
}}
"""

        try:
            response = self.llm_client.chat.completions.create(
                model=self.config.LLM_MODEL,
                messages=[
                    {"role": "system", "content": "당신은 금융 뉴스 분석 전문가입니다."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                response_format={"type": "json_object"}
            )

            result = json.loads(response.choices[0].message.content)
            return result

        except Exception as e:
            logger.error(f"LLM 분석 실패: {e}")
            return {}
        

    def save_report(self, section_report: SectionReport):
        """리포트를 DB에 저장"""
        try:
            with get_postgres_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                    INSERT INTO section_reports
                    (section, report_start_date, report_end_date, keywords, main_trends, key_news, summary, news_urls, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (section, report_start_date, report_end_date)
                    DO UPDATE SET
                        keywords = EXCLUDED.keywords,
                        main_trends = EXCLUDED.main_trends,
                        key_news = EXCLUDED.key_news,
                        summary = EXCLUDED.summary,
                        news_urls = EXCLUDED.news_urls,
                        updated_at = NOW()
                    """, (
                        section_report.section,
                        section_report.report_start_date,
                        section_report.report_end_date,
                        json.dumps(section_report.keywords, ensure_ascii=False),
                        section_report.main_trends,
                        json.dumps(section_report.key_news, ensure_ascii=False),
                        section_report.summary,
                        section_report.news_urls,
                        section_report.created_at
                    ))
                    conn.commit()
                    logger.info(f"DB 저장 완료: 섹션 리포트 ({section_report.section})")
        except Exception as e:
            logger.error(f"DB 저장 실패: {e}", exc_info=True)
            raise

    def fetch_tagged_news(self, news_urls: List[str]) -> List[Dict]:
        """tagged_news 테이블에서 뉴스 원문 조회"""
        if not news_urls:
            return []

        query = """
                SELECT
                    id,
                    news_id,
                    url,
                    title,
                    content,
                    published_at,
                    publisher,
                    ticker,
                    stock_name,
                    companies,
                    persons,
                    locations,
                    organizations,
                    event_type,
                    impact,
                    reason,
                    summary,
                    created_at,
                    updated_at
                FROM tagged_news
                WHERE url = ANY(%s)
            """

        try:
            with get_postgres_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(query, (news_urls,))
                    columns = [desc[0] for desc in cur.description]
                    rows = cur.fetchall()
                    return [dict(zip(columns, row)) for row in rows]
        except Exception as e:
            logger.error(f"tagged_news 조회 실패: {e}")
            return []

    def process_message(self, message_dict: Dict):
        """메시지 처리 및 리포트 생성"""
        try:
            # Input: StockMovementMessage 검증
            movement = HeadlineAnalysisMessage(**message_dict)
        except ValidationError as e:
            logger.error(f"메시지 검증 실패: {e}")
            return

        tagged_news_list = self.fetch_tagged_news(movement.news_urls)

        logger.info(f"리포트 생성 시작: {movement.section}")
        # 분석 수행
        analysis_result = self.analyze_stock_movement(tagged_news_list, movement)
        start_date = movement.news_start_date
        end_date = movement.news_end_date
        if isinstance(start_date, str):
            start_date = datetime.fromisoformat(start_date).date()
        elif isinstance(start_date, datetime):
            start_date = start_date.date()
        if isinstance(end_date, str):
            end_date = datetime.fromisoformat(end_date).date()
        elif isinstance(end_date, datetime):
            end_date = end_date.date()
        section_report = SectionReport(
            section=movement.section,
            report_start_date=start_date,
            report_end_date=end_date,
            keywords=analysis_result.get("keywords", []),
            main_trends=analysis_result.get("macro_trends", []),
            key_news=analysis_result.get("key_news", []),
            summary=analysis_result.get("summary", ""),
            news_urls=movement.news_urls,
            created_at=datetime.now()
        )
        self.save_report(section_report)
        logger.info(f"리포트 저장 완료: {movement.section})")

    def start(self):
        """워커 시작"""
        logger.info("=" * 50)
        logger.info("리포팅 워커 시작")
        logger.info(f"Input Topic: {self.config.INPUT_TOPIC}")
        logger.info(f"Consumer Group: {self.config.CONSUMER_GROUP}")
        logger.info("=" * 50)

        try:
            for message in self.consumer:
                logger.info(f"[Kafka] 메시지 수신: partition={message.partition}, offset={message.offset}")

                try:
                    self.process_message(message.value)
                    self.consumer.commit()
                    logger.info("[Kafka] 커밋 완료")

                except Exception as e:
                    logger.error(f"처리 실패: {e}", exc_info=True)
                    self.consumer.commit()

        except KeyboardInterrupt:
            logger.info("워커 종료")
        finally:
            self.close()

    def close(self):
        """리소스 정리"""
        self.consumer.close()
        logger.info("워커 종료 완료")


if __name__ == "__main__":
    worker = ReportingWorker()
    worker.start()