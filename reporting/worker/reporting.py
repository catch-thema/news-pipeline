import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List
from kafka import KafkaConsumer
from pydantic import ValidationError

from shared.config import ReportingWorkerConfig
from shared.db_manager import DBManager
from shared.models.messages import StockMovementMessage
from shared.services.rag_service import RAGService
from shared.services.bm25_service import BM25Service
from shared.services.hybrid_search_service import HybridSearchService

from reporting.services.cause_analyzer import analyze_causes
from reporting.services.related_stocks_analyzer import analyze_related_stocks
from reporting.models.report import StockReport
from reporting.services.market_context_analyzer import analyze_market_context


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ReportingWorker:
    """주식 리포트 생성 및 저장 워커"""

    def __init__(self):
        self.config = ReportingWorkerConfig()
        self.db_manager = DBManager(
            host=self.config.DB_HOST,
            port=self.config.DB_PORT,
            database=self.config.DB_NAME,
            user=self.config.DB_USER,
            password=self.config.DB_PASSWORD
        )

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

        # 분석 서비스 초기화
        self.rag_service = RAGService()
        self.bm25_service = BM25Service()
        self.hybrid_search = HybridSearchService(self.rag_service, self.bm25_service)

        # BM25 인덱스 구축
        logger.info("BM25 인덱스 구축 중...")
        all_chunks = self.rag_service.get_all_chunks()
        if all_chunks:
            self.bm25_service.build_index(all_chunks)
            logger.info(f"BM25 인덱스 구축 완료: {len(all_chunks)} chunks")

    def analyze_stock_movement(self, movement: StockMovementMessage) -> Dict:
        """주식 변동 분석"""
        try:
            # 1. 메시지의 뉴스 수집 기간 사용
            news_start_str = movement.news_start_date.replace('.', '-')
            news_end_str = movement.news_end_date.replace('.', '-')

            # timezone 없이 날짜만 파싱 (DB와 일치)
            start_date = datetime.fromisoformat(f"{news_start_str}T00:00:00")
            end_date = datetime.fromisoformat(f"{news_end_str}T23:59:59")

            logger.info(f"검색 기간: {start_date} ~ {end_date}")

        except Exception as e:
            logger.error(f"날짜 파싱 오류: {e}")
            return None

        magnitude = abs(movement.change_rate)

        if movement.trend_type == "plunge":
            movement_text = "급락"
            keywords = ["하락", "폭락", "하락세", "급락", "부진", "악재"]
        else:
            movement_text = "급등"
            keywords = ["상승", "급등", "상승세", "호재", "성장", "강세"]

        try:
            analysis_date = datetime.strptime(movement.date, "%Y.%m.%d")
        except ValueError:
            analysis_date = datetime.strptime(movement.date, "%Y-%m-%d")

        # 1. 명시적 원인 검색
        explicit_chunks = []
        logger.info(f"티커 검색: {movement.ticker}, 종목명: {movement.stock_name}")

        base_query = f"{movement.stock_name} {movement_text} 원인 이유 배경"
        explicit_chunks.extend(self.hybrid_search.search(
            query=base_query,
            ticker=movement.ticker,
            stock_name=movement.stock_name,
            start_date=start_date,
            end_date=end_date,
            k=10
        ))

        # 검색 결과 로그
        logger.info(f"Base query 결과: {len(explicit_chunks)} chunks")

        # 변동률 강조 (5% 이상일 때)
        if magnitude >= 5.0:
            magnitude_query = f"{movement.stock_name} {magnitude:.1f}% {movement_text}"
            explicit_chunks.extend(self.hybrid_search.search(
                query=magnitude_query,
                ticker=movement.ticker,
                stock_name=movement.stock_name,
                start_date=start_date,
                end_date=end_date,
                k=10
            ))

        for keyword in keywords[:3]:
            keyword_query = f"{movement.stock_name} {keyword}"
            explicit_chunks.extend(self.hybrid_search.search(
                query=keyword_query,
                ticker=movement.ticker,
                stock_name=movement.stock_name,
                start_date=start_date,
                end_date=end_date,
                k=5
            ))

        explicit_chunks = list({chunk['id']: chunk for chunk in explicit_chunks}.values())[:30]

        logger.info(f"Explicit chunks 총: {len(explicit_chunks)}")

        # 2. 이벤트 검색
        event_queries = [
            movement.stock_name,
            f"{movement.stock_name} 실적",
            f"{movement.stock_name} 발표",
            f"{movement.stock_name} 공시"
        ]

        event_chunks = []
        for query in event_queries:
            event_chunks.extend(self.hybrid_search.search(
                query=query,
                ticker=movement.ticker,
                stock_name=movement.stock_name,
                start_date=start_date,
                end_date=end_date,
                k=8
            ))

        event_chunks = list({chunk['id']: chunk for chunk in event_chunks}.values())[:30]

        logger.info(f"Event chunks 총: {len(event_chunks)}")

        if not explicit_chunks and not event_chunks:
            logger.warning(f"관련 뉴스 없음: {movement.stock_name} ({movement.date})")

            # 디버깅: 날짜 필터 없이 검색
            debug_chunks = self.hybrid_search.search(
                query=movement.stock_name,
                ticker=movement.ticker,
                stock_name=movement.stock_name,
                k=5
            )
            logger.info(f"날짜 필터 제거 시 결과: {len(debug_chunks)} chunks")
            if debug_chunks:
                logger.info(f"샘플 metadata: {debug_chunks[0].get('metadata', {})}")

            return {
                "causes": [],
                "summary": "관련 뉴스를 찾을 수 없습니다.",
                "total_confidence": "Low",
                "explicit_chunks_found": 0,
                "event_chunks_found": 0
            }

        # 3. 원인 분석
        causes_result = analyze_causes(
            explicit_cause_chunks=explicit_chunks,
            event_chunks=event_chunks,
            ticker=movement.ticker,
            date=analysis_date,
            movement_type="down" if movement.trend_type == "plunge" else "up",
            change_rate=movement.change_rate
        )

        # 4. 각 원인에 뉴스 참조 추가
        for cause in causes_result.get("causes", []):
            news_refs, news_dates = self._extract_news_references(
                explicit_chunks + event_chunks,
                cause.get("evidence", [])
            )
            cause["news_references"] = news_refs
            cause["news_dates"] = news_dates

        # 5. 연관 종목 분석
        # related_stocks_result = analyze_related_stocks(
        #     stock_name=movement.stock_name,
        #     movement_type="down" if movement.trend_type == "plunge" else "up",
        #     causes=causes_result.get("causes", [])
        # )

        # 6. 시장 맥락 분석
        market_context = analyze_market_context(
            ticker=movement.ticker,
            stock_name=movement.stock_name,
            causes=causes_result.get("causes", [])
        )

        return {
            **causes_result,
            # "related_stocks": related_stocks_result.get("related_stocks", []),
            "market_context": market_context,
            "explicit_chunks_found": len(explicit_chunks),
            "event_chunks_found": len(event_chunks)
        }

    def _extract_news_references(self, chunks: List[Dict], evidence: List[str]) -> tuple[List[Dict], List[str]]:
        """증거에 사용된 청크에서 뉴스 참조 및 날짜 추출"""
        references = []
        news_dates = set()
        seen_urls = set()

        for chunk in chunks:
            chunk_text = chunk.get('content') or chunk.get('text', '')

            # evidence는 문자열 리스트
            if any(ev and ev in chunk_text for ev in evidence):
                metadata = chunk.get('metadata', {})
                url = metadata.get('url')
                published_at = metadata.get('published_at', '')

                # 날짜 수집
                if published_at:
                    try:
                        # ISO 형식으로 변환 (YYYY-MM-DD)
                        date_obj = datetime.fromisoformat(published_at.replace('Z', '+00:00'))
                        news_dates.add(date_obj.strftime('%Y-%m-%d'))
                    except:
                        pass

                if url and url not in seen_urls:
                    seen_urls.add(url)
                    references.append({
                        "title": metadata.get('title', '제목 없음'),
                        "url": url,
                        "published_at": published_at,
                        "publisher": metadata.get('publisher', ''),
                        "relevance_score": chunk.get('score', 0.0)
                    })

        sorted_refs = sorted(references, key=lambda x: x['relevance_score'], reverse=True)[:5]
        return sorted_refs, sorted(list(news_dates))


    def save_report(self, report: StockReport):
        """리포트를 DB에 저장"""
        try:
            with self.db_manager.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                    INSERT INTO stock_reports
                    (ticker, stock_name, analysis_date, movement_type, change_rate, change_magnitude,
                     causes, summary, total_confidence, related_stocks, market_context,
                     triggered_at, metadata, analysis_quality, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (ticker, analysis_date, movement_type)
                    DO UPDATE SET
                        change_rate = EXCLUDED.change_rate,
                        change_magnitude = EXCLUDED.change_magnitude,
                        causes = EXCLUDED.causes,
                        summary = EXCLUDED.summary,
                        total_confidence = EXCLUDED.total_confidence,
                        related_stocks = EXCLUDED.related_stocks,
                        market_context = EXCLUDED.market_context,
                        triggered_at = EXCLUDED.triggered_at,
                        metadata = EXCLUDED.metadata,
                        analysis_quality = EXCLUDED.analysis_quality,
                        updated_at = NOW()
                """, (
                        report.ticker,
                        report.stock_name,
                        report.analysis_date,
                        report.movement_type,
                        report.change_rate,
                        report.change_magnitude,
                        json.dumps([c.dict() if hasattr(c, 'dict') else c for c in report.causes],
                                   ensure_ascii=False),
                        report.summary,
                        report.total_confidence,
                        json.dumps([s.dict() if hasattr(s, 'dict') else s for s in report.related_stocks],
                                   ensure_ascii=False),
                        json.dumps(report.market_context.dict() if report.market_context else None,
                                   ensure_ascii=False),
                        report.triggered_at,
                        json.dumps(report.metadata, ensure_ascii=False),
                        json.dumps(report.analysis_quality, ensure_ascii=False),
                        report.created_at
                    ))
                    conn.commit()
                    logger.info(f"DB 저장 완료: {report.ticker} - {report.analysis_date}")
        except Exception as e:
            logger.error(f"DB 저장 실패 ({report.ticker}): {e}", exc_info=True)
            raise

    def get_stock_price_data(self, ticker: str, date: str) -> Dict:
        """주가 변동 데이터 조회"""
        try:
            with self.db_manager.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT stock_name, change_rate, trend_type
                        FROM stock_prices
                        WHERE ticker = %s AND date = %s
                    """, (ticker, date))

                    result = cursor.fetchone()
                    if result:
                        stock_name, change_rate, trend_type = result
                        logger.info(
                            f"주가 데이터 조회: {stock_name} ({ticker}) - "
                            f"{date} {trend_type} ({change_rate:.2f}%)"
                        )
                        return {
                            'stock_name': stock_name,
                            'change_rate': change_rate,
                            'trend_type': trend_type
                        }

            # 데이터 없을 경우 기본값
            logger.warning(f"주가 데이터 없음: {ticker} ({date})")
            return {
                'stock_name': ticker,
                'change_rate': 0.0,
                'trend_type': 'neutral'
            }

        except Exception as e:
            logger.error(f"주가 데이터 조회 실패: {e}")
            return {
                'stock_name': ticker,
                'change_rate': 0.0,
                'trend_type': 'neutral'
            }

    def process_message(self, message_dict: Dict):
        """메시지 처리 및 리포트 생성"""
        try:
            # Input: StockMovementMessage 검증
            movement = StockMovementMessage(**message_dict)
        except ValidationError as e:
            logger.error(f"메시지 검증 실패: {e}")
            return

        logger.info(f"리포트 생성 시작: {movement.stock_name} ({movement.ticker}) - "
                    f"{movement.date}")
        # 분석 수행
        analysis_result = self.analyze_stock_movement(movement)

        if not analysis_result:
            logger.warning(f"분석 결과 없음: {movement.stock_name}")
            return

        movement_type = "down" if movement.trend_type == "plunge" else "up"

        # 변동 크기 텍스트
        magnitude = abs(movement.change_rate)
        if magnitude >= 10:
            change_magnitude = "급등" if movement_type == "up" else "급락"
        elif magnitude >= 5:
            change_magnitude = "강한 상승" if movement_type == "up" else "강한 하락"
        else:
            change_magnitude = "상승" if movement_type == "up" else "하락"

        try:
            analysis_date = datetime.strptime(movement.date, "%Y.%m.%d")
        except ValueError:
            analysis_date = datetime.strptime(movement.date, "%Y-%m-%d")

        # 신뢰도 분포 계산
        confidence_distribution = {}
        for cause in analysis_result.get("causes", []):
            conf = cause.get("confidence", "Low")
            confidence_distribution[conf] = confidence_distribution.get(conf, 0) + 1

        # 리포트 생성
        report = StockReport(
            ticker=movement.ticker,
            stock_name=movement.stock_name,
            analysis_date=analysis_date,
            movement_type=movement_type,
            change_rate=movement.change_rate,
            change_magnitude=change_magnitude,
            causes=analysis_result.get("causes", []),
            summary=analysis_result.get("summary", ""),
            total_confidence=analysis_result.get("total_confidence", "Low"),
            related_stocks=analysis_result.get("related_stocks", []),
            market_context=analysis_result.get("market_context"),
            created_at=datetime.now(),
            triggered_at=datetime.fromisoformat(movement.triggered_at),
            metadata={
                "trend_type": movement.trend_type,
                "original_message": movement.dict()
            },
            analysis_quality={
                "total_news_count": analysis_result.get("explicit_chunks_found", 0) +
                                    analysis_result.get("event_chunks_found", 0),
                "explicit_chunks_found": analysis_result.get("explicit_chunks_found", 0),
                "event_chunks_found": analysis_result.get("event_chunks_found", 0),
                "confidence_distribution": confidence_distribution
            }
        )

        # DB 저장
        self.save_report(report)
        logger.info(f"리포트 저장 완료: {movement.stock_name} ({movement.ticker})")

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
        self.db_manager.close()
        logger.info("워커 종료 완료")


if __name__ == "__main__":
    worker = ReportingWorker()
    worker.start()