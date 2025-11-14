# File: app/main.py
import os
import json
import aio_pika
from aio_pika import DeliveryMode
from fastapi import FastAPI
from pydantic import BaseModel
from typing import Optional
from app.database import init_db
from collections import defaultdict
from datetime import datetime, timedelta
from app.database import get_db_connection


RABBIT_URL = os.getenv("RABBIT_URL", "amqp://guest:guest@localhost/")

class QueryRequest(BaseModel):
    query: str
    ticker: Optional[str] = None
    pages: Optional[int] = 1
    delay: Optional[float] = 1.0
    crawl_concurrency: Optional[int] = 8
    out: Optional[str] = None
    date: Optional[str] = None
    start_date: Optional[str] = "2000.01.01"
    end_date: Optional[str] = "2099.12.31"
    stock_name: Optional[str] = None
    change_rate: Optional[float] = None
    trend_type: Optional[str] = None  # "plunge" or "surge"

app = FastAPI()

def build_stock_relationship_graph(file_path: str = "data/asiae_notable_labeled.jsonl"):
    """주식 관계 그래프 생성"""

    # 기업 그룹 및 관계 정의
    relationships = {
        # 삼성 그룹
        "삼성그룹": ["삼성전자", "삼성SDI", "삼성물산", "삼성바이오로직스", "삼성중공업"],

        # SK 그룹
        "SK그룹": ["SK하이닉스", "SK이노베이션", "SK바이오사이언스", "SK케미칼"],

        # 현대차 그룹
        "현대차그룹": ["현대차", "기아", "현대글로비스", "현대오토에버"],

        # HD현대 그룹
        "HD현대그룹": ["HD현대", "HD현대중공업", "HD한국조선해양", "HD현대미포",
                   "HD현대일렉트릭", "HD현대에너지솔루션"],

        # LG 그룹
        "LG그룹": ["LG화학", "LG에너지솔루션", "LG이노텍"],

        # HLB 그룹
        "HLB그룹": ["HLB", "HLB생명과학", "HLB제약", "HLB바이오스텝",
                  "HLB테라퓨틱스", "HLB파나진", "HLB이노베이션"],

        # 한화 그룹
        "한화그룹": ["한화오션"],

        # 효성 그룹
        "효성그룹": ["효성중공업", "HS효성첨단소재"],

        # 엔터테인먼트
        "엔터테인먼트": ["JYP Ent.", "에스엠", "하이브", "와이지엔터테인먼트", "SAMG엔터"],

        # 조선업
        "조선산업": ["HD현대중공업", "HD한국조선해양", "삼성중공업", "HD현대미포",
                 "한화오션", "HJ중공업", "동일스틸럭스"],

        # 로봇산업
        "로봇산업": ["두산로보틱스", "레인보우로보틱스", "유일로보틱스", "하이젠알앤엠",
                 "로보티즈", "로보스타", "클로봇", "로보로보", "휴림로봇", "엔젤로보틱스"],

        # 증권업
        "증권업": ["키움증권", "미래에셋증권", "한국금융지주", "한양증권"],

        # 반도체 소재/장비
        "반도체산업": ["삼성전자", "SK하이닉스", "알파칩스", "메가터치", "한성크린텍"],

        # 배터리 산업
        "배터리산업": ["삼성SDI", "LG에너지솔루션", "SK이노베이션", "엘앤에프",
                  "HS효성첨단소재", "케이이엠텍"],

        # 원전/에너지
        "원전에너지": ["두산에너빌리티", "현대건설", "강원에너지"],

        # 남북경협
        "남북경협": ["코데즈컴바인", "양지사", "좋은사람들", "제이에스티나",
                 "아난티", "인디에프", "신원", "형지엘리트"],

        # AI/소프트웨어
        "AI산업": ["카카오", "마음AI", "비아이매트릭스", "신테카바이오", "플리토"],
    }

    # 역방향 매핑 생성 (기업명 -> 속한 그룹들)
    stock_to_groups = defaultdict(list)
    for group, stocks in relationships.items():
        for stock in stocks:
            stock_to_groups[stock].append(group)

    # 관계 그래프 생성
    graph = {
        "groups": relationships,
        "stock_relationships": {},
        "temporal_comovement": []
    }

    # JSONL에서 실제 등장한 종목 수집
    stock_events = defaultdict(list)

    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            data = json.loads(line)
            stock_names = data.get("stock_names", [])
            published_at = data.get("published_at_kst", "")
            url = data.get("url", "")

            for stock in stock_names:
                stock_events[stock].append({
                    "date": published_at,
                    "url": url,
                    "groups": stock_to_groups.get(stock, [])
                })

    # 각 종목의 관련 기업 매핑
    for stock, events in stock_events.items():
        related_stocks = set()

        # 같은 그룹에 속한 기업들 찾기
        for group in stock_to_groups.get(stock, []):
            related_stocks.update(relationships[group])

        # 자기 자신 제외
        related_stocks.discard(stock)

        graph["stock_relationships"][stock] = {
            "related_companies": list(related_stocks),
            "groups": stock_to_groups.get(stock, []),
            "event_count": len(events)
        }

    return graph


@app.post("/generate_relationship_graph")
async def generate_relationship_graph(
        file_path: str = "data/asiae_notable_labeled.jsonl",
        output_path: str = "data/stock_relationships.json"
):
    """주식 관계 그래프를 생성하고 파일로 저장"""

    graph = build_stock_relationship_graph(file_path)

    # JSON 파일로 저장
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(graph, f, ensure_ascii=False, indent=2)

    return {
        "status": "completed",
        "output_file": output_path,
        "total_stocks": len(graph["stock_relationships"]),
        "total_groups": len(graph["groups"])
    }


@app.post("/publish_with_relationships")
async def publish_with_relationships(
        file_path: str = "data/asiae_notable_labeled.jsonl",
        include_related: bool = True
):
    """관계사를 포함하여 크롤링 태스크 생성"""

    published_count = 0
    graph = build_stock_relationship_graph(file_path)

    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            data = json.loads(line)
            stock_names = data.get("stock_names", [])
            published_at = data.get("published_at_kst", "")

            if not stock_names or not published_at:
                continue

            dt = datetime.fromisoformat(published_at.replace('+09:00', ''))
            start_date = (dt - timedelta(days=1)).strftime("%Y.%m.%d")
            end_date = (dt - timedelta(days=1)).strftime("%Y.%m.%d")

            # 검색 대상 종목 리스트
            search_stocks = set(stock_names)

            # 관계사 추가
            if include_related:
                for stock in stock_names:
                    if stock in graph["stock_relationships"]:
                        related = graph["stock_relationships"][stock]["related_companies"]
                        search_stocks.update(related)

            # 각 종목별로 태스크 생성
            for stock_name in search_stocks:
                start_date_safe = start_date.replace(".", "_")
                end_date_safe = end_date.replace(".", "_")
                out_filename = f"{stock_name}_{start_date_safe}_{end_date_safe}.jsonl"

                payload = {
                        "query": stock_name,
                        "pages": 2,
                        "delay": 1.0,
                        "crawl_concurrency": 8,
                        "start_date": start_date,
                        "end_date": end_date,
                        "out": out_filename
                }

                message = aio_pika.Message(
                    body=json.dumps(payload, ensure_ascii=False).encode(),
                    delivery_mode=DeliveryMode.PERSISTENT,
                )
                await app.state.channel.default_exchange.publish(
                    message, routing_key="crawl_tasks"
                )
                published_count += 1

    return {
        "status": "completed",
        "tasks_published": published_count,
        "relationship_graph_used": include_related
    }

def parse_date(date_str: str) -> str:
    """ISO 날짜를 YYYY.MM.DD 형식으로 변환"""
    dt = datetime.fromisoformat(date_str.replace('+09:00', ''))
    return dt.strftime("%Y.%m.%d")


@app.post("/publish_from_jsonl")
async def publish_from_jsonl(file_path: str = "data/asiae_notable_labeled.jsonl"):
    """JSONL 파일에서 종목명과 날짜를 추출하여 크롤링 태스크 생성"""
    published_count = 0

    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            data = json.loads(line)
            stock_names = data.get("stock_names", [])
            published_at = data.get("published_at_kst", "")

            if not stock_names or not published_at:
                continue

            # 날짜 파싱 (발행일 기준 ±1일)
            target_date = parse_date(published_at)
            dt = datetime.fromisoformat(published_at.replace('+09:00', ''))
            start_date = (dt - timedelta(days=1)).strftime("%Y.%m.%d")
            end_date = (dt - timedelta(days=1)).strftime("%Y.%m.%d")

            # 각 종목별로 태스크 생성
            for stock_name in stock_names:
                payload = {
                    "query": stock_name,
                    "pages": 5,
                    "delay": 1.0,
                    "crawl_concurrency": 8,
                    "start_date": start_date,
                    "end_date": end_date
                }

                message = aio_pika.Message(
                    body=json.dumps(payload, ensure_ascii=False).encode(),
                    delivery_mode=DeliveryMode.PERSISTENT,
                )
                await app.state.channel.default_exchange.publish(
                    message, routing_key="crawl_tasks"
                )
                published_count += 1

    return {
        "status": "completed",
        "tasks_published": published_count
    }

@app.on_event("startup")
async def startup():
    await init_db()
    app.state.rabbit_conn = await aio_pika.connect_robust(RABBIT_URL)
    app.state.channel = await app.state.rabbit_conn.channel()
    await app.state.channel.declare_queue("crawl_tasks", durable=True)

@app.on_event("shutdown")
async def shutdown():
    await app.state.channel.close()
    await app.state.rabbit_conn.close()

@app.post("/publish")
async def publish_task(req: QueryRequest):
    if req.ticker and req.change_rate is not None and req.trend_type:
        try:
            conn = await get_db_connection()

            # 날짜 문자열을 date 객체로 변환
            date_obj = datetime.strptime(req.end_date, "%Y.%m.%d").date()

            await conn.execute("""
                            INSERT INTO stock_prices
                            (ticker, stock_name, date, change_rate, trend_type)
                            VALUES ($1, $2, $3, $4, $5)
                            ON CONFLICT (ticker, date)
                            DO UPDATE SET
                                stock_name = EXCLUDED.stock_name,
                                change_rate = EXCLUDED.change_rate,
                                trend_type = EXCLUDED.trend_type,
                                updated_at = NOW()
                        """,
                               req.ticker,
                               req.stock_name or req.query,
                               date_obj,  # date 객체 전달
                               req.change_rate,
                               req.trend_type
                               )

            await conn.close()

        except Exception as e:
            print(f"주가 데이터 저장 실패: {e}")

    payload = req.dict()
    message = aio_pika.Message(
        body=json.dumps(payload, ensure_ascii=False).encode(),
        delivery_mode=DeliveryMode.PERSISTENT,
    )
    await app.state.channel.default_exchange.publish(message, routing_key="crawl_tasks")
    return {"status": "published", "query": req.query}

@app.get("/health")
async def health():
    return {"status": "ok"}