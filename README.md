# 뉴스 기반 자동 분석 파이프라인 

본 프로젝트는 급등락 종목의 원인 분석, 뉴스 엔티티 태깅, LLM 기반 의미 분석,   
임베딩 및 RAG 기반 리포트 생성까지 이어지는 **엔드-투-엔드 자동화 파이프라인**입니다.

Kafka · RabbitMQ · PostgreSQL · MySQL 기반 스트리밍 구조로 구성되어 있으며  
FastAPI + Multi-Worker 구조로 고성능 병렬 처리가 가능합니다.

---

## 시스템 개요

이 시스템은 대규모 뉴스 데이터를 지속적으로 수집하고, 의미 분석을 거쳐  
**종목별 시장 이벤트 분석 리포트**를 자동으로 생성합니다.  
각 작업은 독립된 워커(worker)로 구성되어 있으며 Kafka 기반 메시지 파이프라인을 따라 흐릅니다.

---

## 전체 아키텍처

```text
                           ┌───────────────────────┐
                           │   crawling-scheduler  │
                           │    (크롤링 요청 생성)     │
                           └───────────┬───────────┘
                                       │
                                       ▼
                           ┌───────────────────────┐
                           │     RabbitMQ Queue    │
                           │     (크롤링 작업 전달)    │
                           └───────────┬───────────┘
                                       │
                                       ▼
                           ┌───────────────────────┐
                           │    crawling-worker    │
                           │    (뉴스 HTML 수집)     │
                           └───────────┬───────────┘
                                       │
                                       ▼
                               Kafka Topic
                            **crawled_news**
                                       │
                                       ▼
        ┌───────────────────────────────────────────────────────────┐
        │                          tagging-worker                   │
        │                     (NER / 엔티티 추출, 구조화)               │
        └──────────────────────────────┬────────────────────────────┘
                                       │
                                       ▼
                                 Kafka Topic
                               **tagged_news**
                                       │
                                       ▼
        ┌───────────────────────────────────────────────────────────┐
        │                           llm-worker                      │
        │               (LLM 기반 뉴스 이벤트 타입, 요약, 영향 분석)         │
        └──────────────────────────────┬────────────────────────────┘
                                       │
                                       ▼
                                  Kafka Topic
                              **analyzed_news**
                                       │
                                       ▼
        ┌───────────────────────────────────────────────────────────┐
        │                     rag-embedding-worker                  │
        │           (뉴스 벡터 임베딩 생성 → ChromaDB 저장)               │
        └──────────────────────────────┬────────────────────────────┘
                                       │
                                       ▼
                                 Kafka Topic
                            **embedding_complete**
                                       │
                                       ▼
        ┌───────────────────────────────────────────────────────────┐
        │                     aggregation-worker                    │
        │               (임베딩 완료된 뉴스 집계, 종목별 분석 트리거 생성)     │
        └──────────────────────────────┬────────────────────────────┘
                                       │
                                       ▼
                                  Kafka Topic
                               **stock_ready**
                                       │
                                       ▼
        ┌───────────────────────────────────────────────────────────┐
        │                     reporting-worker                      │
        │            (종목 분석 리포트 생성, 뉴스 근거·요약·파급효과 리포트)    │
        └───────────────────────────────────────────────────────────┘

```

---

## 실행방법

```bash
docker compose up -d --build
```

## 주요 기능

### 뉴스 데이터 처리

- 뉴스 HTML 수집 및 원문 정제
- NER 기반 개체명 인식(기업명, 기관명, 산업 키워드 등)
- LLM 기반 의미 해석(이벤트 타입, 원인-결과, 시장 영향 분석)
- 기사 요약 및 근거 추출

### 임베딩 및 RAG 기반 검색

- 뉴스/문서 벡터 임베딩 생성
- pgvector를 활용한 벡터 검색 구조
- 관련 뉴스 재탐색 및 증거 기반 리포트 생성

### 종목 분석 및 리포트 생성

- 하루 단위 종목 이벤트 자동 분석
- 상승·하락 원인 랭킹 분석
- 그래프 기반 영향 전파(Impact Propagation)
- 계열사/관계사 기반 추가적 시장 영향 평가

## 시스템 구성요소

- Scheduler: 크롤링 작업 스케줄링
- Crawler Worker: 뉴스 HTML 수집
- Tagging Worker: 엔티티 추출 및 구조화
- LLM Worker: 이벤트 의미 분석 및 요약
- Embedding Worker: 임베딩 생성 및 DB 저장
- Aggregation Worker: 종목별 분석 트리거 생성
- Reporting Worker: 최종 리포트 생성


## 결과

### 경제 뉴스 분석 결과

2025년 12월 6일의 경제 뉴스 요약입니다.

```json
{
    "id": 1,
    "section": "101",
    "report_start_date": "2025-12-06",
    "report_end_date": "2025-12-06",
    "keywords": [
        {
            "keyword": "부동산",
            "description": "주택 시장의 가격 상승과 정부의 규제 정책에 대한 논의가 활발히 이루어지고 있다."
        },
        {
            "keyword": "금리 인하",
            "description": "미국 연준의 금리 인하 가능성이 높아지며, 이는 글로벌 경제와 환율에 영향을 미칠 것으로 예상된다."
        },
        {
            "keyword": "패닉 바잉",
            "description": "2030세대가 집값 상승에 대한 불안감으로 인해 급하게 주택을 구매하는 현상."
        },
        {
            "keyword": "AI",
            "description": "인공지능 기술의 발전과 관련된 기업들의 주가 상승 및 ETF 투자에 대한 관심이 증가하고 있다."
        }
    ],
    "main_trends": [],
    "key_news": [
        {
            "url": "https://n.news.naver.com/mnews/article/243/0000089197",
            "summary": "현대차의 주가가 11% 급등하며 사상 최고가를 경신했다. 투자업계는 지배구조 개편을 통해 AI 소프트웨어 역량이 강화될 경우 기업 가치가 재평가될 것으로 전망하고 있다.",
            "headline": "현대차 11% 급등·최고가 경신… 증권가 '43만원 간다' [증시이슈]"
        },
        {
            "url": "https://n.news.naver.com/mnews/article/011/0004564212",
            "summary": "한국 청년들이 내 집 마련을 포기하고 주식 투자로 방향을 전환하고 있다. 이는 부동산 시장의 과열과 전세 제도의 붕괴로 인해 상대적으로 진입 장벽이 낮은 금융 시장으로 이동한 결과이다.",
            "headline": "韓 청년들, 영끌해서 '집' 말고 '주식' 사더라… 외신이 본 '한국 청년의 현실'"
        }
    ],
    "summary": "현재 경제 시장은 부동산 가격 상승과 관련된 정부 규제의 효과가 미미하며, 2030세대의 패닉 바잉 현상이 나타나고 있다. 미국의 금리 인하 가능성이 높아지고 있으며, 이는 한국 주식 시장에 긍정적인 영향을 미치고 있다. AI 기술과 관련된 기업들의 주가 상승이 두드러지며, 소비자 물가 상승과 고환율 상황이 소비자들에게 부담을 주고 있다. 쿠팡과 같은 대기업의 사회적 책임 문제와 관련된 논란이 기업 이미지에 부정적인 영향을 미치고 있다.",
    "news_urls": [
    ],
    "created_at": "2025-12-06T05:08:01.689867Z",
    "updated_at": "2025-12-06T05:08:01.723412Z"
}
```

### 뉴스 리포트 생성 결과

삼성전자(005930) 분석 예시입니다.


- 주요 상승/하락 원인(랭킹 기반)
- 뉴스 근거(evidence)
- 신뢰도(Confidence)

관계 그래프 기반 영향 전파(Impact Propagation)

요약 Summary

계열사 및 관계사 Neighbors 추출

아래는 실제 생성된 

```json
{
  "id": 3,
  "ticker": "005930",
  "stock_name": "삼성전자",
  "analysis_date": "2025-12-05",
  "movement_type": "up",
  "change_rate": "3.14",
  "causes": [
    {
      "rank": 1,
      "title": "SSD 시장 점유율 확대",
      "evidence": [
        "3분기 기업용 SSD 시장 점유율 35.1%",
        "매출 28.6% 증가하며 SK하이닉스와 격차 확대"
      ],
      "confidence": "High"
    },
    {
      "rank": 2,
      "title": "HBM4 공급 계약",
      "evidence": [
        "구글 차세대 AI 칩용 HBM4 공급 계약 체결",
        "주가에 긍정적 영향"
      ],
      "confidence": "High"
    }
  ],
  "summary": "2025년 12월 5일 삼성전자는 3.14% 상승했다. 주요 원인은 SSD 시장 점유율 확대와 HBM4 공급 계약 체결이다...",
  "impact_propagation": {
    "impacted_stocks": [
      { "stock": "삼성전기", "impact": "상승", "confidence": "High" },
      { "stock": "삼성바이오로직스", "impact": "상승", "confidence": "High" },
      { "stock": "현대건설", "impact": "상승", "confidence": "High" }
    ]
  },
  "graph_neighbors": [
    { "name": "Samsung Electronics America, Inc.", "distance": 1 },
    { "name": "삼성전기㈜", "stock_code": "009150", "distance": 1 }
  ]
}
```

# 메시지 구조 (Message Schemas)

아래는 파이프라인에서 사용되는 Kafka / RabbitMQ 메시지들의  
**실제 JSON 구조를 이해하기 쉬운 형태로 표현한 Schema 예시**입니다.

---

## 1. 크롤링 요청 메시지 (crawl-task)

```json
{
  "query": "삼성전자",
  "pages": 3,
  "delay": 0.3,
  "start_date": "2025-12-01",
  "end_date": "2025-12-05",
  "ticker": "005930",
  "stock_name": "삼성전자",
  "change_rate": 3.14,
  "trend_type": "surge",
  "date": "2025-12-05"
}
```

## 2. 크롤링 결과 메시지 (crawled_news)

```json
{
  "url": "https://news.example.com/123",
  "title": "삼성전자, SSD 점유율 확대",
  "content": "삼성전자가 3분기 기업용 SSD 시장에서...",
  "published_at": "2025-12-05 17:32:00",
  "publisher": "이데일리",
  "ticker": "005930",
  "stock_name": "삼성전자",
  "change_rate": 3.14,
  "trend_type": "surge",
  "section": "101"
}
```

## 3.태깅 완료 메시지 (tagged_news)

```json
{
  "url": "https://news.example.com/123",
  "title": "삼성전자, SSD 점유율 확대",
  "content": "삼성전자가 3분기 기업용 SSD 시장에서...",
  "entities": {
    "companies": ["삼성전자", "SK하이닉스"],
    "persons": [],
    "locations": ["한국"],
    "organizations": ["삼성", "구글"]
  }
}
```
## 4. LLM 분석 메시지 (analyzed_news)

```json
{
  "url": "https://news.example.com/123",
  "title": "삼성전자, SSD 점유율 확대",
  "content": "삼성전자가 3분기 기업용 SSD 시장에서...",
  "entities": {
    "companies": ["삼성전자", "SK하이닉스"],
    "persons": [],
    "locations": ["한국"],
    "organizations": ["삼성", "구글"]
  },
  "llm_analysis": {
    "event_type": "사업 성과",
    "impact": "positive",
    "reason": "SSD 시장 점유율 확대는 매출과 수익 개선으로 이어질 수 있음",
    "summary": "삼성전자가 SSD 시장에서 높은 점유율을 확보하면서 실적 성장 기대가 증가했다."
  }
}
```

## 5. 임베딩 완료 메시지 (embedding_complete)

```json
{
  "news_url": "https://news.example.com/123",
  "ticker": "005930",
  "change_rate": 3.14,
  "embedded_at": "2025-12-05T18:03:22Z",
  "chunk_count": 12
}
```

## 6. 종목 분석 준비 완료 메시지 (stock_ready)

```json
{
  "ticker": "005930",
  "stock_name": "삼성전자",
  "change_rate": 3.14,
  "news_count": 18,
  "analyzed_count": 15
}
```
## 7. 최종 리포트 메시지 (report_generated)

```json
{
  "ticker": "005930",
  "stock_name": "삼성전자",
  "report_date": "2025-12-05",
  "movement_type": "up",
  "change_rate": 3.14,
  "summary": "삼성전자는 SSD 시장 점유율 확대와 HBM4 공급 계약으로 상승했다.",
  "causes": [
    {
      "rank": 1,
      "title": "SSD 시장 점유율 확대",
      "evidence": [
        "기업용 SSD 시장 점유율 35.1%",
        "매출 28.6% 증가"
      ],
      "confidence": "High"
    },
    {
      "rank": 2,
      "title": "HBM4 공급 계약",
      "evidence": [
        "구글과 HBM4 공급 계약 체결"
      ],
      "confidence": "High"
    }
  ],
  "impact_propagation": {
    "impacted_stocks": [
      { "stock": "삼성전기", "impact": "상승", "confidence": "High" },
      { "stock": "삼성바이오로직스", "impact": "상승", "confidence": "High" }
    ]
  }
}
```

