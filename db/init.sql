-- ===================================
-- PostgreSQL 확장 설치
-- ===================================

CREATE EXTENSION IF NOT EXISTS vector;

-- ===================================
-- 뉴스 크롤링 테이블
-- ===================================

CREATE TABLE IF NOT EXISTS news (
    id SERIAL PRIMARY KEY,
    url TEXT UNIQUE NOT NULL,
    title TEXT NOT NULL,
    content TEXT,
    ticker VARCHAR(20),
    stock_name VARCHAR(100) NOT NULL,
    published_at TIMESTAMP WITH TIME ZONE,
    publisher VARCHAR(100),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_news_ticker ON news(ticker);
CREATE INDEX IF NOT EXISTS idx_news_stock_name ON news(stock_name);
CREATE INDEX IF NOT EXISTS idx_news_published_at ON news(published_at DESC);
CREATE INDEX IF NOT EXISTS idx_news_url ON news(url);

-- ===================================
-- 주가 데이터 테이블
-- ===================================

CREATE TABLE stock_prices (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(20) NOT NULL,
    stock_name VARCHAR(100) NOT NULL,
    date DATE NOT NULL,
    change_rate DECIMAL(5, 2) NOT NULL,
    trend_type VARCHAR(20) NOT NULL CHECK (trend_type IN ('plunge', 'surge', 'neutral')),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(ticker, date)
);

-- 인덱스 생성
CREATE INDEX idx_stock_prices_ticker_date ON stock_prices(ticker, date DESC);
CREATE INDEX idx_stock_prices_date ON stock_prices(date DESC);
CREATE INDEX idx_stock_prices_trend_type ON stock_prices(trend_type);
CREATE INDEX idx_stock_prices_ticker_trend ON stock_prices(ticker, trend_type, date DESC);


-- ===================================
-- NER 태깅 결과 테이블
-- ===================================

CREATE TABLE IF NOT EXISTS tagged_news (
    id SERIAL PRIMARY KEY,
    news_id VARCHAR(500) UNIQUE NOT NULL,
    url TEXT,
    title TEXT,
    content TEXT,
    published_at TIMESTAMP WITH TIME ZONE,
    publisher VARCHAR(100),
    ticker VARCHAR(20),
    stock_name VARCHAR(100) NOT NULL,
    companies TEXT[],
    persons TEXT[],
    locations TEXT[],
    organizations TEXT[],
    event_type VARCHAR(100),
    impact VARCHAR(50),
    reason TEXT,
    summary TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tagged_news_news_id ON tagged_news(news_id);
CREATE INDEX IF NOT EXISTS idx_tagged_news_ticker ON tagged_news(ticker);
CREATE INDEX IF NOT EXISTS idx_tagged_news_stock_name ON tagged_news(stock_name);
CREATE INDEX IF NOT EXISTS idx_tagged_news_published_at ON tagged_news(published_at DESC);
CREATE INDEX IF NOT EXISTS idx_tagged_news_event_type ON tagged_news(event_type);
CREATE INDEX IF NOT EXISTS idx_tagged_news_impact ON tagged_news(impact);

-- ===================================
-- RAG 임베딩 테이블 (뉴스 청크)
-- ===================================

CREATE TABLE IF NOT EXISTS news_chunks (
    id SERIAL PRIMARY KEY,
    chunk_text TEXT NOT NULL,
    chunk_type VARCHAR(50) DEFAULT 'original',  -- 'original' | 'analyzed'
    chunk_index INTEGER,  -- NULL 허용 (분석 결과는 index 없음)
    embedding vector(1536),
    metadata JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    CONSTRAINT metadata_required CHECK (
        metadata ? 'news_id' AND
        metadata ? 'url' AND
        metadata ? 'title' AND
        metadata ? 'published_at' AND
        metadata ? 'publisher' AND
        metadata ? 'ticker' AND
        metadata ? 'stock_name'
    )
);

CREATE INDEX idx_news_chunks_chunk_type ON news_chunks(chunk_type);
CREATE INDEX idx_news_chunks_metadata ON news_chunks USING GIN(metadata);
CREATE INDEX idx_news_chunks_metadata_news_id ON news_chunks((metadata->>'news_id'));
CREATE INDEX idx_news_chunks_metadata_ticker ON news_chunks((metadata->>'ticker'));
CREATE INDEX idx_news_chunks_metadata_published_at ON news_chunks((metadata->>'published_at') DESC);

CREATE INDEX idx_news_chunks_embedding ON news_chunks
USING hnsw (embedding vector_cosine_ops)
WITH (m = 16, ef_construction = 64);

-- ===================================
-- 주식 리포트 테이블
-- ===================================

CREATE TABLE IF NOT EXISTS stock_reports (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(20) NOT NULL,
    stock_name VARCHAR(100) NOT NULL,
    analysis_date DATE NOT NULL,
    movement_type VARCHAR(10) NOT NULL CHECK (movement_type IN ('up', 'down')),

    -- 주가 변동 정보
    change_rate NUMERIC(10, 2) NOT NULL,  -- 변동률 (%)
    change_magnitude VARCHAR(20) NOT NULL,  -- "급등", "급락", "상승", "하락"

    -- 원인 분석 결과
    causes JSONB NOT NULL,  -- CauseDetail[] 배열
    summary TEXT NOT NULL,
    total_confidence VARCHAR(20) NOT NULL CHECK (total_confidence IN ('High', 'Medium', 'Low')),

    -- 연관 종목 정보
    related_stocks JSONB DEFAULT '[]',  -- RelatedStock[] 배열

    -- 시장 맥락 정보
    market_context JSONB,  -- MarketContext 객체

    graph_neighbors JSONB DEFAULT '[]',

    -- 추가: LLM 영향 전파 분석
    impact_propagation JSONB,

    -- 타임스탬프
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    triggered_at TIMESTAMP WITH TIME ZONE,  -- 트리거 시각

    -- 메타데이터
    metadata JSONB DEFAULT '{}',

    -- 분석 품질 지표
    analysis_quality JSONB DEFAULT '{
        "total_news_count": 0,
        "explicit_chunks_found": 0,
        "event_chunks_found": 0,
        "confidence_distribution": {}
    }',

    -- 중복 방지
    CONSTRAINT unique_stock_report UNIQUE (ticker, analysis_date, movement_type)
);

-- 기본 인덱스
CREATE INDEX idx_stock_reports_ticker ON stock_reports(ticker);
CREATE INDEX idx_stock_reports_date ON stock_reports(analysis_date DESC);
CREATE INDEX idx_stock_reports_ticker_date ON stock_reports(ticker, analysis_date DESC);
CREATE INDEX idx_stock_reports_confidence ON stock_reports(total_confidence);
CREATE INDEX idx_stock_reports_movement_type ON stock_reports(movement_type);
CREATE INDEX idx_stock_reports_change_rate ON stock_reports(change_rate DESC);
CREATE INDEX idx_stock_reports_created_at ON stock_reports(created_at DESC);

-- JSONB 인덱스
CREATE INDEX idx_stock_reports_causes ON stock_reports USING GIN(causes);
CREATE INDEX idx_stock_reports_related_stocks ON stock_reports USING GIN(related_stocks);
CREATE INDEX idx_stock_reports_metadata ON stock_reports USING GIN(metadata);
CREATE INDEX idx_stock_reports_analysis_quality ON stock_reports USING GIN(analysis_quality);

-- 특정 JSONB 필드 검색용
CREATE INDEX idx_stock_reports_total_news ON stock_reports((analysis_quality->>'total_news_count'));

-- ===================================
-- 뉴스 요약 리포트
-- ===================================

CREATE TABLE IF NOT EXISTS section_reports (
    id SERIAL PRIMARY KEY,
    section VARCHAR(100) NOT NULL,
    report_start_date DATE NOT NULL,
    report_end_date DATE NOT NULL,
    keywords JSONB DEFAULT '[]',
    main_trends TEXT[], -- 주요 흐름 리스트
    key_news JSONB,     -- 핵심 뉴스 배열 (headline, summary, url 등)
    summary TEXT,       -- 전체 요약
    news_urls TEXT[],   -- 포함된 뉴스 URL 목록
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    CONSTRAINT unique_section_report UNIQUE (section, report_start_date, report_end_date)
);

-- ===================================
-- 자동 업데이트 트리거
-- ===================================

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_tagged_news_updated_at BEFORE UPDATE ON tagged_news
FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_stock_reports_updated_at BEFORE UPDATE ON stock_reports
FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ===================================
-- 샘플 데이터 확인용 뷰
-- ===================================

CREATE OR REPLACE VIEW v_stock_reports_summary AS
SELECT
    ticker,
    stock_name,
    analysis_date,
    movement_type,
    change_rate,
    change_magnitude,
    total_confidence,
    jsonb_array_length(causes) AS causes_count,
    jsonb_array_length(related_stocks) AS related_stocks_count,
    (analysis_quality->>'total_news_count')::INTEGER AS total_news,
    created_at,
    triggered_at
FROM stock_reports
ORDER BY analysis_date DESC, created_at DESC;