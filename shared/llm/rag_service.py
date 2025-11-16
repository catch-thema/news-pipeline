# shared/llm/rag_service.py

import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from psycopg2.extras import RealDictCursor, Json

import openai

from shared.common.config import get_postgres_connection
from shared.common.config import RAGServiceConfig

logger = logging.getLogger(__name__)


class RAGService:
    def __init__(self):
        self.config = RAGServiceConfig()

        # OpenAI 클라이언트 초기화
        self.client = openai.OpenAI(api_key=self.config.OPENAI_API_KEY)
        self.model = self.config.EMBEDDING_MODEL

    def get_embedding(self, text: str) -> List[float]:
        """텍스트를 임베딩 벡터로 변환"""
        response = self.client.embeddings.create(
            input=text,
            model=self.model
        )
        return response.data[0].embedding

    def add_news_chunks(self, chunks: List[str], metadata: Dict):
        """청크들을 DB에 임베딩과 함께 저장"""
        try:
            with get_postgres_connection() as conn:
                with conn.cursor() as cursor:
                    for idx, chunk in enumerate(chunks):
                        embedding = self.get_embedding(chunk)

                        chunk_metadata = metadata.copy()
                        if metadata.get("chunk_type") == "original":
                            chunk_metadata["chunk_index"] = idx

                        cursor.execute("""
                            INSERT INTO news_chunks (
                                chunk_text, chunk_type, chunk_index,
                                embedding, metadata
                            ) VALUES (%s, %s, %s, %s, %s)
                        """, (
                            chunk,
                            metadata.get("chunk_type", "original"),
                            idx if metadata.get("chunk_type") == "original" else None,
                            embedding,
                            Json(chunk_metadata)
                        ))

                    conn.commit()
                    logger.debug(f"DB 저장 완료: {len(chunks)} chunks")

        except Exception as e:
            logger.error(f"DB 저장 실패: {e}")
            raise

    def search(
        self,
        query: str,
        stock_name: Optional[str] = None,
        ticker: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        n_results: int = 5,
        distance_threshold: float = 0.7
    ) -> List[Dict[str, Any]]:
        """
        RAG 벡터 검색 수행

        Args:
            query: 검색 쿼리
            stock_name: 종목명 필터 (선택)
            start_date: 시작 날짜 필터 (선택)
            end_date: 종료 날짜 필터 (선택)
            n_results: 반환할 결과 수
            distance_threshold: 유사도 임계값 (1 - cosine similarity)

        Returns:
            검색 결과 chunk 리스트
        """
        try:
            query_embedding = self.get_embedding(query)

            conditions = ["embedding IS NOT NULL"]
            params = []

            if ticker:
                conditions.append("metadata->>'ticker' = %s")
                params.append(str(ticker))  # 명시적 문자열 변환
            elif stock_name:
                conditions.append("metadata->>'stock_name' = %s")
                params.append(str(stock_name))

            if start_date:
                conditions.append("(metadata->>'published_at')::timestamp >= %s")
                params.append(start_date)

            if end_date:
                conditions.append("(metadata->>'published_at')::timestamp <= %s")
                params.append(end_date)

            where_clause = " AND ".join(conditions)

            sql = f"""
                        SELECT
                            chunk_text as content,
                            metadata,
                            1 - (embedding <=> %s::vector) as similarity,
                            embedding <=> %s::vector as distance
                        FROM news_chunks
                        WHERE {where_clause}
                        ORDER BY embedding <=> %s::vector
                        LIMIT %s
                    """

            all_params = [query_embedding, query_embedding] + params + [query_embedding, n_results]

            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute(sql, all_params)
                    results = cursor.fetchall()

            formatted_results = []
            for row in results:
                if row['distance'] <= distance_threshold:
                    formatted_results.append({
                        "content": row['content'],
                        "metadata": row['metadata'],
                        "similarity": float(row['similarity']),
                        "distance": float(row['distance'])
                    })

            logger.debug(f"Vector search found {len(formatted_results)} results")
            return formatted_results

        except Exception as e:
            logger.error(f"Vector search failed: {e}")
            return []

    def search_by_stock_and_date(
        self,
        stock_name: str,
        target_date: datetime,
        n_results: int = 10
    ) -> List[Dict[str, Any]]:
        """특정 종목과 날짜에 대한 뉴스 검색"""
        start_date = target_date - timedelta(days=1)
        end_date = target_date + timedelta(days=1)

        query = f"{stock_name} 주가 변동 원인 분석"

        return self.search(
            query=query,
            stock_name=stock_name,
            start_date=start_date,
            end_date=end_date,
            n_results=n_results
        )

    def get_all_chunks(
        self,
        stock_name: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """모든 chunk 조회 (BM25 인덱스 구축용)"""
        try:
            conditions = []
            params = []

            if stock_name:
                conditions.append("metadata->>'stock_name' = %s")
                params.append(stock_name)

            if start_date:
                conditions.append("(metadata->>'published_at')::timestamp >= %s")
                params.append(start_date)

            if end_date:
                conditions.append("(metadata->>'published_at')::timestamp <= %s")
                params.append(end_date)

            where_clause = " AND ".join(conditions) if conditions else "TRUE"

            sql = f"""
                SELECT
                    id,
                    chunk_text as content,
                    metadata
                FROM news_chunks
                WHERE {where_clause}
                ORDER BY id
            """

            if limit:
                sql += f" LIMIT {limit}"

            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute(sql, params)
                    results = cursor.fetchall()

            return [
                {
                    "id": row['id'],
                    "content": row['content'],
                    "metadata": row['metadata']
                }
                for row in results
            ]

        except Exception as e:
            logger.error(f"Error getting all chunks: {e}")
            return []

    def close(self):
        """리소스 정리"""
        pass