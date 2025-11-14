from __future__ import annotations

from shared.services.rag_service import RAGService
from shared.services.bm25_service import BM25Service
from typing import List, Dict, Any, Optional
from datetime import datetime


class HybridSearchService:
    """하이브리드 검색 서비스: Dense (벡터) + BM25 (키워드) + 메타데이터 필터"""

    def __init__(
            self,
            rag_service: Optional[RAGService] = None,
            bm25_service: Optional[BM25Service] = None,
            dense_weight: float = 0.55,
            bm25_weight: float = 0.35,
            tag_weight: float = 0.10
    ):
        self.rag_service = rag_service or RAGService()
        self.bm25_service = bm25_service or BM25Service()

        self.dense_weight = dense_weight
        self.bm25_weight = bm25_weight
        self.tag_weight = tag_weight

    def search(
            self,
            query: str,
            ticker: Optional[str] = None,
            stock_name: Optional[str] = None,
            start_date: Optional[datetime] = None,
            end_date: Optional[datetime] = None,
            tags: Optional[Dict[str, Any]] = None,
            k: int = 50
    ) -> List[Dict[str, Any]]:
        """
        하이브리드 검색: Dense + BM25 + 태그 필터

        Args:
            query: 검색 쿼리
            ticker: 종목명 필터
            start_date: 시작 날짜
            end_date: 종료 날짜
            tags: 추가 메타데이터 필터
            k: 반환할 결과 수

        Returns:
            검색 결과 리스트 (final_score 포함)
        """
        # 1. Dense 검색 (ChromaDB)
        dense_results = self.rag_service.search(
            query=query,
            stock_name=stock_name,
            ticker=ticker,
            start_date=start_date,
            end_date=end_date,
            n_results=k * 2,  # 더 많이 가져와서 재랭킹
        )

        # 2. BM25 검색
        bm25_results = self.bm25_service.search(query, top_k=k * 2)

        # 3. 점수 정규화 및 결합
        combined = self._combine_results(
            dense_results, bm25_results
        )

        # 4. 상위 k개 반환
        return sorted(combined, key=lambda x: x.get("final_score", 0.0), reverse=True)[:k]

    def _normalize_dense_score(self, distance: Optional[float]) -> float:
        """
        Dense 검색 거리를 0~1 점수로 변환
        distance가 작을수록 유사도가 높음
        """
        if distance is None:
            return 0.0
        # Cosine distance: 0(완전 일치) ~ 2(완전 반대)
        return max(0.0, 1.0 - (distance / 2.0))

    def _normalize_bm25_score(self, score: float) -> float:
        """
        BM25 점수를 0~1로 정규화
        """
        if score <= 0:
            return 0.0
        # BM25 점수는 보통 0~10 범위
        return min(1.0, score / 10.0)

    def _combine_results(
            self,
            dense_results: List[Dict],
            bm25_results: List[Dict]
    ) -> List[Dict]:
        """결과 결합 및 점수 계산"""
        # content 해시 또는 metadata의 id 사용
        dense_map = {}
        for r in dense_results:
            # metadata에서 id 추출 또는 content 해시
            doc_id = r.get("metadata", {}).get("id") or str(hash(r.get("content", "")))
            if doc_id not in dense_map:
                dense_map[doc_id] = r

        bm25_map = {}
        for r in bm25_results:
            doc_id = r.get("metadata", {}).get("id") or str(hash(r.get("content", "")))
            if doc_id not in bm25_map:
                bm25_map[doc_id] = r

        all_doc_ids = set(dense_map.keys()) | set(bm25_map.keys())

        combined = []
        for doc_id in all_doc_ids:
            dense = dense_map.get(doc_id, {})
            bm25 = bm25_map.get(doc_id, {})

            dense_score = self._normalize_dense_score(dense.get("distance"))
            bm25_score = self._normalize_bm25_score(bm25.get("score", 0.0))

            final_score = (
                    self.dense_weight * dense_score +
                    self.bm25_weight * bm25_score
            )

            metadata = dense.get("metadata") or bm25.get("metadata", {})

            result = {
                "id": doc_id,  # 명시적으로 id 추가
                "text": dense.get("content") or bm25.get("content", ""),
                "content": dense.get("content") or bm25.get("content", ""),
                "metadata": metadata,
                "distance": dense.get("distance"),
                "similarity": dense.get("similarity"),
                "dense_score": dense_score,
                "bm25_score": bm25_score,
                "final_score": final_score,
                "score": final_score
            }

            combined.append(result)

        return combined

    def _calculate_tag_boost(self, metadata: Dict, tags: Optional[Dict]) -> float:
        """
        태그 일치도 계산

        Args:
            metadata: chunk 메타데이터
            tags: 태그 필터

        Returns:
            부스트 점수 (0.0 ~ 1.0)
        """
        if not tags:
            return 0.0

        boost = 0.0

        # ticker 일치
        if tags.get("ticker") and metadata.get("stock_name") == tags["ticker"]:
            boost += 1.0
        elif tags.get("ticker") and tags["ticker"] in str(metadata.get("stock_name", "")):
            boost += 0.5

        # sector 일치
        if tags.get("sector") and metadata.get("sector") == tags["sector"]:
            boost += 0.3

        # event_type 일치
        if tags.get("event_type") and metadata.get("event_type") == tags["event_type"]:
            boost += 0.5

        # direction 일치
        if tags.get("direction") and metadata.get("direction") == tags["direction"]:
            boost += 0.3

        return min(boost, 1.0)

