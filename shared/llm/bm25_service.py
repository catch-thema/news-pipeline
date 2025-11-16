from __future__ import annotations

from rank_bm25 import BM25Okapi
from typing import List, Dict, Any, Optional
import re


class BM25Service:
    """BM25 기반 키워드 검색 서비스"""
    
    def __init__(self):
        self.bm25_index: Optional[BM25Okapi] = None
        self.doc_ids: List[str] = []
        self.corpus: List[List[str]] = []
        
    def build_index(self, chunks: List[Dict[str, Any]]):
        """
        ChromaDB chunk들로부터 BM25 인덱스 구축
        
        Args:
            chunks: chunk 리스트 (각 chunk는 "content"와 "id" 또는 "metadata"를 포함)
        """
        self.corpus = []
        self.doc_ids = []
        
        for chunk in chunks:
            content = chunk.get("content", "")
            if not content:
                continue
            
            # 토크나이징
            tokenized = self._tokenize(content)
            if not tokenized:
                continue
            
            self.corpus.append(tokenized)
            
            # doc_id 추출
            doc_id = chunk.get("id") or chunk.get("metadata", {}).get("doc_id")
            if not doc_id:
                # metadata에서 article_url 기반으로 생성
                url = chunk.get("metadata", {}).get("article_url", "")
                chunk_idx = chunk.get("metadata", {}).get("chunk_index", 0)
                doc_id = f"{url}_{chunk_idx}" if url else f"chunk_{len(self.doc_ids)}"
            
            self.doc_ids.append(doc_id)
        
        if self.corpus:
            self.bm25_index = BM25Okapi(self.corpus)
    
    def search(self, query: str, top_k: int = 50) -> List[Dict[str, Any]]:
        """
        BM25 검색 수행
        
        Args:
            query: 검색 쿼리
            top_k: 반환할 상위 k개 결과
            
        Returns:
            검색 결과 리스트 (doc_id, score 포함)
        """
        if not self.bm25_index or not self.corpus:
            return []
        
        tokenized_query = self._tokenize(query)
        if not tokenized_query:
            return []
        
        scores = self.bm25_index.get_scores(tokenized_query)
        
        # 상위 k개 반환
        top_indices = sorted(
            range(len(scores)), 
            key=lambda i: scores[i], 
            reverse=True
        )[:top_k]
        
        return [
            {
                "doc_id": self.doc_ids[i], 
                "score": float(scores[i])
            }
            for i in top_indices if scores[i] > 0
        ]
    
    def _tokenize(self, text: str) -> List[str]:
        """
        한국어 토크나이징 (간단 버전)
        
        Args:
            text: 토크나이징할 텍스트
            
        Returns:
            토큰 리스트
        """
        if not text:
            return []
        
        # 소문자 변환
        text_lower = text.lower()
        
        # 단어 추출 (한글, 영문, 숫자)
        words = re.findall(r'\b\w+\b', text_lower)
        
        # 빈 문자열 제거
        return [w for w in words if w.strip()]

