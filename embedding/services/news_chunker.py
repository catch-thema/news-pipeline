from typing import List
from langchain.text_splitter import RecursiveCharacterTextSplitter


class NewsChunker:
    def __init__(self, chunk_size: int = 500, chunk_overlap: int = 50):
        self.text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
            length_function=len,
            separators=["\n\n", "\n", ". ", " ", ""]
        )

    def chunk_news(self, text: str) -> List[str]:
        """뉴스 본문을 청크로 분할"""
        chunks = self.text_splitter.split_text(text)
        return chunks