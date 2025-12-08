# File: worker/scripts/async_crawl.py
import asyncio
import json
from typing import Optional, Dict, List
import httpx
from bs4 import BeautifulSoup

TIMEOUT = 20
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
}


async def fetch_and_parse(client: httpx.AsyncClient, url: str) -> Dict:
    """단일 URL 크롤링 및 파싱 (핵심 함수)"""
    try:
        resp = await client.get(url, follow_redirects=True)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # 제목 추출 (우선순위 순서)
        title = ""
        title_tag = (
                soup.select_one("h2.media_end_head_headline") or
                soup.select_one("h3#articleTitle") or
                soup.select_one("h2#title_area")
        )
        if title_tag:
            title = title_tag.get_text(strip=True)
        elif soup.title:
            title = soup.title.string.strip() if soup.title.string else ""

        # 본문 추출 (여러 셀렉터 시도)
        content = ""
        content_tag = (
                soup.select_one("article#dic_area") or
                soup.select_one("div#articleBodyContents") or
                soup.select_one("div#newsEndContents") or
                soup.select_one("div.news_end_body_content")
        )
        if content_tag:
            # 불필요한 태그 제거
            for unwanted in content_tag(["script", "style", "iframe", "ins", "a.link_figure"]):
                unwanted.decompose()
            content = content_tag.get_text(separator="\n", strip=True)

        # 발행일자
        published_at = ""
        date_tag = (
                soup.select_one("span.media_end_head_info_datestamp_time._ARTICLE_DATE_TIME") or
                soup.select_one("span.media_end_head_info_datestamp_time") or
                soup.select_one("span.t11") or
                soup.select_one("span.author em")
        )
        if date_tag:
            # data-date-time 속성 우선 사용
            published_at = date_tag.get("data-date-time") or date_tag.get_text(strip=True)

        # 발행사
        publisher = ""
        pub_tag = (
                soup.select_one("a.media_end_head_top_logo img") or
                soup.select_one("div.press_logo img") or
                soup.select_one("a.press img")
        )
        if pub_tag:
            publisher = pub_tag.get("alt", "").strip()

        return {
            "url": url,
            "title": title,
            "content": content,
            "published_at": published_at,
            "publisher": publisher,
        }

    except httpx.TimeoutException:
        return {"url": url, "error": "timeout"}
    except httpx.HTTPStatusError as e:
        return {"url": url, "error": f"HTTP {e.response.status_code}"}
    except Exception as e:
        return {"url": url, "error": str(e)}


async def crawl_single_url(url: str, timeout: int = TIMEOUT) -> Optional[Dict]:
    """단일 URL 크롤링 (consumer.py용 간단 래퍼)"""
    async with httpx.AsyncClient(
            timeout=timeout,
            headers=HEADERS,
            follow_redirects=True
    ) as client:
        result = await fetch_and_parse(client, url)

        # 에러 또는 빈 데이터 필터링
        if "error" in result:
            return None
        if not result.get("title") or not result.get("content"):
            return None

        return result


async def crawl_batch(
        urls: List[str],
        concurrency: int = 10,
        timeout: int = TIMEOUT,
        on_progress: Optional[callable] = None
) -> List[Dict]:
    """여러 URL을 동시에 크롤링 (배치 처리)"""
    semaphore = asyncio.Semaphore(concurrency)

    async def crawl_with_semaphore(url: str, client: httpx.AsyncClient) -> Dict:
        async with semaphore:
            result = await fetch_and_parse(client, url)
            if on_progress:
                on_progress(url, result)
            return result

    async with httpx.AsyncClient(
            timeout=timeout,
            headers=HEADERS,
            follow_redirects=True
    ) as client:
        tasks = [crawl_with_semaphore(url, client) for url in urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    # Exception을 에러 딕셔너리로 변환
    return [
        {"url": urls[i], "error": str(r)} if isinstance(r, Exception) else r
        for i, r in enumerate(results)
    ]


async def crawl_and_save_stream(
        urls: List[str],
        out_path: str,
        concurrency: int = 10,
        batch_size: int = 50
) -> Dict[str, int]:
    """URL을 스트리밍 방식으로 크롤링하여 JSONL로 저장"""
    success = 0
    failed = 0

    with open(out_path, "w", encoding="utf-8") as f:
        for i in range(0, len(urls), batch_size):
            batch = urls[i:i + batch_size]
            results = await crawl_batch(batch, concurrency)

            for result in results:
                f.write(json.dumps(result, ensure_ascii=False) + "\n")
                if "error" in result:
                    failed += 1
                else:
                    success += 1

            if (i + batch_size) % 100 == 0:
                print(f"[Progress] {i + batch_size}/{len(urls)} processed "
                      f"(success: {success}, failed: {failed})")

    return {"success": success, "failed": failed, "total": len(urls)}


# CLI 실행용 메인
async def main():
    import sys

    if len(sys.argv) < 3:
        print("Usage: python -m worker.scripts.async_crawl <urls.txt> <output.jsonl> [concurrency]")
        sys.exit(1)

    url_file = sys.argv[1]
    out_file = sys.argv[2]
    concurrency = int(sys.argv[3]) if len(sys.argv) > 3 else 10

    with open(url_file, "r", encoding="utf-8") as f:
        urls = [line.strip() for line in f if line.strip()]

    print(f"[*] Crawling {len(urls)} URLs (concurrency={concurrency})")

    stats = await crawl_and_save_stream(urls, out_file, concurrency)

    print(f"\n✓ Completed: {stats['success']} success, {stats['failed']} failed")
    print(f"  Output: {out_file}")


if __name__ == "__main__":
    asyncio.run(main())