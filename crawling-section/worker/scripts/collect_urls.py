import argparse
import asyncio
import time
import random
from urllib.parse import quote_plus
from typing import List, Set, AsyncGenerator
import aiohttp
from bs4 import BeautifulSoup

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]


def get_random_headers():
    """랜덤 User-Agent 헤더 생성"""
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate",  # br 제거
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }


def extract_urls(html: str) -> List[str]:
    """HTML에서 뉴스 URL 추출"""
    soup = BeautifulSoup(html, "html.parser")
    urls = []

    for a in soup.select("a.news_tit"):
        href = a.get("href")
        if href:
            urls.append(href)

    for a in soup.select("a"):
        href = a.get("href")
        if href and "news.naver.com" in href:
            urls.append(href)

    return urls


async def collect_urls_stream(
        query: str,
        pages: int,
        delay: float = 3.0,
        start_date: str = "2000.01.01",
        end_date: str = "2099.12.31"
) -> AsyncGenerator[str, None]:
    """비동기로 URL을 하나씩 yield (스트리밍 방식)"""
    q = quote_plus(query)
    seen: Set[str] = set()

    connector = aiohttp.TCPConnector(limit=1, limit_per_host=1)
    timeout = aiohttp.ClientTimeout(total=30, connect=15)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        for page in range(pages):
            start = 1 + page * 10
            url = (
                f"https://search.naver.com/search.naver?"
                f"where=news&query={q}&pd=3"
                f"&ds={start_date}&de={end_date}&start={start}"
            )

            retry_count = 0
            max_retries = 3

            while retry_count < max_retries:
                try:
                    await asyncio.sleep(random.uniform(delay, delay + 3))

                    headers = get_random_headers()
                    async with session.get(url, headers=headers, allow_redirects=True) as response:
                        if response.status == 403:
                            print(f"[Warning] Page {page + 1}: 403 Forbidden - 재시도 {retry_count + 1}/{max_retries}")
                            retry_count += 1
                            await asyncio.sleep(random.uniform(10, 20))
                            continue

                        response.raise_for_status()
                        html = await response.text(encoding='utf-8')
                        urls = extract_urls(html)

                        for extracted_url in urls:
                            if extracted_url not in seen:
                                seen.add(extracted_url)
                                yield extracted_url

                        break

                except asyncio.TimeoutError:
                    print(f"[Error] Page {page + 1}: Timeout - 재시도 {retry_count + 1}/{max_retries}")
                    retry_count += 1
                    await asyncio.sleep(random.uniform(5, 10))
                except Exception as e:
                    print(f"[Error] Page {page + 1}: {e}")
                    break


async def collect_urls_async(
        query: str,
        pages: int = 1,
        delay: float = 3.0,
        max_concurrent: int = 3,
        start_date: str = "2000.01.01",
        end_date: str = "2099.12.31"
) -> List[str]:
    q = quote_plus(query)
    semaphore = asyncio.Semaphore(max_concurrent)

    async def fetch_page(session: aiohttp.ClientSession, page_num: int) -> List[str]:
        async with semaphore:
            start = 1 + page_num * 10
            url = (
                f"https://search.naver.com/search.naver?"
                f"where=news&query={q}&pd=3"
                f"&ds={start_date}&de={end_date}&start={start}"
            )

            retry_count = 0
            max_retries = 3

            while retry_count < max_retries:
                try:
                    await asyncio.sleep(random.uniform(delay, delay + 3))

                    headers = get_random_headers()
                    async with session.get(url, headers=headers, allow_redirects=True) as response:
                        if response.status == 403:
                            print(f"[Warning] Page {page_num + 1}: 403 Forbidden - 재시도 {retry_count + 1}/{max_retries}")
                            retry_count += 1
                            await asyncio.sleep(random.uniform(10, 20))
                            continue

                        response.raise_for_status()
                        html = await response.text(encoding='utf-8')
                        return extract_urls(html)

                except asyncio.TimeoutError:
                    print(f"[Error] Page {page_num + 1}: Timeout - 재시도 {retry_count + 1}/{max_retries}")
                    retry_count += 1
                    await asyncio.sleep(random.uniform(5, 10))
                except Exception as e:
                    print(f"[Error] Page {page_num + 1}: {e}")
                    break

            return []

    connector = aiohttp.TCPConnector(limit=max_concurrent, limit_per_host=max_concurrent)
    timeout = aiohttp.ClientTimeout(total=30, connect=15)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        tasks = [fetch_page(session, p) for p in range(pages)]
        results = await asyncio.gather(*tasks)

    seen: Set[str] = set()
    unique_urls = []
    for urls in results:
        for url in urls:
            if url not in seen:
                seen.add(url)
                unique_urls.append(url)

    return unique_urls

async def collect_naver_econ_headline_urls(section: str) -> list:
    url = f"https://news.naver.com/section/{section}"

    headers = get_random_headers()
    async with aiohttp.ClientSession(headers=headers) as session:
        async with session.get(url) as resp:
            html = await resp.text()
    soup = BeautifulSoup(html, "html.parser")
    links = []
    for a in soup.select("a.sa_text_title"):
        href = a.get("href")
        if not href:
            continue

        if href.startswith("/"):
            href = "https://news.naver.com" + href
        elif href.startswith("http") is False:
            href = "https://news.naver.com/" + href.lstrip("/")

        links.append(href)

    return list(set(links))

def collect_urls(
        query: str,
        pages: int = 1,
        start_date: str = "2000.01.01",
        end_date: str = "2099.12.31"
) -> List[str]:
    """동기 래퍼 함수"""
    return asyncio.run(collect_urls_async(query, pages, start_date=start_date, end_date=end_date))


def main():
    parser = argparse.ArgumentParser(description="네이버 뉴스 검색 URL 수집")
    parser.add_argument("query", help="검색어")
    parser.add_argument("--pages", type=int, default=1, help="검색 페이지 수 (기본: 1)")
    parser.add_argument("--concurrent", type=int, default=3, help="동시 요청 수 (기본: 3)")
    parser.add_argument("--delay", type=float, default=3.0, help="페이지 간 지연시간(초) (기본: 3.0)")
    parser.add_argument("--start-date", default="2000.01.01", help="시작 날짜 (기본: 2000.01.01)")
    parser.add_argument("--end-date", default="2099.12.31", help="종료 날짜 (기본: 2099.12.31)")
    parser.add_argument("--out", help="출력 파일")
    parser.add_argument("--stream", action="store_true", help="스트리밍 모드 사용")
    args = parser.parse_args()

    # 출력 파일명 생성: query_startdate_enddate.txt
    if not args.out:
        start_safe = args.start_date.replace(".", "_")
        end_safe = args.end_date.replace(".", "_")
        args.out = f"{args.query}_{start_safe}_{end_safe}.txt"

    start_time = time.time()

    if args.stream:
        async def stream_to_file():
            count = 0
            with open(args.out, "w", encoding="utf-8") as f:
                async for url in collect_urls_stream(
                        args.query,
                        args.pages,
                        args.delay,
                        args.start_date,
                        args.end_date
                ):
                    f.write(url + "\n")
                    count += 1
                    if count % 10 == 0:
                        print(f"Collected {count} URLs...")
            return count

        url_count = asyncio.run(stream_to_file())
    else:
        urls = asyncio.run(collect_urls_async(
            args.query,
            args.pages,
            args.delay,
            args.concurrent,
            args.start_date,
            args.end_date
        ))

        with open(args.out, "w", encoding="utf-8") as f:
            for u in urls:
                f.write(u + "\n")

        url_count = len(urls)

    elapsed = time.time() - start_time
    print(f"✓ Collected {url_count} URLs -> {args.out} (took {elapsed:.2f}s)")

if __name__ == "__main__":
    main()