# File: worker/scripts/export_db.py
import asyncio
import json
import csv
import argparse
from datetime import datetime
from sqlalchemy import select
from app.database import async_session_factory
from app.models import CrawlResult


async def export_to_jsonl(output_path: str, limit: int = None):
    """DB 데이터를 JSONL 파일로 내보내기"""
    async with async_session_factory() as session:
        stmt = select(CrawlResult).order_by(CrawlResult.created_at.desc())
        if limit:
            stmt = stmt.limit(limit)
        result = await session.execute(stmt)
        records = result.scalars().all()

    with open(output_path, "w", encoding="utf-8") as f:
        for r in records:
            data = {
                "id": r.id,
                "url": r.url,
                "title": r.title,
                "content": r.content,
                "publisher": r.publisher,
                "published_date": r.published_date,
                "ticker": r.ticker,
                "company_name": r.company_name,
                "keyword": r.keyword,
                "relation": r.relation,
                "created_at": r.created_at.isoformat() if r.created_at else None
            }
            f.write(json.dumps(data, ensure_ascii=False) + "\n")
    print(f"[Export] {len(records)} records -> {output_path}")


async def export_to_csv(output_path: str, limit: int = None):
    """DB 데이터를 CSV 파일로 내보내기"""
    async with async_session_factory() as session:
        stmt = select(CrawlResult).order_by(CrawlResult.created_at.desc())
        if limit:
            stmt = stmt.limit(limit)
        result = await session.execute(stmt)
        records = result.scalars().all()

    fieldnames = ["id", "url", "title", "content", "publisher", "published_date",
                  "ticker", "company_name", "keyword", "relation", "created_at"]

    with open(output_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in records:
            writer.writerow({
                "id": r.id,
                "url": r.url,
                "title": r.title,
                "content": r.content,
                "publisher": r.publisher,
                "published_date": r.published_date,
                "ticker": r.ticker,
                "company_name": r.company_name,
                "keyword": r.keyword,
                "relation": r.relation,
                "created_at": r.created_at.isoformat() if r.created_at else None
            })
    print(f"[Export] {len(records)} records -> {output_path}")


def main():
    parser = argparse.ArgumentParser(description="DB 데이터 내보내기")
    parser.add_argument("--format", choices=["jsonl", "csv"], default="jsonl", help="출력 형식")
    parser.add_argument("--output", help="출력 파일 경로 (미지정시 자동 생성)")
    parser.add_argument("--limit", type=int, help="내보낼 최대 레코드 수")
    args = parser.parse_args()

    if not args.output:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        args.output = f"data/export_{ts}.{args.format}"

    if args.format == "jsonl":
        asyncio.run(export_to_jsonl(args.output, args.limit))
    else:
        asyncio.run(export_to_csv(args.output, args.limit))


if __name__ == "__main__":
    main()