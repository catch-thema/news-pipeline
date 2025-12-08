from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import date, datetime
from app.schemas import StockReport, SectionReport
from app.models import StockReportSummary, StockReportDetail, SectionReportSummary

from shared.common.config import get_postgres_session

router = APIRouter(prefix="/reports", tags=["reports"])


@router.get("/", response_model=List[StockReportSummary])
def get_reports(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    ticker: Optional[str] = None,
    movement_type: Optional[str] = None,
    confidence: Optional[str] = None,
    db: Session = Depends(get_postgres_session)
):
    """리포트 목록 조회"""
    query = db.query(StockReport)

    if ticker:
        query = query.filter(StockReport.ticker == ticker)
    if movement_type:
        query = query.filter(StockReport.movement_type == movement_type)
    if confidence:
        query = query.filter(StockReport.total_confidence == confidence)

    reports = query.order_by(
        StockReport.analysis_date.desc(),
        StockReport.created_at.desc()
    ).offset(skip).limit(limit).all()

    return reports


@router.get("/date/{target_date}", response_model=List[StockReportSummary])
def get_reports_by_date(
    target_date: date,
    db: Session = Depends(get_postgres_session)
):
    """특정 날짜의 리포트 조회"""
    reports = db.query(StockReport).filter(
        StockReport.analysis_date == target_date
    ).order_by(
        StockReport.change_rate.desc()
    ).all()

    if not reports:
        raise HTTPException(status_code=404, detail=f"{target_date} 날짜의 리포트가 없습니다.")

    return reports


@router.get("/today", response_model=List[StockReportSummary])
def get_today_reports(db: Session = Depends(get_postgres_session)):
    """오늘의 리포트 조회"""
    today = date.today()
    reports = db.query(StockReport).filter(
        StockReport.analysis_date == today
    ).order_by(
        StockReport.change_rate.desc()
    ).all()

    if not reports:
        raise HTTPException(status_code=404, detail="오늘의 리포트가 없습니다.")

    return reports


@router.get("/section", response_model=List[SectionReportSummary])
def get_section_reports(
    section: Optional[str] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    db: Session = Depends(get_postgres_session)
):
    """섹션별 뉴스 요약 리포트 조회"""
    query = db.query(SectionReport)
    if section:
        query = query.filter(SectionReport.section == section)
    if start_date:
        query = query.filter(SectionReport.report_start_date >= start_date)
    if end_date:
        query = query.filter(SectionReport.report_end_date <= end_date)
    reports = query.order_by(
        SectionReport.report_start_date.desc(),
        SectionReport.created_at.desc()
    ).offset(skip).limit(limit).all()
    return reports

@router.get("/section/today", response_model=List[SectionReportSummary])
def get_today_section_reports(
    db: Session = Depends(get_postgres_session)
):
    today = date.today()
    reports = db.query(SectionReport).filter(
        (SectionReport.report_start_date == today) | (SectionReport.report_end_date == today)
    ).order_by(
        SectionReport.report_start_date.desc(),
        SectionReport.created_at.desc()
    ).all()

    if not reports:
        raise HTTPException(status_code=404, detail="오늘의 섹션 리포트가 없습니다.")

    return reports

@router.get("/{report_id}", response_model=StockReportDetail)
def get_report_detail(
    report_id: int,
    db: Session = Depends(get_postgres_session)
):
    """리포트 상세 조회"""
    report = db.query(StockReport).filter(StockReport.id == report_id).first()

    if not report:
        raise HTTPException(status_code=404, detail="리포트를 찾을 수 없습니다.")

    # metadata 필드 변환
    report_dict = report.__dict__
    report_dict["metadata"] = report_dict.get("metadata", {}) if isinstance(report_dict.get("metadata"),
                                                                            dict) else {}

    return report