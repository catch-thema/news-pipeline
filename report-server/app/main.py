from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routers import reports
from app.routers import chat

app = FastAPI(
    title="Stock Report API",
    description="주가 변동 원인 분석 리포트 조회 API",
    version="1.0.0"
)

# CORS 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 라우터 등록
app.include_router(reports.router)
app.include_router(chat.router)


@app.get("/")
def root():
    return {
        "message": "Stock Report API",
        "docs": "/docs",
        "health": "/health"
    }


@app.get("/health")
def health_check():
    return {"status": "healthy"}