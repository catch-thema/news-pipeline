FROM python:3.11-slim

WORKDIR /app

# Playwright 의존성 설치
RUN apt-get update && apt-get install -y --no-install-recommends \
    libglib2.0-0 \
    libnss3 \
    libnspr4 \
    libdbus-1-3 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libpango-1.0-0 \
    libcairo2 \
    libasound2 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Playwright Chromium만 설치 (의존성은 수동 설치했으므로 --with-deps 제외)
RUN playwright install chromium

COPY . /app/crawling

ENV PYTHONPATH=/app:$PYTHONPATH

WORKDIR /app/crawling

CMD ["python", "-m", "worker.crawling"]