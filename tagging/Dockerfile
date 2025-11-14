FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .

# 필수 패키지만 설치
RUN pip install --no-cache-dir torch==2.1.0 --index-url https://download.pytorch.org/whl/cpu && \
    pip install --no-cache-dir -r requirements.txt

# 워커 코드 복사
COPY . /app/tagging

ENV PYTHONPATH=/app:$PYTHONPATH

WORKDIR /app/tagging

CMD ["python", "-m", "worker.tagging"]