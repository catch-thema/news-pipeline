FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app/embedding

ENV PYTHONPATH=/app:/app/shared/common:$PYTHONPATH

WORKDIR /app/embedding

CMD ["python", "-m", "worker.rag_embedding"]