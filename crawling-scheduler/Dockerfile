FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    default-libmysqlclient-dev \
    pkg-config \
    cron \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

ENV TZ=Asia/Seoul
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt pytz

COPY . /app

COPY crontab /etc/cron.d/stock-crawler-cron
RUN chmod 0644 /etc/cron.d/stock-crawler-cron && \
    crontab /etc/cron.d/stock-crawler-cron && \
    touch /var/log/cron.log

COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app:$PYTHONPATH

WORKDIR /app

ENTRYPOINT ["/docker-entrypoint.sh"]
