#!/bin/sh
set -e

echo "Waiting for MySQL to be ready..."
sleep 10

echo "Initializing database tables..."
python /app/scripts/init_db_once.py

echo "Starting cron daemon..."
cron

echo "Cron daemon started. Watching for scheduled tasks..."
echo "Active cron schedule:"
crontab -l | grep -v "^#" | grep -v "^$" || echo "No active cron jobs"

# 로그 파일 생성 및 실시간 출력
touch /var/log/cron.log
tail -f /var/log/cron.log
