#!/usr/bin/env bash
set -euo pipefail

APP_DIR="/var/www/u3395358/data/www/school.peaky-minds.ru"
VENV="$APP_DIR/venv/bin/python"
LOG_DIR="$APP_DIR/logs"
LOG_FILE="$LOG_DIR/telegram_bot.log"
PID_FILE="$APP_DIR/telegram_bot.pid"

mkdir -p "$LOG_DIR"

# остановить старый процесс
if [[ -f "$PID_FILE" ]]; then
  OLD_PID=$(cat "$PID_FILE")
  if ps -p "$OLD_PID" >/dev/null 2>&1; then
    kill "$OLD_PID" || true
    sleep 1
  fi
fi

cd "$APP_DIR"

nohup "$VENV" telegram_bot.py >> "$LOG_FILE" 2>&1 &

echo $! > "$PID_FILE"
echo "Telegram bot started, pid=$(cat "$PID_FILE")"
