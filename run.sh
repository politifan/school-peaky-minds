#!/usr/bin/env bash
set -e

APP_DIR="/var/www/u3395358/data/www/school.peaky-minds.ru"
PIDFILE="$APP_DIR/uvicorn.pid"
LOGFILE="$APP_DIR/uvicorn.log"

cd "$APP_DIR"

echo "=== Pulling latest code ==="
git pull --rebase

echo "=== Stopping old uvicorn (if running) ==="
if [ -f "$PIDFILE" ]; then
    OLD_PID=$(cat "$PIDFILE")
    if ps -p "$OLD_PID" > /dev/null 2>&1; then
        kill "$OLD_PID"
        sleep 2
    fi
fi

echo "=== Starting uvicorn ==="
nohup venv/bin/uvicorn main:app --host 0.0.0.0 --port 8000 \
    --workers 2 \
    > "$LOGFILE" 2>&1 &

echo $! > "$PIDFILE"

echo "Started uvicorn with PID $(cat $PIDFILE)"
echo "Logs: $LOGFILE"
