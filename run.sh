#!/usr/bin/env bash

APP_DIR="/var/www/u3395358/data/www/school.peaky-minds.ru"
PIDFILE="$APP_DIR/uvicorn.pid"

cd "$APP_DIR"

if [ -f "$PIDFILE" ]; then
    OLD_PID=$(cat "$PIDFILE")
    if ps -p "$OLD_PID" > /dev/null 2>&1; then
        echo "Stopping old uvicorn ($OLD_PID)"
        kill "$OLD_PID"
        sleep 2
    fi
fi

nohup venv/bin/uvicorn main:app --host 0.0.0.0 --port 8000 > uvicorn.log 2>&1 &

echo $! > "$PIDFILE"
echo "Started uvicorn with PID $(cat $PIDFILE)"
