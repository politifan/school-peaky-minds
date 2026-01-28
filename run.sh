cat > run.sh <<'SH'
#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
source venv/bin/activate

export PYTHONUNBUFFERED=1
nohup uvicorn main:app --host 0.0.0.0 --port 8000 --workers 2 \
  > uvicorn.log 2>&1 &

echo $! > uvicorn.pid
echo "Started uvicorn with PID $(cat uvicorn.pid)"
SH

chmod +x run.sh
