#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

echo "Updating code..."
git pull --ff-only

echo "Restarting site..."
bash run.sh

echo "Restarting bot..."
bash run_bot.sh
