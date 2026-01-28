#!/usr/bin/env bash
set -euo pipefail

APP_DIR="/var/www/u3395358/data/www/school.peaky-minds.ru"
VENV_ACTIVATE="/var/www/u3395358/data/www/school.peaky-minds.ru/venv/bin/activate"
RESTART_FILE="${APP_DIR}/passenger_wsgi.py"

cd "$APP_DIR"

# 1) Активируем виртуальное окружение
source "$VENV_ACTIVATE"

# 2) Обновляем зависимости (если есть requirements.txt)
if [ -f requirements.txt ]; then
  python -m pip install --upgrade pip
  python -m pip install -r requirements.txt
fi

# (опционально) если у вас зависимости в pyproject.toml/poetry — скажите, подстрою

# 3) Триггерим перезапуск Passenger
# Обычно Passenger реагирует на изменение файла WSGI entrypoint.
touch "$RESTART_FILE"

# 4) Быстрая диагностика путей (не валит деплой, но помогает понять, что на месте)
python -c "import sys; print('Python:', sys.version)"
echo "Touched restart file: $RESTART_FILE"
