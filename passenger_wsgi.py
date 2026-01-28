import os
import sys

# 1) путь к корню проекта
PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))

# 2) путь к venv (важно: именно к site-packages)
VENV_SITE_PACKAGES = os.path.join(PROJECT_DIR, "venv", "lib", "python3.10", "site-packages")

sys.path.insert(0, PROJECT_DIR)
sys.path.insert(0, VENV_SITE_PACKAGES)

# 3) если нужно — активировать переменные окружения
os.environ.setdefault("PYTHONPATH", PROJECT_DIR)

# 4) импорт приложения
from main import app  # FastAPI ASGI

# !!! Passenger ждёт WSGI "application"
# Если Passenger на вашем тарифе умеет ASGI напрямую — можно было бы отдать app.
# Но чаще всего нужно WSGI-адаптер:
from asgiref.wsgi import WsgiToAsgi

application = WsgiToAsgi(app)
