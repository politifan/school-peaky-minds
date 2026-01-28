import os, sys

APP_ROOT = os.path.dirname(os.path.abspath(__file__))

# 1) Подключаем проект в sys.path
if APP_ROOT not in sys.path:
    sys.path.insert(0, APP_ROOT)

# 2) Активируем venv (важно на шаред-хостинге)
activate_this = os.path.join(APP_ROOT, "venv", "bin", "activate_this.py")
if os.path.exists(activate_this):
    with open(activate_this, "r") as f:
        code = compile(f.read(), activate_this, "exec")
        exec(code, {"__file__": activate_this})

# 3) Импортируем FastAPI-приложение
from main import app as fastapi_app

# 4) Оборачиваем ASGI (FastAPI) в WSGI для Passenger
from a2wsgi import ASGIMiddleware
application = ASGIMiddleware(fastapi_app)
