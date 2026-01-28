import os
import sys

# чтобы "import main" работал
sys.path.insert(0, os.path.dirname(__file__))

# .env (у тебя он в config/.env)
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), "config", ".env"))
except Exception:
    pass

from main import app
from asgiref.wsgi import AsgiToWsgi

application = AsgiToWsgi(app)
