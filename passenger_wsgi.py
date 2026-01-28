import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

# .env опционально, у тебя main.py и так грузит config/.env
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), "config", ".env"))
except Exception:
    pass

from main import app
from a2wsgi import ASGIMiddleware

application = ASGIMiddleware(app)
