import sys
import os

INTERP = os.path.expanduser("/var/www/u3395358/data/venv/bin/python")
if sys.executable != INTERP:
    os.execl(INTERP, INTERP, *sys.argv)

sys.path.append(os.getcwd())

BASE_DIR = os.path.dirname(__file__)
os.chdir(BASE_DIR)

from a2wsgi import ASGIMiddleware
from main import app as asgi_app

application = ASGIMiddleware(asgi_app)