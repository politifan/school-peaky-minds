import os, sys

APP_ROOT = os.path.dirname(os.path.abspath(__file__))

# project in path
if APP_ROOT not in sys.path:
    sys.path.insert(0, APP_ROOT)

# venv activate (shared hosting safe)
activate_this = os.path.join(APP_ROOT, "venv", "bin", "activate_this.py")
if os.path.exists(activate_this):
    with open(activate_this, "r") as f:
        exec(compile(f.read(), activate_this, "exec"), {"__file__": activate_this})

from main import app as fastapi_app  # must exist in main.py

from a2wsgi import ASGIMiddleware
application = ASGIMiddleware(fastapi_app)