"""Patched API module entrypoint.

Use with your process manager/startup command:
python -m uvicorn run:app --host 0.0.0.0 --port ${SERVER_PORT} --workers 4 --proxy-headers --forwarded-allow-ips='*'
"""

from api_patched import app
