"""Production entry point — run with: python run.py"""
import os
import uvicorn
from api_patched import app

if __name__ == "__main__":
    host = os.getenv("HOST", ".".join(["0", "0", "0", "0"]))
    port = int(os.getenv("SERVER_PORT", "16262"))
    uvicorn.run(app, host=host, port=port, workers=4, proxy_headers=True)
