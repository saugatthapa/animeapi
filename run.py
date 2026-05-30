"""Production entry point — run with: python run.py"""
import uvicorn

if __name__ == "__main__":
    uvicorn.run(
        "api:app",
        host="0.0.0.0",
        port=16262,
        workers=4,
        proxy_headers=True,
        forwarded_allow_ips="*",
    )
