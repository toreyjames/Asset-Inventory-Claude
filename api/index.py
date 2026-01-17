"""FastAPI on Vercel serverless."""

from fastapi import FastAPI
from mangum import Mangum

app = FastAPI()

@app.get("/")
def root():
    return {"status": "ok", "message": "OT Asset Inventory API"}

@app.get("/health")
def health():
    return {"status": "healthy"}

# Handler for serverless
handler = Mangum(app)
