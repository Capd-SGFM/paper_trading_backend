from fastapi import FastAPI
from pydantic import BaseModel
from typing import List
import datetime
from collector import collector
from routers import orders, market

app = FastAPI(title="Paper Trading Backend")

app.include_router(orders.router)
app.include_router(market.router)

@app.on_event("startup")
async def startup_event():
    # 앱 시작 시 마켓 데이터 초기화 (Leverage Brackets 등)
    await collector.initialize_market_data()

@app.get("/")
def read_root():
    return {"status": "ok", "service": "paper_trading_backend"}

@app.get("/health")
def health_check():
    return {"status": "healthy"}

@app.post("/collect/start")
async def start_collection():
    # 실제 수집기 시작 (BTCUSDT, ETHUSDT 등)
    target_symbols = ["BTCUSDT", "ETHUSDT", "XRPUSDT", "SOLUSDT", "DOGEUSDT"]
    await collector.start(target_symbols)
    return {"message": "Data collection started", "status": "started"}

@app.post("/collect/stop")
async def stop_collection():
    await collector.stop()
    return {"message": "Data collection stopped", "status": "stopped"}

@app.get("/collect/status")
def get_status():
    return collector.get_status()
