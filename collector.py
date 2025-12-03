import asyncio
import json
import logging
import aiohttp
import hmac
import hashlib
import os
import redis
from datetime import datetime
from typing import List, Dict, Optional
from sqlalchemy import text
from database import DBConnectionManager

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("BinanceCollector")

class BinanceCollector:
    def __init__(self):
        self.is_running = False
        self.active_symbols: List[str] = []
        self.ws_task: Optional[asyncio.Task] = None
        self.session: Optional[aiohttp.ClientSession] = None
        self.last_updated: Optional[datetime] = None
        
        # Redis Client
        self.redis_client = redis.Redis.from_url("redis://redis:6379/0")

        # Batch processing buffers
        self.trade_buffer: List[Dict] = []
        self.last_batch_save_time = datetime.now()

    async def start(self, symbols: List[str]):
        """수집 시작"""
        if self.is_running:
            logger.warning("Collector is already running.")
            return

        self.is_running = True
        self.active_symbols = symbols
        self.session = aiohttp.ClientSession()
        
        # 1. 초기 데이터 로드 (REST)
        await self.initialize_market_data()

        # 2. 웹소켓 연결 (Background Task)
        self.ws_task = asyncio.create_task(self.start_websocket())
        logger.info(f"Collector started for symbols: {symbols}")

    async def stop(self):
        """수집 중지"""
        if not self.is_running:
            return

        self.is_running = False
        if self.ws_task:
            self.ws_task.cancel()
            try:
                await self.ws_task
            except asyncio.CancelledError:
                pass
        
        if self.session:
            await self.session.close()

        self.active_symbols = []
        logger.info("Collector stopped.")

    def _get_headers(self, params: Dict = None):
        """API 요청 헤더 생성 (서명 포함)"""
        api_key = os.getenv("BINANCE_API_KEY")
        api_secret = os.getenv("BINANCE_API_SECRET")
        
        if not api_key or not api_secret:
            return None

        headers = {"X-MBX-APIKEY": api_key}
        
        if params:
            query_string = "&".join([f"{k}={v}" for k, v in params.items()])
            signature = hmac.new(
                api_secret.encode("utf-8"),
                query_string.encode("utf-8"),
                hashlib.sha256
            ).hexdigest()
            params["signature"] = signature
            
        return headers

    async def initialize_market_data(self):
        """REST API로 초기 데이터(Exchange Info, Leverage Brackets) 수집"""
        logger.info("Initializing market data...")
        session = self.session if self.session else aiohttp.ClientSession()
        should_close = self.session is None

        try:
            # Leverage Brackets 수집
            timestamp = int(datetime.now().timestamp() * 1000)
            params = {"timestamp": timestamp}
            headers = self._get_headers(params)
            
            url = "https://fapi.binance.com/fapi/v1/leverageBracket"
            
            # 헤더가 있으면(키가 있으면) 인증 요청, 없으면 공개 요청(401 예상)
            if headers:
                query_string = "&".join([f"{k}={v}" for k, v in params.items()])
                url += f"?{query_string}"
                async with session.get(url, headers=headers) as resp:
                    status = resp.status
                    if status == 200:
                        brackets = await resp.json()
                        await self._save_leverage_brackets(brackets)
                        logger.info("Leverage brackets initialized from Binance API.")
                        return
                    else:
                        logger.warning(f"Failed to fetch with API Key: {status}")
            
            # 인증 실패하거나 키가 없으면 Fallback 사용
            logger.info("Using fallback leverage brackets for BTCUSDT (Paper Trading Mode)")
            # Fallback data for BTCUSDT (Customized to 150x as per user request)
            brackets = [{
                "symbol": "BTCUSDT",
                "brackets": [
                    {"bracket": 1, "initialLeverage": 150, "notionalCap": 20000, "notionalFloor": 0, "maintMarginRatio": 0.004, "cum": 0},
                    {"bracket": 2, "initialLeverage": 125, "notionalCap": 50000, "notionalFloor": 20000, "maintMarginRatio": 0.005, "cum": 20},
                    {"bracket": 3, "initialLeverage": 100, "notionalCap": 250000, "notionalFloor": 50000, "maintMarginRatio": 0.01, "cum": 1270},
                    {"bracket": 4, "initialLeverage": 50, "notionalCap": 1000000, "notionalFloor": 250000, "maintMarginRatio": 0.025, "cum": 15020},
                    {"bracket": 5, "initialLeverage": 20, "notionalCap": 5000000, "notionalFloor": 1000000, "maintMarginRatio": 0.05, "cum": 140020},
                    {"bracket": 6, "initialLeverage": 10, "notionalCap": 20000000, "notionalFloor": 5000000, "maintMarginRatio": 0.1, "cum": 1140020},
                    {"bracket": 7, "initialLeverage": 5, "notionalCap": 50000000, "notionalFloor": 20000000, "maintMarginRatio": 0.15, "cum": 3640020},
                    {"bracket": 8, "initialLeverage": 2, "notionalCap": 100000000, "notionalFloor": 50000000, "maintMarginRatio": 0.2, "cum": 8640020},
                    {"bracket": 9, "initialLeverage": 1, "notionalCap": 200000000, "notionalFloor": 100000000, "maintMarginRatio": 0.25, "cum": 18640020}
                ]
            }]
            await self._save_leverage_brackets(brackets)
            logger.info("Fallback leverage brackets initialized.")

        except Exception as e:
            logger.error(f"Failed to initialize market data: {e}")
        finally:
            if should_close:
                await session.close()

    async def _save_leverage_brackets(self, brackets: List[Dict]):
        async_engine = DBConnectionManager.get_async_engine()
        async with async_engine.begin() as conn:
            for symbol_data in brackets:
                symbol = symbol_data['symbol']
                for bracket in symbol_data['brackets']:
                    await conn.execute(text("""
                        INSERT INTO futures.leverage_brackets (
                            symbol, bracket_id, initial_leverage, max_notional, 
                            min_notional, maint_margin_rate, cum_fast_maint_amount, updated_at
                        ) VALUES (
                            :symbol, :bracket_id, :initial_leverage, :max_notional,
                            :min_notional, :maint_margin_rate, :cum_fast_maint_amount, NOW()
                        )
                        ON CONFLICT (symbol, bracket_id) DO UPDATE SET
                            initial_leverage = EXCLUDED.initial_leverage,
                            max_notional = EXCLUDED.max_notional,
                            min_notional = EXCLUDED.min_notional,
                            maint_margin_rate = EXCLUDED.maint_margin_rate,
                            cum_fast_maint_amount = EXCLUDED.cum_fast_maint_amount,
                            updated_at = NOW()
                    """), {
                        "symbol": symbol,
                        "bracket_id": bracket['bracket'],
                        "initial_leverage": bracket['initialLeverage'],
                        "max_notional": bracket['notionalCap'],
                        "min_notional": bracket['notionalFloor'],
                        "maint_margin_rate": bracket['maintMarginRatio'],
                        "cum_fast_maint_amount": bracket['cum']
                    })

    async def start_websocket(self):
        """Binance WebSocket 연결 및 데이터 처리"""
        base_url = "wss://fstream.binance.com/stream?streams="
        # streams = [f"{s.lower()}@aggTrade/{s.lower()}@depth20@100ms" for s in self.active_symbols]
        # url = base_url + "/".join(streams)
        
        # 테스트용: BTCUSDT만 연결 (Ticker, Trade, Depth)
        url = base_url + "btcusdt@ticker/btcusdt@aggTrade/btcusdt@depth20@100ms"

        logger.info(f"Connecting to WebSocket: {url}")

        while self.is_running:
            try:
                async with self.session.ws_connect(url) as ws:
                    logger.info("WebSocket connected.")
                    async for msg in ws:
                        if not self.is_running:
                            break
                        
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = json.loads(msg.data)
                            await self._handle_message(data)
                            self.last_updated = datetime.now()
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            logger.error(f"WebSocket error: {msg.data}")
                            break
            except Exception as e:
                logger.error(f"WebSocket connection failed: {e}")
                await asyncio.sleep(5)  # 재연결 대기

    async def _handle_message(self, data: Dict):
        """메시지 처리 (DB 저장 및 Redis Publish)"""
        stream = data.get("stream")
        payload = data.get("data")

        if not payload:
            return

        try:
            if "ticker" in stream:
                # Ticker 데이터 Redis Publish
                message = json.dumps({
                    "type": "ticker",
                    "symbol": payload['s'],
                    "price": float(payload['c']),
                    "timestamp": datetime.fromtimestamp(payload['E'] / 1000).isoformat()
                })
                self.redis_client.publish("market_data", message)

            elif "aggTrade" in stream:
                # Trade 데이터 버퍼링
                trade_data = {
                    "symbol": payload['s'],
                    "price": float(payload['p']),
                    "quantity": float(payload['q']),
                    "side": "SELL" if payload['m'] else "BUY", 
                    "trade_time": datetime.fromtimestamp(payload['T'] / 1000),
                    "trade_id": str(payload['a'])
                }
                self.trade_buffer.append(trade_data)
                
                # Redis Publish
                message = json.dumps({
                    "type": "trade",
                    "symbol": trade_data['symbol'],
                    "price": trade_data['price'],
                    "quantity": trade_data['quantity'],
                    "side": trade_data['side'],
                    "timestamp": trade_data['trade_time'].isoformat()
                })
                self.redis_client.publish("market_data", message)
                
                # 매칭 엔진 호출 (지정가 주문 체결 확인)
                from matching_engine import matching_engine
                await matching_engine.process_limit_orders(trade_data)
                
                # 버퍼가 차거나 일정 시간이 지나면 저장
                if len(self.trade_buffer) >= 50 or (datetime.now() - self.last_batch_save_time).total_seconds() > 1.0:
                    await self._save_trades(self.trade_buffer)
                    self.trade_buffer = []
                    self.last_batch_save_time = datetime.now()

            elif "depth" in stream:
                # Orderbook 데이터 Redis Publish
                message = json.dumps({
                    "type": "depth",
                    "symbol": payload['s'],
                    "bids": payload['b'],
                    "asks": payload['a'],
                    "timestamp": datetime.fromtimestamp(payload['E'] / 1000).isoformat()
                })
                self.redis_client.publish("market_data", message)

                await self._save_orderbook(payload)

        except Exception as e:
            logger.error(f"Error handling message: {e}")

    async def _save_trades(self, trades: List[Dict]):
        """체결 내역 일괄 저장"""
        if not trades:
            return

        async_engine = DBConnectionManager.get_async_engine()
        async with async_engine.begin() as conn:
            stmt = text("""
                INSERT INTO futures.market_trades (symbol, price, quantity, side, trade_time, trade_id)
                VALUES (:symbol, :price, :quantity, :side, :trade_time, :trade_id)
            """)
            await conn.execute(stmt, trades)

    async def _save_orderbook(self, payload: Dict):
        """오더북 업데이트 (Upsert)"""
        symbol = payload['s']
        bids = payload['b'] # [[price, qty], ...]
        asks = payload['a']
        
        async_engine = DBConnectionManager.get_async_engine()
        async with async_engine.begin() as conn:
            # Bids 처리
            for price, qty in bids:
                if float(qty) == 0:
                    await conn.execute(text("""
                        DELETE FROM futures.orderbook 
                        WHERE symbol = :symbol AND side = 'BID' AND price = :price
                    """), {"symbol": symbol, "price": float(price)})
                else:
                    await conn.execute(text("""
                        INSERT INTO futures.orderbook (symbol, side, price, quantity, updated_at)
                        VALUES (:symbol, 'BID', :price, :quantity, NOW())
                        ON CONFLICT (symbol, side, price) 
                        DO UPDATE SET quantity = EXCLUDED.quantity, updated_at = NOW()
                    """), {"symbol": symbol, "price": float(price), "quantity": float(qty)})

            # Asks 처리
            for price, qty in asks:
                if float(qty) == 0:
                    await conn.execute(text("""
                        DELETE FROM futures.orderbook 
                        WHERE symbol = :symbol AND side = 'ASK' AND price = :price
                    """), {"symbol": symbol, "price": float(price)})
                else:
                    await conn.execute(text("""
                        INSERT INTO futures.orderbook (symbol, side, price, quantity, updated_at)
                        VALUES (:symbol, 'ASK', :price, :quantity, NOW())
                        ON CONFLICT (symbol, side, price) 
                        DO UPDATE SET quantity = EXCLUDED.quantity, updated_at = NOW()
                    """), {"symbol": symbol, "price": float(price), "quantity": float(qty)})

    def get_status(self):
        return {
            "is_active": self.is_running,
            "active_symbols": self.active_symbols,
            "last_updated": self.last_updated.isoformat() if self.last_updated else None
        }

# 싱글톤 인스턴스
collector = BinanceCollector()
