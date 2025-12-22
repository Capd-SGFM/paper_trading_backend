import asyncio
import logging
from datetime import datetime
from sqlalchemy import text
from database import DBConnectionManager

logger = logging.getLogger("LiquidationMonitor")

class LiquidationMonitor:
    def __init__(self):
        self.running = False
        self._task = None
    
    async def start(self):
        """Start monitoring loop"""
        if self.running:
            return
            
        self.running = True
        logger.info("Starting Liquidation Monitor...")
        while self.running:
            try:
                await self.check_liquidations()
            except Exception as e:
                logger.error(f"Liquidation monitor error: {e}", exc_info=True)
            
            await asyncio.sleep(1)  # Check every 1 second
            
    def stop(self):
        self.running = False
        
    async def check_liquidations(self):
        """
        Check all OPEN positions for liquidation conditions
        """
        async_engine = DBConnectionManager.get_async_engine()
        async with async_engine.begin() as conn:
            # 1. Get current mark prices
            mark_prices = await self.get_mark_prices(conn)
            if not mark_prices:
                return

            # 2. Find positions that should be liquidated
            # liquidation_price가 설정된 포지션만 조회
            stmt = text("""
                SELECT id, account_id, symbol, position_side, 
                       quantity, entry_price, liquidation_price, leverage, margin, google_id
                FROM futures.positions
                WHERE status = 'OPEN'
                  AND liquidation_price IS NOT NULL
            """)
            result = await conn.execute(stmt)
            positions = result.mappings().all()
            
            for pos in positions:
                symbol = pos['symbol']
                if symbol not in mark_prices:
                    continue
                    
                mark_price = mark_prices[symbol]
                liq_price = float(pos['liquidation_price'])
                
                should_liquidate = False
                if pos['position_side'] == 'LONG':
                    # LONG liquidates when mark price <= liquidation price
                    if mark_price <= liq_price:
                        should_liquidate = True
                else:  # SHORT
                    # SHORT liquidates when mark price >= liquidation price
                    if mark_price >= liq_price:
                        should_liquidate = True
                
                if should_liquidate:
                    await self.liquidate_position(conn, pos, mark_price)
    
    async def get_mark_prices(self, conn) -> dict:
        """Get current mark price for each symbol from orderbook"""
        # 간단하게 orderbook의 중간값 사용 (실제로는 Mark Price 스트림 사용 권장)
        stmt = text("""
            SELECT symbol, 
                   (SELECT price FROM futures.orderbook 
                    WHERE symbol = o.symbol AND side = 'BID' 
                    ORDER BY price DESC LIMIT 1) as bid,
                   (SELECT price FROM futures.orderbook 
                    WHERE symbol = o.symbol AND side = 'ASK' 
                    ORDER BY price ASC LIMIT 1) as ask
            FROM (SELECT DISTINCT symbol FROM futures.orderbook) o
        """)
        result = await conn.execute(stmt)
        rows = result.fetchall()
        
        mark_prices = {}
        for row in rows:
            if row[1] is not None and row[2] is not None:
                mark_prices[row[0]] = (float(row[1]) + float(row[2])) / 2
        return mark_prices
    
    async def liquidate_position(self, conn, position, mark_price):
        """Execute liquidation"""
        logger.warning(f"LIQUIDATING position {position['id']}: {position['position_side']} {position['quantity']} {position['symbol']} @ Mark:{mark_price} Liq:{position['liquidation_price']}")
        
        # 1. Update position status to LIQUIDATED
        await conn.execute(text("""
            UPDATE futures.positions
            SET status = 'LIQUIDATED',
                closed_at = NOW(),
                updated_at = NOW()
            WHERE id = :id
        """), {"id": position['id']})
        
        # 2. Calculate loss (for isolated, loss = entire margin)
        loss = float(position['margin'])
        
        # 3. Update Account Balance
        # Isolated Margin: Margin is already deducted from available_balance and moved to margin_balance.
        # On liquidation: margin_balance is removed (lost), total_balance decreases by loss.
        await conn.execute(text("""
            UPDATE futures.accounts
            SET margin_balance = margin_balance - :margin,
                total_balance = total_balance - :loss,
                updated_at = NOW()
            WHERE id = :account_id
        """), {
            "account_id": position['account_id'],
            "margin": position['margin'],
            "loss": loss
        })
        
        # 4. Record Liquidation Event (Optional but good for history)
        # futures.trades에 청산 기록 추가
        await conn.execute(text("""
            INSERT INTO futures.trades (
                account_id, google_id, symbol, side, 
                quantity, price, realized_pnl, order_id
            ) VALUES (
                :account_id, :google_id, :symbol, :side,
                :quantity, :price, :pnl, NULL
            )
        """), {
            "account_id": position['account_id'],
            "google_id": position['google_id'],
            "symbol": position['symbol'],
            "side": "SELL" if position['position_side'] == "LONG" else "BUY",
            "quantity": position['quantity'],
            "price": mark_price,
            "pnl": -loss
        })
        
        logger.info(f"Position {position['id']} liquidated. Loss: {loss} USDT")

liquidation_monitor = LiquidationMonitor()
