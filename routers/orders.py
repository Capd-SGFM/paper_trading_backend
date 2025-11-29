from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from typing import Optional
from sqlalchemy import text
from database import get_async_db
from sqlalchemy.ext.asyncio import AsyncSession
from matching_engine import matching_engine
import logging

router = APIRouter(prefix="/orders", tags=["orders"])
logger = logging.getLogger("OrderRouter")

class OrderCreate(BaseModel):
    symbol: str
    side: str  # BUY, SELL
    type: str  # MARKET, LIMIT
    quantity: float
    price: Optional[float] = None
    leverage: int = 1
    google_id: str

@router.post("/")
async def place_order(order: OrderCreate, db: AsyncSession = Depends(get_async_db)):
    """
    주문 접수 API
    """
    try:
        # 1. 기본 검증
        if order.quantity <= 0:
            raise HTTPException(status_code=400, detail="Quantity must be positive")
        
        if order.type == "LIMIT" and (order.price is None or order.price <= 0):
            raise HTTPException(status_code=400, detail="Price is required for LIMIT orders")

        # 2. 사용자 계좌 확인 및 생성 (Auto-create default account)
        # futures.accounts에서 google_id로 조회
        stmt_account = text("SELECT id FROM futures.accounts WHERE google_id = :google_id AND is_default = TRUE")
        result_account = await db.execute(stmt_account, {"google_id": order.google_id})
        account_id = result_account.scalar()

        if not account_id:
            # 계좌가 없으면 생성 (기본 자산 100,000 USDT 지급)
            logger.info(f"Creating new futures account for {order.google_id}")
            stmt_create_account = text("""
                INSERT INTO futures.accounts (
                    google_id, account_name, is_default, 
                    total_balance, available_balance, margin_balance, unrealized_pnl
                ) VALUES (
                    :google_id, 'Default Account', TRUE,
                    100000, 100000, 0, 0
                ) RETURNING id
            """)
            result_create = await db.execute(stmt_create_account, {"google_id": order.google_id})
            account_id = result_create.scalar()
            await db.commit() # 계좌 생성 확정

        # 3. 주문 생성 (PENDING)
        stmt = text("""
            INSERT INTO futures.orders (
                account_id, google_id, symbol, side, order_type, 
                quantity, price, leverage, status, created_at, updated_at
            ) VALUES (
                :account_id, :google_id, :symbol, :side, :type, 
                :quantity, :price, :leverage, 'PENDING', NOW(), NOW()
            ) RETURNING id
        """)
        
        result = await db.execute(stmt, {
            "account_id": account_id,
            "google_id": order.google_id,
            "symbol": order.symbol,
            "side": order.side,
            "type": order.type,
            "quantity": order.quantity,
            "price": order.price,
            "leverage": order.leverage
        })
        order_id = result.scalar()
        await db.commit()

        # 4. 시장가 주문 즉시 체결 시도
        if order.type == "MARKET":
            execution_result = await matching_engine.process_market_order(
                order.symbol, order.side, order.quantity
            )
            
            if execution_result['filled_qty'] > 0:
                # 주문 상태 업데이트 (FILLED)
                await db.execute(text("""
                    UPDATE futures.orders
                    SET status = 'FILLED',
                        executed_quantity = :filled_qty,
                        avg_price = :avg_price,
                        filled_at = NOW(),
                        updated_at = NOW()
                    WHERE id = :order_id
                """), {
                    "filled_qty": execution_result['filled_qty'],
                    "avg_price": execution_result['avg_price'],
                    "order_id": order_id
                })
                
                # 체결 내역 생성 (Trades)
                await db.execute(text("""
                    INSERT INTO futures.trades (
                        order_id, account_id, google_id, symbol, side, 
                        quantity, price
                    ) VALUES (
                        :order_id, :account_id, :google_id, :symbol, :side,
                        :quantity, :price
                    )
                """), {
                    "order_id": order_id,
                    "account_id": account_id,
                    "google_id": order.google_id,
                    "symbol": order.symbol,
                    "side": order.side,
                    "quantity": execution_result['filled_qty'],
                    "price": execution_result['avg_price']
                })
                
                # 포지션 업데이트 (Upsert)
                # position_side는 일단 LONG/SHORT와 동일하게 가정 (단방향 모드)
                position_side = "LONG" if order.side == "BUY" else "SHORT"
                
                # 간단한 포지션 로직: 매수면 수량 증가, 매도면 수량 감소 (단순화)
                # 실제로는 양방향 모드, 청산 등을 고려해야 함. 여기서는 단순히 레코드 생성만.
                await db.execute(text("""
                    INSERT INTO futures.positions (
                        account_id, google_id, symbol, position_side, 
                        quantity, initial_quantity, entry_price, leverage, margin,
                        updated_at
                    ) VALUES (
                        :account_id, :google_id, :symbol, :position_side,
                        :quantity, :quantity, :price, :leverage, :margin,
                        NOW()
                    )
                    ON CONFLICT (account_id, symbol, position_side, status)
                    DO UPDATE SET 
                        quantity = futures.positions.quantity + EXCLUDED.quantity,
                        updated_at = NOW()
                """), {
                    "account_id": account_id,
                    "google_id": order.google_id,
                    "symbol": order.symbol,
                    "position_side": position_side,
                    "quantity": execution_result['filled_qty'],
                    "price": execution_result['avg_price'],
                    "leverage": order.leverage,
                    "margin": (execution_result['avg_price'] * execution_result['filled_qty']) / order.leverage
                })

                await db.commit()
                
                return {
                    "order_id": order_id, 
                    "status": "FILLED", 
                    "avg_price": execution_result['avg_price']
                }

        return {"order_id": order_id, "status": "PENDING"}

    except Exception as e:
        await db.rollback()
        logger.error(f"Order placement failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))
