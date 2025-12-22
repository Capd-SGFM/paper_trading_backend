from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from typing import Optional, List
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
    account_id: Optional[int] = None

@router.post("/")
async def place_order(order: OrderCreate, db: AsyncSession = Depends(get_async_db)):
    """
    주문 접수 API
    """
    try:
        # Leverage validation
        if order.leverage is None or order.leverage < 1 or order.leverage > 125:
            logger.error(f"Invalid leverage received: {order.leverage}")
            raise HTTPException(status_code=400, detail=f"Invalid leverage: {order.leverage}. Must be between 1 and 125")
        
        logger.info(f"Placing order: {order.side} {order.type} {order.quantity} {order.symbol} @ leverage={order.leverage}")
        
        # 1. 기본 검증
        if order.quantity <= 0:
            raise HTTPException(status_code=400, detail="Quantity must be positive")
        
        if order.type == "LIMIT" and (order.price is None or order.price <= 0):
            raise HTTPException(status_code=400, detail="Price is required for LIMIT orders")

        # 2. 사용자 계좌 확인
        account_id = order.account_id
        
        if not account_id:
            # account_id가 없으면 기본 계좌 조회 (기존 로직 유지)
            stmt_account = text("SELECT id FROM futures.accounts WHERE google_id = :google_id AND is_default = TRUE")
            result_account = await db.execute(stmt_account, {"google_id": order.google_id})
            account_id = result_account.scalar()

            if not account_id:
                # 계좌가 없으면 생성 (기본 자산 100,000 USDT 지급)
                logger.info(f"Creating new futures account for {order.google_id}")
                stmt_create_account = text("""
                    INSERT INTO futures.accounts (
                        google_id, account_name, is_default, 
                        total_balance, available_balance, margin_balance, unrealized_pnl,
                        created_at, updated_at
                    ) VALUES (
                        :google_id, 'Default Account', TRUE,
                        100000, 100000, 0, 0,
                        NOW(), NOW()
                    ) RETURNING id
                """)
                result_create = await db.execute(stmt_create_account, {"google_id": order.google_id})
                account_id = result_create.scalar()
                await db.commit() # 계좌 생성 확정
        
        # 계좌 유효성 검증 (입력받은 account_id가 해당 유저의 것인지)
        if order.account_id:
             stmt_verify = text("SELECT id FROM futures.accounts WHERE id = :id AND google_id = :google_id")
             result_verify = await db.execute(stmt_verify, {"id": account_id, "google_id": order.google_id})
             if not result_verify.scalar():
                 raise HTTPException(status_code=403, detail="Invalid account for this user")

        # 2.5 잔고 확인 및 증거금 차감 (간소화된 로직)
        # 필요 증거금 계산: (가격 * 수량) / 레버리지
        # 시장가 주문의 경우, 현재가(혹은 약간의 버퍼)를 기준으로 추산해야 함. 여기서는 order.price가 있으면 사용, 없으면 패스(실제로는 현재가 조회 필요)
        
        estimated_price = order.price
        if order.type == "MARKET":
            # 시장가 주문은 현재가를 알 수 없으므로, 매칭 엔진에서 체결 후 차감하거나, 
            # 여기서 현재가를 조회해서 가차감 해야 함. 
            # 일단은 시장가 주문은 잔고 검사를 건너뛰고 체결 후 정산하는 방식(Risk!) 혹은 
            # 매칭 엔진 호출 전에 Ticker를 조회해야 함.
            # 여기서는 간단히 구현하기 위해 시장가 주문은 잔고 검사 패스 (TODO: 개선 필요)
            pass
        elif estimated_price:
            required_margin = (estimated_price * order.quantity) / order.leverage
            
            # 잔고 조회
            stmt_balance = text("SELECT available_balance FROM futures.accounts WHERE id = :id")
            result_balance = await db.execute(stmt_balance, {"id": account_id})
            available_balance = result_balance.scalar()
            
            if available_balance < required_margin:
                raise HTTPException(status_code=400, detail="Insufficient balance")
            
            # 잔고 차감 (증거금 잠금)
            stmt_deduct = text("""
                UPDATE futures.accounts 
                SET available_balance = available_balance - :margin,
                    margin_balance = margin_balance + :margin,
                    updated_at = NOW()
                WHERE id = :id
            """)
            await db.execute(stmt_deduct, {"id": account_id, "margin": required_margin})
            # 주의: 여기서 commit하지 않고, 주문 생성과 함께 commit 함.


        # 3. 주문 생성 (PENDING)
        # position_side는 LONG/SHORT로 설정 (단방향 모드 가정: BUY->LONG, SELL->SHORT)
        position_side = "LONG" if order.side == "BUY" else "SHORT"

        stmt = text("""
            INSERT INTO futures.orders (
                account_id, google_id, symbol, side, order_type, 
                quantity, price, leverage, position_side, status, created_at, updated_at
            ) VALUES (
                :account_id, :google_id, :symbol, :side, :type, 
                :quantity, :price, :leverage, :position_side, 'PENDING', NOW(), NOW()
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
            "leverage": order.leverage,
            "position_side": position_side
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
                

                # 포지션 업데이트 (Netting Logic)
                # 1. 반대 방향 포지션 정리 (Netting)
                opposite_side = "SHORT" if order.side == "BUY" else "LONG"
                logger.info(f"Checking for existing position to net: Account={account_id}, Symbol={order.symbol}, Side={opposite_side}")
                
                stmt_check_pos = text("""
                    SELECT quantity, entry_price, margin, margin_type 
                    FROM futures.positions 
                    WHERE account_id = :account_id 
                      AND symbol = :symbol 
                      AND position_side = :opposite_side
                      AND quantity > 0
                """)
                result_pos = await db.execute(stmt_check_pos, {
                    "account_id": account_id,
                    "symbol": order.symbol,
                    "opposite_side": opposite_side
                })
                existing_pos = result_pos.fetchone()
                
                if existing_pos:
                    logger.info(f"Found existing position: Qty={existing_pos[0]}")
                else:
                    logger.info("No existing position found for netting")

                remaining_qty = execution_result['filled_qty']
                realized_pnl = 0

                if existing_pos:
                    pos_qty = float(existing_pos[0])
                    entry_price = float(existing_pos[1])
                    pos_margin = float(existing_pos[2])
                    margin_type = existing_pos[3]
                    
                    # PnL 계산 (Long 청산: Sell - Entry, Short 청산: Entry - Sell)
                    # Direction: Long(Buy) -> 1, Short(Sell) -> -1
                    # 기존 포지션이 Long이면 이번 주문은 Sell -> (Price - Entry)
                    # 기존 포지션이 Short이면 이번 주문은 Buy -> (Entry - Price)
                    pnl_direction = 1 if opposite_side == "LONG" else -1
                    
                    if remaining_qty >= pos_qty:
                        # 기존 포지션 전량 청산
                        close_qty = pos_qty
                        
                        await db.execute(text("""
                            DELETE FROM futures.positions 
                            WHERE account_id = :account_id 
                              AND symbol = :symbol 
                              AND position_side = :opposite_side
                        """), {
                            "account_id": account_id,
                            "symbol": order.symbol,
                            "opposite_side": opposite_side
                        })
                        
                        remaining_qty -= pos_qty
                        released_margin = pos_margin
                        
                    else:
                        # 기존 포지션 일부 청산
                        close_qty = remaining_qty
                        close_ratio = remaining_qty / pos_qty
                        released_margin = pos_margin * close_ratio
                        
                        await db.execute(text("""
                            UPDATE futures.positions 
                            SET quantity = quantity - :qty,
                                margin = margin - :released_margin,
                                updated_at = NOW()
                            WHERE account_id = :account_id 
                              AND symbol = :symbol 
                              AND position_side = :opposite_side
                        """), {
                            "account_id": account_id,
                            "symbol": order.symbol,
                            "opposite_side": opposite_side,
                            "qty": remaining_qty,
                            "released_margin": released_margin
                        })
                        
                        remaining_qty = 0

                    # PnL 계산 및 반영
                    current_pnl = (execution_result['avg_price'] - entry_price) * close_qty * pnl_direction
                    
                    # Isolated Margin Bankruptcy Protection
                    # 손실이 증거금을 초과하면, 손실을 증거금만큼으로 제한 (거래소 보험 기금 역할 - 여기서는 단순 탕감)
                    if margin_type == 'ISOLATED' and current_pnl < -released_margin:
                        logger.warning(f"Bankruptcy detected! PnL {current_pnl} exceeds Margin {released_margin}. Clamping loss.")
                        current_pnl = -released_margin

                    realized_pnl += current_pnl

                    # 증거금 반환 및 PnL 반영
                    await db.execute(text("""
                        UPDATE futures.accounts
                        SET available_balance = available_balance + :margin + :pnl,
                            margin_balance = margin_balance - :margin,
                            total_balance = total_balance + :pnl,
                            updated_at = NOW()
                        WHERE id = :account_id
                    """), {
                        "account_id": account_id,
                        "margin": released_margin,
                        "pnl": current_pnl
                    })

                # 2. 남은 수량이 있으면 신규 포지션 진입 (또는 기존 동일 방향 포지션 추가)
                if remaining_qty > 0:
                    position_side = "LONG" if order.side == "BUY" else "SHORT"
                    new_margin = (execution_result['avg_price'] * remaining_qty) / order.leverage
                    
                    # 청산가 계산 (Isolated Margin)
                    # Maintenance Margin Rate: 0.4% (0.004)
                    mmr = 0.004
                    entry_price = float(execution_result['avg_price'])
                    lev = float(order.leverage)
                    
                    if position_side == "LONG":
                        # Long: Entry * (1 - 1/Lev + MMR)
                        liquidation_price = entry_price * (1 - (1/lev) + mmr)
                    else:
                        # Short: Entry * (1 + 1/Lev - MMR)
                        liquidation_price = entry_price * (1 + (1/lev) - mmr)

                    await db.execute(text("""
                        INSERT INTO futures.positions (
                            account_id, google_id, symbol, position_side, 
                            quantity, initial_quantity, entry_price, leverage, margin,
                            liquidation_price, updated_at
                        ) VALUES (
                            :account_id, :google_id, :symbol, :position_side,
                            :quantity, :quantity, :price, :leverage, :margin,
                            :liquidation_price, NOW()
                        )
                        ON CONFLICT (account_id, symbol, position_side, status)
                        DO UPDATE SET 
                            quantity = futures.positions.quantity + EXCLUDED.quantity,
                            margin = futures.positions.margin + EXCLUDED.margin,
                            entry_price = (futures.positions.quantity * futures.positions.entry_price + EXCLUDED.quantity * EXCLUDED.entry_price) / (futures.positions.quantity + EXCLUDED.quantity),
                            -- 청산가는 평단가 변경에 따라 재계산 필요하지만, 단순화를 위해 신규 진입 시에는 기존 청산가 유지하거나 재계산 로직 필요
                            -- 여기서는 단순화를 위해 기존 값 유지 (정확한 로직은 평단가 변경 시 청산가도 재계산해야 함)
                            updated_at = NOW()
                    """), {
                        "account_id": account_id,
                        "google_id": order.google_id,
                        "symbol": order.symbol,
                        "position_side": position_side,
                        "quantity": remaining_qty,
                        "price": execution_result['avg_price'],
                        "leverage": order.leverage,
                        "margin": new_margin,
                        "liquidation_price": liquidation_price
                    })

                    # 시장가 주문 신규 진입분 증거금 차감
                    if order.type == "MARKET":
                        stmt_deduct_market = text("""
                            UPDATE futures.accounts 
                            SET available_balance = available_balance - :margin,
                                margin_balance = margin_balance + :margin,
                                updated_at = NOW()
                            WHERE id = :id
                        """)
                        await db.execute(stmt_deduct_market, {"id": account_id, "margin": new_margin})

                # 3. 거래 내역 생성 (Trades)
                await db.execute(text("""
                    INSERT INTO futures.trades (
                        order_id, account_id, google_id, symbol, side, 
                        quantity, price, realized_pnl
                    ) VALUES (
                        :order_id, :account_id, :google_id, :symbol, :side,
                        :quantity, :price, :pnl
                    )
                """), {
                    "order_id": order_id,
                    "account_id": account_id,
                    "google_id": order.google_id,
                    "symbol": order.symbol,
                    "side": order.side,
                    "quantity": execution_result['filled_qty'],
                    "price": execution_result['avg_price'],
                    "pnl": realized_pnl
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

@router.get("/", response_model=List[dict])
async def get_orders(
    google_id: str, 
    account_id: Optional[int] = None, 
    status: Optional[str] = None,
    db: AsyncSession = Depends(get_async_db)
):
    """
    주문 내역 조회
    """
    try:
        query = "SELECT * FROM futures.orders WHERE google_id = :google_id"
        params = {"google_id": google_id}

        if account_id:
            query += " AND account_id = :account_id"
            params["account_id"] = account_id
        
        if status:
            if status == 'OPEN':
                query += " AND status IN ('PENDING', 'PARTIALLY_FILLED')"
            elif status == 'HISTORY':
                query += " AND status IN ('FILLED', 'CANCELLED', 'REJECTED', 'EXPIRED')"
            else:
                query += " AND status = :status"
                params["status"] = status
        
        query += " ORDER BY created_at DESC LIMIT 100"
        
        result = await db.execute(text(query), params)
        orders = result.mappings().all()
        
        return [dict(order) for order in orders]
    except Exception as e:
        logger.error(f"Get orders failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/{order_id}")
async def cancel_order(
    order_id: int, 
    google_id: str, 
    db: AsyncSession = Depends(get_async_db)
):
    """
    주문 취소 API
    """
    try:
        # 1. 주문 조회
        stmt = text("SELECT * FROM futures.orders WHERE id = :order_id AND google_id = :google_id")
        result = await db.execute(stmt, {"order_id": order_id, "google_id": google_id})
        order = result.mappings().first()

        if not order:
            raise HTTPException(status_code=404, detail="Order not found")

        # 2. 상태 확인 (PENDING만 취소 가능)
        if order['status'] not in ['PENDING', 'PARTIALLY_FILLED']:
            raise HTTPException(status_code=400, detail="Cannot cancel order in current status")

        # 3. 주문 상태 업데이트 (CANCELLED)
        await db.execute(text("""
            UPDATE futures.orders 
            SET status = 'CANCELLED', updated_at = NOW() 
            WHERE id = :order_id
        """), {"order_id": order_id})

        # 4. 환불 처리 (LIMIT 주문인 경우)
        if order['order_type'] == 'LIMIT':
            # 묶여있던 증거금 계산 (전량 취소 가정)
            # PARTIALLY_FILLED가 있다면 남은 수량만큼만 환불해야 하지만, 현재 매칭엔진은 전량 체결만 지원하므로 전체 환불
            refund_margin = (order['price'] * order['quantity']) / order['leverage']
            
            await db.execute(text("""
                UPDATE futures.accounts 
                SET available_balance = available_balance + :margin,
                    margin_balance = margin_balance - :margin,
                    updated_at = NOW()
                WHERE id = :account_id
            """), {"account_id": order['account_id'], "margin": refund_margin})

        await db.commit()
        return {"message": "Order cancelled successfully"}

    except HTTPException as he:
        raise he
    except Exception as e:
        await db.rollback()
        logger.error(f"Cancel order failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))
