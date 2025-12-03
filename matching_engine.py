import logging
from typing import Dict, List, Optional, Tuple
from sqlalchemy import text
from database import DBConnectionManager

logger = logging.getLogger("MatchingEngine")

class MatchingEngine:
    async def process_market_order(self, symbol: str, side: str, quantity: float) -> Dict:
        """
        시장가 주문 체결 시뮬레이션 (Slippage 계산)
        :param symbol: 종목 코드
        :param side: 주문 방향 (BUY/SELL)
        :param quantity: 주문 수량
        :return: 체결 결과 {avg_price, filled_qty, total_cost}
        """
        # 시장가 매수(BUY) -> 매도 호가(ASK)를 긁음
        # 시장가 매도(SELL) -> 매수 호가(BID)를 긁음
        target_side = 'ASK' if side == 'BUY' else 'BID'
        
        # 정렬 순서: 매수 호가는 높은 가격부터(DESC), 매도 호가는 낮은 가격부터(ASC)
        order_by = "ASC" if target_side == 'ASK' else "DESC"

        async_engine = DBConnectionManager.get_async_engine()
        async with async_engine.connect() as conn:
            # 호가창 조회
            stmt = text(f"""
                SELECT price, quantity 
                FROM futures.orderbook 
                WHERE symbol = :symbol AND side = :side 
                ORDER BY price {order_by}
                LIMIT 100
            """)
            result = await conn.execute(stmt, {"symbol": symbol, "side": target_side})
            orderbook = result.fetchall()

        if not orderbook:
            logger.warning(f"No liquidity for {symbol} {side}")
            return {"avg_price": 0, "filled_qty": 0, "total_cost": 0}

        remaining_qty = quantity
        total_cost = 0.0
        filled_qty = 0.0

        for price, qty in orderbook:
            price = float(price)
            qty = float(qty)

            if remaining_qty <= 0:
                break

            # 체결 가능한 수량 계산
            trade_qty = min(remaining_qty, qty)
            
            total_cost += price * trade_qty
            filled_qty += trade_qty
            remaining_qty -= trade_qty

        avg_price = total_cost / filled_qty if filled_qty > 0 else 0

        return {
            "avg_price": avg_price,
            "filled_qty": filled_qty,
            "total_cost": total_cost
        }

    async def process_limit_orders(self, trade_data: Dict):
        """
        실시간 체결 내역을 기반으로 지정가 주문 체결 처리
        :param trade_data: {symbol, price, quantity, side, ...}
        """
        symbol = trade_data['symbol']
        current_price = float(trade_data['price'])
        
        async_engine = DBConnectionManager.get_async_engine()
        async with async_engine.begin() as conn:
            # 1. 체결 가능한 주문 조회 (PENDING 상태)
            # 매수(BUY): 주문가 >= 현재가
            # 매도(SELL): 주문가 <= 현재가
            stmt = text("""
                SELECT id, account_id, google_id, symbol, side, quantity, price, leverage
                FROM futures.orders
                WHERE symbol = :symbol 
                  AND status = 'PENDING'
                  AND (
                    (side = 'BUY' AND price >= :price) OR
                    (side = 'SELL' AND price <= :price)
                  )
                FOR UPDATE SKIP LOCKED
            """)
            result = await conn.execute(stmt, {"symbol": symbol, "price": current_price})
            orders = result.mappings().all()

            if not orders:
                return

            for order in orders:
                # 2. 주문 상태 업데이트 (FILLED)
                await conn.execute(text("""
                    UPDATE futures.orders
                    SET status = 'FILLED',
                        executed_quantity = quantity,
                        avg_price = :price,
                        updated_at = NOW()
                    WHERE id = :order_id
                """), {"order_id": order['id'], "price": order['price']}) # 체결가는 주문가(Limit Price)로 가정 (더 유리한 가격 체결은 추후 구현)

                # 3. 체결 내역 생성 (Trades)
                await conn.execute(text("""
                    INSERT INTO futures.trades (
                        order_id, account_id, google_id, symbol, side, 
                        quantity, price
                    ) VALUES (
                        :order_id, :account_id, :google_id, :symbol, :side,
                        :quantity, :price
                    )
                """), {
                    "order_id": order['id'],
                    "account_id": order['account_id'],
                    "google_id": order['google_id'],
                    "symbol": order['symbol'],
                    "side": order['side'],
                    "quantity": order['quantity'],
                    "price": order['price']
                })

                # 4. 포지션 업데이트 (Netting Logic)
                opposite_side = "SHORT" if order['side'] == "BUY" else "LONG"
                
                # 반대 방향 포지션 조회
                stmt_check_pos = text("""
                    SELECT quantity, entry_price, margin 
                    FROM futures.positions 
                    WHERE account_id = :account_id 
                      AND symbol = :symbol 
                      AND position_side = :opposite_side
                      AND quantity > 0
                """)
                result_pos = await conn.execute(stmt_check_pos, {
                    "account_id": order['account_id'],
                    "symbol": order['symbol'],
                    "opposite_side": opposite_side
                })
                existing_pos = result_pos.fetchone()

                remaining_qty = float(order['quantity'])
                realized_pnl = 0
                
                if existing_pos:
                    pos_qty = float(existing_pos[0])
                    entry_price = float(existing_pos[1])
                    pos_margin = float(existing_pos[2])
                    
                    # PnL 계산
                    pnl_direction = 1 if opposite_side == "LONG" else -1
                    
                    if remaining_qty >= pos_qty:
                        # 기존 포지션 전량 청산
                        close_qty = pos_qty
                        
                        await conn.execute(text("""
                            DELETE FROM futures.positions 
                            WHERE account_id = :account_id 
                              AND symbol = :symbol 
                              AND position_side = :opposite_side
                        """), {
                            "account_id": order['account_id'],
                            "symbol": order['symbol'],
                            "opposite_side": opposite_side
                        })
                        
                        remaining_qty -= pos_qty
                        released_margin = pos_margin

                    else:
                        # 기존 포지션 일부 청산
                        close_qty = remaining_qty
                        close_ratio = remaining_qty / pos_qty
                        released_margin = pos_margin * close_ratio
                        
                        await conn.execute(text("""
                            UPDATE futures.positions 
                            SET quantity = quantity - :qty,
                                margin = margin - :released_margin,
                                updated_at = NOW()
                            WHERE account_id = :account_id 
                              AND symbol = :symbol 
                              AND position_side = :opposite_side
                        """), {
                            "account_id": order['account_id'],
                            "symbol": order['symbol'],
                            "opposite_side": opposite_side,
                            "qty": remaining_qty,
                            "released_margin": released_margin
                        })
                        
                        remaining_qty = 0

                    # PnL 계산 및 반영
                    current_pnl = (float(order['price']) - entry_price) * close_qty * pnl_direction
                    realized_pnl += current_pnl
                    
                    # 증거금 반환 및 PnL 반영
                    await conn.execute(text("""
                        UPDATE futures.accounts
                        SET available_balance = available_balance + :margin + :pnl,
                            margin_balance = margin_balance - :margin,
                            total_balance = total_balance + :pnl,
                            updated_at = NOW()
                        WHERE id = :account_id
                    """), {
                        "account_id": order['account_id'],
                        "margin": released_margin,
                        "pnl": current_pnl
                    })
                    
                    # 거래 내역에 PnL 업데이트
                    await conn.execute(text("""
                        UPDATE futures.trades
                        SET realized_pnl = :pnl
                        WHERE order_id = :order_id
                    """), {
                        "pnl": realized_pnl,
                        "order_id": order['id']
                    })

                # 남은 수량이 있으면 신규 포지션 진입
                if remaining_qty > 0:
                    position_side = "LONG" if order['side'] == "BUY" else "SHORT"
                    margin = (float(order['price']) * remaining_qty) / order['leverage']

                    await conn.execute(text("""
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
                            margin = futures.positions.margin + EXCLUDED.margin,
                            entry_price = (futures.positions.quantity * futures.positions.entry_price + EXCLUDED.quantity * EXCLUDED.entry_price) / (futures.positions.quantity + EXCLUDED.quantity),
                            updated_at = NOW()
                    """), {
                        "account_id": order['account_id'],
                        "google_id": order['google_id'],
                        "symbol": order['symbol'],
                        "position_side": position_side,
                        "quantity": remaining_qty,
                        "price": order['price'],
                        "leverage": order['leverage'],
                        "margin": margin
                    })

                
                logger.info(f"Limit order filled: {order['id']}")
            
            # logger.info(f"Processed limit orders for {symbol} at {current_price}")

# 싱글톤 인스턴스
matching_engine = MatchingEngine()
