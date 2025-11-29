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
            # 1. 매수 주문 (Long) 체결 확인
            # 조건: 주문가 >= 현재가 (싸게 샀거나 그 가격에 도달)
            # PENDING 상태인 주문만 조회
            # TODO: futures.orders 테이블이 아직 없으므로 스키마 확인 필요
            # 임시로 futures.orders 테이블이 있다고 가정하고 작성
            
            # 매수 주문 체결 (지정가 >= 현재가)
            # 예: 50000에 매수 걸었는데 49900에 거래됨 -> 체결
            await conn.execute(text("""
                UPDATE futures.orders
                SET status = 'FILLED',
                    filled_quantity = quantity,
                    avg_price = :price,
                    updated_at = NOW()
                WHERE symbol = :symbol 
                  AND side = 'BUY' 
                  AND status = 'PENDING'
                  AND price >= :price
            """), {"symbol": symbol, "price": current_price})

            # 2. 매도 주문 (Short) 체결 확인
            # 조건: 주문가 <= 현재가 (비싸게 팔았거나 그 가격에 도달)
            # 예: 50000에 매도 걸었는데 50100에 거래됨 -> 체결
            await conn.execute(text("""
                UPDATE futures.orders
                SET status = 'FILLED',
                    filled_quantity = quantity,
                    avg_price = :price,
                    updated_at = NOW()
                WHERE symbol = :symbol 
                  AND side = 'SELL' 
                  AND status = 'PENDING'
                  AND price <= :price
            """), {"symbol": symbol, "price": current_price})
            
            # logger.info(f"Processed limit orders for {symbol} at {current_price}")

# 싱글톤 인스턴스
matching_engine = MatchingEngine()
