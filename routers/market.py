from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from database import DBConnectionManager

router = APIRouter(prefix="/market", tags=["market"])

async def get_async_db():
    async_engine = DBConnectionManager.get_async_engine()
    async with AsyncSession(async_engine) as session:
        yield session

@router.get("/leverage-brackets/{symbol}")
async def get_leverage_brackets(symbol: str, db: AsyncSession = Depends(get_async_db)):
    """
    특정 심볼의 레버리지 브래킷 정보 조회
    """
    try:
        stmt = text("""
            SELECT bracket_id, initial_leverage, max_notional, min_notional, maint_margin_rate
            FROM futures.leverage_brackets
            WHERE symbol = :symbol
            ORDER BY bracket_id ASC
        """)
        result = await db.execute(stmt, {"symbol": symbol})
        rows = result.fetchall()
        
        if not rows:
            # 데이터가 없으면 빈 리스트 반환 (또는 404)
            return []

        brackets = [
            {
                "bracket_id": row.bracket_id,
                "initial_leverage": row.initial_leverage,
                "max_notional": float(row.max_notional),
                "min_notional": float(row.min_notional),
                "maint_margin_rate": float(row.maint_margin_rate)
            }
            for row in rows
        ]
        return brackets

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
