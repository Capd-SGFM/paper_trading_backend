from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from typing import List, Optional
from sqlalchemy import text
from database import get_async_db
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime
import logging

router = APIRouter(prefix="/accounts", tags=["accounts"])
logger = logging.getLogger("AccountRouter")

class AccountCreate(BaseModel):
    google_id: str
    account_name: str
    initial_balance: float

class AccountResponse(BaseModel):
    id: int
    google_id: str
    account_name: str
    is_default: bool
    total_balance: float
    available_balance: float
    margin_balance: float
    unrealized_pnl: float
    total_pnl: float
    created_at: datetime

    class Config:
        orm_mode = True

class PositionResponse(BaseModel):
    id: int
    symbol: str
    position_side: str
    quantity: float
    entry_price: float
    leverage: int
    margin: float
    unrealized_pnl: float
    roe_percent: float
    liquidation_price: Optional[float]

@router.post("/", response_model=AccountResponse)
async def create_account(account: AccountCreate, db: AsyncSession = Depends(get_async_db)):
    """
    새로운 서브 계좌 생성
    """
    try:
        # 최대 계좌 수 제한 확인 (예: 5개)
        stmt_count = text("SELECT COUNT(*) FROM futures.accounts WHERE google_id = :google_id")
        result_count = await db.execute(stmt_count, {"google_id": account.google_id})
        count = result_count.scalar()
        
        if count >= 5:
            raise HTTPException(status_code=400, detail="Maximum number of accounts (5) reached")

        # 이름 중복 확인
        stmt_check = text("SELECT id FROM futures.accounts WHERE google_id = :google_id AND account_name = :account_name")
        result_check = await db.execute(stmt_check, {"google_id": account.google_id, "account_name": account.account_name})
        if result_check.scalar():
            raise HTTPException(status_code=400, detail="Account name already exists")

        # 계좌 생성
        stmt = text("""
            INSERT INTO futures.accounts (
                google_id, account_name, is_default, 
                total_balance, available_balance, margin_balance, unrealized_pnl,
                created_at, updated_at
            ) VALUES (
                :google_id, :account_name, FALSE,
                :balance, :balance, 0, 0,
                NOW(), NOW()
            ) RETURNING id, google_id, account_name, is_default, total_balance, available_balance, margin_balance, unrealized_pnl, total_pnl, created_at
        """)
        
        result = await db.execute(stmt, {
            "google_id": account.google_id,
            "account_name": account.account_name,
            "balance": account.initial_balance
        })
        new_account = result.mappings().one()
        await db.commit()
        
        return dict(new_account)

    except HTTPException as he:
        raise he
    except Exception as e:
        await db.rollback()
        logger.error(f"Account creation failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{google_id}", response_model=List[AccountResponse])
async def get_accounts(google_id: str, db: AsyncSession = Depends(get_async_db)):
    """
    사용자의 모든 계좌 조회
    """
    try:
        stmt = text("""
            SELECT id, google_id, account_name, is_default, 
                   total_balance, available_balance, margin_balance, unrealized_pnl, total_pnl, created_at
            FROM futures.accounts
            WHERE google_id = :google_id
            ORDER BY is_default DESC, created_at ASC
        """)
        result = await db.execute(stmt, {"google_id": google_id})
        accounts = result.mappings().all()
        
        # 계좌가 하나도 없으면 기본 계좌 자동 생성 (기존 로직 호환성)
        if not accounts:
             logger.info(f"No accounts found for {google_id}, creating default account")
             stmt_create = text("""
                INSERT INTO futures.accounts (
                    google_id, account_name, is_default, 
                    total_balance, available_balance, margin_balance, unrealized_pnl,
                    created_at, updated_at
                ) VALUES (
                    :google_id, 'Default Account', TRUE,
                    100000, 100000, 0, 0,
                    NOW(), NOW()
                ) RETURNING id, google_id, account_name, is_default, total_balance, available_balance, margin_balance, unrealized_pnl, total_pnl, created_at
            """)
             result_create = await db.execute(stmt_create, {"google_id": google_id})
             new_account = result_create.mappings().one()
             await db.commit()
             return [dict(new_account)]

        return [dict(acc) for acc in accounts]
    except Exception as e:
        logger.error(f"Get accounts failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{account_id}/positions", response_model=List[PositionResponse])
async def get_account_positions(account_id: int, db: AsyncSession = Depends(get_async_db)):
    """
    특정 계좌의 열린 포지션 조회
    """
    try:
        stmt = text("""
            SELECT id, symbol, position_side, quantity, entry_price, leverage, 
                   margin, unrealized_pnl, roe_percent, liquidation_price
            FROM futures.positions
            WHERE account_id = :account_id AND status = 'OPEN' AND quantity > 0
        """)
        result = await db.execute(stmt, {"account_id": account_id})
        positions = result.mappings().all()
        return [dict(pos) for pos in positions]
    except Exception as e:
        logger.error(f"Get positions failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/{account_id}")
async def delete_account(account_id: int, db: AsyncSession = Depends(get_async_db)):
    """
    계좌 삭제 (관련 포지션 및 주문은 DB Cascade 설정에 의해 자동 삭제됨)
    """
    try:
        # 계좌 존재 확인
        stmt_check = text("SELECT id FROM futures.accounts WHERE id = :account_id")
        result_check = await db.execute(stmt_check, {"account_id": account_id})
        if not result_check.scalar():
            raise HTTPException(status_code=404, detail="Account not found")

        # 계좌 삭제
        stmt_delete = text("DELETE FROM futures.accounts WHERE id = :account_id")
        await db.execute(stmt_delete, {"account_id": account_id})
        await db.commit()
        
        return {"message": "Account deleted successfully"}
    except HTTPException as he:
        raise he
    except Exception as e:
        await db.rollback()
        logger.error(f"Delete account failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))
