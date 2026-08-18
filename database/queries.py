import logging
from datetime import datetime, timedelta, timezone
from typing import Sequence

from sqlalchemy import select, update, or_
from sqlalchemy.ext.asyncio import AsyncSession

from .models import Category, Account, Order, OrderStatus

logger = logging.getLogger(__name__)

async def add_new_category(session: AsyncSession, name: str) -> Category:
    """
    Inserts a new category into the database safely.
    
    Args:
        session (AsyncSession): The active asynchronous database session.
        name (str): The name of the new category.
        
    Returns:
        Category: The newly created category object.
    """
    new_category = Category(name=name)
    try:
        session.add(new_category)
        await session.commit()
        await session.refresh(new_category)
        return new_category
    except Exception as e:
        await session.rollback()
        logger.error(f"Failed to add new category '{name}': {e}")
        raise


async def get_available_accounts(
    session: AsyncSession, category_id: int, limit: int
) -> Sequence[Account]:
    """
    Fetches active, unbanned accounts for a specific category that are not 
    currently under a FloodWait penalty.
    
    Args:
        session (AsyncSession): The active asynchronous database session.
        category_id (int): The ID of the category.
        limit (int): Maximum number of accounts to retrieve.
        
    Returns:
        Sequence[Account]: A list of available account objects.
    """
    now = datetime.now(timezone.utc)
    
    stmt = (
        select(Account)
        .where(
            Account.category_id == category_id,
            Account.is_banned == False,
            # Account is available if flood_wait is NULL or the penalty time has passed
            or_(
                Account.flood_wait_until.is_(None),
                Account.flood_wait_until <= now
            )
        )
        .limit(limit)
    )
    
    try:
        result = await session.execute(stmt)
        return result.scalars().all()
    except Exception as e:
        logger.error(f"Failed to fetch available accounts for category_id {category_id}: {e}")
        raise


async def update_account_flood_wait(
    session: AsyncSession, account_id: int, penalty_hours: int
) -> None:
    """
    Applies a FloodWait penalty timestamp to a specific account.
    
    Args:
        session (AsyncSession): The active asynchronous database session.
        account_id (int): The ID of the affected account.
        penalty_hours (int): The duration of the penalty in hours.
    """
    penalty_time = datetime.now(timezone.utc) + timedelta(hours=penalty_hours)
    
    stmt = (
        update(Account)
        .where(Account.id == account_id)
        .values(flood_wait_until=penalty_time)
    )
    
    try:
        await session.execute(stmt)
        await session.commit()
    except Exception as e:
        await session.rollback()
        logger.error(f"Failed to update flood wait for account_id {account_id}: {e}")
        raise


async def create_new_order(
    session: AsyncSession, category_id: int, order_type: str, target_data: str
) -> Order:
    """
    Inserts a new bulk sending or extraction order with 'pending' status.
    
    Args:
        session (AsyncSession): The active asynchronous database session.
        category_id (int): The ID of the category executing the order.
        order_type (str): The type of order (e.g., 'bulk_send', 'extract').
        target_data (str): Target information (e.g., group link or user list).
        
    Returns:
        Order: The newly created order object.
    """
    new_order = Order(
        category_id=category_id,
        order_type=order_type,
        target_data=target_data,
        status=OrderStatus.pending
    )
    
    try:
        session.add(new_order)
        await session.commit()
        await session.refresh(new_order)
        return new_order
    except Exception as e:
        await session.rollback()
        logger.error(f"Failed to create new order for category_id {category_id}: {e}")
        raise