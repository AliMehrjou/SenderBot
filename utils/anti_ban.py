import re
import random
import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from database.models import Account, GlobalSettings

logger = logging.getLogger(__name__)

def parse_spintax(text: str) -> str:
    """
    Parses Spintax formatted strings to ensure message uniqueness.
    Example: "{Hello|Hi} there, {how are you|how is it going}?"
    Dynamically supports nested Spintax blocks.
    """
    pattern = re.compile(r'\{([^{}]+)\}')
    
    while True:
        match = pattern.search(text)
        if not match:
            break
            
        options = match.group(1).split('|')
        choice = random.choice(options)
        
        text = text[:match.start()] + choice + text[match.end():]
        
    return text

async def apply_adaptive_flood_wait(
    session: AsyncSession, 
    account_id: int, 
    wait_seconds: int
) -> None:
    """
    محاسبه و اعمال جریمه زمانی تطبیقی.
    اصلاح شده: حذف session.commit() برای جلوگیری از نشت تراکنش و 
    بهم ریختن Batch Commit در ورکر اصلی.
    """
    if wait_seconds < 20:
        penalty_hours = 1
    elif 20 <= wait_seconds <= 60:
        penalty_hours = 6
    else:
        stmt = select(GlobalSettings).where(GlobalSettings.id == 1)
        result = await session.execute(stmt)
        settings = result.scalar_one_or_none()
        
        penalty_days = settings.spam_penalty_days if settings else 1
        penalty_hours = penalty_days * 24

    penalty_time = datetime.now(timezone.utc) + timedelta(hours=penalty_hours)
    
    try:
        update_stmt = (
            update(Account)
            .where(Account.id == account_id)
            .values(flood_wait_until=penalty_time)
        )
        # فقط کوئری را اجرا می‌کنیم. کامیت نهایی بر عهده تابع صدازننده است
        await session.execute(update_stmt)
        
        logger.warning(
            f"Account user_{account_id}/ hit FloodWait ({wait_seconds}s). "
            f"Locked for {penalty_hours} hours until {penalty_time}."
        )
    except Exception as e:
        logger.error(f"Failed to apply flood wait penalty for user_{account_id}/: {e}")
        raise