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
    اصلاح شده: محاسبه جریمه‌ها بر حسب ثانیه برای حداکثر شدن توان خروجی.
    """
    # 🛡 اصلاح ساختاری: تفکیک جریمه‌های استاندارد FloodWait از جریمه‌های سنگین (اسپم/PeerFlood)
    MIN_PEERFLOOD_PENALTY = 3600      # حداقل ۱ ساعت برای خطاهای شدید
    MAX_PENALTY_SECONDS = 30 * 86400  # سقف منطقی ۳۰ روز

    if wait_seconds > 1800:
        # اگر ورودی بیش از ۳۰ دقیقه است، یعنی یک جریمه قطعی اسپم/محدودیت است (نه FloodWait ساده)
        penalty_seconds = min(max(wait_seconds, MIN_PEERFLOOD_PENALTY), MAX_PENALTY_SECONDS)
    elif wait_seconds < 20:
        penalty_seconds = max(wait_seconds + 30, 120)      
    elif wait_seconds <= 60:
        penalty_seconds = max(wait_seconds * 2, 300)       
    else:
        penalty_seconds = min(wait_seconds * 3, 1800)

    penalty_time = datetime.now(timezone.utc) + timedelta(seconds=penalty_seconds)
    
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
            f"Locked for {penalty_seconds} seconds until {penalty_time}."
        )
    except Exception as e:
        logger.error(f"Failed to apply flood wait penalty for user_{account_id}/: {e}")
        raise