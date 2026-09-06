import time
import re
from typing import Callable, Dict, Any, Awaitable, Tuple

from aiogram import BaseMiddleware
from aiogram.types import TelegramObject, Message, CallbackQuery
from sqlalchemy import select
from config import config
from database.models import Admin, GlobalSettings

_role_cache: Dict[int, Tuple[bool, float]] = {}
_last_reject_at: Dict[int, float] = {}

_TRACKING_QUERY_RE = re.compile(r"^(?:/gtg_)?((?:ORD|EXT)-[A-Za-z0-9]{1,20})$", re.IGNORECASE)
_public_access_cache: Tuple[bool, float] = (False, 0.0)

def _role_cache_ttl() -> int:
    try:
        ttl = int(getattr(config, "ADMIN_ROLE_CACHE_TTL", 120))
    except (TypeError, ValueError):
        ttl = 120
    return max(60, min(ttl, 300))

class AdminMiddleware(BaseMiddleware):
    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any]
    ) -> Any:

        user = data.get("event_from_user")
        if not user:
            return

        if user.id == config.ADMIN_ID:
            return await handler(event, data)

        if await self._is_sub_admin(user.id, data.get("session")):
            return await handler(event, data)

        # B10a: مسیر دسترسی عمومی برای استعلام کد رهگیری
        if (isinstance(event, Message) and event.text
                and _TRACKING_QUERY_RE.fullmatch(event.text.strip())
                and await self._public_order_access_enabled(data.get("session"))):
            return await handler(event, data)

        now = time.monotonic()
        cooldown = max(0, int(getattr(config, "ADMIN_REJECT_REPLY_COOLDOWN", 3600)))
        if now - _last_reject_at.get(user.id, 0.0) >= cooldown:
            _last_reject_at[user.id] = now
            if isinstance(event, Message):
                await event.answer("⛔️ <b>دسترسی غیرمجاز.</b> شما ادمین این سیستم نیستید.")
            elif isinstance(event, CallbackQuery):
                await event.answer("⛔️ دسترسی غیرمجاز.", show_alert=True)
            if len(_last_reject_at) > 4096:
                for uid in [k for k, ts in _last_reject_at.items() if now - ts >= cooldown]:
                    _last_reject_at.pop(uid, None)
        elif isinstance(event, CallbackQuery):
            try:
                await event.answer()
            except Exception:
                pass
        return

    async def _is_sub_admin(self, telegram_id: int, session) -> bool:
        now = time.monotonic()
        cached = _role_cache.get(telegram_id)
        if cached is not None and (now - cached[1]) < _role_cache_ttl():
            return cached[0]

        if session is None:
            return False

        try:
            stmt = select(Admin.telegram_id).where(Admin.telegram_id == telegram_id)
            result = await session.execute(stmt)
            is_admin = result.scalar_one_or_none() is not None
        except Exception:
            if cached is not None:
                return cached[0]
            return False

        _role_cache[telegram_id] = (is_admin, now)
        if len(_role_cache) > 1024:
            ttl = _role_cache_ttl()
            for uid in [k for k, (_, ts) in _role_cache.items() if now - ts >= ttl]:
                _role_cache.pop(uid, None)
        return is_admin

    async def _public_order_access_enabled(self, session) -> bool:
        global _public_access_cache
        now = time.monotonic()
        cached_val, cached_time = _public_access_cache
        if (now - cached_time) < 60.0:
            return cached_val

        if session is None:
            return False

        try:
            stmt = select(GlobalSettings.public_order_access).limit(1)
            result = await session.execute(stmt)
            val = result.scalar_one_or_none()
            is_enabled = bool(val)
        except Exception:
            is_enabled = cached_val

        _public_access_cache = (is_enabled, now)
        return is_enabled