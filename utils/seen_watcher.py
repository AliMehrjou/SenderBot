"""
🧠 جریان هوشمند (Smart Flow) — رصد «سین» تارگت

روی هر کلاینتِ ورکر دو هندلر نصب می‌شود:
1. RawUpdateHandler: آپدیت UpdateReadHistoryOutbox را گوش می‌دهد — این آپدیت
   زمانی از سرور می‌آید که طرفِ مقابل (تارگت) پیام‌های خروجیِ ما را خوانده باشد.
2. MessageHandler (filters.private & ~filters.me): اگر خودِ تارگت پیامی بفرستد
   (یعنی ریپلای داده باشد)، همان لحظه رویداد «دیده شدن» برایش set می‌شود.

رویدادها در client.seen_events (dict از asyncio.Event با کلید peer_id) نگه داشته
می‌شوند و تابع wait_for_seen روی همان کلاینت منتظرشان می‌ماند.

⚠️ نکته‌ی مهم گروه‌بندی: در Pyrogram در هر گروه فقط «اولین» هندلرِ منطبق اجرا
می‌شود و هندلر CRM (crm_catcher) از قبل در گروه ۰ نشسته است؛ بنابراین هندلرهای
این ماژول در گروه‌های ۱ و ۲ ثبت می‌شوند تا هم حتماً اجرا شوند و هم ذره‌ای در
رفتار CRM تغییر ایجاد نکنند.
"""
import asyncio
import logging
from typing import Optional

from pyrogram import Client, filters
from pyrogram.handlers import MessageHandler, RawUpdateHandler
from pyrogram.raw.types import PeerUser, UpdateReadHistoryOutbox

logger = logging.getLogger(__name__)

# گروه‌های اختصاصی هندلرهای «سین» — جدا از گروه ۰ (هندلر CRM)
SEEN_RAW_HANDLER_GROUP = 1
SEEN_MESSAGE_HANDLER_GROUP = 2


def _get_or_create_seen_event(client: Client, peer_id: int) -> asyncio.Event:
    """رویداد «دیده شدن» یک peer را از client.seen_events برمی‌گرداند؛ در صورت نبود می‌سازد."""
    if not hasattr(client, "seen_events") or client.seen_events is None:
        client.seen_events = {}
    event = client.seen_events.get(peer_id)
    if event is None:
        event = asyncio.Event()
        client.seen_events[peer_id] = event
    return event


def _try_set_seen_event(client: Client, peer_id: int) -> None:
    """
    اگر رویدادِ «در حال انتظار» برای این peer وجود داشته باشد، set می‌شود.
    عمداً رویداد جدید نمی‌سازد تا:
      - سینال‌های کهنه (خواندن پیام‌های کمپین‌های قبلی) باعث پرشِ فوری مرحله‌ی بنر نشوند
      - رویدادهای بی‌صاحاب در dict انباشته و حافظه نشت نکنند
    """
    seen_events = getattr(client, "seen_events", None)
    if not seen_events:
        return
    event = seen_events.get(peer_id)
    if event is not None and not event.is_set():
        event.set()
        logger.debug(f"SmartFlow/seen: peer {peer_id} marked as seen.")


async def _outbox_read_raw_handler(client: Client, update, users, chats) -> None:
    """
    هندلر raw: وقتی تارگت پیام‌های خروجی ما را می‌خواند، تلگرام
    UpdateReadHistoryOutbox می‌فرستد → رویداد همان peer فعال می‌شود.
    """
    if isinstance(update, UpdateReadHistoryOutbox):
        peer = getattr(update, "peer", None)
        if isinstance(peer, PeerUser):
            _try_set_seen_event(client, peer.user_id)


async def _incoming_message_seen_handler(client: Client, message) -> None:
    """
    هندلر پیام ورودی: اگر خودِ تارگت پیامی بفرستد (یعنی ریپلای داده)،
    یعنی پیام را دیده است → رویداد همان peer فعال می‌شود.
    """
    if message is None:
        return

    peer_id = None
    if getattr(message, "from_user", None) is not None:
        peer_id = message.from_user.id
    elif getattr(message, "chat", None) is not None:
        # در چت خصوصی، chat.id همان آیدی تارگت است
        peer_id = message.chat.id

    if peer_id is not None:
        _try_set_seen_event(client, peer_id)


def attach_seen_listener(client: Client) -> None:
    """
    نصب شنود «سین» روی کلاینت ورکر (idempotent — نصب دوباره بی‌اثر است).
    """
    if getattr(client, "_seen_listener_attached", False):
        return

    client.seen_events = {}

    # RawUpdateHandler: خوانده شدن پیام‌های خروجی توسط تارگت (UpdateReadHistoryOutbox)
    client.add_handler(
        RawUpdateHandler(_outbox_read_raw_handler),
        group=SEEN_RAW_HANDLER_GROUP,
    )

    # MessageHandler: ریپلای/پیام دادن خودِ تارگت
    client.add_handler(
        MessageHandler(
            _incoming_message_seen_handler,
            filters.private & ~filters.me,
        ),
        group=SEEN_MESSAGE_HANDLER_GROUP,
    )

    client._seen_listener_attached = True
    logger.debug(f"SmartFlow/seen: listener attached to worker client '{getattr(client, 'name', '?')}'.")


def arm_seen_event(client: Client, peer_id: int) -> None:
    """
    🛡 فاز ۷ (BUG-18): مسلح‌کردن رویداد «سین» یک peer — باید «قبل از ارسال» فراخوانی
    شود. رویداد ساخته/بازنشانی (clear) می‌شود تا هندلرها بتوانند سیگنال «خواندن» را
    از همین لحظه به بعد روی آن ثبت کنند. قبلاً رویداد داخل wait_for_seen (یعنی بعد
    از send) ساخته و clear می‌شد و سیگنالی که در فاصله‌ی send → wait می‌رسید پاک
    می‌شد ← انتظار بی‌دلیل تا timeout کامل.
    """
    event = _get_or_create_seen_event(client, peer_id)
    event.clear()


def dismiss_seen_event(client: Client, peer_id: Optional[int]) -> None:
    """
    🛡 فاز ۷ (BUG-18): جمع‌کردن رویدادِ مسلح‌شده وقتی ارسال مرحله‌ی ۱ شکست خورد و
    wait_for_seen هرگز فراخوانی نمی‌شود — جلوگیری از انباشت رویدادهای بی‌صاحاب در
    client.seen_events (نشت حافظه روی تارگت‌های انبوه).
    """
    if peer_id is None:
        return
    seen_events = getattr(client, "seen_events", None)
    if seen_events is not None:
        seen_events.pop(peer_id, None)


async def wait_for_seen(client: Client, peer_id: int, timeout: float) -> bool:
    """
    منتظر «دیده شدن» پیام توسط تارگت می‌ماند (خوانده شدن پیام یا پیام دادن خودش).

    - 🛡 فاز ۷ (BUG-18): دیگر event.clear() نمی‌کنیم — رویداد باید «قبل از send» با
      arm_seen_event مسلح شده باشد؛ clear بعد از send، سیگنالِ رسیده در فاصله‌ی
      send → wait را پاک می‌کرد و ورکر تا timeout کامل بی‌دلیل منتظر می‌ماند.
    - در پایان، رویداد از dict حذف می‌شود تا حافظه روی تارگت‌های انبوه نشت نکند.

    خروجی: True اگر قبل از پایان timeout دیده شد / False در صورت تایم‌اوت.
    """
    seen_events = getattr(client, "seen_events", None)
    event = (seen_events or {}).get(peer_id)
    if event is None:
        # رویداد مسلح نشده (مسیر غیرعادی) — ساخته می‌شود تا فراخوانی این تابع هرگز
        # نشکند؛ سیگنال‌های رسیده در بازه‌ی send → اینجا از دست رفته‌اند.
        logger.debug(
            f"SmartFlow/seen: peer {peer_id} event was not armed before send; "
            f"early read-signals may have been lost."
        )
        event = _get_or_create_seen_event(client, peer_id)

    try:
        await asyncio.wait_for(event.wait(), timeout=timeout)
        return True
    except asyncio.TimeoutError:
        return False
    finally:
        seen_events = getattr(client, "seen_events", None)
        if seen_events is not None:
            seen_events.pop(peer_id, None)