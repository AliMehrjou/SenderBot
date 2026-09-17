import logging
from pyrogram import Client
from pyrogram.handlers import ChatMemberUpdatedHandler
from pyrogram.enums import ChatMemberStatus
from workers.sender import _get_redis

logger = logging.getLogger(__name__)

# استفاده از گروه مجزا برای جلوگیری از تداخل با CRM (0) و Seen Watcher (1,2)
JOIN_REQUEST_HANDLER_GROUP = 3

async def _join_request_handler(client: Client, chat_member_updated):
    try:
        # بررسی وجود آپدیت ممبر جدید
        new_member = chat_member_updated.new_chat_member
        if not new_member or not new_member.user:
            return

        # 🟢 رفع باگ Pyrogram: دریافت قطعی آیدی خود ورکر به جای استفاده از is_self
        me = getattr(client, "me", None)
        my_id = me.id if me else (await client.get_me()).id

        # اگر کاربری که وضعیتش تغییر کرده، خود ورکر ما نیست، بی‌خیال شو
        if new_member.user.id != my_id:
            return

        chat_id = chat_member_updated.chat.id
        user_id = new_member.user.id
        status = new_member.status

        redis = _get_redis()
        mapping_key = f"join_request:chat_id:{chat_id}:user_id:{user_id}"
        backup_key = f"join_request:any_chat:user_id:{user_id}"
        
        order_ids = []
        
        # 🟢 خواندن ایمن کلید مستقیم (جلوگیری از خطای decode روی رشته‌ها)
        order_id_raw = await redis.get(mapping_key)
        if order_id_raw:
            safe_val = order_id_raw.decode('utf-8') if isinstance(order_id_raw, bytes) else str(order_id_raw)
            order_ids.append(safe_val)
        else:
            # 🟢 خواندن ایمن کلید بک‌آپ (فیلتر per-chat برای بستن R5/B4)
            members = await redis.smembers(backup_key)
            if members:
                if status in (ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR):
                    for m in members:
                        safe_m = m.decode('utf-8') if isinstance(m, bytes) else str(m)
                        
                        # بررسی تطابق chat_id ذخیره‌شده برای این سفارش با chat_id رویداد
                        expected_chat_raw = await redis.get(f"join_request:order:{safe_m}:chat_id")
                        if expected_chat_raw:
                            expected_chat = expected_chat_raw.decode('utf-8') if isinstance(expected_chat_raw, bytes) else str(expected_chat_raw)
                            if str(expected_chat) == str(chat_id):
                                order_ids.append(safe_m)
                                await redis.srem(backup_key, safe_m) # حذف فقط همین سفارش
                        else:
                            # اگر chat_id از قبل نامشخص بود، تأیید می‌کنیم اما فقط همین کلید را از ست خارج می‌کنیم
                            order_ids.append(safe_m)
                            await redis.srem(backup_key, safe_m)
                else:
                    return

        if not order_ids:
            return

        # اعمال وضعیت برای تمام سفارشاتی که منتظر این ورکر بوده‌اند
        for order_id in order_ids:
            result_key = f"join_request:{order_id}:result"

            if status in (ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR):
                await redis.set(result_key, "approved", ex=86400)
                logger.info(f"Join request APPROVED for Order #{order_id} in Chat {chat_id}")
                
            elif status in (ChatMemberStatus.BANNED, ChatMemberStatus.RESTRICTED):
                await redis.set(result_key, "rejected", ex=86400)
                logger.info(f"Join request REJECTED for Order #{order_id} in Chat {chat_id}")

    except Exception as e:
        logger.error(f"Error in join_request_listener: {e}", exc_info=True)

def attach_join_request_listener(client: Client) -> None:
    """
    نصب شنود تشخیص تأیید/رد درخواست عضویت روی هر ورکر.
    """
    if getattr(client, "_join_listener_attached", False):
        return
        
    client.add_handler(
        ChatMemberUpdatedHandler(_join_request_handler),
        group=JOIN_REQUEST_HANDLER_GROUP
    )
    client._join_listener_attached = True
    logger.debug(f"Join Request Listener attached to worker '{client.name}'.")