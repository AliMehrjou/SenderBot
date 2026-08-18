import asyncio
import logging
from pyrogram import Client
from pyrogram.errors import InviteRequestSent, UserAlreadyParticipant
from pyrogram.enums import ChatType, ChatMemberStatus

logger = logging.getLogger(__name__)

async def check_channel_admin_status(client: Client, chat_id: int | str, admin_tg_id: int) -> bool:
    """
    بررسی می‌کند که اگر تارگت کانال است، آیا اکانت ورکر در آن ادمین می‌باشد یا خیر.
    """
    try:
        chat = await client.get_chat(chat_id)
        
        # اگر گروه یا سوپرگروه بود، نیازی به ادمین بودن نیست
        if chat.type in [ChatType.GROUP, ChatType.SUPERGROUP]:
            return True
            
        # اگر کانال بود، وضعیت ممبر بررسی می‌شود
        if chat.type == ChatType.CHANNEL:
            member = await client.get_chat_member(chat.id, "me")
            if member.status in [ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER]:
                return True
            else:
                await client.send_message(
                    chat_id=admin_tg_id,
                    text=(
                        "⚠️ <b>اخطار عدم دسترسی!</b>\n\n"
                        f"لینک وارد شده مربوط به یک <b>کانال</b> است ({chat.title}).\n"
                        "برای استخراج آیدی از کانال، اکانت ورکر حتماً باید ادمینِ کانال باشد. استخراج لغو شد."
                    )
                )
                return False
                
    except Exception as e:
        logger.error(f"Error checking admin status: {e}")
        return False


async def join_and_wait_for_approval(client: Client, link: str, admin_tg_id: int) -> int | None:
    """
    تلاش برای ورود به گروه/کانال.
    اگر لینک نیاز به تایید داشته باشد، تا ۲۴ ساعت در یک حلقه بک‌گراند منتظر می‌ماند.
    """
    try:
        # ۱. تلاش برای عضویت مستقیم
        chat = await client.join_chat(link)
        logger.info(f"Worker {client.name} joined {chat.title} directly.")
        
        # بررسی شرط ادمین بودن برای کانال‌ها
        is_valid = await check_channel_admin_status(client, chat.id, admin_tg_id)
        return chat.id if is_valid else None

    except UserAlreadyParticipant:
        # اگر از قبل عضو گروه باشد
        chat = await client.get_chat(link)
        logger.info(f"Worker {client.name} is already in {chat.title}.")
        
        is_valid = await check_channel_admin_status(client, chat.id, admin_tg_id)
        return chat.id if is_valid else None

    except InviteRequestSent:
        # ۲. اگر گروه خصوصی باشد و نیاز به تایید (Request to Join) داشته باشد
        logger.info(f"Worker {client.name} sent join request to {link}. Waiting for approval...")
        
        # ارسال پیام اطلاع‌رسانی طبق سناریوی کارفرما
        await client.send_message(
            chat_id=admin_tg_id,
            text=(
                "⏳ <b>درخواست عضویت ارسال شد!</b>\n\n"
                "درخواست عضویت برای گروه مورد نظر ارسال شد و در صورت اکسپت شدن توسط ادمین این گروه، "
                "ربات به صورت اتوماتیک لیست را استخراج کرده و برای شما در همینجا ارسال می‌کند.\n\n"
                f"🔗 <b>لینک:</b> {link}"
            )
        )

        # ۳. حلقه انتظار ۲۴ ساعته (هر ۱ ساعت یک‌بار چک می‌کند = ۲۴ بار)
        for attempt in range(24):
            await asyncio.sleep(3600)  # وقفه ۱ ساعته
            
            try:
                # تلاش برای گرفتن اطلاعات چت (اگر تایید شده باشد، ارور نمی‌دهد)
                chat = await client.get_chat(link)
                
                is_valid = await check_channel_admin_status(client, chat.id, admin_tg_id)
                return chat.id if is_valid else None
                
            except Exception:
                # هنوز توسط ادمینِ گروهِ تارگت تایید نشده است، ادامه حلقه...
                continue
        
        # ۴. پایان مهلت ۲۴ ساعته و صرف نظر کردن
        await client.send_message(
            chat_id=admin_tg_id,
            text=(
                "❌ <b>صرف نظر از استخراج (پایان مهلت ۲۴ ساعته)</b>\n\n"
                f"درخواست ورود اکانت ورکر به لینک زیر تا ۲۴ ساعت تایید نشد و عملیات استخراج متوقف گردید:\n{link}"
            )
        )
        return None

    except Exception as e:
        logger.error(f"Failed to process link {link}: {e}")
        await client.send_message(
            chat_id=admin_tg_id,
            text=f"⚠️ <b>خطا در پردازش لینک:</b>\n{link}\n\n<i>ممکن است لینک منقضی شده باشد یا ربات محدود باشد.</i>"
        )
        return None