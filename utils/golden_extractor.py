import asyncio
import random
import logging
from datetime import datetime, timedelta, timezone

from pyrogram import Client
from pyrogram.enums import UserStatus, ChatMembersFilter

logger = logging.getLogger(__name__)

async def extract_golden_list(client: Client, chat_id: int | str) -> list[str]:
    """
    الگوریتم هوشمند استخراج و فیلترینگ گلدن لیست:
    - حذف ادمین‌ها و ربات‌ها
    - اعتبارسنجی یوزرنیم
    - بررسی وضعیت آنلاین (تا 24 ساعت)
    - اسکن 500 پیام آخر برای افراد با وضعیت مخفی
    - اعمال قانون 10-20 (Anti-Ban Throttling)
    """
    logger.info(f"Worker {client.name} started golden extraction for {chat_id}")
    
    now_utc = datetime.now(timezone.utc)
    twenty_four_hours_ago = now_utc - timedelta(hours=24)
    
    # ==========================================
    # ۱. استخراج مالکان و ادمین‌ها (برای بلک‌لیست کردن)
    # ==========================================
    admin_ids = set()
    try:
        async for admin in client.get_chat_members(chat_id, filter=ChatMembersFilter.ADMINISTRATORS):
            admin_ids.add(admin.user.id)
    except Exception as e:
        logger.warning(f"Could not fetch admins for {chat_id}. (Maybe not a group?): {e}")

    # ==========================================
    # ۲. اسکن هیستوری چت (پیدا کردن فعالیت افراد با Last Seen مخفی)
    # ==========================================
    active_users_from_history = {}
    try:
        logger.info(f"Worker {client.name} scanning last 500 messages...")
        # خواندن ۵۰۰ پیام آخر
        async for message in client.get_chat_history(chat_id, limit=500):
            if message.from_user and not message.from_user.is_bot:
                user_id = message.from_user.id
                msg_date = message.date
                
                # ذخیره جدیدترین زمانی که کاربر پیام داده است
                if user_id not in active_users_from_history:
                    active_users_from_history[user_id] = msg_date
                elif msg_date > active_users_from_history[user_id]:
                    active_users_from_history[user_id] = msg_date
    except Exception as e:
        logger.warning(f"Could not fetch history for {chat_id}: {e}")

    # ==========================================
    # ۳. پیمایش اعضا، فیلترینگ و استخراج نهایی
    # ==========================================
    golden_list = []
    users_activity_map = {} # ذخیره زمان آخرین فعالیت برای مرتب‌سازی
    
    count = 0
    try:
        async for member in client.get_chat_members(chat_id):
            user = member.user
            
            # فیلترهای اولیه (حذف نویز)
            if user.is_bot: 
                continue
            if user.id in admin_ids: 
                continue
            if not user.username: 
                continue
            
            is_active = False
            last_activity_time = None

            # بررسی از طریق Status (کاربرانی که مخفی نکرده‌اند)
            if user.status in [UserStatus.ONLINE, UserStatus.RECENTLY]:
                is_active = True
                last_activity_time = now_utc
            elif user.status == UserStatus.OFFLINE and user.last_online_date:
                # اطمینان از تایم‌زون
                if user.last_online_date >= twenty_four_hours_ago:
                    is_active = True
                    last_activity_time = user.last_online_date
                    
            # بررسی از طریق اسکن هیستوری (برای کاربرانی که وضعیت را Nobody گذاشته‌اند)
            if not is_active and user.id in active_users_from_history:
                msg_date = active_users_from_history[user.id]
                if msg_date >= twenty_four_hours_ago:
                    is_active = True
                    last_activity_time = msg_date
                    
            # اگر کاربر زنده و فعال تشخیص داده شد
            if is_active:
                golden_list.append(user.username)
                users_activity_map[user.username] = last_activity_time or twenty_four_hours_ago

            # ----------------------------------------------------
            # پروتکل امنیتی: قانون ۱۰-۲۰ (Throttling)
            # ----------------------------------------------------
            count += 1
            if count % 15 == 0:
                # وقفه تصادفی بین ۳ تا ۷ ثانیه بعد از هر ۱۵ ریکوئست/پردازش
                await asyncio.sleep(random.uniform(3, 7))
                
    except Exception as e:
        logger.error(f"Error extracting members from {chat_id}: {e}")

    # ==========================================
    # ۴. مرتب‌سازی لیست و تولید خروجی
    # ==========================================
    # مرتب‌سازی بر اساس زمان آخرین فعالیت (نزولی: کسانی که اخیراً فعالیت کرده‌اند در صدر هستند)
    golden_list.sort(key=lambda uname: users_activity_map.get(uname, twenty_four_hours_ago), reverse=True)
    
    # الحاق @ به ابتدای یوزرنیم‌ها
    final_formatted_list = [f"@{uname}" for uname in golden_list]
    
    logger.info(f"Worker {client.name} successfully extracted {len(final_formatted_list)} golden targets.")
    return final_formatted_list