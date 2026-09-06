import asyncio
import logging
import os
import random
import re
import uuid
from datetime import datetime
from typing import AsyncIterator, Dict, List, Optional, Set, Tuple
from pyrogram.errors import FloodWait
import aiofiles
from pyrogram import Client
from pyrogram.enums import ChatMemberStatus, ChatMembersFilter, ChatType, UserStatus
from pyrogram.errors import (
    FloodWait,
    UserAlreadyParticipant,
    InviteHashExpired,
    InviteHashInvalid,
    InviteRequestSent,
    PeerIdInvalid,
    UsernameInvalid,
    UsernameNotOccupied
)
from pyrogram.types import Chat, User

logger = logging.getLogger(__name__)

# اطمینان از وجود مسیر ذخیره‌سازی فایل‌های خروجی
os.makedirs("exports", exist_ok=True)

# 🔵 سقف رسمی API تلگرام برای متد get_chat_members (مشترک بین هر دو مسیر)
MEMBERS_API_LIMIT = 10000


async def safe_join_chat(client: Client, group_link: str) -> Tuple[str, Optional[Chat], bool]:
    chat_obj = None
    joined_now = False
    
    target_chat = group_link.strip()
    is_private = "+" in target_chat or "joinchat" in target_chat
    
    if not is_private:
        target_chat = re.sub(r"^https?://(www\.)?t\.me/", "", target_chat)
        target_chat = target_chat.replace("t.me/", "").strip("/")
        if not target_chat.startswith("@"):
            target_chat = f"@{target_chat}"

    try:
        await asyncio.sleep(random.uniform(2, 5))
        chat_obj = await client.join_chat(target_chat)
        if chat_obj and getattr(chat_obj, "id", None):
            chat_obj = await client.get_chat(chat_obj.id)
        joined_now = True
        logger.info(f"Worker {client.name} joined {chat_obj.title} successfully.")

    except FloodWait as e:
        logger.warning(f"Worker {client.name} hit FloodWait of {e.value}s while joining. Sleeping...")
        await asyncio.sleep(e.value + random.uniform(2, 5))
        try:
            chat_obj = await client.join_chat(target_chat)
            if chat_obj and getattr(chat_obj, "id", None):
                chat_obj = await client.get_chat(chat_obj.id)
            joined_now = True
        except UserAlreadyParticipant:
            logger.info(f"Worker {client.name} is already a member (after retry).")
            joined_now = False
            # 🟢 فیکس قطعی: استفاده از لینک اصلی برای بازسازی کش در صورت عضویت قبلی
            chat_obj = await client.get_chat(group_link)
        except Exception as inner_e:
            logger.error(f"Worker {client.name} failed after FloodWait retry: {inner_e}")
            return ("error", None, False)

    except UserAlreadyParticipant:
        logger.info(f"Worker {client.name} is already a member of the target group.")
        joined_now = False
        try:
            # 🟢 فیکس قطعی: برخلاف تصور قبلی، get_chat از لینک‌های خصوصی پشتیبانی می‌کند
            # این کار باعث دانلود اطلاعات گروه و آپدیت شدن حافظه Pyrogram می‌شود.
            chat_obj = await client.get_chat(group_link)
        except Exception as e:
            logger.error(f"Worker {client.name}: get_chat failed for already-joined link {group_link}: {e}")
            return ("error", None, False)

    except InviteRequestSent:
        logger.warning(f"Worker {client.name} sent join request to {target_chat}. Requires admin approval.")
        return ("pending_approval", None, False)

    except (InviteHashExpired, InviteHashInvalid, PeerIdInvalid, UsernameInvalid, UsernameNotOccupied) as e:
        logger.error(f"Worker {client.name} failed: Invalid link {target_chat}. Error: {e.__class__.__name__}")
        return ("error", None, False)

    except Exception as e:
        logger.error(f"Worker {client.name} unexpected error joining {target_chat}: {e}")
        return ("error", None, False)

    if not chat_obj:
        return ("error", None, False)

    chat_id = chat_obj.id

    if chat_obj.type == ChatType.CHANNEL:
        try:
            member = await client.get_chat_member(chat_id, "me")
            if member.status not in [ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER]:
                logger.warning(f"Worker {client.name} is NOT admin in channel {target_chat}. Aborting.")
                return ("error_not_admin", chat_obj, joined_now)
        except Exception as e:
            logger.error(f"Worker {client.name} could not verify admin status in {target_chat}: {e}")
            return ("error_not_admin", chat_obj, joined_now)

    await asyncio.sleep(random.uniform(3, 6))
    return ("success", chat_obj, joined_now)


# ==========================================
# 🔵 زیرساخت مشترک: گام دوم — جمع‌آوری ادمین‌ها جهت فیلترینگ
# ==========================================
async def _collect_admin_ids(client: Client, chat_id: int) -> Set[int]:
    """
    جمع‌آوری لیست ادمین‌ها جهت حذف از خروجی.
    (رفتار قبلی حفظ شده: خطا در این گام فرایند را متوقف نمی‌کند)
    """
    admin_ids: Set[int] = set()
    try:
        async for admin in client.get_chat_members(chat_id, filter=ChatMembersFilter.ADMINISTRATORS):
            admin_ids.add(admin.user.id)
    except Exception as e:
        logger.warning(f"Could not fetch admins for {chat_id}, extraction will proceed without admin filtering: {e}")
    return admin_ids


# ==========================================
# 🔵 زیرساخت مشترک: گام سوم — پیمایش امن اعضای گروه
# ==========================================
async def iter_group_members(client: Client, chat_id: int, admin_ids: Set[int]) -> AsyncIterator[User]:
    member_count = 0
    seen_ids = set()
    retries = 3

    while retries > 0:
        try:
            async for member in client.get_chat_members(chat_id, limit=MEMBERS_API_LIMIT):
                if member.user and member.user.id in seen_ids:
                    continue
                if member.user:
                    seen_ids.add(member.user.id)
                
                member_count += 1
                if member_count % 200 == 0:
                    await asyncio.sleep(random.uniform(3, 7))

                user = member.user
                if not user:
                    continue

                if user.id in admin_ids or member.status in [ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER]:
                    continue

                yield user

                if member_count >= MEMBERS_API_LIMIT:
                    logger.warning(f"Worker {client.name} hit API limit of {MEMBERS_API_LIMIT} for {chat_id}.")
                    return
            
            # خروج از حلقه در صورت موفقیت کامل
            break
            
        except FloodWait as e:
            logger.warning(f"FloodWait {e.value}s in iter_group_members. Retries left: {retries-1}")
            await asyncio.sleep(e.value + random.uniform(2, 5))
            retries -= 1
            
        except (PeerIdInvalid, ChannelPrivate) as e:
            # 🟢 جلوگیری از موفقیت قلابی: خطاهای بحرانی عدم دسترسی پرتاب می‌شوند
            logger.error(f"Critical access error in iter_group_members for chat {chat_id}: {e.__class__.__name__}")
            raise 
            
        except Exception as e:
            logger.error(f"Error in iter_group_members: {e}")
            # اگر هیچ عضوی استخراج نشده، یعنی عملیات کاملاً شکست خورده است
            if member_count == 0:
                raise
            # بازگشت بدون ارور فقط برای حفظ نتایج جزئی (Partial Success)
            break


# ==========================================
# ⚡️ بخش الف — فیلتر مشترک کاربران (_passes_filter)
# (🔵 رفع باگ لینک: این فیلتر قبلاً با نام _member_matches_send_filter فقط برای
# «سفارش ارسال از نوع link» بود؛ حالا فیلترِ مشترک بین مسیر استخراج
# extract_active_users و مسیر ارسال لینکی extract_members_for_sending است.)
# ==========================================
def _passes_filter(user: User, filter_type: Optional[str]) -> bool:
    """
    فیلتر مشترک کاربران بر اساس نوع فیلتر:
    - online → وضعیت ONLINE یا RECENTLY (UserStatus از pyrogram.enums)
    - real → غیربات و حذف‌نشده
    - fake → بات یا حساب حذف‌شده
    - phone → شماره‌دار (مخصوص مسیر ارسال لینکی)
    - all یا None → بدون فیلتر

    تعریف «online» دقیقاً از الگوی شمارش آمار در
    bot/handlers/order_handlers.py (proceed_to_filter_selection) پیروی می‌کند.
    """
    if filter_type == "online":
        # وضعیت ONLINE یا RECENTLY (همان تعریف شمارش «کاربران آنلاین» در آمار سفارش)
        if user.status not in (UserStatus.ONLINE, UserStatus.RECENTLY):
            return False
        # بات/حذف‌شده عملاً وضعیت آنلاین معتبر ندارند؛ برای «قابل ارسال» بودن حذف می‌شوند
        return not (user.is_bot or user.is_deleted)

    if filter_type == "real":
        # کاربر واقعی: غیربات و حذف‌نشده
        return not (user.is_bot or user.is_deleted)

    if filter_type == "fake":
        # بات یا حساب حذف‌شده (همان تعریف شمارش «کاربران فیک» در آمار سفارش)
        return bool(user.is_bot or user.is_deleted)

    if filter_type == "phone":
        # شماره‌دار (همان تعریف شمارش «کاربران شماره‌دار» در آمار سفارش)
        return bool(user.phone_number)

    # all / None / مقدار ناشناخته → بدون فیلتر
    return True

async def extract_active_users(client: Client, group_link: str, filter_type: str = "golden") -> Tuple[str, Optional[str], Optional[int], bool]:
    logger.info(f"Worker {client.name} starting '{filter_type.upper()}' extraction for {group_link}")

    chat_id = None
    joined_now = False
    golden_usernames: Set[str] = set()
    is_partial = False

    try:
        join_status, chat_obj, joined_now = await safe_join_chat(client, group_link)
        chat_id = chat_obj.id if chat_obj else None
        
        if join_status != "success" or not chat_obj:
            return (join_status, None, chat_id, joined_now)

        admin_ids: Set[int] = await _collect_admin_ids(client, chat_id)

        if filter_type in ["users", "golden", "online"]:
            valid_members: Dict[int, str] = {}
            logger.info("Fetching and filtering chat members (API Limit: max 10,000 members)...")

            async for user in iter_group_members(client, chat_id, admin_ids):
                if filter_type == "online" and not _passes_filter(user, "online"):
                    continue
                if user.is_deleted or user.is_bot or not user.username:
                    continue
                valid_members[user.id] = user.username

            if filter_type in ["users", "online"]:
                golden_usernames = set(valid_members.values())

            elif filter_type == "golden":
                active_user_ids: Set[int] = set()
                message_count = 0
                last_msg_id = 0  # 🟢 فاز ۶: ذخیره آخرین شناسه پیام برای Resume
                logger.info("Fetching chat history to bypass Hidden Last Seen...")
                
                retries = 3
                while retries > 0:
                    try:
                        async for message in client.get_chat_history(chat_id, limit=2000, offset_id=last_msg_id):
                            last_msg_id = message.id  # 🟢 فاز ۶: آپدیت نقطه توقف
                            message_count += 1
                            if message_count % 100 == 0:
                                await asyncio.sleep(random.uniform(3, 7))

                            if message.from_user:
                                active_user_ids.add(message.from_user.id)
                        break
                    except FloodWait as e:
                        logger.warning(f"FloodWait {e.value}s in chat_history. Retries left: {retries-1}")
                        await asyncio.sleep(e.value + random.uniform(2, 5))
                        retries -= 1
                        if retries == 0:
                            is_partial = True
                    except Exception as e:
                        logger.error(f"Error in golden chat_history: {e}")
                        is_partial = True
                        break

                intersected_users = [
                    valid_members[uid] for uid in active_user_ids if uid in valid_members
                ]
                golden_usernames = set(intersected_users)

        elif filter_type == "messages":
            logger.info("Extracting users purely based on chat history (Messages)...")
            message_count = 0
            last_msg_id = 0  # 🟢 فاز ۶: جلوگیری از لوپ بی‌نهایت
            retries = 3

            while retries > 0:
                try:
                    async for message in client.get_chat_history(chat_id, limit=5000, offset_id=last_msg_id):
                        last_msg_id = message.id  # 🟢 فاز ۶: آپدیت نقطه توقف
                        message_count += 1
                        if message_count % 200 == 0:
                            await asyncio.sleep(random.uniform(3, 7))

                        user = message.from_user
                        if user and not user.is_deleted and not user.is_bot and user.username:
                            if user.id not in admin_ids:
                                golden_usernames.add(user.username)
                    break
                except FloodWait as e:
                    logger.warning(f"FloodWait {e.value}s in messages history. Retries left: {retries-1}")
                    await asyncio.sleep(e.value + random.uniform(2, 5))
                    retries -= 1
                    if retries == 0:
                        is_partial = True
                except Exception as e:
                    logger.error(f"Error in messages chat_history: {e}")
                    is_partial = True
                    break

    except Exception as e:
        logger.error(f"Critical error during extraction logic execution: {e}")
        return ("error", None, chat_id, joined_now)

    if not golden_usernames:
        logger.warning(f"Extraction yielded no valid targets from {group_link}.")
        return ("error", None, chat_id, joined_now)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    unique_id = uuid.uuid4().hex[:8]
    raw_name = group_link.split("/")[-1].replace("+", "").replace("joinchat-", "")
    safe_link_name = re.sub(r'[\\/*?:"<>|]', "", raw_name)
    file_path = f"exports/ext_{filter_type}_{safe_link_name}_{timestamp}_{unique_id}.txt"

    try:
        async with aiofiles.open(file_path, "w", encoding="utf-8") as f:
            for username in golden_usernames:
                await f.write(f"@{username}\n")
    except Exception as e:
        logger.error(f"Failed to write extracted data to file system: {e}")
        return ("error", None, chat_id, joined_now)

    logger.info(f"Extraction complete! {len(golden_usernames)} targets saved to {file_path}")

    return ("partial_success" if is_partial else "success", file_path, chat_id, joined_now)

# ==========================================
# 🔵 رفع باگ لینک: استخراج ممبرها برای «سفارش ارسال از نوع link»
# (فیلترینگ اعضا با helper مشترک _passes_filter انجام می‌شود)
# ==========================================
async def extract_members_for_sending(
    client: Client,
    group_link: str,
    filter_type: Optional[str] = None,
) -> Tuple[str, Optional[List[str]], Optional[int], bool]:
    """
    Returns:
        Tuple[str, Optional[List[str]], Optional[int], bool]: (کد وضعیت, لیست تارگت‌ها, chat_id, joined_now)
        ...
        chat_id/joined_now (🚪 فاز ۶ R3-ب): برای ثبت order_joins و leave بعد از اتمام سفارش.
    """
    logger.info(f"Worker {client.name} resolving link-order members of {group_link} (filter: {filter_type})")

    # گام اول: Safe-Join (کد مشترک با مسیر استخراج)
    join_status, chat_obj, joined_now = await safe_join_chat(client, group_link)
    chat_id = chat_obj.id if chat_obj else None
    if join_status != "success" or not chat_obj:
        return (join_status, None, chat_id, joined_now)

    # گام دوم: ادمین‌ها (کد مشترک — ادمین‌ها هرگز تارگت نمی‌شوند)
    admin_ids: Set[int] = await _collect_admin_ids(client, chat_id)

    members_out: List[str] = []
    seen_ids: Set[int] = set()

    try:
        # گام سوم: پیمایش امن اعضا (کد مشترک — throttle + سقف ۱۰هزار + حذف ادمین‌ها)
        async for user in iter_group_members(client, chat_id, admin_ids):
            # ⚡️ فیلتر مشترک (بخش الف): online / real / fake / phone / all
            if not _passes_filter(user, filter_type):
                continue

            # محافظت در برابر تکرار (در صورت بازگشت کاربر از API)
            if user.id in seen_ids:
                continue
            seen_ids.add(user.id)

            # فرمت قابل ارسال: یوزرنیم در اولویت، در غیر این صورت user_id
            members_out.append(f"@{user.username}" if user.username else str(user.id))

    except Exception as e:
        logger.error(f"Critical error during link-order member resolution for {group_link}: {e}", exc_info=True)
        # 🛡 فاز ۲ (قرارداد خروجی): خطا هم ۴-تایی — chat_id/joined_now برای bookkeeping عضویت
        return ("error", None, chat_id, joined_now)

    logger.info(f"Link-order resolution for {group_link} finished with {len(members_out)} sendable targets.")
    # 🛡 فاز ۲ (قرارداد خروجی): موفقیت هم ۴-تایی — (status, members, chat_id, joined_now)
    return ("success", members_out, chat_id, joined_now)