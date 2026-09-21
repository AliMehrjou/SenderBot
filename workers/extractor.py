import asyncio
import html
import logging
import os
import random
import re
import time
import uuid
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import List, Optional, Tuple, Set, Dict, AsyncIterator, Callable, Awaitable

import aiofiles
import redis.asyncio as aioredis
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from pyrogram import Client
from pyrogram.types import Chat, User, InlineKeyboardButton, InlineKeyboardMarkup
from pyrogram.enums import ChatMemberStatus, ChatMembersFilter, ChatType, UserStatus
from pyrogram.errors import (
    FloodWait, UserIsBlocked, PeerIdInvalid, UsernameInvalid,
    UsernameNotOccupied, UserIsBot, UserRestricted, ChannelInvalid,
    ChatForwardsRestricted, PeerFlood, UserDeactivated, AuthKeyUnregistered,
    Unauthorized, UserNotParticipant, UserAlreadyParticipant, InviteRequestSent,
    InviteHashExpired, InviteHashInvalid, ChannelPrivate
)

try:
    import python_socks
except ImportError:
    python_socks = None

from config import config
from database.engine import async_session
from database.models import OrderLog, Order, OrderStatus, Admin, Account, WorkerEvent
from utils.speed_profile import SpeedProfile, SAFE_PROFILE
from utils.anti_ban import parse_spintax, apply_adaptive_flood_wait

# --- اضافه‌شده برای مدیریت سلامت پروکسی (B3) ---
from utils.health_checker import report_proxy_result


from utils.advanced_anti_ban import check_spambot_status
from utils.seen_watcher import arm_seen_event, dismiss_seen_event, wait_for_seen
from utils.limit_handler import LIMIT_FLOOD_WAIT

# تعریف تایپ‌هینت سراسری
progress_cb_type = Optional[Callable[[int, int], Awaitable[bool]]]

logger = logging.getLogger(__name__)

# اطمینان از وجود مسیر ذخیره‌سازی فایل‌های خروجی
os.makedirs("exports", exist_ok=True)

# 🔵 سقف رسمی API تلگرام برای متد get_chat_members
MEMBERS_API_LIMIT = 10000

_NETWORK_ERRORS = (ConnectionError, TimeoutError, OSError)
if python_socks:
    _NETWORK_ERRORS += (
        getattr(python_socks, 'ProxyError', ConnectionError),
        getattr(python_socks, 'ProxyTimeoutError', TimeoutError),
        getattr(python_socks, 'ProxyConnectionError', OSError)
    )

def _report_proxy(client: Client, is_success: bool):
    """گزارش‌دهی non-blocking سلامت پروکسی بر اساس وضعیت فعلی کلاینت"""
    try:
        proxy_str = None
        if hasattr(client, "proxy_string") and client.proxy_string:
            proxy_str = client.proxy_string
        elif getattr(client, "proxy", None) and isinstance(client.proxy, dict):
            p = client.proxy
            auth = f"{p['username']}:{p['password']}@" if p.get('username') and p.get('password') else ""
            proxy_str = f"{p.get('scheme', 'socks5')}://{auth}{p['hostname']}:{p['port']}"
        
        if proxy_str:
            asyncio.create_task(report_proxy_result(proxy_str, is_success))
    except Exception:
        pass
# ------------------------------------------------


async def safe_join_chat(
    client: Client, 
    group_link: str,
    chat_id_hint: Optional[int] = None,
    order_id: Optional[int] = None,
    speed_profile: Optional[SpeedProfile] = None
) -> tuple[str, Optional[Chat], bool]:
    from pyrogram.enums import ChatMemberStatus
    profile = speed_profile or SAFE_PROFILE
    chat_obj = None
    joined_now = False
    
    target_chat = group_link.strip()
    is_private = "+" in target_chat or "joinchat" in target_chat
    
    if not is_private:
        target_chat = re.sub(r"^https?://(www\.)?t\.me/", "", target_chat)
        target_chat = target_chat.replace("t.me/", "").strip("/")
        if not target_chat.startswith("@"):
            target_chat = f"@{target_chat}"

    # ۱. بررسی عضویت از طریق chat_id_hint
    if chat_id_hint:
        try:
            member = await client.get_chat_member(chat_id_hint, "me")
            if member.status in [ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER]:
                logger.info(f"Worker {client.name} is already a member (direct check via chat_id_hint).")
                chat_obj = await client.get_chat(chat_id_hint)
                return ("already_member", chat_obj, False)
        except UserNotParticipant:  
            # 🟢 اصلاح: اگر ربات از گروه ریمو و آنبن شده بود، به جای ارور دادن، اجازه می‌دهیم
            # کد ادامه پیدا کند تا ربات دوباره روی لینک جوین/ریکوئست بزند.
            logger.warning(f"Worker {client.name} not member (probably kicked/unbanned). Proceeding to rejoin...")
            pass
        except Exception as e:
            logger.debug(f"Worker {client.name} direct member check failed: {e}")

    redis_client = None
    try:
        from workers.sender import _get_redis
        redis_client = _get_redis()
    except Exception as e:
        logger.error(f"Could not get redis client in safe_join_chat: {e}")

    # R4: پیش‌نویس کلیدهای عضویت
    worker_user_id = None
    backup_key = None
    if order_id and redis_client:
        try:
            me = getattr(client, "me", None)
            worker_user_id = me.id if me else (await client.get_me()).id
            
            backup_key = f"join_request:any_chat:user_id:{worker_user_id}"
            await redis_client.sadd(backup_key, order_id)
            await redis_client.expire(backup_key, 86400)
            
            if chat_id_hint:
                mapping_key = f"join_request:chat_id:{chat_id_hint}:user_id:{worker_user_id}"
                await redis_client.set(mapping_key, order_id, ex=86400)
                await redis_client.set(f"join_request:order:{order_id}:chat_id", chat_id_hint, ex=86400)
        except Exception as e:
            logger.warning(f"Failed to pre-write Redis keys for R4 (Order #{order_id}): {e}")

    # 🛡 جلوگیری قطعی از FloodWait بخاطر اسپم Join Request
    cooldown_key = f"join_cd:{client.name}:{target_chat}"

    if redis_client:
        try:
            cd_val = await redis_client.get(cooldown_key)
            if cd_val:
                logger.info(f"Worker {client.name} deferring join_chat for {target_chat} (Cooldown {await redis_client.ttl(cooldown_key)}s active to prevent FloodWait).")
                
                # بررسی خاموش: آیا در این مدت درخواست تایید شده است؟
                try:
                    # 🟢 اصلاح: متد get_chat لینک پرایوت را قبول نمی‌کند. اگر آیدی داریم از آن استفاده می‌کنیم.
                    check_target = chat_id_hint if (chat_id_hint and is_private) else target_chat
                    temp_chat = await client.get_chat(check_target)
                    if temp_chat and getattr(temp_chat, "id", None):
                        member = await client.get_chat_member(temp_chat.id, "me")
                        if member.status in [ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER]:
                            logger.info(f"Worker {client.name} was approved for {target_chat} during cooldown!")
                            await redis_client.delete(cooldown_key)
                            return ("success", temp_chat, False)
                        elif member.status in [ChatMemberStatus.BANNED, ChatMemberStatus.RESTRICTED]:
                            logger.info(f"Worker {client.name} was REJECTED for {target_chat} during cooldown!")
                            await redis_client.delete(cooldown_key)
                            return ("rejected", temp_chat, False)
                except UserNotParticipant:
                    pass # هنوز درخواست در حالت انتظار است
                except Exception as check_err:
                    logger.debug(f"Silent check during cooldown failed: {check_err}")
                    
                return ("pending_approval", None, False)
        except Exception as e:
            logger.error(f"Redis get cooldown failed: {e}")
    try:
        # 🛡 پلکانی‌سازی و سقف عضویت (Join Staggering & Rate Cap)
        if redis_client and worker_user_id:
            # 1. Staggering
            s_min, s_max = profile.join_stagger_seconds
            if s_max > 0:
                s_time = random.uniform(s_min, s_max)
                locked = await redis_client.set(f"join_stagger:{worker_user_id}", "1", nx=True, px=int(s_time * 1000))
                if not locked:
                    logger.info(f"Worker {client.name} delaying join due to stagger lock.")
                    return (f"limit:join_stagger:{int(s_time)}", None, False)
            
            # 2. Hourly Rate Cap
            if profile.join_rate_cap_per_hour > 0:
                cap_key = f"join_rate:{worker_user_id}"
                current_joins = await redis_client.get(cap_key)
                if current_joins and int(current_joins) >= profile.join_rate_cap_per_hour:
                    logger.warning(f"Worker {client.name} hit hourly join cap ({profile.join_rate_cap_per_hour}). Deferring.")
                    return ("limit:join_rate:3600", None, False)

        # 📊 متریک زمان‌بندی
        logger.info(f"[METRIC] join_request_sent_start: worker={client.name}, target={target_chat}, ts={time.time()}")
        
        await asyncio.sleep(random.uniform(profile.pre_join_sleep[0], profile.pre_join_sleep[1]))
        
        # 🟢 فراخوانی اصلی Join
        chat_obj = await client.join_chat(target_chat)
        
        # ثبت موفقیت join در Rate Cap
        if redis_client and worker_user_id and profile.join_rate_cap_per_hour > 0:
            await redis_client.incr(f"join_rate:{worker_user_id}")
            if await redis_client.ttl(f"join_rate:{worker_user_id}") == -1:
                await redis_client.expire(f"join_rate:{worker_user_id}", 3600)

        if chat_obj and getattr(chat_obj, "id", None):
            chat_obj = await client.get_chat(chat_obj.id)
        joined_now = True
        logger.info(f"Worker {client.name} joined successfully.")

        if redis_client and order_id and backup_key:
            try:
                await redis_client.srem(backup_key, order_id)
                cid = chat_id_hint or (chat_obj.id if chat_obj else None)
                if cid:
                    await redis_client.delete(f"join_request:chat_id:{cid}:user_id:{worker_user_id}")
                    await redis_client.delete(f"join_request:order:{order_id}:chat_id")
            except Exception:
                pass

    except FloodWait as e:
        if e.value > 60:
            logger.warning(f"Worker {client.name} hit huge FloodWait ({e.value}s) while joining. Aborting.")
            return (f"limit:flood_wait:{e.value}", None, False)
            
        logger.warning(f"Worker {client.name} hit FloodWait of {e.value}s while joining. Sleeping...")
        await asyncio.sleep(e.value + random.uniform(profile.floodwait_padding_join[0], profile.floodwait_padding_join[1]))
        try:
            chat_obj = await client.join_chat(target_chat)
            if chat_obj and getattr(chat_obj, "id", None):
                chat_obj = await client.get_chat(chat_obj.id)
            joined_now = True
            
            if redis_client and order_id and backup_key:
                try:
                    await redis_client.srem(backup_key, order_id)
                    cid = chat_id_hint or (chat_obj.id if chat_obj else None)
                    if cid:
                        await redis_client.delete(f"join_request:chat_id:{cid}:user_id:{worker_user_id}")
                        await redis_client.delete(f"join_request:order:{order_id}:chat_id")
                except Exception:
                    pass
                    
        except UserAlreadyParticipant:  # 🟢 اصلاح شد
            logger.info(f"Worker {client.name} is already a member (after retry).")
            joined_now = False
            chat_obj = await client.get_chat(group_link)
        except FloodWait as inner_fw:
            logger.warning(f"Worker {client.name} hit secondary FloodWait ({inner_fw.value}s). Aborting.")
            return (f"limit:flood_wait:{inner_fw.value}", None, False)
        except Exception as inner_e:
            logger.error(f"Worker {client.name} failed after FloodWait retry: {inner_e}")
            return ("error", None, False)

    except UserAlreadyParticipant:  # 🟢 اصلاح شد
        logger.info(f"Worker {client.name} is already a member of the target group.")
        joined_now = False
        try:
            chat_obj = await client.get_chat(group_link)
        except Exception as e:
            logger.error(f"Worker {client.name}: get_chat failed for already-joined link {group_link}: {e}")
            return ("members_hidden", None, False)
            
        if redis_client and order_id and backup_key:
            try:
                await redis_client.srem(backup_key, order_id)
                cid = chat_id_hint or (chat_obj.id if chat_obj else None)
                if cid:
                    await redis_client.delete(f"join_request:chat_id:{cid}:user_id:{worker_user_id}")
                    await redis_client.delete(f"join_request:order:{order_id}:chat_id")
            except Exception:
                pass

    except InviteRequestSent:  # 🟢 اصلاح شد
        logger.warning(f"Worker {client.name} sent join request to {target_chat}.")
        # 🛑 تنظیم کول‌داون کوتاه‌مدت (۱۰ دقیقه) برای جلوگیری از فلادویت
        if redis_client:
            try:
                await redis_client.set(cooldown_key, "1", ex=600)
                logger.info(f"Worker {client.name}: Cooldown of 600s set for {target_chat} to prevent FloodWait.")
            except Exception as e:
                logger.error(f"Redis set cooldown failed: {e}")
        try:
            chat_obj = await client.get_chat(target_chat)
        except Exception as e:
            logger.warning(f"Worker {client.name} could not resolve chat {target_chat} after InviteRequestSent: {e}")
        return ("pending_approval", chat_obj, False)
        
    except (InviteHashExpired, InviteHashInvalid, PeerIdInvalid, UsernameInvalid, UsernameNotOccupied) as e:
        logger.error(f"Worker {client.name} failed: Invalid link {target_chat}. Error: {e.__class__.__name__}")
        return ("error", None, False)
    except _NETWORK_ERRORS as e:
        logger.error(f"Worker {client.name} proxy/network error joining {target_chat}: {e}")
        _report_proxy(client, False)
        return ("proxy_connection_error", None, False)
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
                return ("error_not_admin", chat_obj, joined_now)
        except Exception:
            return ("error_not_admin", chat_obj, joined_now)

    await asyncio.sleep(random.uniform(profile.join_pause[0], profile.join_pause[1]))
    _report_proxy(client, True)
    return ("success" if joined_now else "already_member", chat_obj, joined_now)


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
async def iter_group_members(
    client: Client, 
    chat_id: int, 
    admin_ids: Set[int], 
    resume_seen_ids: Optional[Set[int]] = None,
    speed_profile: Optional[SpeedProfile] = None
) -> AsyncIterator[User]:
    from config import config
    profile = speed_profile or SAFE_PROFILE
    member_count = 0
    seen_ids = resume_seen_ids or set()
    retries = 3
    offset = 0  # 🟢 حفظ آفست برای جلوگیری از اسکن مجددِ اعضای قبلی

    while retries > 0:
        try:
            limit = config.EXTRACT_MEMBERS_API_LIMIT - offset
            if limit <= 0:
                break
            
            async for member in client.get_chat_members(chat_id, limit=limit):
                offset += 1  # پیشروی آفست با هر عضو دریافتی
                
                if member.user and member.user.id in seen_ids:
                    continue
                if member.user:
                    seen_ids.add(member.user.id)
                
                member_count += 1
                if member_count % 200 == 0:
                    await asyncio.sleep(random.uniform(profile.iter_pause[0], profile.iter_pause[1]))

                user = member.user
                if not user:
                    continue

                if user.id in admin_ids or member.status in [ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER]:
                    continue

                yield user

                if member_count >= config.EXTRACT_MEMBERS_API_LIMIT:
                    logger.warning(f"Worker {client.name} hit API limit of {config.EXTRACT_MEMBERS_API_LIMIT} for {chat_id}.")
                    return
            break
        except FloodWait as e:
            logger.warning(f"FloodWait {e.value}s in iter_group_members. Retries left: {retries-1}")
            await asyncio.sleep(e.value + random.uniform(profile.floodwait_padding_iter[0], profile.floodwait_padding_iter[1]))
            retries -= 1
            if retries <= 0:
                logger.error(f"Worker {client.name} exhausted FloodWait retries in iter_group_members.")
                raise TimeoutError("flood_wait_exhausted")
        except (PeerIdInvalid, ChannelPrivate) as e:
            logger.error(f"Critical access error in iter_group_members for chat {chat_id}: {e.__class__.__name__}")
            raise 
        except _NETWORK_ERRORS as e:
            logger.error(f"Network/Proxy error in iter_group_members: {e}")
            _report_proxy(client, False)
            if member_count == 0:
                raise
            break
        except Exception as e:
            logger.error(f"Error in iter_group_members: {e}")
            if member_count == 0:
                raise
            break

    _report_proxy(client, True)

# ==========================================
# ⚡️ بخش الف — فیلتر مشترک کاربران (_passes_filter)
# (🔵 رفع باگ لینک: این فیلتر قبلاً با نام _member_matches_send_filter فقط برای
# «سفارش ارسال از نوع link» بود؛ حالا فیلترِ مشترک بین مسیر استخراج
# extract_active_users و مسیر ارسال لینکی extract_members_for_sending است.)
# ==========================================
def _passes_filter(user: User, filter_type: Optional[str]) -> tuple[bool, str]:
    """
    پشتیبانی از فیلترهای ویزارد JSON یا مقادیر قدیمی.
    خروجی: (آیا عبور کرد؟, دلیل فیلتر شدن)
    """
    import json
    try:
        options = json.loads(filter_type) if filter_type and filter_type.startswith("{") else {}
    except:
        options = {}

    # 🟢 پشتیبانی همزمان از فرمت JSON و فرمت کلاسیکِ استخراج
    check_online = options.get("online_only") or (filter_type == "online")

    if check_online and user.status not in (UserStatus.ONLINE, UserStatus.RECENTLY):
        return False, "آفلاین"
        
    if options.get("has_photo") and not getattr(user, "photo", None):
        return False, "بدون عکس"
        
    if options.get("no_bots") and (user.is_bot or user.is_deleted):
        return False, "ربات/دلیت شده"
        
    return True, "تایید"
async def extract_active_users(
    client: Client, 
    group_link: str, 
    filter_type: str = "golden",
    progress_cb: Optional[Callable[[int], Awaitable[None]]] = None,
    resume_seen_ids: Optional[Set[int]] = None,
    chat_id_hint: Optional[int] = None,
    order_id: Optional[int] = None,
    speed_profile: Optional[SpeedProfile] = None # 🟢 این پارامتر به تابع اضافه شد
) -> tuple[str, Optional[str], Optional[int], bool, int]:
    from config import config
    logger.info(f"Worker {client.name} starting '{filter_type.upper()}' extraction for {group_link}")

    chat_id = None
    joined_now = False
    golden_usernames: Set[str] = set()
    is_partial = False
    is_stopped = False
    total_yielded = 0
    stats = {"total_scanned": 0, "filtered_out": 0, "no_username": 0}

    try:
        join_status, chat_obj, joined_now = await safe_join_chat(
            client, group_link, chat_id_hint=chat_id_hint, order_id=order_id
        )
        chat_id = getattr(chat_obj, "id", None)
        
        if join_status not in ("success", "already_member") or not chat_obj:
            return (join_status, None, chat_id, joined_now, 0)

        admin_ids: Set[int] = await _collect_admin_ids(client, chat_id)
        
        # 🟢 ساخت فایل خروجی در ابتدا
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        unique_id = uuid.uuid4().hex[:8]
        raw_name = group_link.split("/")[-1].replace("+", "").replace("joinchat-", "")
        safe_link_name = re.sub(r'[\\/*?:"<>|]', "", raw_name)
        file_path = f"exports/ext_{filter_type}_{safe_link_name}_{timestamp}_{unique_id}.txt"
        
        override_status = None
        async with aiofiles.open(file_path, "w", encoding="utf-8") as f:
            if filter_type in ["users", "golden", "online"]:
                valid_members: Dict[int, str] = {}
                logger.info(f"Fetching and filtering chat members (API Limit: max {config.EXTRACT_MEMBERS_API_LIMIT} members)...")
                last_report_time = time.time()

                try:
                    async for user in iter_group_members(client, chat_id, admin_ids, resume_seen_ids):
                        stats["total_scanned"] += 1
                        now = time.time()
                        if progress_cb and (stats["total_scanned"] % config.EXTRACT_CHECKPOINT_EVERY == 0 or now - last_report_time >= 10):
                            last_report_time = now
                            if await progress_cb(total_yielded, total_yielded) is False:
                                is_partial = True
                                is_stopped = True
                                break  # 🟢 خروج فوری از حلقه استخراج

                        passed, reason = _passes_filter(user, filter_type)
                        if not passed:
                            stats["filtered_out"] += 1
                            continue
                            
                        # 🟢 فیلتر کردن کاربرانی که یوزرنیم ندارند
                        if not user.username:
                            stats["no_username"] += 1
                            stats["filtered_out"] += 1
                            continue
                        
                        target_id = f"@{user.username}"
                        valid_members[user.id] = target_id
                        total_yielded += 1
                        if filter_type in ["users", "online"]:
                            await f.write(f"{target_id}\n")
                            golden_usernames.add(target_id)
                except TimeoutError as te:
                    # 🟢 B2: هندلینگ سیگنال خطای خستگی FloodWait و تبدیل به partial_success
                    if "flood_wait_exhausted" in str(te):
                        logger.warning(f"Worker {client.name} iter_group_members partially stopped due to FloodWait limits.")
                        is_partial = True
                    else:
                        raise
                except Exception as iter_e:
                    # شکار هوشمندانه محدودیتِ ادمین بودن برای لیست‌های مخفی
                    if "ChatAdminRequired" in str(iter_e.__class__.__name__):
                        logger.warning(f"Worker {client.name}: Members hidden (ChatAdminRequired).")
                        override_status = "members_hidden"
                    else:
                        raise

                if filter_type in ["users", "golden", "online"] and total_yielded == 0:
                    # 🟢 تبدیل هوشمندانه: وقتی تلگرام به جای ارور، فقط ادمین‌ها را برمی‌گرداند،
                    # نتیجه صفر می‌شود. آن را مستقیماً به عنوان "لیست مخفی" در نظر می‌گیریم تا فال‌بک اتوماتیک به پیام‌ها فعال شود.
                    if not is_partial and override_status != "members_hidden":
                        override_status = "members_hidden"

                # 🟢 مکانیزم Fallback: تغییر مسیر به استخراج از پیام‌ها در صورت مخفی بودن اعضا
                if override_status == "members_hidden":
                    if getattr(config, "EXTRACT_AUTO_FALLBACK_TO_MESSAGES", True):
                        logger.info(f"Worker {client.name} applying fallback: switching to message extraction.")
                        override_status = "success_fallback"
                        filter_type = "messages"  # تغییر رفتار بلوک‌های بعدی به پردازش تاریخچه پیام‌ها
                        is_partial = False
                        if order_id:
                            try:
                                async with async_session() as session:
                                    await session.execute(
                                        update(Order).where(Order.id == order_id).values(reject_reason="fallback_messages")
                                    )
                                    await session.commit()
                            except Exception as e:
                                logger.error(f"Failed to update order fallback status: {e}")
                    else:
                        logger.info(f"Worker {client.name}: Members hidden, but EXTRACT_AUTO_FALLBACK_TO_MESSAGES is disabled.")

                if total_yielded >= config.EXTRACT_MEMBERS_API_LIMIT:
                    is_partial = True

                if filter_type == "golden":
                    active_user_ids: Set[int] = set()
                    message_count = 0
                    last_msg_id = 0
                    
                    retries = 3
                    while retries > 0:
                        try:
                            async for message in client.get_chat_history(chat_id, limit=2000, offset_id=last_msg_id):
                                last_msg_id = message.id
                                message_count += 1
                                if message_count % 200 == 0: # 🟢 کاهش وقفه در چت هیستوری
                                    await asyncio.sleep(random.uniform(config.EXTRACT_PAUSE_MIN, config.EXTRACT_PAUSE_MAX))

                                if message.from_user:
                                    active_user_ids.add(message.from_user.id)
                            break
                        except FloodWait as e:
                            logger.warning(f"FloodWait {e.value}s in chat_history. Retries left: {retries-1}")
                            await asyncio.sleep(e.value + random.uniform(1, 3))
                            retries -= 1
                            if retries == 0:
                                is_partial = True
                        except Exception as e:
                            logger.error(f"Error in golden chat_history: {e}")
                            is_partial = True
                            break

                    intersected_users = [valid_members[uid] for uid in active_user_ids if uid in valid_members]
                    golden_usernames = set(intersected_users)
                    # 🟢 نوشتن تقاطع طلایی
                    for target_str in golden_usernames:
                        await f.write(f"{target_str}\n")
            
            # نوشتن آمار تفکیکی انتهای فایل (برای ویزارد)
            if filter_type and filter_type.startswith("{"):
                await f.write(f"\n--- آمار خروجی ---\n")
                await f.write(f"کل بررسی شده: {stats['total_scanned']}\n")
                await f.write(f"حذف شده توسط فیلتر: {stats['filtered_out']}\n")
                await f.write(f"استخراج موفق: {len(golden_usernames)}\n")

            elif filter_type == "messages":
                message_count = 0
                last_msg_id = 0
                retries = 3
                last_report_time = time.time()

                while retries > 0:
                    try:
                        async for message in client.get_chat_history(chat_id, limit=5000, offset_id=last_msg_id):
                            last_msg_id = message.id
                            message_count += 1
                            if message_count % 300 == 0: # 🟢 بافر بزرگتر و استراحت کمتر
                                await asyncio.sleep(random.uniform(config.EXTRACT_PAUSE_MIN, config.EXTRACT_PAUSE_MAX))

                            user = message.from_user
                            if user and not user.is_deleted and not user.is_bot:
                                if user.id not in admin_ids:
                                    # 🟢 فیلتر کردن آیدی‌های عددی و ادمین‌های بدون یوزرنیم
                                    if not user.username:
                                        stats["no_username"] += 1
                                        continue
                                        
                                    target_id = f"@{user.username}"
                                    if target_id not in golden_usernames:
                                        golden_usernames.add(target_id)
                                        await f.write(f"{target_id}\n")
                                        total_yielded += 1
                                        
                            now = time.time()
                            if progress_cb and (message_count % 300 == 0 or now - last_report_time >= 10):
                                last_report_time = now
                                if await progress_cb(total_yielded, message_count) is False:
                                    is_partial = True
                                    is_stopped = True
                                    break  # 🟢 خروج فوری از حلقه پیام‌ها
                        break
                    except FloodWait as e:
                        logger.warning(f"FloodWait {e.value}s in messages history. Retries left: {retries-1}")
                        await asyncio.sleep(e.value + random.uniform(1, 3))
                        retries -= 1
                        if retries == 0:
                            is_partial = True
                    except Exception as e:
                        logger.error(f"Error in messages chat_history: {e}")
                        is_partial = True
                        break

    except _NETWORK_ERRORS as e:
        logger.error(f"Proxy/Network error during extraction for worker {client.name}: {e}")
        _report_proxy(client, False)
        if total_yielded > 0:
            return ("partial_success", file_path, chat_id, joined_now, stats["no_username"])
        if os.path.exists(file_path): os.remove(file_path)
        return ("proxy_connection_error", None, chat_id, joined_now, stats["no_username"])

    except Exception as e:
        logger.error(f"Critical error during extraction logic execution: {e}")
        # 🛡 فاز ۹: هندلینگ خطاهای بن اکانت و فعال‌سازی Circuit Breaker
        err_name = str(e.__class__.__name__)
        if any(banned_err in err_name for banned_err in ["UserDeactivated", "UserDeactivatedBan", "AuthKeyUnregistered", "SessionRevoked"]):
            logger.error(f"Worker {client.name} BANNED during extraction.")
            return ("limit:banned:0", file_path if total_yielded > 0 else None, chat_id, joined_now, stats["no_username"]) 
        
        # 🟢 تشخیص هوشمندانه: اگر ارور مربوط به نداشتن ادمینی برای دیدن اعضا بود، یعنی لیست مخفی است
        if "ChatAdminRequired" in err_name:
            return ("members_hidden", None, chat_id, joined_now, stats["no_username"])
            
        # 🟢 تعمیم منطق Partial: حفظ فایل اگر بخشی از اعضا استخراج شده‌اند
        if total_yielded > 0:
            logger.warning(f"Worker {client.name} crashed but extracted {total_yielded} users. Returning partial_success.")
            return ("partial_success", file_path, chat_id, joined_now, stats["no_username"])
            
        if os.path.exists(file_path): os.remove(file_path)
        return ("error", None, chat_id, joined_now, stats["no_username"])

    if override_status == "members_hidden":
        if os.path.exists(file_path):
            os.remove(file_path)
        return ("members_hidden", None, chat_id, joined_now, stats["no_username"])

    if override_status == "empty_filter_result":
        if os.path.exists(file_path):
            os.remove(file_path)
        return ("empty_filter_result", None, chat_id, joined_now, stats["no_username"])

    if not golden_usernames:
        logger.warning(f"Extraction yielded no valid targets from {group_link}.")
        # حذف فایل خالی
        if os.path.exists(file_path):
            os.remove(file_path)
            
        if is_stopped:
            return ("stopped", None, chat_id, joined_now, stats["no_username"])
        elif override_status == "success_fallback":
            return ("fallback_empty", None, chat_id, joined_now, stats["no_username"])
        else:
            return ("error", None, chat_id, joined_now, stats["no_username"])

    if is_stopped:
        final_status = "stopped"
    elif is_partial:
        final_status = "partial_success"
    else:
        final_status = "success"

    if override_status == "success_fallback" and not is_stopped:
        final_status = "success_fallback"

    _report_proxy(client, True)
    return (final_status, file_path, chat_id, joined_now, stats["no_username"])


# ==========================================
# 🔄 استخراج موازی (Parallel Extraction)
# این پیاده‌سازی جایگزین کاملِ نسخه‌ی بلااستفاده‌ی extract_members_parallel است.
# ==========================================

_PAGE_SIZE = 200          # سقف اندازه‌ی هر صفحه‌ی get_chat_members (محدودیت API)
_RESOLVE_TIMEOUT = 25.0   # ثانیه — گارد ضد-گیرکردنِ resolve/شمارش

# فیلترهایی که «پارتیشن‌بندی آفست» روی آن‌ها معنا دارد (لیست اعضای عادی).
# فیلترهای کوچک (ادمین/بن‌شده/…) → مسیر تک‌کلاینت.
# 💡 اگر مقادیر filter_type پروژه‌تان متفاوت است، این مجموعه را هم‌تراز کنید.
_PARALLEL_SAFE_FILTERS = {None, "", "all", "recent", "members", "search", "users", "golden", "online"}


def _member_to_line(member) -> Optional[str]:
    """🔄 ChatMember ← یک خط خروجی (هم‌تراز با فرمت فایل extract_active_users)."""
    user = getattr(member, "user", None)
    if user is None:
        return None
    username = getattr(user, "username", None)
    if username:
        return f"@{username}"
    return None  # 🟢 عدم استخراج آیدی‌های عددی


class _MergedCounter:
    """🔄 شمارنده‌ی «ادغام‌شده‌ی» پیشرفت بین ورکرهای موازی (async-safe)."""

    def __init__(self, cb):
        self._cb = cb
        self._count = 0
        self._lock = asyncio.Lock()

    async def add(self, n: int) -> None:
        async with self._lock:
            self._count += n
            snapshot = self._count
        if self._cb is None:
            return True
        try:
            if await self._cb(snapshot, snapshot) is False:
                return False  # 🟢 سیگنال توقف برای ورکرهای موازی
        except Exception:
            pass
        return True


async def _fetch_page(client: Client, chat_id: int, offset: int) -> list:
    """🔄 یک صفحه‌ی ۲۰۰تایی در آفست داده‌شده (get_chat_members یک async-generator است)."""
    return [m async for m in client.get_chat_members(chat_id, offset=offset, limit=_PAGE_SIZE)]

async def _ensure_member(
    client: Client,
    chat_id_hint: Optional[int],
    group_link: str,
    order_id: Optional[int] = None,
    speed_profile: Optional[SpeedProfile] = None
) -> tuple[Optional[int], bool, str]:
    """
    🔄 تضمین عضویت یک ورکر در گروهِ هدف با رعایت دقیق پروفایل سرعت.
    """
    from utils.speed_profile import SAFE_PROFILE
    import random
    import asyncio
    
    profile = speed_profile or SAFE_PROFILE

    # ۱) از قبل عضو است؟ (بستن R6 و چک کردن rejoin_on_redispatch)
    if chat_id_hint is not None:
        try:
            m = await client.get_chat_member(chat_id_hint, "me")
            if m.status in [ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER]:
                return chat_id_hint, False, "already_member"
        except UserNotParticipant:
            if not profile.rejoin_on_redispatch:
                logger.warning(f"Worker {client.name} not member and rejoin_on_redispatch is False.")
                return None, False, "error"
        except FloodWait as e:
            pad_min, pad_max = profile.floodwait_padding_join
            return None, False, f"limit:{LIMIT_FLOOD_WAIT}:{int(e.value + random.uniform(pad_min, pad_max))}"
        except Exception:
            pass  # peer هنوز resolve نشده → با join ادامه می‌دهیم

    # 🟢 بستن R4: پیش‌نویس کلیدهای Redis برای استخراج موازی و اعمال Stagger/RateCap
    worker_user_id = None
    redis_client = None
    backup_key = None
    if order_id:
        try:
            from workers.sender import _get_redis
            redis_client = _get_redis()
            me = getattr(client, "me", None)
            worker_user_id = me.id if me else (await client.get_me()).id
            
            backup_key = f"join_request:any_chat:user_id:{worker_user_id}"
            await redis_client.sadd(backup_key, order_id)
            await redis_client.expire(backup_key, 86400)
            
            if chat_id_hint:
                mapping_key = f"join_request:chat_id:{chat_id_hint}:user_id:{worker_user_id}"
                await redis_client.set(mapping_key, order_id, ex=86400)
                await redis_client.set(f"join_request:order:{order_id}:chat_id", chat_id_hint, ex=86400)
        except Exception as e:
            logger.warning(f"Failed to pre-write Redis keys for R4 parallel (Order #{order_id}): {e}")

    # 🛡 پلکانی‌سازی و سقف عضویت بر اساس پروفایل (Join Staggering & Rate Cap)
    if redis_client and worker_user_id:
        s_min, s_max = profile.join_stagger_seconds
        if s_max > 0:
            s_time = random.uniform(s_min, s_max)
            locked = await redis_client.set(f"join_stagger:{worker_user_id}", "1", nx=True, px=int(s_time * 1000))
            if not locked:
                return None, False, f"limit:join_stagger:{int(s_time)}"
        
        if profile.join_rate_cap_per_hour > 0:
            cap_key = f"join_rate:{worker_user_id}"
            current_joins = await redis_client.get(cap_key)
            if current_joins and int(current_joins) >= profile.join_rate_cap_per_hour:
                return None, False, "limit:join_rate:3600"

    # ۲) عضویت از طریق لینک
    joined_now = True
    try:
        await asyncio.sleep(random.uniform(profile.pre_join_sleep[0], profile.pre_join_sleep[1]))
        await client.join_chat(group_link)
        
        # ثبت موفقیت join در Rate Cap
        if redis_client and worker_user_id and profile.join_rate_cap_per_hour > 0:
            await redis_client.incr(f"join_rate:{worker_user_id}")
            if await redis_client.ttl(f"join_rate:{worker_user_id}") == -1:
                await redis_client.expire(f"join_rate:{worker_user_id}", 3600)
                
        if redis_client and order_id and backup_key:
            try:
                await redis_client.srem(backup_key, order_id)
                if chat_id_hint:
                    await redis_client.delete(f"join_request:chat_id:{chat_id_hint}:user_id:{worker_user_id}")
                    await redis_client.delete(f"join_request:order:{order_id}:chat_id")
            except Exception:
                pass
                
    except FloodWait as e:
        pad_min, pad_max = profile.floodwait_padding_join
        return None, False, f"limit:floodwait:{int(e.value + random.uniform(pad_min, pad_max))}"
    except UserAlreadyParticipant:
        joined_now = False
        if redis_client and order_id and backup_key:
            try:
                await redis_client.srem(backup_key, order_id)
                if chat_id_hint:
                    await redis_client.delete(f"join_request:chat_id:{chat_id_hint}:user_id:{worker_user_id}")
                    await redis_client.delete(f"join_request:order:{order_id}:chat_id")
            except Exception:
                pass
    except ChannelPrivate:
        return None, False, "error"
    except Exception as e:
        logger.warning(f"_ensure_member: join_chat failed for {group_link}: {e}")
        return None, False, "error"

    if target_chat_id is not None:
        try:
            await client.get_chat_member(target_chat_id, "me")
            return target_chat_id, joined_now, "success" if joined_now else "already_member"
        except UserNotParticipant:
            return None, False, "pending_approval"
        except FloodWait as e:
            pad_min, pad_max = profile.floodwait_padding_join
            return None, False, f"limit:{LIMIT_FLOOD_WAIT}:{int(e.value + random.uniform(pad_min, pad_max))}"
        except Exception:
            return target_chat_id, joined_now, "success" if joined_now else "already_member"

    return None, joined_now, "success" if joined_now else "already_member"

async def extract_members_parallel(
    clients: List[Client],
    group_link: str,
    filter_type: Optional[str] = None,
    progress_cb=None,
    total_estimate: Optional[int] = None,
    chat_id_hint: Optional[int] = None,
    order_id: Optional[int] = None,
    speed_profile: Optional[SpeedProfile] = None
) -> Dict:
    result: Dict = {
        "status_code": "error",
        "file_path": None,
        "join_records": [],
        "limit_records": [],
        "total_collected": 0,
        "total_slices": 0,
        "failed_slices": 0,
        "no_username_count": 0,
    }

    try:
        fail_threshold = float(getattr(config, "EXTRACT_PARALLEL_FAIL_THRESHOLD", 0.3))
    except (TypeError, ValueError):
        fail_threshold = 0.3

    async def _single(idx: int, client: Client) -> Dict:
        status, path, join_chat_id, joined_now, no_user_cnt = await extract_active_users(
            client, group_link, filter_type=filter_type, progress_cb=progress_cb,
            chat_id_hint=chat_id_hint, order_id=order_id, speed_profile=speed_profile
        )
        out = dict(result)
        out["status_code"] = status
        out["file_path"] = path
        out["no_username_count"] = no_user_cnt
        out["join_records"] = [{"index": idx, "join_chat_id": join_chat_id, "joined_now": joined_now, "status": status}]
        if status.startswith("limit:"):
            parts = status.split(":")
            out["limit_records"] = [{"index": idx, "limit_type": parts[1] if len(parts) > 1 else "unknown", "wait_seconds": int(parts[2]) if len(parts) > 2 else 0}]
        return out

    if not clients:
        return result
        
    # 🟢 پشتیبانی از فیلترهای ویزارد (JSON) در موتور موازی
    is_valid_parallel = (filter_type in _PARALLEL_SAFE_FILTERS) or (isinstance(filter_type, str) and filter_type.startswith("{"))
    if len(clients) == 1 or not is_valid_parallel:
        return await _single(0, clients[0])

    lead = clients[0]
    try:
        chat = await asyncio.wait_for(lead.get_chat(group_link), timeout=_RESOLVE_TIMEOUT)
    except FloodWait as e:
        result["status_code"] = f"limit:{LIMIT_FLOOD_WAIT}:{int(e.value)}"
        result["limit_records"].append({"index": 0, "limit_type": LIMIT_FLOOD_WAIT, "wait_seconds": int(e.value)})
        return result
    except Exception as e:
        logger.warning(f"parallel extract: chat resolve failed ({group_link}): {e} → single fallback")
        return await _single(0, lead)

    if getattr(chat, "type", None) != ChatType.SUPERGROUP:
        return await _single(0, lead)

    lead_chat_id, lead_joined, lead_status = await _ensure_member(lead, chat.id, group_link, order_id=order_id)
    result["join_records"].append({
        "index": 0, "join_chat_id": lead_chat_id, "joined_now": lead_joined, "status": lead_status,
    })
    
    if lead_status == "pending_approval":
        result["status_code"] = "pending_approval"
        return result
    if lead_status.startswith("limit:"):
        result["status_code"] = lead_status
        result["limit_records"].append({"index": 0, "limit_type": LIMIT_FLOOD_WAIT, "wait_seconds": int(lead_status.rsplit(":", 1)[-1]) if ":" in lead_status else 0})
        return result
    if lead_status == "error":
        result["status_code"] = "error"
        return result

    secondary_outcomes = await asyncio.gather(
        *[asyncio.create_task(_ensure_member(clients[idx], chat.id, group_link, order_id=order_id, speed_profile=speed_profile)) for idx in range(1, len(clients))],
        return_exceptions=True,
    )

    roster: List[tuple[int, Client]] = [(0, lead)]
    for idx, outcome in enumerate(secondary_outcomes, start=1):
        if isinstance(outcome, BaseException):
            result["join_records"].append({"index": idx, "join_chat_id": None, "joined_now": False, "status": "error"})
            continue
        j_chat_id, joined_now, status = outcome
        result["join_records"].append({"index": idx, "join_chat_id": j_chat_id, "joined_now": joined_now, "status": status})
        if status in ("success", "already_member"):
            roster.append((idx, clients[idx]))
        elif status.startswith("limit:"):
            result["limit_records"].append({"index": idx, "limit_type": LIMIT_FLOOD_WAIT, "wait_seconds": int(status.rsplit(":", 1)[-1]) if ":" in status else 0})

    total = total_estimate
    if not total or int(total) <= 0:
        try:
            total = await asyncio.wait_for(lead.get_chat_member_count(chat.id), timeout=_RESOLVE_TIMEOUT)
        except Exception:
            total = None
            
    if not total or int(total) <= 0:
        return await _single(0, lead)

    api_limit = int(getattr(config, "EXTRACT_MEMBERS_API_LIMIT", 10000))
    scan_range = max(1, min(int(total), api_limit))
    k = len(roster)
    seg_size = (scan_range + k - 1) // k

    slices = []
    for i, (idx, client) in enumerate(roster):
        slices.append({
            "slice_id": i, 
            "start": i * seg_size, 
            "end": min((i + 1) * seg_size, scan_range),
            "current_offset": i * seg_size,  # 🟢 رهگیری دقیق پیشرفت برای Seamless Resume
            "client": client, 
            "idx": idx, 
            "status": "pending", 
            "retries": 0
        })

    counter = _MergedCounter(progress_cb)
    sinks: Dict[int, List[str]] = {s["slice_id"]: [] for s in slices}
    worker_states: Dict[int, tuple[str, Optional[int]]] = {}

    async def _scan(sl: dict) -> None:
        idx, client = sl["idx"], sl["client"]
        sink = sinks.setdefault(sl["slice_id"], [])
        try:
            # 🟢 شروع استخراج دقیقاً از نقطه‌ی رهاشده (current_offset)
            while sl["current_offset"] < sl["end"]:
                try:
                    page = await _fetch_page(client, chat.id, sl["current_offset"])
                except FloodWait as e:
                    sl["status"] = LIMIT_FLOOD_WAIT
                    worker_states[idx] = (LIMIT_FLOOD_WAIT, int(e.value))
                    _report_proxy(client, True)
                    return
                except _NETWORK_ERRORS as e:
                    _report_proxy(client, False)
                    sl["status"] = "net_error"
                    worker_states[idx] = ("net_error", None)
                    return
                except Exception as e:
                    # 🟢 خطاهای غیرشبکه‌ای (مثل ChatAdminRequired) بدون سیگنال به پروکسی متوقف می‌شوند
                    sl["status"] = "error"
                    worker_states[idx] = ("error", None)
                    return

                if not page: break
                for m in page:
                    user = getattr(m, "user", None)
                    if user and not user.username:
                        sl["no_user_count"] = sl.get("no_user_count", 0) + 1
                    
                    # 🟢 ممیزی و اعمال فیلتر مشترک در مسیر ادغام موازی
                    if user:
                        passed, _ = _passes_filter(user, filter_type)
                        if not passed:
                            continue
                        
                    line = _member_to_line(m)
                    if line: sink.append(line)
                    
                # 🟢 آپدیت آفست برای حفظ پیشرفت در صورت قطعی
                sl["current_offset"] += len(page)
                
                if await counter.add(len(page)) is False:
                    sl["status"] = "stopped"
                    worker_states[idx] = ("stopped", None)
                    return

            sl["status"] = "ok"
            worker_states[idx] = ("ok", None)
            _report_proxy(client, True)
            
        except _NETWORK_ERRORS as e:
            _report_proxy(client, False)
            sl["status"] = "net_error"
            worker_states[idx] = ("net_error", None)
        except Exception as e:
            sl["status"] = "error"
            worker_states[idx] = ("error", None)

    # فاز اول استخراج
    await asyncio.gather(*[asyncio.create_task(_scan(sl)) for sl in slices])

    # فاز دوم: Retry برای اسلایس‌های ناموفق با حفظ موقعیت دقیق (بدون پرش کانتر)
    failed_slices = [sl for sl in slices if sl["status"] in ("net_error", "error")]
    if failed_slices:
        healthy_roster = [r for r in roster if worker_states.get(r[0], ("error",))[0] == "ok"]
        if healthy_roster:
            retry_tasks = []
            for sl in failed_slices:
                if not healthy_roster: break
                fallback_idx, fallback_client = healthy_roster.pop(0)
                # برگرداندن ورکر به انتهای صف برای استفاده‌ی مجدد در صورت نیاز
                healthy_roster.append((fallback_idx, fallback_client))
                
                sl["client"], sl["idx"] = fallback_client, fallback_idx
                sl["retries"] += 1
                sl["status"] = "pending"
                # 🟢 دقت کنید: sinks را اینجا پاک نمی‌کنیم تا دیتای قبلی حفظ شود.
                retry_tasks.append(asyncio.create_task(_scan(sl)))
                
            if retry_tasks:
                await asyncio.gather(*retry_tasks)

    # ادغام نتایج
    merged: List[str] = []
    seen: set = set()
    for sl in slices:
        for line in sinks.get(sl["slice_id"], []):
            if line not in seen:
                seen.add(line)
                merged.append(line)

    file_path = None
    if merged:
        os.makedirs("exports", exist_ok=True)
        file_path = f"exports/ext_parallel_{int(time.time() * 1000)}_{random.randint(100, 999)}.txt"
        try:
            with open(file_path, "w", encoding="utf-8", newline="\n") as f:
                f.write("\n".join(merged) + "\n")
        except Exception as e:
            logger.error(f"parallel extract: failed to write merged file: {e}")
            file_path = None
            
    result["file_path"] = file_path
    result["total_collected"] = len(merged)
    result["total_slices"] = len(slices)
    result["no_username_count"] = sum(sl.get("no_user_count", 0) for sl in slices)

    # ثبت محدودیت‌ها برای هر ورکری که در هر مرحله فِلاد-ویت خورده
    for sl in slices:
        if sl["status"] == LIMIT_FLOOD_WAIT:
            w_state = worker_states.get(sl["idx"], (LIMIT_FLOOD_WAIT, 0))
            result["limit_records"].append({"index": sl["idx"], "limit_type": LIMIT_FLOOD_WAIT, "wait_seconds": int(w_state[1] or 0)})

    final_failed = [sl for sl in slices if sl["status"] not in ("ok", "stopped")]
    result["failed_slices"] = len(final_failed)
    fail_ratio = len(final_failed) / len(slices) if slices else 0
    floodwait_slices = [sl for sl in slices if sl["status"] == LIMIT_FLOOD_WAIT]

    # 🟢 ارزیابی وضعیت نهایی بر اساس آستانه خطا (B4-1)
    if merged:
        if fail_ratio > fail_threshold:
            result["status_code"] = "error"
        else:
            result["status_code"] = "partial_success" if final_failed else "success"
    elif floodwait_slices and len(floodwait_slices) == len(slices):
        # 🟢 محاسبه دقیق تایمر انتظار بر اساس ماکزیممِ همه‌ی ورکرهای محدودشده
        max_wait = max((int(worker_states.get(sl["idx"], ("", 0))[1] or 0) for sl in floodwait_slices), default=0)
        result["status_code"] = f"limit:{LIMIT_FLOOD_WAIT}:{max_wait}"
    else:
        result["status_code"] = "members_hidden"

    logger.info(f"parallel extract {group_link}: {len(merged)} unique, {k} workers, fail_ratio: {fail_ratio:.2f}")
    return result


# ==========================================
# 🔵 رفع باگ لینک: استخراج ممبرها برای «سفارش ارسال از نوع link»
# (فیلترینگ اعضا با helper مشترک _passes_filter انجام می‌شود)
# ==========================================
async def extract_members_for_sending(
    client: Client,
    group_link: str,
    filter_type: Optional[str] = None,
    progress_cb: Optional[Callable[[int, int], Awaitable[bool]]] = None,
    chat_id_hint: Optional[int] = None,
    order_id: Optional[int] = None,
    speed_profile: Optional[SpeedProfile] = None
) -> tuple[str, Optional[List[str]], Optional[int], bool]:
    """
    Returns:
        tuple[str, Optional[List[str]], Optional[int], bool]: (کد وضعیت, لیست تارگت‌ها, chat_id, joined_now)
        ...
        chat_id/joined_now (🚪 فاز ۶ R3-ب): برای ثبت order_joins و leave بعد از اتمام سفارش.
    """
    logger.info(f"Worker {client.name} resolving link-order members of {group_link} (filter: {filter_type})")

    # گام اول: Safe-Join (کد مشترک با مسیر استخراج)
    # 🟢 تزریق پروفایل سرعت به پروسه عضویت
    join_status, chat_obj, joined_now = await safe_join_chat(
        client, group_link, chat_id_hint=chat_id_hint, order_id=order_id, speed_profile=speed_profile
    )
    chat_id = getattr(chat_obj, "id", None)
    # 👈 اصلاح باگ: اضافه شدن "already_member" به وضعیت‌های مجاز
    if join_status not in ("success", "already_member") or not chat_obj:
        return (join_status, None, chat_id, joined_now)

    # گام دوم: ادمین‌ها (کد مشترک — ادمین‌ها هرگز تارگت نمی‌شوند)
    admin_ids: Set[int] = await _collect_admin_ids(client, chat_id)

    # 🟢 درخواست کارفرما (فاز اول): استفاده از یک لیست میانی برای نگهداری کاربران همراه با اولویت فعالیتشان
    members_with_priority = []
    seen_ids: Set[int] = set()
    total_yielded = 0
    scanned_count = 0  # 🟢 اضافه شدن شمارنده اسکن واقعی برای تشخیص لیست مخفی

    try:
        # گام سوم: پیمایش امن اعضا (کد مشترک)
        # 🟢 تزریق پروفایل سرعت به پروسه پیمایش و FloodWaitهای احتمالی
        last_report_time = time.time()
        async for user in iter_group_members(client, chat_id, admin_ids, resume_seen_ids=None, speed_profile=speed_profile):
            scanned_count += 1  # 🟢 هر کاربری که تلگرام به ما داد شمرده می‌شود
            
            if not _passes_filter(user, filter_type)[0]:  # اصلاح: _passes_filter خروجی tuple دارد
                continue

            # 🟢 الزام داشتن یوزرنیم برای جلوگیری از خطای PeerIdInvalid در ورکرها
            if not user.username:
                continue

            if user.id in seen_ids:
                continue
            seen_ids.add(user.id)

            # 🟢 تعیین اولویت بر اساس وضعیت فعالیت (1 بالاترین اولویت، 6 کمترین)
            priority = 6
            status = getattr(user, "status", None)
            
            if status == UserStatus.ONLINE:
                priority = 1
            elif status == UserStatus.RECENTLY:
                priority = 2
            elif status == UserStatus.LAST_WEEK:
                priority = 3
            elif status == UserStatus.LAST_MONTH:
                priority = 4
            elif status == UserStatus.OFFLINE:
                priority = 5

            # چون در بالا شرط یوزرنیم گذاشتیم، اینجا همیشه یوزرنیم داریم
            target_id = f"@{user.username}"

            # ذخیره کاربر به همراه اولویت عددی‌اش
            members_with_priority.append((target_id, priority))

            total_yielded += 1
            now = time.time()
            if progress_cb and (total_yielded % 200 == 0 or now - last_report_time >= 10):
                last_report_time = now
                if await progress_cb(total_yielded, total_yielded) is False:
                    break  # 🟢 توقف فوری در زمان آماده‌سازی تارگت‌های ارسال

        # 🟢 تشخیص قطعی و بدون خطای مخفی بودن اعضا
        if scanned_count == 0:
            # اگر پیمایشگر هیچ عضوی پیدا نکرد، قطعا لیست اعضا مخفی است
            return ("members_hidden", None, chat_id, joined_now)
            
        if total_yielded == 0:
            # 🟢 تبدیل هوشمندانه: اگر خروجی صفر شد (مثلاً فقط ادمین‌ها برگشتند)،
            # مستقیماً آن را "لیست مخفی" در نظر می‌گیریم تا فال‌بک به پیام‌ها روشن شود.
            return ("members_hidden", None, chat_id, joined_now)

        # 🟢 مرتب‌سازی لیست تارگت‌ها بر اساس اولویت (از 1 تا 6)
        members_with_priority.sort(key=lambda x: x[1])
        
        # استخراج نام‌های کاربریِ مرتب‌شده نهایی برای تحویل به صف ارسال
        members_out = [m[0] for m in members_with_priority]

    except _NETWORK_ERRORS as e:
        logger.error(f"Network error during link-order member resolution for {group_link}: {e}")
        _report_proxy(client, False)
        return ("proxy_connection_error", None, chat_id, joined_now)
    except Exception as e:
        logger.error(f"Critical error during link-order member resolution for {group_link}: {e}", exc_info=True)
        err_name = str(e.__class__.__name__)
        
        # تفکیک دقیق خطاهای پروکسی و شبکه که ممکن است به‌صورت خام توسط کلاینت بروز کنند
        if any(kw in err_name for kw in ["Proxy", "Connection", "Timeout", "OSError"]):
            _report_proxy(client, False)
            return ("proxy_connection_error", None, chat_id, joined_now)
            
        # 🟢 سوییچ هوشمندانه برای لیست مخفی در ارسال انبوه
        if "ChatAdminRequired" in err_name:
            return ("members_hidden", None, chat_id, joined_now)
        return ("error", None, chat_id, joined_now)

    _report_proxy(client, True)
    logger.info(f"Link-order resolution for {group_link} finished with {len(members_out)} sendable targets.")
    
    status_code = "success"
    if not members_out and filter_type in ("phone", "fake", "online", "real"):
        status_code = "empty_filter_result"
        
    # 🛡 فاز ۲ (قرارداد خروجی): موفقیت هم ۴-تایی — (status, members, chat_id, joined_now)
    return (status_code, members_out, chat_id, joined_now)