import asyncio
import logging
import os
import random
import re
import uuid
from datetime import datetime
from typing import Set, Dict, Optional
import aiofiles
from pyrogram import Client
from pyrogram.enums import ChatMemberStatus
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

logger = logging.getLogger(__name__)

# Ensure the exports directory exists
os.makedirs("exports", exist_ok=True)

async def extract_active_users(client: Client, group_link: str) -> Optional[str]:
    """
    Extracts a 'Golden List' of active users from a Telegram group.
    Bypasses hidden 'Last Seen' privacy by validating actual chat history participation.
    
    NOTE: Telegram's MTProto API strictly limits `get_chat_members` to the first 10,000 members.
    This script explicitly acknowledges this limitation to avoid excessive API requests and bans.
    For groups larger than 10k, it processes the maximum allowed members before proceeding to validation.
    
    Args:
        client (Client): The active Pyrogram MTProto worker client.
        group_link (str): The invite link or public username of the target group.
        
    Returns:
        Optional[str]: The file path of the generated .txt document containing the usernames, 
                       or None if the extraction fails due to an invalid link or permissions.
    """
    logger.info(f"Worker {client.name} starting extraction for {group_link}")

    # ==========================================
    # STEP 1: Join the Group
    # ==========================================
    chat_id = None
    try:
        # Simulate human hesitation before joining
        await asyncio.sleep(random.uniform(2, 5))
        chat = await client.join_chat(group_link)
        chat_id = chat.id
        logger.info(f"Worker {client.name} joined {chat.title} successfully.")
        
    except FloodWait as e:
        logger.warning(f"Worker {client.name} hit FloodWait of {e.value}s while joining. Sleeping...")
        await asyncio.sleep(e.value + random.uniform(2, 5))
        # Optional: In a production system with strict retry limits, you might want to wrap this in a loop.
        chat = await client.join_chat(group_link)
        chat_id = chat.id
        
    except UserAlreadyParticipant:
        logger.info(f"Worker {client.name} is already a member of the group.")
        chat = await client.get_chat(group_link)
        chat_id = chat.id
        
    # --- NEW: Edge Case Error Handlers ---
    except InviteRequestSent:
        logger.error(f"Worker {client.name} failed: {group_link} is private and requires admin approval. Aborting.")
        return None
        
    except (InviteHashExpired, InviteHashInvalid):
        logger.error(f"Worker {client.name} failed: The invite link {group_link} is expired or invalid. Aborting.")
        return None
        
    except (PeerIdInvalid, UsernameInvalid, UsernameNotOccupied):
        logger.error(f"Worker {client.name} failed: The target group {group_link} does not exist. Aborting.")
        return None
        
    except Exception as e:
        logger.error(f"Worker {client.name} encountered an unexpected error joining {group_link}: {e}")
        return None

    # Safeguard if chat_id wasn't set somehow
    if not chat_id:
        return None

    # Simulated delay after joining
    await asyncio.sleep(random.uniform(3, 6))


    # ==========================================
    # STEP 2: Fetch & Filter Members (Noise Reduction)
    # ==========================================
    valid_members: Dict[int, str] = {}
    member_count = 0
    
    logger.info("Fetching and filtering chat members (API Limit: max 10,000 members)...")
    
    async for member in client.get_chat_members(chat_id):
        member_count += 1
        
        if member_count % 200 == 0:
            await asyncio.sleep(random.uniform(3, 7))

        user = member.user
        
        if not user or user.is_deleted or user.is_bot or not user.username:
            continue
            
        if member.status in [ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER]:
            continue

        valid_members[user.id] = user.username

    if member_count >= 10000:
        logger.warning(
            f"Worker {client.name} hit the Telegram API 10,000 member limit for {group_link}. "
            "Proceeding to history validation with the fetched users."
        )

    logger.info(f"Filtered down to {len(valid_members)} valid standard members out of {member_count} fetched.")


    # ==========================================
    # STEP 3 & 4: Fetch History & Activity Validation
    # ==========================================
    active_user_ids: Set[int] = set()
    message_count = 0
    
    logger.info("Fetching chat history to determine true activity...")

    async for message in client.get_chat_history(chat_id, limit=1000):
        message_count += 1
        
        if message_count % 100 == 0:
            await asyncio.sleep(random.uniform(3, 7))

        if message.from_user:
            active_user_ids.add(message.from_user.id)

    logger.info(f"Found {len(active_user_ids)} unique senders in the last 1000 messages.")

    golden_usernames = [
        valid_members[uid] for uid in active_user_ids if uid in valid_members
    ]


    # ==========================================
    # STEP 5: File Generation (Optimized for Concurrency)
    # ==========================================
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    unique_id = uuid.uuid4().hex[:8]
    
    raw_name = group_link.split("/")[-1].replace("+", "").replace("joinchat-", "")
    safe_link_name = re.sub(r'[\\/*?:"<>|]', "", raw_name)
    
    file_path = f"exports/golden_list_{safe_link_name}_{timestamp}_{unique_id}.txt"
    
    # استفاده از aiofiles برای جلوگیری از قفل شدن Event Loop
    async with aiofiles.open(file_path, "w", encoding="utf-8") as f:
        for username in golden_usernames:
            await f.write(f"@{username}\n")

    logger.info(f"Extraction complete! {len(golden_usernames)} highly active users saved to {file_path}")
    
    return file_path