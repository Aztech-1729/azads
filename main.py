import asyncio
import io
import json
import logging
import os
import random
import re
import requests
import string
import subprocess
import sys
import tempfile
import threading
import time
from datetime import datetime, timedelta
from typing import Dict, Any, List, Tuple, Optional, Union
from zoneinfo import ZoneInfo
from cryptography.fernet import Fernet, InvalidToken
from telethon import TelegramClient, functions, types, events
from telethon.sessions import StringSession
from telethon.tl.functions.channels import GetForumTopicsRequest
from telethon.tl.types import ForumTopic, PeerChannel
from telethon.errors import (
    SessionPasswordNeededError,
    FloodWaitError,
    UpdateAppToLoginError,
    PhoneNumberInvalidError,
    PhoneCodeInvalidError,
    PhoneCodeExpiredError,
    SessionExpiredError,
    PasswordHashInvalidError,
    RPCError,
    ChannelInvalidError,
    ChatWriteForbiddenError
)
from pyrogram import Client as PyroClient, filters, idle
from pyrogram.types import (
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    InputMediaPhoto,
    ReplyKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardRemove,
)
from pyrogram.errors import (
    UserNotParticipant,
    PeerIdInvalid,
    ChatWriteForbidden,
    FloodWait,
    MessageNotModified,
)
from pyrogram.enums import ParseMode, ChatType
import config
from database import EnhancedDatabaseManager

IST = ZoneInfo("Asia/Kolkata")

def get_ist_now():
    """Get current time in IST timezone"""
    return datetime.now(IST)

async def get_telegram_client(phone_number, session_string):
    """
    Create and connect a Telegram client for the given account.
    Returns connected TelegramClient instance.
    """
    try:
        # Validate session string
        if not session_string or not isinstance(session_string, str) or len(session_string) < 10:
            raise Exception(f"Invalid session string for {phone_number}")
        
        # Decrypt session string
        try:
            decrypted_session = cipher_suite.decrypt(session_string.encode()).decode()
        except Exception as e:
            raise Exception(f"Failed to decrypt session for {phone_number}: {e}")
        
        credentials = {
            'api_id': config.BOT_API_ID,
            'api_hash': config.BOT_API_HASH
        }
        
        tg_client = TelegramClient(
            StringSession(decrypted_session),
            credentials['api_id'],
            credentials['api_hash']
        )
        
        await tg_client.connect()
        
        # Verify connection
        if not await tg_client.is_user_authorized():
            await tg_client.disconnect()
            raise Exception(f"Client for {phone_number} is not authorized")
        
        return tg_client
    except Exception as e:
        logger.error(f"Error creating Telegram client for {phone_number}: {e}")
        raise

def _strip_query_frag(s: str) -> str:
    """Remove query string and fragment from URL"""
    s = s.split('?')[0]
    s = s.split('#')[0]
    return s

def parse_post_link(link: str) -> Optional[Tuple[Union[int, str], int]]:
    """
    Parse Telegram post links.
    Accepts links like:
      - https://t.me/username/123
      - t.me/username/123
      - https://t.me/c/123456/789 (private supergroup/channel)
    Returns (from_peer, msg_id) where from_peer is 'username' or a numeric -100... id.
    """
    s = (link or "").strip()
    # Allow plain "username/123" too
    s = s.replace("https://", "").replace("http://", "").lstrip("@")
    if s.startswith("t.me/"):
        s = s[len("t.me/"):]
    s = _strip_query_frag(s).strip("/")

    # t.me/c/<internal_id>/<msg_id>
    m = re.fullmatch(r"c/(\d+)/(\d+)", s)
    if m:
        internal_id = int(m.group(1))
        msg_id = int(m.group(2))
        from_peer_disp = -100 * internal_id
        return (from_peer_disp, msg_id)
    
    # t.me/username/msg_id
    m = re.fullmatch(r"([a-zA-Z0-9_]+)/(\d+)", s)
    if m:
        username = m.group(1)
        msg_id = int(m.group(2))
        return (username, msg_id)
    
    return None

def is_within_schedule(user_data: dict) -> Tuple[bool, str]:
    """
    Check if current time is within user's scheduled time range (IST timezone).
    Returns (is_within_schedule, message)
    """
    try:
        if not user_data.get("schedule_enabled", False):
            return True, "Schedule disabled"
        
        start_time_str = user_data.get("schedule_start_time", "12:00 AM")
        end_time_str = user_data.get("schedule_end_time", "11:59 PM")
        
        current_time = get_ist_now()
        
        start_time = datetime.strptime(start_time_str, "%I:%M %p").time()
        end_time = datetime.strptime(end_time_str, "%I:%M %p").time()
        
        current_time_only = current_time.time()
        
        if start_time <= end_time:
            within_schedule = start_time <= current_time_only <= end_time
        else:
            within_schedule = current_time_only >= start_time or current_time_only <= end_time
        
        if within_schedule:
            return True, "Within schedule"
        else:
            if start_time <= end_time:
                if current_time_only < start_time:
                    next_start = datetime.combine(current_time.date(), start_time, tzinfo=IST)
                else:
                    next_start = datetime.combine(current_time.date() + timedelta(days=1), start_time, tzinfo=IST)
            else:
                if current_time_only > end_time and current_time_only < start_time:
                    next_start = datetime.combine(current_time.date(), start_time, tzinfo=IST)
                else:
                    next_start = datetime.combine(current_time.date() + timedelta(days=1), start_time, tzinfo=IST)
            
            wait_duration = next_start - current_time
            hours = int(wait_duration.total_seconds() // 3600)
            minutes = int((wait_duration.total_seconds() % 3600) // 60)
            
            return False, f"Outside schedule. Resumes at {start_time_str} (in {hours}h {minutes}m)"
    
    except Exception as e:
        logger.error(f"Error checking schedule: {e}")
        return True, "Schedule check error - continuing"

def calculate_remaining_time_today(user_data: dict) -> Tuple[bool, int, str]:
    """
    Calculate how much time is remaining in today's schedule (IST timezone).
    Returns (should_continue, remaining_seconds, message)
    
    This handles the case where user starts ads after schedule start time,
    and should only run until end time (not full duration).
    """
    try:
        if not user_data.get("schedule_enabled", False):
            return True, float('inf'), "Schedule disabled - no time limit"
        
        start_time_str = user_data.get("schedule_start_time", "12:00 AM")
        end_time_str = user_data.get("schedule_end_time", "11:59 PM")
        
        current_time = get_ist_now()
        
        start_time = datetime.strptime(start_time_str, "%I:%M %p").time()
        end_time = datetime.strptime(end_time_str, "%I:%M %p").time()
        current_time_only = current_time.time()
        
        if start_time <= end_time:
            if current_time_only < start_time:
                return False, 0, f" <b>Schedule Not Started</b>\n\nYou started the ads before the scheduled time.\n\n<b>Start Time:</b> {start_time_str}\n<b>Current Time:</b> {current_time.strftime('%I:%M %p')}\n\n<i>Ads will start automatically tomorrow at {start_time_str}</i>"
            elif current_time_only > end_time:
                return False, 0, f" <b>Schedule Already Ended</b>\n\nYou started the ads after the scheduled end time.\n\n<b>End Time:</b> {end_time_str}\n<b>Current Time:</b> {current_time.strftime('%I:%M %p')}\n\n<i>Ads will start automatically tomorrow at {start_time_str}</i>"
            else:
                end_datetime = datetime.combine(current_time.date(), end_time, tzinfo=IST)
                remaining_seconds = (end_datetime - current_time).total_seconds()
                
                hours = int(remaining_seconds // 3600)
                minutes = int((remaining_seconds % 3600) // 60)
                
                return True, remaining_seconds, f" <b>Time-Based Schedule Active</b>\n\n<b>End Time:</b> {end_time_str}\n<b>Remaining:</b> {hours}h {minutes}m\n\n<i>Ads will run until {end_time_str} today and continue tomorrow from {start_time_str}</i>"
        else:
            if current_time_only >= start_time:
                next_day_end = datetime.combine(current_time.date() + timedelta(days=1), end_time, tzinfo=IST)
                remaining_seconds = (next_day_end - current_time).total_seconds()
            elif current_time_only <= end_time:
                today_end = datetime.combine(current_time.date(), end_time, tzinfo=IST)
                remaining_seconds = (today_end - current_time).total_seconds()
            else:
                return False, 0, f" <b>Schedule Already Ended</b>\n\nYou started the ads after the scheduled end time.\n\n<b>End Time:</b> {end_time_str}\n<b>Current Time:</b> {current_time.strftime('%I:%M %p')}\n\n<i>Ads will start automatically at {start_time_str}</i>"
            
            hours = int(remaining_seconds // 3600)
            minutes = int((remaining_seconds % 3600) // 60)
            
            return True, remaining_seconds, f" <b>Time-Based Schedule Active</b>\n\n<b>End Time:</b> {end_time_str}\n<b>Remaining:</b> {hours}h {minutes}m\n\n<i>Ads will run until {end_time_str} and continue tomorrow from {start_time_str}</i>"
    
    except Exception as e:
        logger.error(f"Error calculating remaining time: {e}")
        return True, float('inf'), "Schedule check error - continuing"

def validate_phone_number(phone: str) -> bool:
    """Validate phone number format"""
    cleaned = re.sub(r'[^\d+]', '', phone)
    pattern = r'^\+\d{10,15}$'
    return bool(re.match(pattern, cleaned))

def generate_progress_bar(completed: int, total: int, length: int = 10) -> str:
    """Generate visual progress bar"""
    if total == 0:
        return "▓" * length + " 0%"
    
    percentage = (completed / total) * 100
    filled = int((completed / total) * length)
    bar = "▓" * filled + "░" * (length - filled)
    
    return f"{bar} {percentage:.1f}%"

def format_duration(td: timedelta) -> str:
    """Format timedelta to human readable string"""
    total_seconds = int(td.total_seconds())
    
    if total_seconds < 60:
        return f"{total_seconds}s"
    elif total_seconds < 3600:
        minutes = total_seconds // 60
        seconds = total_seconds % 60
        return f"{minutes}m {seconds}s"
    else:
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        return f"{hours}h {minutes}m"

def validate_delay(delay_str: str) -> Tuple[bool, int]:
    """Validate and return delay value"""
    try:
        delay = int(delay_str)
        if delay < 10 or delay > 600:
            return False, 0
        return True, delay
    except ValueError:
        return False, 0

def calculate_success_rate(sent: int, failed: int) -> float:
    """Calculate success rate percentage"""
    total = sent + failed
    if total == 0:
        return 0.0
    return (sent / total) * 100

def mask_phone_number(phone: str) -> str:
    """Mask phone number for privacy"""
    if not phone:
        return "Unknown"
    
    # +919332618150 -> +91********150
    if len(phone) > 6:
        return phone[:3] + "*" * (len(phone) - 6) + phone[-3:]
    return phone

def format_runtime(seconds: int) -> str:
    """Format runtime in human-readable format"""
    if seconds < 60:
        return f"{seconds}s"
    
    minutes = seconds // 60
    remaining_seconds = seconds % 60
    
    if minutes < 60:
        return f"{minutes}m {remaining_seconds}s"
    
    hours = minutes // 60
    remaining_minutes = minutes % 60
    return f"{hours}h {remaining_minutes}m"

def format_broadcast_summary(sent: int, failed: int, duration: timedelta) -> str:
    """Format broadcast completion summary"""
    total = sent + failed
    success_rate = (sent / total * 100) if total > 0 else 0
    
    return (
        f"<b>BROADCAST SUMMARY</b>\n\n"
        f"+ <b>Sent:</b> {sent:,}\n"
        f"- <b>Failed:</b> {failed:,}\n"
        f"📈 <b>Success Rate:</b> {success_rate:.1f}%\n"
        f" <b>Duration:</b> {format_duration(duration)}\n"
        f" <b>Performance:</b> {generate_progress_bar(sent, total)}"
    )

def create_analytics_summary(analytics: Dict) -> str:
    """Create formatted analytics summary"""
    total_sent = analytics.get('total_sent', 0)
    total_failed = analytics.get('total_failed', 0)
    success_rate = calculate_success_rate(total_sent, total_failed)
    
    return (
        f"<b>PERFORMANCE ANALYTICS</b>\n\n"
        f"📈 <b>Broadcasts:</b> {analytics.get('total_broadcasts', 0):,}\n"
        f"+ <b>Sent:</b> {total_sent:,}\n"
        f"- <b>Failed:</b> {total_failed:,}\n"
        f" <b>Success Rate:</b> {success_rate:.1f}%\n"
        f"# <b>Accounts:</b> {analytics.get('total_accounts', 0)}"
    )

def format_error_message(error_type: str, context: str = "") -> str:
    """Format error messages consistently"""
    base_message = config.ERROR_MESSAGES.get(error_type, "! An error occurred")
    if context:
        return f"{base_message}\n\n🔍 <b>Context:</b> {context}"
    return base_message

def format_success_message(success_type: str, context: str = "") -> str:
    """Format success messages consistently"""
    base_message = config.SUCCESS_MESSAGES.get(success_type, "[OK] Operation successful")
    if context:
        return f"{base_message}\n\n📋 <b>Details:</b> {context}"
    return base_message

def kb(buttons: List[List[Any]]) -> InlineKeyboardMarkup:
    """Create inline keyboard from button list"""
    keyboard = []
    for row in buttons:
        row_buttons = []
        for button in row:
            if isinstance(button, dict):
                if 'url' in button:
                    row_buttons.append(InlineKeyboardButton(button['text'], url=button['url']))
                else:
                    row_buttons.append(InlineKeyboardButton(button['text'], callback_data=button['callback_data']))
            else:
                row_buttons.append(button)
        keyboard.append(row_buttons)
    return InlineKeyboardMarkup(keyboard)

# =======================================================
#  INITIALIZATION & CONFIGURATION
# =======================================================

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="ignore")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="ignore")

os.environ["PYTHONIOENCODING"] = "utf-8"

# =======================================================
# 🧠 LOGGING CONFIGURATION
# =======================================================
logging.getLogger("__main__").setLevel(logging.INFO)
logging.getLogger("pyrogram").setLevel(logging.ERROR)
logging.getLogger("telethon").setLevel(logging.ERROR)

db_logger = logging.getLogger("database")
db_logger.setLevel(logging.INFO)

def _ignore_socket_warnings(loop, context):
    """Suppress harmless asyncio 'socket.send() raised exception' warnings."""
    msg = context.get("message", "")
    exc = context.get("exception")

    if isinstance(exc, OSError) or "socket.send" in msg:
        logging.getLogger("asyncio").debug(f"Ignored asyncio socket warning: {msg}")
        return

    loop.default_exception_handler(context)

try:
    asyncio.get_event_loop().set_exception_handler(_ignore_socket_warnings)
except RuntimeError:
    pass

# =======================================================
# 🧩 OTHER GLOBALS & SETUP
# =======================================================

MAIN_LOOP = None

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

print("AZTECH ADS Bot Started Successfully ")

# =======================================================
# 🔐 ENCRYPTION KEY INITIALIZATION
# =======================================================

ENCRYPTION_KEY = getattr(config, 'ENCRYPTION_KEY', None)
KEY_FILE = 'encryption.key'

if not ENCRYPTION_KEY:
    logger.warning("No ENCRYPTION_KEY in config. Loading or generating from file.")
    if os.path.exists(KEY_FILE):
        with open(KEY_FILE, 'r', encoding='utf-8') as f:
            ENCRYPTION_KEY = f.read().strip()
    else:
        ENCRYPTION_KEY = Fernet.generate_key().decode()
        with open(KEY_FILE, 'w', encoding='utf-8') as f:
            f.write(ENCRYPTION_KEY)
        logger.info("Generated and saved new encryption key to encryption.key")
else:
    with open(KEY_FILE, 'w', encoding='utf-8') as f:
        f.write(ENCRYPTION_KEY)
    logger.info("Using ENCRYPTION_KEY from config and saved to file.")

cipher_suite = Fernet(ENCRYPTION_KEY.encode())

# =======================================================
# 🗄️ DATABASE INITIALIZATION
# =======================================================
db = EnhancedDatabaseManager()

# =======================================================
# 🗣️ GROUPS MANAGEMENT SYSTEM
# =======================================================

def ensure_db_methods(db):
    """Ensure database has required group management methods"""
    
    if not hasattr(db, 'add_target_group'):
        def add_target_group(self, user_id, group_id, title):
            """Add a target group for broadcasting"""
            self.db.target_groups.update_one(
                {
                    'user_id': user_id,
                    'group_id': group_id
                },
                {
                    '$set': {
                        'user_id': user_id,
                        'group_id': group_id,
                        'title': title,
                        'added_at': datetime.now()
                    }
                },
                upsert=True
            )
        setattr(db.__class__, 'add_target_group', add_target_group)
    
    if not hasattr(db, 'remove_target_group'):
        def remove_target_group(self, user_id, group_id):
            """Remove a target group from broadcasting"""
            self.db.target_groups.delete_one({
                'user_id': user_id,
                'group_id': group_id
            })
        setattr(db.__class__, 'remove_target_group', remove_target_group)
    
    if not hasattr(db, 'get_target_group'):
        def get_target_group(self, user_id, group_id):
            """Get a specific target group"""
            return self.db.target_groups.find_one({
                'user_id': user_id,
                'group_id': group_id
            })
        setattr(db.__class__, 'get_target_group', get_target_group)
    

# ========================================
# MONGODB GROUPS CACHE SYSTEM
# ========================================

async def get_groups_from_mongo_cache(uid):
    """INSTANT: Get groups from MongoDB cache"""
    try:
        cached_groups = db.get_cached_groups(uid)
        
        if cached_groups:
            groups = []
            for doc in cached_groups:
                groups.append({
                    'id': doc.get('group_id'),
                    'title': doc.get('title', 'Unknown'),
                    'username': doc.get('username'),
                    'type': doc.get('type', 'group'),
                    'members_count': doc.get('members_count', 0),
                    'account_phone': doc.get('account_phone'),
                    'selected': True
                })
            logger.info(f"[CACHE] INSTANT MongoDB retrieval: {len(groups)} groups for user {uid}")
            return groups
        else:
            logger.info(f"[CACHE] No cache found for user {uid}, fetching fresh...")
            return await fetch_and_cache_groups_to_mongo(uid)
            
    except Exception as e:
        logger.error(f"[CACHE] Error getting groups from MongoDB for user {uid}: {e}")
        return []

async def fetch_and_cache_groups_to_mongo(uid):
    """Fetch all groups from Telegram and save to MongoDB"""
    try:
        accounts = db.get_user_accounts(uid)
        if not accounts:
            logger.warning(f"[CACHE] No accounts for user {uid}")
            return []
        
        async def fetch_account_groups(acc):
            tg_client = None
            try:
                session_str = cipher_suite.decrypt(acc['session_string'].encode()).decode()
                credentials = db.get_user_api_credentials(acc['user_id'])
                if not credentials:
                    return []
                    
                tg_client = TelegramClient(StringSession(session_str), credentials['api_id'], credentials['api_hash'])
                await tg_client.connect()
                
                groups = []
                async for dialog in tg_client.iter_dialogs():
                    if dialog.is_group:
                        try:
                            entity = await tg_client.get_entity(dialog.id)
                            is_forum = getattr(entity, 'forum', False)
                            if not is_forum:
                                groups.append({
                                    'id': dialog.id,
                                    'title': dialog.title,
                                    'username': getattr(dialog.entity, 'username', None),
                                    'type': 'group',
                                    'members_count': getattr(entity, 'participants_count', 0),
                                    'account_phone': acc.get('phone_number')
                                })
                        except:
                            pass
                return groups
            except Exception as e:
                logger.error(f"[CACHE] Error fetching groups for {acc.get('phone_number')}: {e}")
                return []
            finally:
                if tg_client:
                    try:
                        await tg_client.disconnect()
                    except:
                        pass
        
        start = time.time()
        tasks = [fetch_account_groups(acc) for acc in accounts]
        all_groups_lists = await asyncio.gather(*tasks, return_exceptions=True)
        
        all_groups = []
        seen_ids = set()
        for groups in all_groups_lists:
            if isinstance(groups, list):
                for g in groups:
                    if g['id'] not in seen_ids:
                        seen_ids.add(g['id'])
                        all_groups.append(g)
        
        if all_groups:
            db.save_groups_to_cache(uid, all_groups)
            logger.info(f"[CACHE] Fetched and cached {len(all_groups)} groups in {time.time()-start:.2f}s")
        
        return [{'id': g['id'], 'title': g['title'], 'selected': True, **g} for g in all_groups]
        
    except Exception as e:
        logger.error(f"[CACHE] Failed to fetch and cache groups for user {uid}: {e}")
        return []

async def refresh_mongo_cache(uid):
    """Refresh: Add only NEW groups not in cache"""
    try:
        accounts = db.get_user_accounts(uid)
        if not accounts:
            return 0
        
        async def fetch_account_groups(acc):
            tg_client = None
            try:
                session_str = cipher_suite.decrypt(acc['session_string'].encode()).decode()
                credentials = db.get_user_api_credentials(acc['user_id'])
                if not credentials:
                    return []
                    
                tg_client = TelegramClient(StringSession(session_str), credentials['api_id'], credentials['api_hash'])
                await tg_client.connect()
                
                groups = []
                async for dialog in tg_client.iter_dialogs():
                    if dialog.is_group:
                        try:
                            entity = await tg_client.get_entity(dialog.id)
                            is_forum = getattr(entity, 'forum', False)
                            if not is_forum:
                                groups.append({
                                    'id': dialog.id,
                                    'title': dialog.title,
                                    'username': getattr(dialog.entity, 'username', None),
                                    'type': 'group',
                                    'members_count': getattr(entity, 'participants_count', 0),
                                    'account_phone': acc.get('phone_number')
                                })
                        except:
                            pass
                return groups
            except Exception as e:
                return []
            finally:
                if tg_client:
                    try:
                        await tg_client.disconnect()
                    except:
                        pass
        
        tasks = [fetch_account_groups(acc) for acc in accounts]
        all_groups_lists = await asyncio.gather(*tasks, return_exceptions=True)
        
        all_groups = []
        seen_ids = set()
        for groups in all_groups_lists:
            if isinstance(groups, list):
                for g in groups:
                    if g['id'] not in seen_ids:
                        seen_ids.add(g['id'])
                        all_groups.append(g)
        
        new_count = db.refresh_groups_cache(uid, all_groups)
        logger.info(f"[CACHE] Refreshed: added {new_count} new groups")
        return new_count
        
    except Exception as e:
        logger.error(f"[CACHE] Refresh failed for user {uid}: {e}")
        return 0

async def fetch_groups_after_account_add(uid):
    """Fetch ALL groups after adding account and save to cache"""
    try:
        logger.info(f"[CACHE] Fetching all groups after account add for user {uid}")
        await fetch_and_cache_groups_to_mongo(uid)
        logger.info(f"[CACHE] Groups fetch complete for user {uid}")
    except Exception as e:
        logger.error(f"[CACHE] Error fetching groups after account add: {e}")

def clear_groups_cache(uid):
    """Clear MongoDB cache for user"""
    db.delete_groups_cache(uid)
    logger.info(f"[CACHE] Cleared MongoDB cache for user {uid}")

async def auto_select_all_groups(uid, phone):
    """Auto-select all groups for a newly added account"""
    try:
        logger.info(f"Auto-selecting all groups for user {uid}, phone {phone}")
        
        accounts = db.get_user_accounts(uid)
        new_account = None
        for acc in accounts:
            if acc['phone_number'] == phone:
                new_account = acc
                break
        
        if not new_account:
            logger.warning(f"Could not find newly added account {phone} for user {uid}")
            return
        
        try:
            session_str = cipher_suite.decrypt(new_account['session_string'].encode()).decode()
            
            credentials = db.get_user_api_credentials(uid)
            
            if not credentials:
                logger.error(f"No API credentials found for user {uid}")
                return
            
            async with TelegramClient(StringSession(session_str), credentials['api_id'], credentials['api_hash']) as tg_client:
                existing_groups = db.get_target_groups(uid) or []
                existing_ids = {g['group_id'] for g in existing_groups}
                
                added_count = 0
                async for dialog in tg_client.iter_dialogs():
                    if dialog.is_group and dialog.id not in existing_ids:
                        try:
                            entity = await tg_client.get_entity(dialog.id)
                            is_forum = getattr(entity, 'forum', False)
                            if not is_forum:
                                db.add_target_group(uid, dialog.id, dialog.title)
                                added_count += 1
                        except Exception as e:
                            logger.warning(f"Failed to add group {dialog.title}: {e}")
                
                logger.info(f"Auto-selected {added_count} groups for user {uid}")
                
        except Exception as e:
            logger.error(f"Error fetching groups for auto-selection: {e}")
        finally:
            db.delete_temp_data(uid, "api_id")
            db.delete_temp_data(uid, "api_hash")
            logger.info(f"Cleaned up temp API credentials for user {uid}")
            
    except Exception as e:
        logger.error(f"Error in auto_select_all_groups: {e}")


# =======================================================
# 🧠 DATABASE INITIALIZATION
# =======================================================
try:
    db = EnhancedDatabaseManager()
    logger.info("[DB] Database initialized successfully.")
    
    ensure_db_methods(db)
    logger.info("[DB] Database methods initialized successfully.")

except Exception as e:
    logger.error(f" Failed to initialize database: {e}. Exiting.")
    print("Bot failed to start due to database error. Check logs/AzTechAdsBot.log for details.")
    exit(1)

# =======================================================
#  ACCOUNT MANAGER INITIALIZATION
# =======================================================

ADMIN_IDS = config.ADMIN_IDS
ALLOWED_BD_IDS = ADMIN_IDS + [6670166083]

def is_owner(uid):
    return uid in ALLOWED_BD_IDS

def get_user_api_credentials_or_error(user_id):
    """Get user API credentials or return error message"""
    try:
        credentials = db.get_user_api_credentials(user_id)
        if not credentials:
            return None, f"<b>API Credentials Required</b>\n\n" \
                        f"You need to set up your API credentials first.\n\n" \
                        f"<b>Get your API credentials:</b>\n" \
                        f"1. Visit https://my.telegram.org\n" \
                        f"2. Login with your phone number\n" \
                        f"3. Go to 'API Development tools'\n" \
                        f"4. Create an app and get API ID & Hash\n\n" \
                        f"Then use the bot to add your first account!"
        return credentials, None
    except Exception as e:
        logger.error(f"Error getting API credentials for {user_id}: {e}")
        return None, "Error retrieving API credentials"

def kb(rows):
    if not isinstance(rows, list) or not all(isinstance(row, list) for row in rows):
        logger.error("Invalid rows format for InlineKeyboardMarkup")
        raise ValueError("Rows must be a list of lists")
    return InlineKeyboardMarkup(rows)

try:
    asyncio.get_running_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

pyro = PyroClient(
    "AzTechAdsBot",
    api_id=config.BOT_API_ID,
    api_hash=config.BOT_API_HASH,
    bot_token=config.BOT_TOKEN,
    workdir="./sessions",
    no_updates=False
)

# Initialize logger bot client
logger_client = PyroClient(
    "logger_bot",
    api_id=config.BOT_API_ID,
    api_hash=config.BOT_API_HASH,
    bot_token=config.LOGGER_BOT_TOKEN,
    workdir="./sessions"
)

telethon_bot = TelegramClient(
    'sessions/telethon_bot',
    config.BOT_API_ID,
    config.BOT_API_HASH
)

os.makedirs("./sessions", exist_ok=True)

# ================================================
# LOGGER BOT COMMAND HANDLER
# ================================================

@logger_client.on_message(filters.command(["start"]))
async def logger_start_command(client, message):
    """Handle logger bot start command"""
    try:
        logger.info(f"[LOGGER BOT] Received /start from user {message.from_user.id}")
        
        uid = message.from_user.id
        username = message.from_user.username or "Unknown"
        first_name = message.from_user.first_name or "User"
        
        db.create_user(uid, username, first_name)
        db.set_logger_status(uid, is_active=True)
        
        await message.reply(
            f"<b>✨ Welcome to AzTech Ads Bot Logger! 📊</b>\n\n"
            f"Logs for your ad broadcasts will be sent here.\n"
            f"Start the main bot (@aztechadsbot) to begin broadcasting! 🚀",
            parse_mode=ParseMode.HTML
        )
        logger.info(f"[LOGGER BOT] Successfully sent welcome message to user {uid}")
        
    except Exception as e:
        logger.error(f"[LOGGER BOT ERROR] {e}", exc_info=True)

# Logger bot lifecycle functions
async def start_logger_bot():
    """Start the logger bot"""
    try:
        await logger_client.start()
        logger.info("✅ Logger bot started successfully")
    except Exception as e:
        logger.error(f"❌ Failed to start logger bot: {e}")
        raise

async def stop_logger_bot():
    """Stop the logger bot"""
    try:
        await logger_client.stop()
        logger.info("✅ Logger bot stopped successfully")
    except Exception as e:
        logger.error(f"❌ Failed to stop logger bot: {e}")

# ================================================
# LOGGER BOT MESSAGING FUNCTIONS
# ================================================

async def send_logger_message(user_id: int, text: str, pyro_client=None):
    """Send a short log message to the user's logger bot DM"""
    try:
        if not db.get_logger_status(user_id):
            return
        
        await logger_client.send_message(user_id, text, parse_mode=ParseMode.HTML)
        
    except Exception as e:
        logger.error(f"Logger message error for user {user_id}: {e}")

async def send_dm_log(user_id: int, log_message: str):
    """Send DM log to user via logger bot"""
    try:
        if not db.get_logger_status(user_id):
            return
        
        await logger_client.send_message(user_id, log_message, parse_mode=ParseMode.HTML)
        
    except Exception as e:
        logger.error(f"DM log error for user {user_id}: {e}")

# Analysis logging functions
async def send_analysis_start(user_id: int, broadcast_mode: str, target_count: int):
    """Send analysis start message"""
    mode_display = {
        "groups_only": " <b>Groups Only Mode</b>",
        "forums_only": " <b>Topics Only Mode</b>",
        "both": " <b>Both Groups & Topics Mode</b>"
    }
    mode_text = mode_display.get(broadcast_mode, " <b>All Groups Mode</b>")
    
    message = (
        f"<b>🔍 ANALYZING GROUPS</b>\n\n"
        f"<b>Mode:</b> {mode_text}\n"
        f"<b>Target Groups:</b> {target_count} groups\n\n"
        f" <b>Checking for:</b>\n"
        f"• Access permissions\n"
        f"• Account bans/restrictions\n"
        f"• Group availability\n"
        f"{'• Forum topics detection ' if broadcast_mode == 'forums_only' else '• Forum topics status'}\n\n"
        f"⏳ <i>Please wait, analyzing all groups...</i>"
    )
    
    return await send_dm_log(user_id, message)

async def send_analysis_complete(user_id: int, total_selected: int, usable_count: int, restricted_count: int, filtered_count: int):
    """Send analysis complete message with results"""
    if restricted_count > 0:
        message = (
            f"<b> GROUP ANALYSIS COMPLETE</b>\n\n"
            f"★ <b>ANALYSIS RESULTS</b>\n"
            f"  → Total Selected: {total_selected}\n"
            f"  → Ready to Send: {usable_count}\n"
            f"  → May Have Issues: {restricted_count}\n"
            f"  → After Mode Filter: {filtered_count}\n\n"
            f"<i>Will attempt to send to {filtered_count} groups matching your broadcast mode. Restricted groups will be marked as failed if sending fails.</i>"
        )
    else:
        message = (
            f"<b> ALL GROUPS READY</b>\n\n"
            f"All {filtered_count} selected groups (after mode filter) are ready for broadcasting!"
        )
    
    await send_dm_log(user_id, message)

async def send_broadcast_started(user_id: int, broadcast_mode: str, use_post_link: bool, delay: int, 
                                  group_msg_delay: int, group_count: int, total_topics: int = 0):
    """Send broadcast started message with settings"""
    mode_display = {
        "groups_only": ("", "Groups Only"),
        "forums_only": ("", "Topics Only"),
        "both": ("", "Both Groups & Topics")
    }
    mode_emoji, mode_name = mode_display.get(broadcast_mode, ("", "Both Groups & Topics"))
    
    message = (
        f" <b>BROADCAST STARTED</b>\n\n"
        f"★ <b>BROADCAST SETTINGS</b>\n"
        f"  → Broadcast Mode: {mode_emoji} {mode_name}\n"
        f"  → Message Mode: {' Saved Messages' if not use_post_link else ' Post Link'}\n"
        f"  → Cycle Interval: {delay}s\n"
        f"  → Message Delay: {group_msg_delay}s\n"
        f"  → Target Groups: {group_count}\n"
    )
    
    if broadcast_mode == 'forums_only' and total_topics > 0:
        message += f"  → Total Topics: {total_topics}\n"
    
    message += f"\n Broadcasting now..."
    
    await send_dm_log(user_id, message)

async def send_setup_complete(user_id: int, account_count: int, usable_groups: int, delay: int, group_msg_delay: int):
    """Send setup complete message"""
    message = (
        f"<b> Setup Complete</b>\n\n"
        f" Accounts: {account_count}\n"
        f" Usable Groups: {usable_groups}\n"
        f" Cycle: {delay}s | Group Delay: {group_msg_delay}s\n\n"
        f" <b>Broadcasting now...</b>"
    )
    
    await send_dm_log(user_id, message)

async def preload_chat_cache(client):
    """Preload chat info to avoid PeerIdInvalid after restart."""
    try:
        await client.get_chat(config.MUST_JOIN_CHANNEL)
        await client.get_chat(config.MUSTJOIN_GROUP)
        logger.info("[CACHE] Chat cache preloaded successfully")
    except Exception as e:
        logger.warning(f" Chat cache preload failed: {e}")

user_tasks = {}

# =======================================================
# 🛠️ HELPER FUNCTIONS (Per-User Logger System)
# =======================================================

async def delete_messages_after_delay(messages, delay_seconds=3):
    """
    Auto-delete messages after a specified delay.
    
    Args:
        messages: List of message objects to delete
        delay_seconds: Number of seconds to wait before deletion
    """
    try:
        await asyncio.sleep(delay_seconds)
        for msg in messages:
            try:
                await msg.delete()
            except Exception as e:
                logger.debug(f"Failed to delete message: {e}")
    except Exception as e:
        logger.error(f"Error in delete_messages_after_delay: {e}")


# =======================================================
#  PROFILE UPDATE FUNCTION (Per-User Logger Integrated)
# =======================================================

JOIN_CACHE = {}

# =======================================================
#  TELETHON-BASED INSTANT JOIN VERIFICATION
# =======================================================

async def instant_join_check(bot_client, user_id, chat_username):
    """
    Instant join verification using bot's Telethon client as admin.
    Bot must be admin in the channel/group to check membership.
    Uses username instead of ID - cleaner and more maintainable.
    FAST and RELIABLE - no temporary client creation needed.
    """
    try:
        try:
            participant = await bot_client(functions.channels.GetParticipantRequest(
                channel=chat_username,
                participant=user_id
            ))
            
            if participant:
                logger.info(f" User {user_id} is a member of @{chat_username}")
                return True
                
        except Exception as e:
            error_str = str(e).lower()
            if 'user not participant' in error_str or 'participant' in error_str or 'user_not_participant' in error_str:
                logger.info(f" User {user_id} is NOT a member of @{chat_username}")
                return False
            else:
                logger.warning(f"Error checking membership for {user_id} in @{chat_username}: {e}")
                return False
                
    except Exception as e:
        logger.error(f"Instant join check failed for user {user_id} in @{chat_username}: {e}")
        return False

async def verify_all_joins(bot_client, user_id, channel_username, group_username):
    """
    Verify user has joined both channel and group using bot's Telethon client.
    Bot must be admin in both channel and group.
    Uses usernames instead of IDs - cleaner and more flexible.
    Returns True only if user is in BOTH.
    """
    try:
        logger.info(f"🔍 Starting instant verification for user {user_id}")
        logger.info(f"Checking @{channel_username} and @{group_username}")
        
        channel_check, group_check = await asyncio.gather(
            instant_join_check(bot_client, user_id, channel_username),
            instant_join_check(bot_client, user_id, group_username),
            return_exceptions=True
        )
        
        channel_joined = channel_check if not isinstance(channel_check, Exception) else False
        group_joined = group_check if not isinstance(group_check, Exception) else False
        
        logger.info(f" Verification result for {user_id}: @{channel_username}={channel_joined}, @{group_username}={group_joined}")
        
        return channel_joined and group_joined
        
    except Exception as e:
        logger.error(f"Error in verify_all_joins for user {user_id}: {e}")
        return False

async def validate_session(session_str, user_id=None):
    """Validate Telegram session string."""
    try:
        tg_client = TelegramClient(StringSession(session_str), config.BOT_API_ID, config.BOT_API_HASH)
        await tg_client.connect()
        is_valid = await tg_client.is_user_authorized()
        await tg_client.disconnect()
        return is_valid
    except Exception as e:
        logger.error(f"Session validation failed: {e}")
        return False

async def stop_broadcast_task(uid):
    """Stop broadcast task for a user and reset cycle counter."""
    state = db.get_broadcast_state(uid)
    running = state.get("running", False)
    if not running:
        logger.info(f"No broadcast running for user {uid}")
        return False

    try:
        db.reset_ad_cycle(uid)
        logger.info(f" Reset cycle counter to 0 for user {uid}")
    except Exception as e:
        logger.error(f"Failed to reset cycle counter: {e}")

    if uid in user_tasks:
        task = user_tasks[uid]
        try:
            task.cancel()
            await task
            logger.info(f"Cancelled broadcast task for {uid}")
        except asyncio.CancelledError:
            logger.info(f"Broadcast task for {uid} was cancelled successfully")
        except Exception as e:
            logger.error(f"Failed to cancel broadcast task for {uid}: {e}")
        finally:
            user_tasks.pop(uid, None)
    
    db.set_broadcast_state(uid, running=False)
    return True

def get_otp_keyboard():
    """Create OTP input keyboard."""
    rows = [
        [InlineKeyboardButton("1", callback_data="otp_1"), InlineKeyboardButton("2", callback_data="otp_2"), InlineKeyboardButton("3", callback_data="otp_3")],
        [InlineKeyboardButton("4", callback_data="otp_4"), InlineKeyboardButton("5", callback_data="otp_5"), InlineKeyboardButton("6", callback_data="otp_6")],
        [InlineKeyboardButton("7", callback_data="otp_7"), InlineKeyboardButton("8", callback_data="otp_8"), InlineKeyboardButton("9", callback_data="otp_9")],
        [InlineKeyboardButton("", callback_data="otp_back"), InlineKeyboardButton("0", callback_data="otp_0"), InlineKeyboardButton("", callback_data="otp_cancel")],
        [InlineKeyboardButton("Show Code", url="tg://openmessage?user_id=777000")]
    ]
    return kb(rows)

# =======================================================
#  GROUP MESSAGE DELAY HANDLERS
# =======================================================

@pyro.on_callback_query(filters.regex("set_group_delay"))
async def set_group_delay_callback(client, callback_query):
    """Handle set group message delay callback"""
    try:
        uid = callback_query.from_user.id
        current_delay = db.get_user_group_msg_delay(uid)
        
        await callback_query.message.edit_media(
            media=InputMediaPhoto(
                media=config.START_IMAGE,
                caption=f"""<b> GROUP MESSAGE DELAY</b>

<b>Current Delay:</b> <code>{current_delay} seconds</code>

Choose your preferred delay between group messages:

• <b>3 seconds</b> - Ultra-fast posting (use with caution)  
• <b>5 seconds</b> - Very fast posting
• <b>10 seconds</b> - Fast posting speed
• <b>15 seconds</b> - Perfect balance  
• <b>30 seconds</b> - Maximum security

<i>Lower delays = faster posting but higher chance of restrictions</i>""",
                parse_mode=ParseMode.HTML
            ),
            reply_markup=kb([
                [InlineKeyboardButton("⏱ 3 Seconds", callback_data="group_delay_3"),
                 InlineKeyboardButton("⏱ 5 Seconds", callback_data="group_delay_5")],
                [InlineKeyboardButton("⏱ 10 Seconds", callback_data="group_delay_10"),
                 InlineKeyboardButton("⏱ 15 Seconds", callback_data="group_delay_15")],
                [InlineKeyboardButton("⏱ 30 Seconds", callback_data="group_delay_30")],
                [InlineKeyboardButton("←", callback_data="menu_main")]
            ])
        )
        logger.info(f"Group delay menu shown for user {uid}")
        
    except Exception as e:
        logger.error(f"Error in set_group_delay callback: {e}")
        await callback_query.answer("Error loading delay setup. Try again.", show_alert=True)

@pyro.on_callback_query(filters.regex(r"group_delay_(\d+)"))
async def group_delay_select_callback(client, callback_query):
    """Handle group delay selection callback"""
    try:
        uid = callback_query.from_user.id
        delay = int(callback_query.matches[0].group(1))
        
        try:
            db.set_user_group_msg_delay(uid, delay)
        except Exception as e:
            logger.error(f"Failed to set group delay for user {uid}: {e}")
            await callback_query.answer("Error setting delay. Try again.", show_alert=True)
            return
        
        await callback_query.message.edit_caption(
            caption=f"""<b> GROUP DELAY UPDATED!</b>

<b>New Delay:</b> <code>{delay} seconds</code>
<i>This will be used for your next broadcast</i>""",
            reply_markup=kb([[InlineKeyboardButton("←", callback_data="menu_main")]]),
            parse_mode=ParseMode.HTML
        )
        await callback_query.answer(f"Group message delay set to {delay}s ", show_alert=True)
        logger.info(f"Group delay set to {delay}s for user {uid}")
        
    except Exception as e:
        logger.error(f"Error in group_delay_select callback: {e}")
        await callback_query.answer("Error setting delay. Try again.", show_alert=True)


# =======================================================
# 📡 ULTRA-FAST GROUP ANALYSIS (2 seconds total)
# =======================================================

def generate_analysis_report(analysis_results, account_phone):
    """Generate a formatted analysis report"""
    report = f"<b> {account_phone}</b>\n"
    report += f"• Total Groups: {analysis_results['total_groups']}\n"
    report += f"• Ready: {analysis_results['total_usable']}\n"
    report += f"• May Fail: {analysis_results['total_restricted']}\n\n"
    
    return report

async def fetch_forum_topics_parallel(client, ent, disp_id, title):
    """Fetch forum topics - optimized for parallel execution"""
    try:
        result = await client(GetForumTopicsRequest(
            channel=ent,
            offset_date=0,
            offset_id=0,
            offset_topic=0,
            limit=500
        ))
        
        topics = []
        for topic in result.topics:
            if isinstance(topic, ForumTopic):
                topic_title = getattr(topic, 'title', f'Topic {topic.id}')
                topics.append({
                    "topic_id": topic.id,
                    "title": topic_title,
                    "display_id": f"{disp_id}:{topic.id}",
                    "parent_title": title,
                    "parent_id": disp_id
                })
        return (disp_id, topics)
    except Exception as e:
        logger.error(f"Error fetching topics for {title}: {e}")
        return (disp_id, [])

def filter_groups_by_keyword(groups_list, keyword):
    """Filter groups by keyword search"""
    if not keyword or keyword.strip() == "":
        return groups_list
    
    keyword_lower = keyword.lower().strip()
    filtered = []
    
    for group in groups_list:
        group_title = group.get('title', '').lower()
        if keyword_lower in group_title:
            filtered.append(group)
    
    return filtered

def bulk_select_all_groups(user_id, groups_list, forum_only_mode=False):
    """Bulk add all groups (excluding topics)"""
    added_count = 0
    
    for group in groups_list:
        group_id = group.get('id')
        group_title = group.get('title', 'Unknown')
        is_forum = group.get('is_forum', False)
        group_type = group.get('group_type', '')
        
        # Skip topics (only add groups and forums)
        if group_type == 'topic':
            continue
        
        try:
            if forum_only_mode:
                if not db.get_forum_group(user_id, group_id):
                    db.add_forum_group(user_id, group_id, group_title)
                    added_count += 1
            else:
                if not db.get_target_group(user_id, group_id):
                    db.add_target_group(user_id, group_id, group_title)
                    added_count += 1
        except Exception as e:
            logger.error(f"Error adding group {group_id}: {e}")
    
    return added_count

def bulk_select_forums_only(user_id, groups_list, forum_only_mode=False):
    """Bulk add only forum groups"""
    added_count = 0
    
    for group in groups_list:
        group_id = group.get('id')
        group_title = group.get('title', 'Unknown')
        is_forum = group.get('is_forum', False)
        
        # Only add if it's a forum
        if not is_forum:
            continue
        
        try:
            if forum_only_mode:
                if not db.get_forum_group(user_id, group_id):
                    db.add_forum_group(user_id, group_id, group_title)
                    added_count += 1
            else:
                if not db.get_target_group(user_id, group_id):
                    db.add_target_group(user_id, group_id, group_title)
                    added_count += 1
        except Exception as e:
            logger.error(f"Error adding forum {group_id}: {e}")
    
    return added_count

def bulk_unselect_all(user_id, groups_list, forum_only_mode=False):
    """Bulk remove all filtered groups"""
    removed_count = 0
    
    for group in groups_list:
        group_id = group.get('id')
        
        try:
            if forum_only_mode:
                if db.get_forum_group(user_id, group_id):
                    db.remove_forum_group(user_id, group_id)
                    removed_count += 1
            else:
                if db.get_target_group(user_id, group_id):
                    db.remove_target_group(user_id, group_id)
                    removed_count += 1
        except Exception as e:
            logger.error(f"Error removing group {group_id}: {e}")
    
    return removed_count

async def analyze_account_groups_fast(tg_client, account_phone, target_group_ids=None, skip_group_ids=None):
    """PRO MAX LEVEL group analysis - skips slow mode and high spam groups for maximum efficiency"""
    try:
        if skip_group_ids is None:
            skip_group_ids = []
            
        all_groups = []
        skipped_groups = []
        usable_groups = []
        
        dialogs = await tg_client.get_dialogs(limit=500)
        
        for dialog in dialogs:
            if not dialog.is_group:
                continue
                
            if dialog.id in skip_group_ids:
                continue
                
            if target_group_ids and dialog.id not in target_group_ids:
                continue
            
            group_data = {
                'id': dialog.id,
                'title': dialog.title,
                'can_send': True,
                'permission_info': "OK",
                'entity': None
            }
            
            try:
                chat = dialog.entity
                
                if hasattr(chat, 'slowmode_seconds') and chat.slowmode_seconds > 0:
                    logger.debug(f"Skipping slow mode group: {dialog.title} ({chat.slowmode_seconds}s)")
                    skipped_groups.append({'id': dialog.id, 'title': dialog.title, 'reason': 'SLOW_MODE'})
                    continue
                
                if hasattr(chat, 'participants_count') and chat.participants_count > 200000:
                    logger.debug(f"Skipping high spam risk group: {dialog.title} ({chat.participants_count} members)")
                    skipped_groups.append({'id': dialog.id, 'title': dialog.title, 'reason': 'HIGH_SPAM_RISK'})
                    continue
                
                try:
                    group_data['entity'] = await tg_client.get_entity(dialog.id)
                except Exception as entity_err:
                    logger.debug(f"Entity cache for {dialog.title}: {entity_err}")
                    group_data['entity'] = chat
                
                if hasattr(chat, 'forum') and chat.forum:
                    group_data['is_forum'] = True
                else:
                    group_data['is_forum'] = False
                
                if hasattr(chat, 'megagroup'):
                    group_data['is_megagroup'] = chat.megagroup
                
                usable_groups.append(group_data)
                all_groups.append(group_data)
                
            except Exception as e:
                logger.debug(f"Detailed analysis skipped for {dialog.title}: {e}")
                group_data['entity'] = dialog.entity
                usable_groups.append(group_data)
                all_groups.append(group_data)
        
        logger.info(f"PRO Analysis for {account_phone}: {len(usable_groups)} usable groups, {len(skipped_groups)} skipped (slow mode/spam)")
        
        return {
            'all_groups': all_groups,
            'restricted_groups': [],
            'slow_mode_groups': [],
            'usable_groups': usable_groups,
            'skipped_groups': skipped_groups,
            'total_groups': len(all_groups),
            'total_restricted': 0,
            'total_slow_mode': 0,
            'total_usable': len(usable_groups),
            'total_skipped': len(skipped_groups)
        }
        
    except Exception as e:
        logger.error(f"Error in PRO group analysis for {account_phone}: {e}")
        return {
            'all_groups': [],
            'restricted_groups': [],
            'slow_mode_groups': [],
            'usable_groups': [],
            'skipped_groups': [],
            'total_groups': 0,
            'total_restricted': 0,
            'total_slow_mode': 0,
            'total_usable': 0,
            'total_skipped': 0
        }

# =======================================================
#  RUN BROADCAST (Clean Logs + FloodWait Skip + Summary)
# =======================================================

async def run_broadcast(client, uid):
    """Run broadcast with clean logs, cycle-wise profile updates, FloodWait handling, and summary reports."""
    try:
        global db
        db = EnhancedDatabaseManager()

        sent_count = 0
        failed_count = 0
        cycle_count = 0

        delay = db.get_user_ad_delay(uid)
        group_msg_delay = db.get_user_group_msg_delay(uid)
        
        current_cycle = db.get_current_ad_cycle(uid) if hasattr(db, 'get_current_ad_cycle') else db.get_ad_cycle(uid)
        
        broadcast_start_time = datetime.utcnow()
        cycle_timeout = db.get_user_cycle_timeout(uid) if hasattr(db, "get_user_cycle_timeout") else 900

        accounts = db.get_user_accounts(uid) or []
        
        # Get broadcast mode from database (NEW SYSTEM)
        broadcast_mode = db.get_broadcast_mode(uid) if hasattr(db, 'get_broadcast_mode') else 'both'
        
        # Check post link mode first (before broadcast info message)
        post_link_data = db.get_user_post_link(uid)
        use_post_link = post_link_data and post_link_data.get("message_source") == "post_link"
        
        # Load groups based on broadcast mode
        if broadcast_mode == 'forums_only':
            target_groups = db.get_forum_groups(uid) or []
        elif broadcast_mode == 'groups_only':
            # Load only non-forum groups
            all_groups = db.get_target_groups(uid) or []
            target_groups = [g for g in all_groups if not g.get('is_forum', False)]
        else:  # both
            target_groups = db.get_target_groups(uid) or []
        
        target_group_ids = [g["group_id"] for g in target_groups] if target_groups else []
        skip_group_ids = []

        if not target_groups:
            mode_display = {"groups_only": "groups", "forums_only": "forum groups", "both": "target groups"}
            mode_text = mode_display.get(broadcast_mode, "target groups")
            await client.send_message(uid,
                                     f"<b> No {mode_text} selected!</b>\n\nPlease select {mode_text} first from the Groups Menu.",
                                     parse_mode=ParseMode.HTML)
            return

        # ============================================================
        # 🔍 PRE-BROADCAST GROUP ANALYSIS & AUTO-FILTERING
        # ============================================================
        analysis_log_msg = await send_analysis_start(uid, broadcast_mode, len(target_groups))
        
        logger.info(f"🔍 Starting INSTANT pre-broadcast group analysis for user {uid}")
        
        if not accounts:
            logger.error("No accounts available for analysis")
            return
            
        first_account = accounts[0]
        session_encrypted = first_account.get("session_string")
        credentials = db.get_user_api_credentials(uid)
        
        from cryptography.fernet import Fernet
        cipher_suite = Fernet(config.ENCRYPTION_KEY.encode())
        session_str = cipher_suite.decrypt(session_encrypted.encode()).decode()
        
        tg_client = TelegramClient(
            StringSession(session_str),
            credentials['api_id'],
            credentials['api_hash']
        )
        await tg_client.connect()
        
        async def check_group(group):
            group_id = group.get("group_id")
            group_title = group.get("title") or group.get("group_name") or group.get("name") or "Unknown Group"
            
            try:
                group_entity = await tg_client.get_entity(group_id)
                
                if hasattr(group_entity, 'title'):
                    group_title = group_entity.title
                elif hasattr(group_entity, 'name'):
                    group_title = group_entity.name
                
                is_forum = getattr(group_entity, 'forum', False)
                topics = []
                
                if broadcast_mode == 'forums_only' and not is_forum:
                    logger.info(f"⏭️ Skipping non-forum group '{group_title}' (Forum Only Mode)")
                    return None, {
                        "id": group_id,
                        "title": group_title,
                        "reason": "Not a forum (Forum Only Mode)"
                    }
                
                if is_forum:
                    try:
                        from telethon.tl.functions.channels import GetForumTopicsRequest
                        result = await tg_client(GetForumTopicsRequest(
                            channel=group_entity,
                            offset_date=None,
                            offset_id=0,
                            offset_topic=0,
                            limit=100
                        ))
                        
                        open_topics_count = 0
                        for topic in result.topics:
                            if hasattr(topic, 'id') and hasattr(topic, 'title'):
                                is_closed = getattr(topic, 'closed', False)
                                if not is_closed:
                                    topics.append({
                                        'id': topic.id,
                                        'title': topic.title,
                                        'closed': False
                                    })
                                    open_topics_count += 1
                        
                        logger.info(f" Forum '{group_title}' - Found {open_topics_count} open topics (out of {len(result.topics)} total)")
                    except Exception as e:
                        logger.warning(f"Could not fetch topics for forum {group_title}: {e}")
                
                if hasattr(group_entity, 'default_banned_rights'):
                    rights = group_entity.default_banned_rights
                    if rights and rights.send_messages:
                        logger.warning(f" Group '{group_title}' - No send permission")
                        return None, {
                            "id": group_id,
                            "title": group_title,
                            "reason": "No send permission"
                        }
                
                if hasattr(group_entity, 'admin_rights'):
                    logger.info(f" Group '{group_title}' - Usable (admin) | is_forum={is_forum} | topics={len(topics)}")
                    return {'id': group_id, 'title': group_title, 'is_forum': is_forum, 'topics': topics}, None
                else:
                    logger.info(f" Group '{group_title}' - Usable | is_forum={is_forum} | topics={len(topics)}")
                    return {'id': group_id, 'title': group_title, 'is_forum': is_forum, 'topics': topics}, None
                    
            except ChatWriteForbiddenError:
                logger.warning(f" Group '{group_title}' - Write forbidden")
                return None, {
                    "id": group_id,
                    "title": group_title,
                    "reason": "Write forbidden"
                }
                
            except ValueError:
                logger.warning(f" Group '{group_title}' - Not a member")
                return None, {
                    "id": group_id,
                    "title": group_title,
                    "reason": "Not a member"
                }
                
            except Exception as e:
                logger.error(f"Error checking group '{group_title}': {e}")
                return group, None
        
        total_selected = len(target_groups)
        results = await asyncio.gather(*[check_group(group) for group in target_groups], return_exceptions=True)
        
        await tg_client.disconnect()
        
        usable_groups = []
        restricted_groups = []
        
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Exception during group check: {result}")
                continue
                
            usable, restricted = result
            if usable:
                usable_groups.append(usable)
            if restricted:
                restricted_groups.append(restricted)
        
        # Include ALL groups (usable + restricted) - don't filter out restricted groups
        all_groups_combined = usable_groups + [{"id": r["id"], "title": r["title"], "is_forum": False, "topics": []} for r in restricted_groups]
        
        # IMPORTANT: Apply broadcast mode filter AGAIN to all_groups_combined
        # because it may include forum groups when in groups_only mode
        if broadcast_mode == 'groups_only':
            # Filter out forum groups
            all_groups_combined = [g for g in all_groups_combined if not g.get('is_forum', False)]
        elif broadcast_mode == 'forums_only':
            # Filter to only forum groups
            all_groups_combined = [g for g in all_groups_combined if g.get('is_forum', False)]
        # If 'both', keep all groups
        
        await send_analysis_complete(uid, total_selected, len(usable_groups), len(restricted_groups), len(all_groups_combined))
        logger.info(f" Analysis complete: {len(usable_groups)} usable, {len(restricted_groups)} may fail, {len(all_groups_combined)} after mode filter")
        
        # Use filtered groups
        target_groups = all_groups_combined
        working_groups = all_groups_combined
        
        total_topics = sum(len(g.get('topics', [])) for g in working_groups if g.get('is_forum', False))
        
        # Send broadcast started message
        await send_broadcast_started(uid, broadcast_mode, use_post_link, delay, group_msg_delay, len(working_groups), total_topics)

        analysis_results = {}
        clients = {}
        usable_groups_map = {}

        analysis_tasks = []
        for acc in accounts:
            try:
                session_encrypted = acc.get("session_string") or ""
                session_str = cipher_suite.decrypt(session_encrypted.encode()).decode()
                if not await validate_session(session_str):
                    db.deactivate_account(acc["_id"])
                    continue

                credentials = db.get_user_api_credentials(acc['user_id'])
                if not credentials:
                    logger.error(f"No API credentials found for user {acc['user_id']}")
                    continue
                
                tg_client = TelegramClient(StringSession(session_str), credentials['api_id'], credentials['api_hash'])
                await tg_client.start()
                clients[acc["_id"]] = tg_client

                task = analyze_account_groups_fast(tg_client, acc["phone_number"], target_group_ids, skip_group_ids)
                analysis_tasks.append((acc["_id"], task))

            except Exception as e:
                await send_dm_log(uid, f"<b> Failed to start account {acc.get('phone_number','unknown')}:</b> {str(e)}")

        if analysis_tasks:
            completed = await asyncio.gather(*[t for _, t in analysis_tasks], return_exceptions=True)
            for (acc_id, _), res in zip(analysis_tasks, completed):
                if isinstance(res, Exception):
                    continue
                analysis_results[acc_id] = res
                
                account_usable_groups = res.get("usable_groups", [])
                accessible_group_ids = [g['id'] for g in account_usable_groups]
                
                account_groups = []
                for group in working_groups:
                    if group["id"] in accessible_group_ids:
                        account_groups.append(group)
                
                usable_groups_map[acc_id] = account_groups

        total_usable = sum(len(v) for v in usable_groups_map.values())
        
        await send_setup_complete(uid, len(clients), total_usable, delay, group_msg_delay)

        if total_usable == 0:
            await send_dm_log(uid, "<b> No usable target groups found!</b>")
            for cl in clients.values():
                try:
                    await cl.disconnect()
                except:
                    pass
            return

        user_data = db.get_user(uid)
        should_continue, remaining_seconds, time_message = calculate_remaining_time_today(user_data)
        
        if not should_continue:
            await send_dm_log(uid, time_message)
            logger.info(f"User {uid} started ads outside schedule time")
            
            for cl in clients.values():
                try:
                    await cl.disconnect()
                except:
                    pass
            return
        
        await send_dm_log(uid, time_message)
        
        if remaining_seconds != float('inf'):
            schedule_end_time = get_ist_now() + timedelta(seconds=remaining_seconds)
            logger.info(f"User {uid} - Broadcast will run until {schedule_end_time.strftime('%I:%M %p')} IST")
        else:
            schedule_end_time = None
            logger.info(f"User {uid} - No schedule limit (schedule disabled)")
        
        db.set_broadcast_state(uid, running=True)

        working_groups_map = {acc_id: groups.copy() for acc_id, groups in usable_groups_map.items()}
        last_message_time = {}

        try:
            while db.get_broadcast_state(uid).get("running", False):
                
                if schedule_end_time is not None:
                    current_time = get_ist_now()
                    if current_time >= schedule_end_time:
                        user_data = db.get_user(uid)
                        start_time_str = user_data.get("schedule_start_time", "12:00 AM")
                        
                        await send_dm_log(uid,
                            f" <b>Schedule Time Ended</b>\n\n"
                            f"Ads have been stopped as scheduled end time has been reached.\n\n"
                            f"<i> Ads will automatically resume tomorrow at {start_time_str}</i>"
                        )
                        
                        logger.info(f"User {uid} - Schedule end time reached, stopping broadcast")
                        db.set_broadcast_state(uid, running=False, paused=False)
                        break
                
                # ============================================================
                #  CHECK SCHEDULED ADS - PAUSE IF OUTSIDE SCHEDULE
                # ============================================================
                user_data = db.get_user(uid)
                within_schedule, schedule_msg = is_within_schedule(user_data)
                
                if not within_schedule:
                    logger.info(f"User {uid} - {schedule_msg}")
                    
                    await send_dm_log(uid,
                        f"<b> SCHEDULED ADS - PAUSED</b>\n\n"
                        f"<b>Status:</b> Outside scheduled hours\n"
                        f"<b>{schedule_msg}</b>\n\n"
                        f"<i>Broadcasting will auto-resume when schedule starts.</i>"
                    )
                    
                    await asyncio.sleep(300)
                    continue
                
                cycle_count += 1

                for acc in accounts:
                    acc_id = acc["_id"]
                    
                    tg_client = clients.get(acc_id)
                    if not tg_client:
                        continue
                    working_groups = working_groups_map.get(acc_id, [])
                    for group in working_groups[:]:
                        if not db.get_broadcast_state(uid).get("running", False):
                            raise asyncio.CancelledError("Stopped by user")

                        # Check if user is using post link or saved messages
                        post_link_data = db.get_user_post_link(uid)
                        use_post_link = post_link_data and post_link_data.get("message_source") == "post_link"
                        
                        try:
                            if use_post_link:
                                # Using post link forwarding
                                saved_from_peer = post_link_data.get("saved_from_peer")
                                saved_msg_id = post_link_data.get("saved_msg_id")
                                post_link = post_link_data.get("post_link")
                                
                                if not saved_from_peer or not saved_msg_id:
                                    logger.error(f"Invalid post link data for user {uid}")
                                    continue
                                
                                logger.info(f"Using post link: {post_link} (from_peer={saved_from_peer}, msg_id={saved_msg_id})")
                            else:
                                # Using saved messages (original logic)
                                user_msg_count = db.get_user_saved_messages_count(uid)
                                
                                saved_msgs_list = []
                                messages = await tg_client.get_messages("me", limit=20)
                                
                                for msg in messages:
                                    if msg.text or msg.media:
                                        saved_msgs_list.append(msg)
                                
                                if not saved_msgs_list:
                                    logger.warning(f"No messages found in Saved Messages for user {uid}")
                                    continue
                                
                                saved_msgs_list.reverse()
                                
                                saved_msgs_list = saved_msgs_list[:user_msg_count]
                                
                                msg_index = current_cycle % len(saved_msgs_list)
                                current_saved_msg = saved_msgs_list[msg_index]
                                
                                logger.debug(f"Cycle {current_cycle + 1}: Using message {msg_index + 1} of {len(saved_msgs_list)} from Saved Messages")
                            
                        except Exception as e:
                            logger.error(f"Error preparing message for user {uid}: {e}")
                            continue

                        current_delay = group_msg_delay

                        try:
                            try:
                                group_entity = await tg_client.get_entity(group["id"])
                            except Exception as peer_err:
                                logger.warning(f"Failed to get entity for group {group['id']}: {peer_err}")
                                failed_count += 1
                                continue
                            
                            is_forum = group.get('is_forum', False)
                            topics = group.get('topics', [])
                            
                            logger.info(f"🔍 Group: {group.get('title', 'Unknown')} | is_forum={is_forum} | topics_count={len(topics)} | group_keys={list(group.keys())}")
                            
                            if is_forum and topics:
                                topics_sent = 0
                                topics_failed = 0
                                topics_skipped = 0
                                
                                await send_dm_log(uid,
                                    f" <b>Broadcasting to Forum</b>\n"
                                    f"<b>Forum:</b> {group.get('title','Unknown')}\n"
                                    f"<b>Topics:</b> {len(topics)}\n"
                                    f"⏳ <i>Sending to each topic with {group_msg_delay}s delay...</i>"
                                )
                                
                                for idx, topic in enumerate(topics, 1):
                                    if topic.get('closed', False):
                                        topics_skipped += 1
                                        logger.info(f"⏭️ Skipping closed topic {idx}/{len(topics)}: {topic['title']}")
                                        continue
                                    
                                    try:
                                        if use_post_link:
                                            # Forward from post link with forward tag
                                            await tg_client.forward_messages(
                                                entity=group_entity,
                                                messages=saved_msg_id,
                                                from_peer=saved_from_peer,
                                                reply_to=topic['id']
                                            )
                                        else:
                                            # Forward from saved messages
                                            await tg_client.forward_messages(
                                                entity=group_entity,
                                                messages=current_saved_msg,
                                                from_peer="me",
                                                reply_to=topic['id']
                                            )
                                        topics_sent += 1
                                        sent_count += 1
                                        db.increment_broadcast_stats(uid, True)
                                        
                                        logger.info(f" Sent to topic {idx}/{len(topics)}: {topic['title']}")
                                        
                                        await asyncio.sleep(group_msg_delay)
                                        
                                    except Exception as topic_err:
                                        topics_failed += 1
                                        error_msg = str(topic_err)
                                        if "TOPIC_CLOSED" in error_msg.upper():
                                            logger.info(f" Topic closed (detected during send) {idx}/{len(topics)}: {topic['title']}")
                                            topics_skipped += 1
                                        else:
                                            logger.warning(f" Failed to send to topic {idx}/{len(topics)}: {topic['title']} - {topic_err}")
                                        
                                        await asyncio.sleep(2)
                                
                                last_message_time[f"{acc['_id']}_{group['id']}"] = time.time()
                                
                                msg_source = f"Post Link: {post_link}" if use_post_link else f"Saved Message #{(msg_index + 1)}"
                                summary = f" <b>FORUM BROADCAST COMPLETE</b>\n\n  → Forum: <b>{group.get('title','Unknown')}</b>\n"
                                summary += f"  → Topics Sent: {topics_sent}/{len(topics)} \n"
                                if topics_failed > 0:
                                    summary += f"<b>Topics Failed:</b> {topics_failed} \n"
                                if topics_skipped > 0:
                                    summary += f"<b>Topics Skipped:</b> {topics_skipped} ⏭️ (Closed)\n"
                                summary += f" Account: <code>{acc.get('phone_number')}</code>\n"
                                summary += f" Message: {msg_source} (Cycle {current_cycle + 1})"
                                
                                await send_dm_log(uid, summary)
                            else:
                                if use_post_link:
                                    # Forward from post link with forward tag
                                    await tg_client.forward_messages(
                                        entity=group_entity,
                                        messages=saved_msg_id,
                                        from_peer=saved_from_peer
                                    )
                                    msg_source = f"Post Link: {post_link}"
                                else:
                                    # Forward from saved messages
                                    await tg_client.forward_messages(
                                        entity=group_entity,
                                        messages=current_saved_msg,
                                        from_peer="me"
                                    )
                                    msg_source = f"Saved Message #{(msg_index + 1)}"
                                    
                                sent_count += 1
                                db.increment_broadcast_stats(uid, True)
                                last_message_time[f"{acc['_id']}_{group['id']}"] = time.time()

                                await send_dm_log(uid,
                                    f" <b>SENT TO GROUP</b>\n\n"
                                    f"  → Group: <b>{group.get('title','Unknown')}</b>\n"
                                    f"  → Type: Regular Group\n"
                                    f"  → Account: <code>{mask_phone_number(acc.get('phone_number', ''))}</code>\n"
                                    f"  → Message: {msg_source}\n"
                                    f"  → Cycle: {current_cycle + 1}\n"
                                    f"  → Time: {datetime.now(IST).strftime('%I:%M %p')}"
                                )

                            await asyncio.sleep(current_delay)

                        except FloodWait as e:
                            wait_time = int(getattr(e, "value", 0) or getattr(e, "x", 0) or 1)
                            failed_count += 1
                            
                            await send_dm_log(uid,
                                f"⏳ <b>Rate Limited</b>\n\n"
                                f"<b>Group:</b> {group.get('title', 'Unknown')}\n"
                                f"<b>Reason:</b> FloodWait ({wait_time}s)\n"
                                f"<b>Action:</b> Will retry in next cycle\n\n"
                                f"<i>Telegram is asking us to slow down. Normal behavior.</i>"
                            )
                            
                            logger.warning(f"FloodWait {wait_time}s for group {group['id']}, will retry next cycle")
                            await asyncio.sleep(wait_time + 2)
                            continue

                        except RPCError as e:
                            error_msg = str(e)
                            err_lower = error_msg.lower()
                            
                            # Count all errors as failed, don't permanently remove groups
                            failed_count += 1
                            
                            if "banned" in err_lower:
                                reason = "Account Banned"
                            elif "forbidden" in err_lower or "chat_write_forbidden" in err_lower:
                                reason = "No Send Permission"
                            elif "kicked" in err_lower:
                                reason = "Bot Removed"
                            elif "rights" in err_lower or "not enough" in err_lower:
                                reason = "Insufficient Rights"
                            elif "restricted" in err_lower:
                                reason = "Group Restricted"
                            else:
                                reason = error_msg[:50]
                            
                            # Don't remove group from working_groups - just mark as failed
                            await send_dm_log(uid,
                                f"<b> Failed to Send</b>\n"
                                f"<b>Group:</b> {group.get('title','Unknown')}\n"
                                f"<b>Reason:</b> {reason}\n"
                                f"<b>Action:</b> Will retry in next cycle"
                            )
                            logger.warning(f"Failed to send to group {group['id']}: {reason}")
                            
                            continue

                        except Exception as e:
                            error_msg = str(e)
                            err = error_msg.lower()
                            
                            failed_count += 1
                            
                            if "banned" in err:
                                reason = "Account Banned"
                            elif "forbidden" in err:
                                reason = "No Permission"
                            elif "kicked" in err:
                                reason = "Bot Removed"
                            elif "rights" in err or "not enough" in err:
                                reason = "Insufficient Rights"
                            elif "peer_id_invalid" in err:
                                reason = "Invalid Group ID"
                            elif "topic_closed" in err:
                                reason = "Forum Topic Closed"
                            else:
                                reason = error_msg[:50]
                            
                            if "topic_closed" in err:
                                await send_dm_log(uid,
                                    f"<b> Forum Topic Closed</b>\n"
                                    f"<b>Group:</b> {group.get('title','Unknown')}\n"
                                    f"<b>Reason:</b> Forum topic is closed\n"
                                    f"<b>Action:</b> Skipped this group"
                                )
                                logger.info(f"Forum topic closed for group {group['id']}")
                            else:
                                
                                await send_dm_log(uid,
                                    f"<b> Send Failed - Skipping Group</b>\n"
                                    f"<b>Group:</b> {group.get('title','Unknown')}\n"
                                    f"<b>Reason:</b> {reason}"
                                )
                            
                            is_permanent = any(k in err for k in ["banned", "forbidden", "kicked", "rights", "not enough"])
                            
                            if is_permanent:
                                try:
                                    working_groups.remove(group)
                                except ValueError:
                                    pass
                            else:
                                
                                if "peer" in err:
                                    reason = "Invalid Peer"
                                elif "timeout" in err or "network" in err:
                                    reason = "Network Timeout"
                                elif "monoforum" in err or "reply" in err:
                                    reason = "Forum Error"
                                else:
                                    reason = str(e)[:40] + "..." if len(str(e)) > 40 else str(e)
                                
                                await send_dm_log(uid,
                                    f" <b>Temporary Error</b>\n\n"
                                    f"<b>Group:</b> {group.get('title', 'Unknown')}\n"
                                    f"<b>Reason:</b> {reason}\n"
                                    f"<b>Action:</b> Will retry in next cycle\n\n"
                                    f"<i>Temporary issue. Retrying next cycle.</i>"
                                )
                                
                                logger.warning(f"Temporary error for group {group['id']}: {err[:80]}, will retry next cycle")
                            continue

                if hasattr(db, 'increment_broadcast_cycle'):
                    db.increment_broadcast_cycle(uid)
                else:
                    db.update_ad_cycle(uid)
                
                current_cycle = db.get_current_ad_cycle(uid) if hasattr(db, 'get_current_ad_cycle') else db.get_ad_cycle(uid)
                logger.debug(f"Updated current_cycle to {current_cycle} for next iteration")
                
                user_msg_count = db.get_user_saved_messages_count(uid)
                next_msg_num = (current_cycle % user_msg_count) + 1
                
                await send_dm_log(uid,
                    f" <b>CYCLE {cycle_count} COMPLETED</b>\n\n★ <b>RESULTS</b>\n"
                    f" Sent: {sent_count}\n"
                    f" Failed: {failed_count}\n"
                    f"🕒 Next cycle in: {delay}s\n"
                    f" Next message: #{next_msg_num} from Saved Messages"
                )

                if cycle_count % 5 == 0:
                    logger.info(f"Cycle {cycle_count}: Adding safety cooldown of {cycle_timeout}s + regular delay of {delay}s")
                    await asyncio.sleep(cycle_timeout)

                logger.info(f"Waiting {delay} seconds before next cycle for user {uid}")
                await asyncio.sleep(delay)

        except asyncio.CancelledError:
            raise

        finally:
            for cl in clients.values():
                try:
                    await cl.disconnect()
                except:
                    pass
            db.set_broadcast_state(uid, running=False)
            if uid in user_tasks:
                del user_tasks[uid]

    except asyncio.CancelledError:
        return

    except Exception as e:
        db.increment_broadcast_stats(uid, False)
        db.set_broadcast_state(uid, running=False)
        if uid in user_tasks:
            del user_tasks[uid]
        
        await send_dm_log(uid, f"<b> Broadcast task failed:</b> {str(e)}")
        for admin_id in ALLOWED_BD_IDS:
            try:
                await client.send_message(admin_id, f"Broadcast task failed for user {uid}: {e}")
                break
            except:
                continue

# =======================================================
# ⌨️ COMMAND HANDLERS
# =======================================================

@pyro.on_message(filters.command("start"))
async def start_command(client, message):
    """Handle /start command"""
    try:
        uid = message.from_user.id
        username = message.from_user.username or "Unknown"
        first_name = message.from_user.first_name or "User"

        db.create_user(uid, username, first_name)

        if is_owner(uid):
            logger.info(f"Admin user {uid} accessing bot - same 1 account limit as regular users")
        else:
            existing_status = db.get_user_status(uid)
            if not existing_status or existing_status.get("user_type") is None:
                db.set_user_status(uid, "free", 1, None)
                logger.info(f"New user {uid} set to default free status")
            else:
                current_type = existing_status.get("user_type", "free")
                current_limit = existing_status.get("accounts_limit", 1)
                logger.info(f"Preserving existing user_type '{current_type}' with {current_limit} accounts limit for user {uid}")

        db.update_user_last_interaction(uid)

        if config.ENABLE_FORCE_JOIN:
            if not telethon_bot.is_connected():
                await telethon_bot.connect()
                await telethon_bot.start(bot_token=config.BOT_TOKEN)
            
            if not await verify_all_joins(telethon_bot, uid, config.MUST_JOIN_CHANNEL, config.MUSTJOIN_GROUP):
                try:
                    await message.reply_photo(
                        photo=config.FORCE_JOIN_IMAGE,
                        caption=(
                            "<b>🤖 WELCOME TO AZTECH ADS BOT</b>\n\n"
                            "To unlock the full <b>AzTech Ads Bot</b> experience, please join our "
                            "official <b>channel</b> and <b>group</b> first!\n\n"
                            "<i>Tip:</i> Click the buttons below to join both. After joining, click "
                            "<b>‘Verify ’</b> to proceed.\n\n"
                            "Your <i>free automation journey</i> starts here "
                        ),
                        reply_markup=kb([
                            [InlineKeyboardButton("+ Join Channel", url=config.MUST_JOIN_CHANNEL_URL)],
                            [InlineKeyboardButton("+ Join Group", url=config.MUSTJOIN_GROUP_URL)],
                            [InlineKeyboardButton("✓ Verify", callback_data="joined_check")]
                        ]),
                        parse_mode=ParseMode.HTML
                    )
                    logger.info(f"Sent force join prompt to user {uid}")
                    return
                except Exception as e:
                    logger.error(f"Failed to send force join message to {uid}: {e}")
                    await message.reply(
                        " Please join our official channel and group to continue.\n"
                        "If the buttons don’t work, contact support.",
                        parse_mode=ParseMode.HTML
                    )
                    return

        await message.reply_photo(
            photo=config.START_IMAGE,
            caption=(
                "<b>🤖 Welcome to AzTech Ads Bot [FREE]</b>\n\n"
                "<b>The Future of Telegram Automation </b>\n\n"
                " <b>Powerful Features:</b>\n"
                "•  <b>Auto Ad Broadcasting</b> — Instantly promote your ads across multiple groups.\n"
                "•  <b>Smart Time Intervals</b> — Schedule ads every 5m, 10m, or 20m.\n"
                "•  <b>Target Group Selection</b> — Choose exactly where your ads go.\n"
                "•  <b>Ad Analytics</b> — Track your ad performance in real time.\n\n"
                "<i>Start your first broadcast and let AzTech Ads Bot handle the rest </i>"
            ),
            reply_markup=kb([
                [InlineKeyboardButton("▸ Start Advertising", callback_data="menu_main")],
                [
                    InlineKeyboardButton("◆ Updates", url=config.UPDATES_CHANNEL_URL),
                    InlineKeyboardButton("◉ Support", url=config.SUPPORT_GROUP_URL)
                ],
                [InlineKeyboardButton("? How To Use", url=config.GUIDE_URL)]
            ]),
            parse_mode=ParseMode.HTML
        )

        logger.info(f"Start command handled successfully for user {uid}")

    except Exception as e:
        logger.error(f"Error in /start command for {uid}: {e}")
        await message.reply(
            " An unexpected error occurred while starting the bot.\n"
            "Please try again later or contact support.",
            parse_mode=ParseMode.HTML
        )

@pyro.on_message(filters.command("go"))
async def go_command(client, message):
    """Handle /go command - instantly start broadcast"""
    try:
        uid = message.from_user.id
        
        # Check if user has accounts
        accounts = db.get_user_accounts(uid)
        if not accounts:
            await message.reply(
                "<b>⚠️ No accounts found!</b>\n\n"
                "<i>Please add an account first before starting broadcast.</i>\n\n"
                "Use the main bot menu to add accounts.",
                parse_mode=ParseMode.HTML
            )
            return
        
        # Check if broadcast is already running
        state = db.get_broadcast_state(uid)
        if state.get("running"):
            await message.reply("<b>⚠️ Broadcast already running!</b>", parse_mode=ParseMode.HTML)
            return
        
        # Start broadcast
        await message.reply("<b>🚀 Starting broadcast...</b>", parse_mode=ParseMode.HTML)
        
        # Create a fake callback query to trigger broadcast
        from pyrogram.types import CallbackQuery
        
        # Set broadcast state to running BEFORE starting task
        db.set_broadcast_state(uid, running=True, paused=False)
        
        # Start broadcast task directly
        if uid in user_tasks and not user_tasks[uid].done():
            await message.reply("<b>⚠️ Broadcast task already exists!</b>", parse_mode=ParseMode.HTML)
            return
            
        task = asyncio.create_task(run_broadcast(client, uid))
        user_tasks[uid] = task
        
        await message.reply("<b>✅ Broadcast started!</b>\n\n<i>check the logger bot @aztechloggersbot</i>", parse_mode=ParseMode.HTML)
        await send_dm_log(uid, "<b>🚀 Broadcast started via /go command!</b>")
        logger.info(f"Broadcast started via /go command for user {uid} - State updated")
        
    except Exception as e:
        logger.error(f"Error in go command: {e}")
        await message.reply(f"<b>❌ Error starting broadcast:</b> {str(e)}", parse_mode=ParseMode.HTML)

@pyro.on_message(filters.command("stop"))
async def stop_command(client, message):
    """Handle /stop command - instantly stop broadcast"""
    try:
        uid = message.from_user.id
        
        # Set broadcast state to stopped BEFORE stopping task
        db.set_broadcast_state(uid, running=False, paused=False)
        
        stopped = await stop_broadcast_task(uid)
        if stopped:
            await message.reply("<b>🛑 Broadcast stopped!</b>\n\n<i>UI will now show broadcast as stopped.</i>", parse_mode=ParseMode.HTML)
            await send_dm_log(uid, "<b>🛑 Broadcast stopped via /stop command!</b>")
            logger.info(f"Broadcast stopped via /stop command for user {uid} - State updated")
        else:
            await message.reply("<b>⚠️ No broadcast running!</b>", parse_mode=ParseMode.HTML)
            
    except Exception as e:
        logger.error(f"Error in stop command: {e}")
        await message.reply("Error stopping broadcast. Please try again.")

@pyro.on_message(filters.command("stats") & filters.user(ALLOWED_BD_IDS))
async def admin_stats_command(client, message):
    """Handle /stats command for admins"""
    try:
        stats = db.get_admin_stats()
        
        stats_text = (
            f"<b>AzTech Ads Bot - ADMIN DASHBOARD</b>\n\n"
            f"<u>Report Date:</u> <i>{datetime.now().strftime('%d/%m/%y • %I:%M %p')}</i>\n\n"
            "<b>USER STATISTICS</b>\n"
            f"• <u>Total Users:</u> <code>{stats.get('total_users', 0)}</code>\n"
            f"• <b>Hosted Accounts:</b> <code>{stats.get('total_accounts', 0)}</code>\n"
            f"• <u>Total Forwards:</u> <i>{stats.get('total_forwards', 0)}</i>\n"
            f"• <b>Active Logger Users:</b> <code>{stats.get('active_logger_users', 0)}</code>\n"
            f"• <u>Total Broadcasts:</u> <code>{stats.get('total_broadcasts', 0)}</code>\n"
            f"• <b>Failed Sends:</b> <code>{stats.get('total_failed', 0)}</code>\n"
        )
        
        await message.reply_photo(
            photo=config.START_IMAGE,
            caption=stats_text,
            parse_mode=ParseMode.HTML
        )
        logger.info(f"Admin stats command handled by {message.from_user.id}")
        
    except Exception as e:
        logger.error(f"Error in admin stats command: {e}")
        await message.reply(f"Error generating stats: {str(e)}", parse_mode=ParseMode.HTML)

@pyro.on_message(filters.command("stats") & ~filters.user(ALLOWED_BD_IDS))
async def non_admin_stats_command(client, message):
    """Handle /stats command for non-admins"""
    await message.reply(f"You Are Not Admin. Admin is @{config.ADMIN_USERNAME}")

@pyro.on_message(filters.command("bd") & filters.user(ALLOWED_BD_IDS))
async def admin_broadcast_command(client, message):
    """Handle /bd command for admins - Forward messages with sender name"""
    try:
        uid = message.from_user.id
        if not is_owner(uid):
            await message.reply("Admin only command.", parse_mode=ParseMode.HTML)
            return
        
        if not message.reply_to_message:
            await message.reply("Reply to a message to broadcast it.", parse_mode=ParseMode.HTML)
            return
        
        all_users = db.get_all_users(limit=0)
        if not all_users:
            await message.reply("No users found.", parse_mode=ParseMode.HTML)
            return
        
        total_users = len(all_users)
        status_msg = await message.reply(
            """<b>📢 AzTech Ads Bot - ADMIN BROADCAST</b>\n\n"""
            "<u>Status: Initializing...</u>",
            parse_mode=ParseMode.HTML
        )
        
        sent_count = 0
        failed_count = 0
        
        reply_msg = message.reply_to_message
        
        for user in all_users:
            user_id = user['user_id']
            try:
                await client.forward_messages(
                    chat_id=user_id,
                    from_chat_id=message.chat.id,
                    message_ids=reply_msg.id
                )
                sent_count += 1
            except PeerIdInvalid:
                logger.error(f"Failed to send broadcast to user {user_id}: PeerIdInvalid")
                failed_count += 1
            except FloodWait as e:
                logger.warning(f"Flood wait for user {user_id}: Wait {e.seconds} seconds")
                await asyncio.sleep(e.seconds)
                try:
                    await client.forward_messages(user_id, message.chat.id, reply_msg.id)
                    sent_count += 1
                except Exception:
                    failed_count += 1
            except Exception as e:
                logger.error(f"Failed to send broadcast to user {user_id}: {e}")
                failed_count += 1
            
            if (sent_count + failed_count) % 10 == 0 or (sent_count + failed_count) == total_users:
                try:
                    await status_msg.edit_text(
                        f"""<b>📢 AzTech Ads Bot - ADMIN BROADCAST</b>\n\n"""
                        f"<u>Status: In Progress...</u> \n"
                        f"<b>Sent:</b> <code>{sent_count}/{total_users}</code>\n"
                        f"<i>Failed:</i> <u>{failed_count}</u>\n"
                        f"Progress: {generate_progress_bar(sent_count + failed_count, total_users)}",
                        parse_mode=ParseMode.HTML
                    )
                except Exception as e:
                    logger.error(f"Failed to update broadcast status: {e}")
            await asyncio.sleep(0.5)
        
        await status_msg.edit_text(
            f"""<b> AzTech Ads Bot - ADMIN BROADCAST COMPLETED</b>\n\n"""
            f"<u>Sent:</u> <code>{sent_count}/{total_users}</code>\n"
            f"<b>Failed:</b> <i>{failed_count}</i> \n"
            f"Success Rate: {generate_progress_bar(sent_count, total_users)} 💹",
            parse_mode=ParseMode.HTML
        )
        await send_dm_log(uid, f"<b>🏁 Admin broadcast completed:</b> Sent {sent_count}/{total_users}, Failed {failed_count} ")
        logger.info(f"Admin broadcast completed by {uid}")
        
    except Exception as e:
        logger.error(f"Error in admin broadcast command: {e}")
        await message.reply(f"Error during broadcast: {str(e)}", parse_mode=ParseMode.HTML)

@pyro.on_message(filters.command("bd") & ~filters.user(ALLOWED_BD_IDS))
async def non_admin_broadcast_command(client, message):
    """Handle /bd command for non-admins"""
    await message.reply("You Are Not Admin")

async def generate_leaderboard_text(uid):
    """Generate leaderboard text (separated for reuse)"""
    pipeline = [
        {
            "$lookup": {
                "from": "analytics",
                "localField": "user_id",
                "foreignField": "user_id",
                "as": "analytics"
            }
        },
        {
            "$lookup": {
                "from": "accounts",
                "localField": "user_id",
                "foreignField": "user_id",
                "as": "accounts"
            }
        },
        {
            "$project": {
                "user_id": 1,
                "username": 1,
                "first_name": 1,
                "total_sent": {"$ifNull": [{"$arrayElemAt": ["$analytics.total_sent", 0]}, 0]},
                "accounts_count": {"$size": "$accounts"}
            }
        },
        {
            "$sort": {"total_sent": -1}
        }
    ]
    
    all_users = list(db.db.users.aggregate(pipeline))
    
    if not all_users:
        return None
    
    sorted_users = sorted(all_users, key=lambda x: x['total_sent'], reverse=True)
    top_10 = sorted_users[:10]
    
    current_user_stats = next((u for u in sorted_users if u['user_id'] == uid), None)
    current_user_rank = next((i+1 for i, u in enumerate(sorted_users) if u['user_id'] == uid), None)
    
    medals = ["#1", "#2", "#3"]
    
    leaderboard_text = f"<b>TOP USERS LEADERBOARD</b>\n"
    leaderboard_text += f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    
    for idx, user in enumerate(top_10, 1):
        medal = medals[idx-1] if idx <= 3 else f"{idx}."
        username = f"@{user.get('username', 'Unknown')}" if user.get('username') and user['username'] != 'Unknown' else user.get('first_name', 'User')
        
        highlight = ">> " if user['user_id'] == uid else ""
        leaderboard_text += f"{highlight}{medal} {username}\n     {user['total_sent']} ads sent\n\n"
    
    leaderboard_text += f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    
    if current_user_stats and current_user_rank:
        if current_user_rank <= 10:
            leaderboard_text += f"★ <b>YOUR RANK: #{current_user_rank}</b> (Top 10!)\n"
        else:
            leaderboard_text += f"★ <b>YOUR RANK: #{current_user_rank}</b> / {len(sorted_users)}\n"
        
        leaderboard_text += f"+ <b>Ads Sent:</b> {current_user_stats['total_sent']}\n\n"
        
        if current_user_rank > 1:
            next_user = sorted_users[current_user_rank - 2]
            gap = next_user['total_sent'] - current_user_stats['total_sent']
            
            if gap > 0:
                leaderboard_text += f"[^] <b>Next rank:</b> {gap} more ads needed!\n\n"
    else:
        leaderboard_text += f"! <b>You're not ranked yet!</b>\n"
        leaderboard_text += f"Start sending ads to appear on the leaderboard.\n\n"
    
    achievements_unlocked = []
    achievements_locked = []
    
    if current_user_stats:
        sent_count = current_user_stats['total_sent']
        
        if sent_count >= 1:
            achievements_unlocked.append("+ First Blood")
        else:
            achievements_locked.append("[ ] First Blood (1 ad)")
        
        if sent_count >= 10:
            achievements_unlocked.append("+ Getting Started")
        else:
            achievements_locked.append(f"[ ] Getting Started ({10 - sent_count} more)")
        
        if sent_count >= 50:
            achievements_unlocked.append("+ Halfway There")
        else:
            achievements_locked.append(f"[ ] Halfway There ({50 - sent_count} more)")
        
        if sent_count >= 100:
            achievements_unlocked.append("+ Century")
        else:
            achievements_locked.append(f"[ ] Century ({100 - sent_count} more)")
        
        if sent_count >= 500:
            achievements_unlocked.append("+ Power User")
        else:
            achievements_locked.append(f"[ ] Power User ({500 - sent_count} more)")
        
        if sent_count >= 1000:
            achievements_unlocked.append("+ Unstoppable")
        else:
            achievements_locked.append(f"[ ] Unstoppable ({1000 - sent_count} more)")
        
        if current_user_rank == 1:
            achievements_unlocked.append("[***] CHAMPION")
        
        if achievements_unlocked:
            leaderboard_text += f"≈ <b>Achievements:</b> {', '.join(achievements_unlocked[:3])}"
            if len(achievements_unlocked) > 3:
                leaderboard_text += f" +{len(achievements_unlocked) - 3}"
            leaderboard_text += "\n\n"
        
        if achievements_locked:
            leaderboard_text += f"▸ <b>Next:</b> {achievements_locked[0]}\n"
    
    return leaderboard_text

@pyro.on_message(filters.command("leaderboard"))
async def leaderboard_command(client, message):
    """Handle /leaderboard command - Ultra-fast user rankings"""
    try:
        uid = message.from_user.id
        
        status_msg = await message.reply("", parse_mode=ParseMode.HTML)
        
        try:
            leaderboard_text = await generate_leaderboard_text(uid)
            
            if not leaderboard_text:
                await status_msg.edit_text(
                    " <b>No users found!</b>",
                    parse_mode=ParseMode.HTML
                )
                return
            
            buttons = [
                [InlineKeyboardButton("↻ Refresh", callback_data="leaderboard_refresh")]
            ]
            
            await status_msg.edit_text(
                leaderboard_text,
                parse_mode=ParseMode.HTML,
                reply_markup=kb(buttons)
            )
            
            logger.info(f"Leaderboard shown to user {uid}")
            
        except Exception as e:
            logger.error(f"Error generating leaderboard: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            await status_msg.edit_text(
                f" <b>Error generating leaderboard:</b>\n<code>{str(e)}</code>",
                parse_mode=ParseMode.HTML
            )
    
    except Exception as e:
        logger.error(f"Error in leaderboard command: {e}")
        if not hasattr(message, 'edit_text'):
            await message.reply(
                f" <b>Error:</b> {str(e)}",
                parse_mode=ParseMode.HTML
            )

@pyro.on_callback_query(filters.regex("^leaderboard_refresh$"))
async def leaderboard_callback(client, callback_query):
    """Handle leaderboard refresh callback"""
    try:
        uid = callback_query.from_user.id
        await callback_query.answer(" Refreshing...")
        
        leaderboard_text = await generate_leaderboard_text(uid)
        
        if not leaderboard_text:
            await callback_query.message.edit_text(
                " <b>No users found!</b>",
                parse_mode=ParseMode.HTML
            )
            return
        
        buttons = [
            [InlineKeyboardButton("↻ Refresh", callback_data="leaderboard_refresh")]
        ]
        
        await callback_query.message.edit_text(
            leaderboard_text,
            parse_mode=ParseMode.HTML,
            reply_markup=kb(buttons)
        )
        
    except Exception as e:
        logger.error(f"Error in leaderboard callback: {e}")
        await callback_query.answer(f"Error: {str(e)}", show_alert=True)

# =======================================================
# 🔘 CALLBACK QUERY HANDLERS
# =======================================================

@pyro.on_callback_query(filters.regex("^otp_"))
async def otp_callback(client, callback_query):
    """Handle OTP input callback."""
    uid = callback_query.from_user.id
    state = db.get_user_state(uid)
    if state != "telethon_wait_otp":
        await callback_query.answer("Invalid state! Please restart with /start.", show_alert=True)
        return

    temp_encrypted = db.get_temp_data(uid, "session")
    if not temp_encrypted:
        await callback_query.answer("Session expired! Please restart.", show_alert=True)
        db.set_user_state(uid, "")
        return

    try:
        temp_json = cipher_suite.decrypt(temp_encrypted.encode()).decode()
        temp_dict = json.loads(temp_json)
        phone = temp_dict["phone"]
        session_str = temp_dict["session_str"]
        phone_code_hash = temp_dict["phone_code_hash"]
        otp = temp_dict.get("otp", "")
    except (json.JSONDecodeError, InvalidToken) as e:
        logger.error(f"Invalid temp data for user {uid}: {e}")
        await callback_query.answer("Error: Corrupted session data. Please restart.", show_alert=True)
        db.set_user_state(uid, "")
        db.delete_temp_data(uid, "session")
        return

    try:
        StringSession(session_str)
    except Exception as e:
        logger.error(f"Invalid session string for user {uid}: {e}")
        await callback_query.answer("Error: Invalid session. Please restart.", show_alert=True)
        db.set_user_state(uid, "")
        db.delete_temp_data(uid, "session")
        return

    action = callback_query.data.replace("otp_", "")
    if action.isdigit():
        if len(otp) < 5:
            otp += action
    elif action == "back":
        otp = otp[:-1] if otp else ""
    elif action == "cancel":
        db.set_user_state(uid, "")
        db.delete_temp_data(uid, "session")
        await callback_query.message.edit_caption("OTP entry cancelled.", reply_markup=None)
        return

    temp_dict["otp"] = otp
    temp_json = json.dumps(temp_dict)
    temp_encrypted = cipher_suite.encrypt(temp_json.encode()).decode()
    db.set_temp_data(uid, "session", temp_encrypted)

    masked = " ".join("*" for _ in otp) if otp else "_____"
    base_caption = (
        f"Phone: {phone}\n\n"
        f"<b>OTP sent!</b>\n\n"
        f"Enter the OTP using the keypad below\n"
        f"<b>Current:</b> <code>{masked}</code>\n"
        f"<b>Format:</b> <code>12345</code> (no spaces needed)\n"
        f"<i>Valid for:</i>{config.OTP_EXPIRY // 60} minutes"
    )

    await callback_query.message.edit_caption(
        caption=base_caption,
        parse_mode=ParseMode.HTML,
        reply_markup=get_otp_keyboard()
    )

    if len(otp) == 5:
        await callback_query.message.edit_caption(base_caption + "\n\n<b>Verifying OTP...</b>", parse_mode=ParseMode.HTML, reply_markup=None)
        max_retries = 3
        retry_delay = 2

        for attempt in range(max_retries):
            credentials = db.get_user_api_credentials(uid)
            
            if not credentials:
                await callback_query.edit_message_text(
                    f" <b>API credentials not found!</b>\n\n"
                    f"Please restart the account addition process.",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb([[InlineKeyboardButton("←", callback_data="menu_main")]])
                )
                return
            
            tg = TelegramClient(StringSession(session_str), credentials['api_id'], credentials['api_hash'])
            try:
                await tg.connect()
                await tg.sign_in(phone, code=otp, phone_code_hash=phone_code_hash)

                session_encrypted = cipher_suite.encrypt(session_str.encode()).decode()
                db.add_user_account(uid, phone, session_encrypted)

                await callback_query.message.edit_caption(
    f"<b>Account Successfully added!</b>\n\n"
    f"Phone: <code>{phone}</code>\n"
    "Your account is ready for broadcasting!\n"
    "<b>Note:</b> Your account is ready for broadcasting!",
    parse_mode=ParseMode.HTML,
    reply_markup=kb([[InlineKeyboardButton("● Dashboard", callback_data="menu_main")]])
)

                await send_dm_log(uid, f"<b> Account added successfully:</b> <code>{phone}</code>")
                
                # Fetch all groups and save to MongoDB cache
                await fetch_groups_after_account_add(uid)
                
                asyncio.create_task(auto_select_all_groups(uid, phone))
                
                db.set_user_state(uid, "")
                db.delete_temp_data(uid, "session")
                break
            except SessionPasswordNeededError:
                temp_dict_2fa = {
                    "phone": phone,
                    "session_str": session_str
                }
                temp_json_2fa = json.dumps(temp_dict_2fa)
                temp_encrypted_2fa = cipher_suite.encrypt(temp_json_2fa.encode()).decode()
                db.set_user_state(uid, "telethon_wait_password")
                db.set_temp_data(uid, "session", temp_encrypted_2fa)
                await callback_query.message.edit_caption(
                    base_caption + "\n\n<b>🔐 2FA Detected!</b>\n\n"
                    "Please send your Telegram cloud password:",
                    parse_mode=ParseMode.HTML,
                    reply_markup=None
                )
                break
            except PhoneCodeInvalidError:
                if attempt < max_retries - 1:
                    logger.warning(f"Invalid OTP attempt {attempt + 1} for {uid}, retrying...")
                    await asyncio.sleep(retry_delay)
                    retry_delay *= 2
                    continue
                await callback_query.message.edit_caption(
                    base_caption + "\n\n<b> Invalid OTP! Try again.</b>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=get_otp_keyboard()
                )
                temp_dict["otp"] = ""
                temp_json = json.dumps(temp_dict)
                temp_encrypted = cipher_suite.encrypt(temp_json.encode()).decode()
                db.set_temp_data(uid, "session", temp_encrypted)
            except PhoneCodeExpiredError:
                await callback_query.message.edit_caption(
                    base_caption + "\n\n<b> OTP expired! Please restart.</b>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=None
                )
                db.set_user_state(uid, "")
                db.delete_temp_data(uid, "session")
                break
            except FloodWaitError as e:
                logger.warning(f"Flood wait during OTP verification for {uid}: Wait {e.seconds} seconds")
                await asyncio.sleep(e.seconds)
                if attempt < max_retries - 1:
                    continue
                await callback_query.message.edit_caption(
                    base_caption + f"\n\n<b> Flood wait limit reached: Please wait {e.seconds}s and try again.</b>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=None
                )
                db.set_user_state(uid, "")
                db.delete_temp_data(uid, "session")
                break
            except Exception as e:
                logger.error(f"Error signing in for {uid} (attempt {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay)
                    retry_delay *= 2
                    continue
                await callback_query.message.edit_caption(
                    base_caption + f"\n\n<b> Login failed:</b>{str(e)}\n\n"
                    f"<b>Contact:</b> <code>@{config.ADMIN_USERNAME}</code>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=None
                )
                await send_dm_log(uid, f"<b> Account login failed:</b> {str(e)}")
                db.set_user_state(uid, "")
                db.delete_temp_data(uid, "session")
                break
            finally:
                await tg.disconnect()

# =======================================================
#   GROUPS MENU SYSTEM
# =======================================================

@pyro.on_callback_query(filters.regex("^groups_menu"))
async def groups_menu_callback(client, callback_query):
    """Handle groups menu callback with forum filter support"""
    try:
        uid = callback_query.from_user.id
        try:
            page = int(callback_query.data.split("_")[-1]) if callback_query.data.count("_") > 1 else 1
        except ValueError:
            page = 1
        accounts = db.get_user_accounts(uid)
        
        if not accounts:
            await callback_query.answer("No accounts added yet!", show_alert=True)
            return

        user = db.db.users.find_one({"user_id": uid})
        forum_only_mode = user.get("forum_only_mode", False) if user else False

        await callback_query.message.edit_caption(
            caption="<b>⏳ Loading groups...</b>",
            parse_mode=ParseMode.HTML
        )

        all_groups = []
        if forum_only_mode:
            selected_groups = db.get_forum_groups(uid) or []
        else:
            selected_groups = db.get_target_groups(uid) or []
        selected_group_ids = [g['group_id'] for g in selected_groups]
        
        async def get_account_groups(acc):
            try:
                session_str = cipher_suite.decrypt(acc['session_string'].encode()).decode()
                credentials = db.get_user_api_credentials(uid)
                if not credentials:
                    logger.error(f"No API credentials found for user {uid}")
                    return []
                
                async with TelegramClient(StringSession(session_str), credentials['api_id'], credentials['api_hash']) as tg_client:
                    groups = []
                    async for dialog in tg_client.iter_dialogs(limit=None):
                        if dialog.is_group:
                            is_forum = False
                            try:
                                entity = await tg_client.get_entity(dialog.id)
                                is_forum = getattr(entity, 'forum', False)
                            except:
                                is_forum = False
                            
                            if forum_only_mode:
                                if not is_forum:
                                    continue
                            else:
                                if is_forum:
                                    continue
                            
                            group_data = {
                                'id': dialog.id,
                                'title': dialog.title,
                                'selected': dialog.id in selected_group_ids,
                                'is_forum': is_forum
                            }
                            groups.append(group_data)
                    return groups
            except Exception as e:
                logger.error(f"Failed to get groups for account {acc['phone_number']}: {e}")
                return []

        tasks = [get_account_groups(acc) for acc in accounts]
        groups_lists = await asyncio.gather(*tasks)
        
        seen_ids = set()
        for groups in groups_lists:
            for group in groups:
                if group['id'] not in seen_ids:
                    seen_ids.add(group['id'])
                    all_groups.append(group)

        items_per_page = 8
        total_pages = (len(all_groups) + items_per_page - 1) // items_per_page
        start_idx = (page - 1) * items_per_page
        end_idx = start_idx + items_per_page
        current_groups = all_groups[start_idx:end_idx]

        total_groups = len(all_groups)
        selected_count = sum(1 for g in all_groups if g['selected'])
        forum_count = sum(1 for g in all_groups if g.get('is_forum', False))

        mode_text = " Forum Groups Only" if forum_only_mode else " All Groups"
        caption = f"<b>BROADCAST GROUPS </b>\n\n"
        caption += f"<b>Mode:</b> {mode_text}\n"
        caption += f"<b>Selected:</b> {selected_count}/{total_groups}\n"
        if forum_count > 0:
            caption += f"<b>Forum Groups:</b> {forum_count}\n"
        caption += "\n<i>Click on groups to toggle selection:</i>\n"

        buttons = []
        group_pairs = [current_groups[i:i+2] for i in range(0, len(current_groups), 2)]
        selected_ids = [g['group_id'] for g in selected_groups]
        
        for pair in group_pairs:
            row = []
            for group in pair:
                status = "" if group['id'] in selected_ids else ""
                forum_icon = " " if group.get('is_forum', False) else ""
                row.append(InlineKeyboardButton(
                    f"{group['title'][:18]}{forum_icon} {status}",
                    callback_data=f"toggle_group_{group['id']}"
                ))
            buttons.append(row)

        nav_buttons = []
        if page > 1:
            nav_buttons.append(InlineKeyboardButton("", callback_data=f"groups_menu_{page-1}"))
        nav_buttons.append(InlineKeyboardButton(f"{page}/{total_pages}", callback_data="noop"))
        if page < total_pages:
            nav_buttons.append(InlineKeyboardButton("", callback_data=f"groups_menu_{page+1}"))
        if nav_buttons:
            buttons.append(nav_buttons)

        toggle_text = " Show All Groups" if forum_only_mode else " Forum Groups Only"
        buttons.append([
            InlineKeyboardButton(toggle_text, callback_data="toggle_forum_mode")
        ])
        buttons.append([
            InlineKeyboardButton("+ Select All", callback_data="select_all_groups"),
            InlineKeyboardButton("- Unselect All", callback_data="unselect_all_groups")
        ])
        buttons.append([
            InlineKeyboardButton("? Search Groups", callback_data="search_groups"),
            InlineKeyboardButton("× Clear Filter", callback_data="clear_search_filter")
        ])
        buttons.append([
            InlineKeyboardButton("++ Add All Groups", callback_data="add_all_groups_bulk"),
            InlineKeyboardButton("◆ Add Topics Only", callback_data="add_forums_only")
        ])
        buttons.append([
            InlineKeyboardButton("-- Remove Filtered", callback_data="unselect_all_filtered")
        ])
        buttons.append([InlineKeyboardButton("✓ Done", callback_data="menu_main")])

        caption += f"\nPage {page}/{total_pages}"
        await callback_query.message.edit_caption(
            caption=caption,
            reply_markup=kb(buttons),
            parse_mode=ParseMode.HTML
        )
        
    except Exception as e:
        logger.error(f"Error in groups menu callback: {e}")
        await callback_query.answer("Error loading groups menu. Try again.", show_alert=True)

@pyro.on_callback_query(filters.regex("^toggle_forum_mode$"))
async def toggle_forum_mode_callback(client, callback_query):
    """Handle forum mode toggle callback"""
    try:
        uid = callback_query.from_user.id
        
        user = db.db.users.find_one({"user_id": uid})
        current_mode = user.get("forum_only_mode", False) if user else False
        
        new_mode = not current_mode
        db.db.users.update_one(
            {"user_id": uid},
            {"$set": {"forum_only_mode": new_mode}},
            upsert=True
        )
        
        if new_mode:
            existing_groups = db.get_target_groups(uid) or []
            groups_without_flag = [g for g in existing_groups if 'is_forum' not in g]
            
            if groups_without_flag:
                await callback_query.answer("⏳ Updating groups... Please wait", show_alert=False)
                
                accounts = db.get_user_accounts(uid)
                if accounts:
                    acc = accounts[0]
                    try:
                        session_str = cipher_suite.decrypt(acc['session_string'].encode()).decode()
                        credentials = db.get_user_api_credentials(uid)
                        if credentials:
                            async with TelegramClient(StringSession(session_str), credentials['api_id'], credentials['api_hash']) as tg_client:
                                for group in groups_without_flag:
                                    try:
                                        entity = await tg_client.get_entity(group['group_id'])
                                        is_forum = getattr(entity, 'forum', False)
                                        
                                        db.db.target_groups.update_one(
                                            {"user_id": uid, "group_id": group['group_id']},
                                            {"$set": {"is_forum": is_forum}}
                                        )
                                    except:
                                        pass
                    except Exception as e:
                        logger.error(f"Error updating groups: {e}")
        
        mode_text = "Forum Groups Only " if new_mode else "All Groups "
        await callback_query.answer(f"Switched to: {mode_text}", show_alert=False)
        
        callback_query.data = "groups_menu"
        await groups_menu_callback(client, callback_query)
        
    except Exception as e:
        logger.error(f"Error in toggle_forum_mode callback: {e}")
        await callback_query.answer("Error toggling mode. Try again.", show_alert=True)

@pyro.on_callback_query(filters.regex("^toggle_group_"))
async def toggle_group_callback(client, callback_query):
    """Handle toggle group selection callback"""
    try:
        uid = callback_query.from_user.id
        group_id = int(callback_query.data.split("_")[2])
        
        user = db.db.users.find_one({"user_id": uid})
        forum_only_mode = user.get("forum_only_mode", False) if user else False
        
        if forum_only_mode:
            group_state = db.get_forum_group(uid, group_id)
        else:
            group_state = db.get_target_group(uid, group_id)
        
        title = None
        is_forum = False
        
        for row in callback_query.message.reply_markup.inline_keyboard:
            for button in row:
                if button.callback_data == f"toggle_group_{group_id}":
                    button_text = button.text
                    is_forum = "" in button_text
                    title = button_text.replace(" ", "").replace(" ", "").replace(" ", "").strip()
                    break
            if title:
                break

        if group_state:
            if forum_only_mode:
                db.remove_forum_group(uid, group_id)
            else:
                db.remove_target_group(uid, group_id)
            await callback_query.answer(" Removed from broadcast", show_alert=False)
        else:
            if title:
                if forum_only_mode:
                    db.add_forum_group(uid, group_id, title)
                else:
                    db.add_target_group(uid, group_id, title)
                await callback_query.answer(" Added to broadcast", show_alert=False)
            else:
                try:
                    accounts = db.get_user_accounts(uid)
                    if accounts:
                        acc = accounts[0]
                        session_str = cipher_suite.decrypt(acc['session_string'].encode()).decode()
                        credentials = db.get_user_api_credentials(uid)
                        if credentials:
                            async with TelegramClient(StringSession(session_str), credentials['api_id'], credentials['api_hash']) as temp_client:
                                group = await temp_client.get_entity(group_id)
                                is_forum = getattr(group, 'forum', False)
                                if forum_only_mode:
                                    db.add_forum_group(uid, group_id, group.title)
                                else:
                                    db.add_target_group(uid, group_id, group.title)
                except Exception as e:
                    logger.error(f"Error adding group {group_id}: {e}")
                    await callback_query.answer("Error adding group", show_alert=True)
                    return

        new_markup = list(callback_query.message.reply_markup.inline_keyboard)
        for i, row in enumerate(new_markup):
            for j, button in enumerate(row):
                if button.callback_data == f"toggle_group_{group_id}":
                    status = "" if group_state else ""
                    new_markup[i][j] = InlineKeyboardButton(
                        f"{title} {status}",
                        callback_data=f"toggle_group_{group_id}"
                    )

        await callback_query.message.edit_reply_markup(
            reply_markup=InlineKeyboardMarkup(new_markup)
        )

    except Exception as e:
        logger.error(f"Error in toggle group callback: {e}")
        await callback_query.answer("Error toggling group. Try again.", show_alert=True)

@pyro.on_callback_query(filters.regex("^select_all_groups$"))
async def select_all_groups_callback(client, callback_query):
    """Handle select all groups callback"""
    try:
        uid = callback_query.from_user.id
        
        user = db.db.users.find_one({"user_id": uid})
        forum_only_mode = user.get("forum_only_mode", False) if user else False
        
        if forum_only_mode:
            selected_groups = db.get_forum_groups(uid) or []
        else:
            selected_groups = db.get_target_groups(uid) or []
        
        all_groups = []
        
        accounts = db.get_user_accounts(uid)
        for acc in accounts:
            try:
                session_str = cipher_suite.decrypt(acc['session_string'].encode()).decode()
                credentials = db.get_user_api_credentials(acc['user_id'])
                if not credentials:
                    logger.error(f"No API credentials found for user {acc['user_id']}")
                    continue
                
                async with TelegramClient(StringSession(session_str), credentials['api_id'], credentials['api_hash']) as tg_client:
                    async for dialog in tg_client.iter_dialogs():
                        if dialog.is_group and dialog.id not in [g['id'] for g in all_groups]:
                            try:
                                entity = await tg_client.get_entity(dialog.id)
                                is_forum = getattr(entity, 'forum', False)
                                
                                if forum_only_mode:
                                    if not is_forum:
                                        continue
                                else:
                                    if is_forum:
                                        continue
                                
                                all_groups.append({
                                    'id': dialog.id,
                                    'title': dialog.title,
                                    'is_forum': is_forum
                                })
                            except:
                                pass
            except Exception as e:
                logger.error(f"Error adding groups for account {acc['phone_number']}: {e}")
                continue
        
        for group in selected_groups:
            if forum_only_mode:
                db.remove_forum_group(uid, group['group_id'])
            else:
                db.remove_target_group(uid, group['group_id'])
        
        for group in all_groups:
            try:
                group_name = group.get('title', 'Unknown')
                if forum_only_mode:
                    db.add_forum_group(uid, group['id'], group_name)
                else:
                    db.add_target_group(uid, group['id'], group_name)
            except Exception as e:
                logger.error(f"Error adding group {group.get('title', 'Unknown')}: {e}")
                continue
        
        mode_text = "forum groups" if forum_only_mode else "groups"
        await callback_query.answer(f" All {mode_text} selected!", show_alert=True)

        callback_query.data = "groups_menu"
        await groups_menu_callback(client, callback_query)

    except Exception as e:
        logger.error(f"Error in select all groups callback: {e}")
        await callback_query.answer("Error selecting groups. Try again.", show_alert=True)

@pyro.on_callback_query(filters.regex("^menu_manage_account$"))
async def menu_manage_account_callback(client, callback_query):
    """Handle manage account menu"""
    try:
        uid = callback_query.from_user.id
        await callback_query.answer()
        
        # Check if account is added
        accounts = db.get_user_accounts(uid)
        
        if not accounts:
            # No account added - show "Host Account" button
            menu_text = (
                "<b> MANAGE ACCOUNT</b>\n\n"
                "No Telegram account connected.\n\n"
                "<i>Add a Telegram account to start broadcasting.</i>"
            )
            
            buttons = [
                [InlineKeyboardButton("+ Host Account", callback_data="host_account")],
                [InlineKeyboardButton("←", callback_data="menu_main")]
            ]
        else:
            # Account added - show "Logout" button
            account = accounts[0]
            phone = account.get("phone_number", "Unknown")
            
            menu_text = (
                f"<b> MANAGE ACCOUNT</b>\n\n"
                f"Connected Account: <code>{phone}</code>\n\n"
                f"<i>You can logout to disconnect this account.</i>"
            )
            
            buttons = [
                [InlineKeyboardButton("- Logout", callback_data="instant_logout")],
                [InlineKeyboardButton("←", callback_data="menu_main")]
            ]
        
        await callback_query.message.edit_text(
            menu_text,
            reply_markup=InlineKeyboardMarkup(buttons),
            parse_mode=ParseMode.HTML
        )
        
    except Exception as e:
        logger.error(f"Error in menu_manage_account callback: {e}")
        await callback_query.answer("Error loading account menu. Try again.", show_alert=True)

@pyro.on_callback_query(filters.regex("^menu_post_link$"))
async def menu_post_link_callback(client, callback_query):
    """Handle post link management menu"""
    try:
        uid = callback_query.from_user.id
        await callback_query.answer()
        
        # Get current post link settings
        post_link_data = db.get_user_post_link(uid)
        
        if post_link_data and post_link_data.get("message_source") == "post_link":
            current_mode = " Post Link"
            post_link = post_link_data.get("post_link", "Not Set")
            status_emoji = "[ON]"
        else:
            current_mode = " Saved Messages"
            post_link = "Not Set"
            status_emoji = "[ON]"
        
        menu_text = (
            f"<b> POST LINK MANAGEMENT</b>\n\n"
            f"{status_emoji} <b>Current Mode:</b> {current_mode}\n"
            f"🔗 <b>Post Link:</b> <code>{post_link}</code>\n\n"
            f"<b>ℹ️ About:</b>\n"
            f"• <b>Saved Messages:</b> Forwards from your Saved Messages (default)\n"
            f"• <b>Post Link:</b> Forwards a specific message from any channel/group\n\n"
            f"<i>Choose an option below:</i>"
        )
        
        buttons = [
            [InlineKeyboardButton("≈ Set Post Link", callback_data="set_post_link")],
            [InlineKeyboardButton("× Clear Post Link", callback_data="clear_post_link")],
            [InlineKeyboardButton("←", callback_data="menu_main")]
        ]
        
        await callback_query.message.edit_text(
            menu_text,
            reply_markup=InlineKeyboardMarkup(buttons),
            parse_mode=ParseMode.HTML
        )
        
    except Exception as e:
        logger.error(f"Error in menu_post_link callback: {e}")
        await callback_query.answer("Error loading post link menu. Try again.", show_alert=True)

@pyro.on_callback_query(filters.regex("^set_post_link$"))
async def set_post_link_callback(client, callback_query):
    """Handle set post link callback"""
    try:
        uid = callback_query.from_user.id
        await callback_query.answer()
        
        # Set user state to wait for post link
        db.set_user_state(uid, "awaiting_post_link")
        
        await callback_query.message.reply_text(
            "<b>🔗 SET POST LINK</b>\n\n"
            "Send me the Telegram post link you want to forward.\n\n"
            "<b>Examples:</b>\n"
            "<code>https://t.me/channelname/123</code>\n"
            "<code>t.me/channelname/123</code>\n"
            "<code>https://t.me/c/1234567890/123</code>\n\n"
            "<i>Send /cancel to cancel.</i>",
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        logger.error(f"Error in set_post_link callback: {e}")
        await callback_query.answer("Error. Try again.", show_alert=True)

@pyro.on_callback_query(filters.regex("^use_saved_messages$"))
async def use_saved_messages_callback(client, callback_query):
    """Handle use saved messages callback"""
    try:
        uid = callback_query.from_user.id
        await callback_query.answer("Switched to Saved Messages mode ")
        
        # Clear post link and switch to saved messages
        db.clear_user_post_link(uid)
        
        await callback_query.message.edit_text(
            "<b> MODE CHANGED</b>\n\n"
            "Now using <b>Saved Messages</b> for broadcasts.\n\n"
            "Messages will be forwarded from your Saved Messages.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("x Back to Post Link Menu", callback_data="menu_post_link")],
                [InlineKeyboardButton("←", callback_data="menu_main")]
            ]),
            parse_mode=ParseMode.HTML
        )
        
    except Exception as e:
        logger.error(f"Error in use_saved_messages callback: {e}")
        await callback_query.answer("Error switching mode. Try again.", show_alert=True)

@pyro.on_callback_query(filters.regex("^toggle_message_mode$"))
async def toggle_message_mode_callback(client, callback_query):
    """Handle toggle message mode callback"""
    try:
        uid = callback_query.from_user.id
        
        # Get current mode
        post_link_data = db.get_user_post_link(uid)
        
        if post_link_data and post_link_data.get("message_source") == "post_link":
            # Switch to saved messages
            db.clear_user_post_link(uid)
            new_mode = " Saved Messages"
            await callback_query.answer("Switched to Saved Messages ")
        else:
            # Check if post link is set
            if post_link_data and post_link_data.get("post_link"):
                # Re-enable post link mode
                db.db.users.update_one(
                    {"user_id": uid},
                    {"$set": {"message_source": "post_link"}}
                )
                new_mode = " Post Link"
                await callback_query.answer("Switched to Post Link ")
            else:
                await callback_query.answer(" No post link set! Set one first.", show_alert=True)
                return
        
        # Refresh menu
        await menu_post_link_callback(client, callback_query)
        
    except Exception as e:
        logger.error(f"Error in toggle_message_mode callback: {e}")
        await callback_query.answer("Error toggling mode. Try again.", show_alert=True)

@pyro.on_callback_query(filters.regex("^clear_post_link$"))
async def clear_post_link_callback(client, callback_query):
    """Handle clear post link callback"""
    try:
        uid = callback_query.from_user.id
        
        # Check if post link exists
        post_link_data = db.get_user_post_link(uid)
        
        if not post_link_data or not post_link_data.get("post_link"):
            await callback_query.answer(" No post link to clear!", show_alert=True)
            return
        
        await callback_query.answer("Post link cleared ")
        
        # Clear post link
        db.clear_user_post_link(uid)
        
        await callback_query.message.edit_text(
            "<b> POST LINK CLEARED</b>\n\n"
            "Post link has been removed.\n"
            "Now using <b>Saved Messages</b> mode.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("x Back to Post Link Menu", callback_data="menu_post_link")],
                [InlineKeyboardButton("←", callback_data="menu_main")]
            ]),
            parse_mode=ParseMode.HTML
        )
        
    except Exception as e:
        logger.error(f"Error in clear_post_link callback: {e}")
        await callback_query.answer("Error clearing post link. Try again.", show_alert=True)

@pyro.on_callback_query(filters.regex("^groups_only_mode$"))
async def groups_only_mode_callback(client, callback_query):
    """Handle groups only mode - show only regular groups (NO topics/forums)"""
    try:
        uid = callback_query.from_user.id
        await callback_query.answer("Loading groups...")
        
        # Get all groups from MongoDB cache
        all_groups = await get_groups_from_mongo_cache(uid)
        
        # Filter: ONLY regular groups (no forum groups with topics enabled)
        regular_groups = [g for g in all_groups if g.get('type') == 'group' and not g.get('is_forum', False)]
        
        if not regular_groups:
            await callback_query.message.edit_text(
                "<b> No Regular Groups Found</b>\n\n"
                "No non-forum groups found in your account.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("x Back to Groups Settings", callback_data="menu_groups")]
                ]),
                parse_mode=ParseMode.HTML
            )
            return
        
        # Get selected groups
        target_groups = db.get_target_groups(uid)
        selected_group_ids = [g.get("group_id", g.get("id")) for g in target_groups if g]
        
        # Pagination setup
        page = 0  # Default to first page
        items_per_page = 10
        start_idx = page * items_per_page
        end_idx = start_idx + items_per_page
        total_pages = (len(regular_groups) + items_per_page - 1) // items_per_page
        
        menu_text = (
            f"<b> GROUPS ONLY MODE</b>\n\n"
            f"Total Regular Groups: <b>{len(regular_groups)}</b>\n"
            f"Selected: <b>{len([g for g in regular_groups if g['id'] in selected_group_ids])}</b>\n"
            f"Page: <b>{page + 1}/{total_pages}</b>\n\n"
            f"<i>Select groups to add to broadcast list (no topics).</i>"
        )
        
        buttons = [
            [
                InlineKeyboardButton("+ Select All", callback_data="groups_only_select_all"),
                InlineKeyboardButton("- Unselect All", callback_data="groups_only_unselect_all")
            ]
        ]
        
        # Add individual group buttons for current page
        for group in regular_groups[start_idx:end_idx]:
            is_selected = group["id"] in selected_group_ids
            emoji = "" if is_selected else ""
            buttons.append([InlineKeyboardButton(
                f"{emoji} {group['title'][:30]}",
                callback_data=f"toggle_group_{group['id']}"
            )])
        
        # Add pagination buttons
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("< Back", callback_data=f"groups_page_{page - 1}"))
        if end_idx < len(all_groups):
            nav_buttons.append(InlineKeyboardButton(">", callback_data=f"groups_page_{page + 1}"))
        
        if nav_buttons:
            buttons.append(nav_buttons)
        
        buttons.append([InlineKeyboardButton("x Back to Groups Settings", callback_data="menu_groups")])
        
        await callback_query.message.edit_text(
            menu_text,
            reply_markup=InlineKeyboardMarkup(buttons),
            parse_mode=ParseMode.HTML
        )
        
    except Exception as e:
        logger.error(f"Error in groups_only_mode callback: {e}")
        await callback_query.answer("Error loading groups. Try again.", show_alert=True)

@pyro.on_callback_query(filters.regex("^refresh_groups_cache$"))
async def refresh_groups_cache_callback(client, callback_query):
    """Manually refresh groups cache"""
    try:
        uid = callback_query.from_user.id
        await callback_query.answer("Refreshing groups cache...", show_alert=False)
        
        # Show loading message
        msg = await callback_query.message.reply_text(
            "<b>🔄 Refreshing Groups Cache...</b>\n\n"
            "Fetching all groups from your accounts...",
            parse_mode=ParseMode.HTML
        )
        
        # Fetch fresh data and update cache
        await fetch_and_cache_groups_to_mongo(uid)
        
        # Get updated count
        cached_groups = db.get_cached_groups(uid)
        
        await msg.edit_text(
            f"<b>✅ Groups Cache Refreshed!</b>\n\n"
            f"<b>Total Groups Cached:</b> {len(cached_groups)}\n\n"
            f"<i>All groups data has been updated from Telegram.</i>",
            parse_mode=ParseMode.HTML
        )
        
    except Exception as e:
        logger.error(f"Error refreshing cache: {e}")
        await callback_query.answer("Error refreshing cache. Try again.", show_alert=True)

@pyro.on_callback_query(filters.regex(r"^groups_page_(\d+)$"))
async def groups_page_callback(client, callback_query):
    """Handle groups pagination"""
    try:
        uid = callback_query.from_user.id
        page = int(callback_query.data.split("_")[-1])
        
        # Use cached groups - NO fetching!
        cached_data = db.get_cached_groups(uid)
        all_groups = cached_data.get("groups", []) if cached_data else []
        
        if not all_groups:
            await callback_query.answer(" No cached groups. Please go back and reload.", show_alert=True)
            return
        
        # Pagination setup
        items_per_page = 10
        start_idx = page * items_per_page
        end_idx = start_idx + items_per_page
        total_pages = (len(all_groups) + items_per_page - 1) // items_per_page
        
        # Get selected groups
        target_groups = db.get_target_groups(uid)
        selected_group_ids = [g.get("group_id", g.get("id")) for g in target_groups if g]
        
        menu_text = (
            f"<b> GROUPS ONLY MODE</b>\n\n"
            f"Total Groups: <b>{len(all_groups)}</b>\n"
            f"Selected: <b>{len([g for g in all_groups if g['id'] in selected_group_ids])}</b>\n"
            f"Page: <b>{page + 1}/{total_pages}</b>\n\n"
            f"<i>Select groups to add to broadcast list.</i>"
        )
        
        buttons = [
            [
                InlineKeyboardButton("+ Select All", callback_data="groups_only_select_all"),
                InlineKeyboardButton("- Unselect All", callback_data="groups_only_unselect_all")
            ]
        ]
        
        # Add individual group buttons for current page
        for group in all_groups[start_idx:end_idx]:
            is_selected = group["id"] in selected_group_ids
            emoji = "" if is_selected else ""
            buttons.append([InlineKeyboardButton(
                f"{emoji} {group['title'][:30]}",
                callback_data=f"toggle_group_{group['id']}"
            )])
        
        # Add pagination buttons
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("< Back", callback_data=f"groups_page_{page - 1}"))
        if end_idx < len(all_groups):
            nav_buttons.append(InlineKeyboardButton(">", callback_data=f"groups_page_{page + 1}"))
        
        if nav_buttons:
            buttons.append(nav_buttons)
        
        buttons.append([InlineKeyboardButton("x Back to Groups Settings", callback_data="menu_groups")])
        
        await callback_query.message.edit_text(
            menu_text,
            reply_markup=InlineKeyboardMarkup(buttons),
            parse_mode=ParseMode.HTML
        )
        
    except Exception as e:
        logger.error(f"Error in groups_page callback: {e}")
        await callback_query.answer("Error loading page. Try again.", show_alert=True)

@pyro.on_callback_query(filters.regex("^groups_only_select_all$"))
async def groups_only_select_all_callback(client, callback_query):
    """Select all regular groups"""
    try:
        uid = callback_query.from_user.id
        await callback_query.answer("Adding all groups...")
        
        # Fetch groups
        accounts = db.get_user_accounts(uid)
        all_groups = []
        for acc in accounts:
            try:
                tg_client = await get_telegram_client(acc["phone_number"], acc["session_string"])
                async for dialog in tg_client.iter_dialogs():
                    if dialog.is_group or dialog.is_channel:
                        entity = dialog.entity
                        is_forum = getattr(entity, 'forum', False)
                        if not is_forum:
                            all_groups.append({"id": entity.id, "title": dialog.title})
                break
            except Exception as e:
                logger.error(f"Error fetching groups: {e}")
                continue
        
        added_count = 0
        for group in all_groups:
            if not db.get_target_group(uid, group["id"]):
                db.add_target_group(uid, group["id"], group["title"])
                added_count += 1
        
        await callback_query.answer(f" Added {added_count} groups")
        await groups_only_mode_callback(client, callback_query)
        
    except Exception as e:
        logger.error(f"Error in groups_only_select_all: {e}")
        await callback_query.answer("Error. Try again.", show_alert=True)

@pyro.on_callback_query(filters.regex("^groups_only_unselect_all$"))
async def groups_only_unselect_all_callback(client, callback_query):
    """Unselect all regular groups"""
    try:
        uid = callback_query.from_user.id
        await callback_query.answer("Removing all groups...")
        
        # Fetch groups
        accounts = db.get_user_accounts(uid)
        all_groups = []
        for acc in accounts:
            try:
                tg_client = await get_telegram_client(acc["phone_number"], acc["session_string"])
                async for dialog in tg_client.iter_dialogs():
                    if dialog.is_group or dialog.is_channel:
                        entity = dialog.entity
                        is_forum = getattr(entity, 'forum', False)
                        if not is_forum:
                            all_groups.append({"id": entity.id})
                break
            except Exception as e:
                logger.error(f"Error fetching groups: {e}")
                continue
        
        removed_count = 0
        for group in all_groups:
            if db.get_target_group(uid, group["id"]):
                db.remove_target_group(uid, group["id"])
                removed_count += 1
        
        await callback_query.answer(f" Removed {removed_count} groups")
        await groups_only_mode_callback(client, callback_query)
        
    except Exception as e:
        logger.error(f"Error in groups_only_unselect_all: {e}")
        await callback_query.answer("Error. Try again.", show_alert=True)

@pyro.on_callback_query(filters.regex("^forums_only_mode$"))
async def forums_only_mode_callback(client, callback_query):
    """Handle topics only mode - show ALL topics directly from all forum groups"""
    try:
        uid = callback_query.from_user.id
        await callback_query.answer("Loading topics...")
        
        # Get topics from MongoDB cache instead of fetching every time
        cached_groups = await get_groups_from_mongo_cache(uid)
        
        # Filter only topics from forum groups
        all_topics = []
        for group in cached_groups:
            if group.get('is_forum', False) and group.get('topics'):
                for topic in group['topics']:
                    all_topics.append({
                        "id": topic.get('id'),
                        "title": topic.get('title', f'Topic {topic.get("id")}'),
                        "forum_id": group.get('id'),
                        "forum_title": group.get('title'),
                        "is_topic": True
                    })
        
        # If no topics in cache, fetch fresh
        if not all_topics:
            accounts = db.get_user_accounts(uid)
            if not accounts:
                await callback_query.message.reply_text(
                    " No accounts found. Please add an account first.",
                    parse_mode=ParseMode.HTML
                )
                return
            
            # Fetch only once
            for acc in accounts:
                tg_client = None
                try:
                    tg_client = await get_telegram_client(acc["phone_number"], acc["session_string"])
                    async for dialog in tg_client.iter_dialogs():
                        if dialog.is_group or dialog.is_channel:
                            entity = dialog.entity
                            is_forum = getattr(entity, 'forum', False)
                            if is_forum:
                                try:
                                    result = await tg_client(GetForumTopicsRequest(
                                        channel=entity,
                                        offset_date=0,
                                        offset_id=0,
                                        offset_topic=0,
                                        limit=100
                                    ))
                                    
                                    for topic in result.topics:
                                        if isinstance(topic, ForumTopic):
                                            all_topics.append({
                                                "id": topic.id,
                                                "title": getattr(topic, 'title', f'Topic {topic.id}'),
                                                "forum_id": entity.id,
                                                "forum_title": dialog.title,
                                                "is_topic": True
                                            })
                                except Exception as e:
                                    logger.error(f"Error fetching topics from {dialog.title}: {e}")
                    
                    await tg_client.disconnect()
                    logger.info(f"✓ Loaded {len(all_topics)} topics (fresh fetch)")
                    break
                except Exception as e:
                    logger.error(f"Error fetching topics: {e}")
                    if tg_client:
                        try:
                            await tg_client.disconnect()
                        except:
                            pass
                    continue
        
        if not all_topics:
            await callback_query.message.edit_text(
                "<b> No Topics Found</b>\n\n"
                "No topics found in your account.\n\n"
                "<i>Make sure you have groups with topics enabled.</i>",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("x Back to Groups Settings", callback_data="menu_groups")]
                ]),
                parse_mode=ParseMode.HTML
            )
            return
        
        # Get selected forum groups to check which topics are selected
        selected_forums = db.get_forum_groups(uid) or []
        selected_forum_ids = [f.get("group_id") for f in selected_forums]
        
        # Pagination setup - 6 per page with next/back buttons
        page = 0
        items_per_page = 6
        start_idx = page * items_per_page
        end_idx = start_idx + items_per_page
        total_pages = (len(all_topics) + items_per_page - 1) // items_per_page
        
        menu_text = (
            f"<b> TOPICS ONLY MODE</b>\n\n"
            f"Total Topics: <b>{len(all_topics)}</b>\n"
            f"Page: <b>{page + 1}/{total_pages}</b>\n\n"
            f"<i>Select topics to broadcast to.</i>"
        )
        
        buttons = [
            [
                InlineKeyboardButton("✅ Select All", callback_data="topics_select_all"),
                InlineKeyboardButton("❌ Unselect All", callback_data="topics_unselect_all")
            ]
        ]
        
        # Add individual topic buttons for current page
        for topic in all_topics[start_idx:end_idx]:
            is_selected = topic["forum_id"] in selected_forum_ids
            emoji = "✅" if is_selected else "⬜"
            buttons.append([InlineKeyboardButton(
                f"{emoji} {topic['title'][:25]} ({topic['forum_title'][:15]})",
                callback_data=f"toggle_topic_{topic['forum_id']}_{topic['id']}"
            )])
        
        # Add pagination buttons (always show both if multiple pages)
        if total_pages > 1:
            nav_buttons = []
            if page > 0:
                nav_buttons.append(InlineKeyboardButton("◀️ Back", callback_data=f"topics_page_{page - 1}"))
            if end_idx < len(all_topics):
                nav_buttons.append(InlineKeyboardButton("Next ▶️", callback_data=f"topics_page_{page + 1}"))
            if nav_buttons:
                buttons.append(nav_buttons)
        
        buttons.append([InlineKeyboardButton("← Back to Groups Settings", callback_data="menu_groups")])
        
        await callback_query.message.edit_text(
            menu_text,
            reply_markup=InlineKeyboardMarkup(buttons),
            parse_mode=ParseMode.HTML
        )
        
    except Exception as e:
        logger.error(f"Error in forums_only_mode callback: {e}")
        await callback_query.answer("Error loading forums. Try again.", show_alert=True)

@pyro.on_callback_query(filters.regex("^toggle_topic_"))
async def toggle_topic_callback(client, callback_query):
    """Toggle topic selection"""
    try:
        uid = callback_query.from_user.id
        data = callback_query.data.replace("toggle_topic_", "")
        parts = data.split("_")
        forum_id = int(parts[0])
        topic_id = int(parts[1])
        
        # Check if forum is already selected
        selected_forums = db.get_forum_groups(uid) or []
        forum_ids = [f.get("group_id") for f in selected_forums]
        
        if forum_id in forum_ids:
            # Unselect - remove forum
            db.remove_forum_group(uid, forum_id)
            await callback_query.answer("✅ Topic unselected")
        else:
            # Select - add forum
            db.add_forum_group(uid, forum_id, topic_id, f"Topic {topic_id}")
            await callback_query.answer("✅ Topic selected")
        
        # Refresh the list
        await forums_only_mode_callback(client, callback_query)
        
    except Exception as e:
        logger.error(f"Error toggling topic: {e}")
        await callback_query.answer("Error. Try again.", show_alert=True)

@pyro.on_callback_query(filters.regex("^topics_select_all$"))
async def topics_select_all_callback(client, callback_query):
    """Select all topics - uses cached data to avoid refetching"""
    try:
        uid = callback_query.from_user.id
        
        # Get topics from MongoDB cache instead of fetching
        cached_groups = await get_groups_from_mongo_cache(uid)
        
        # Filter only topics from forum groups
        all_topics = []
        for group in cached_groups:
            if group.get('is_forum', False) and group.get('topics'):
                for topic in group['topics']:
                    all_topics.append({
                        "forum_id": group.get('id'),
                        "topic_id": topic.get('id'),
                        "title": topic.get('title', f'Topic {topic.get("id")}')
                    })
        
        # If no topics in cache, just show message
        if not all_topics:
            await callback_query.answer("⚠️ No topics found in cache. Please refresh cache first.", show_alert=True)
            return
        
        # Select all topics
        for topic in all_topics:
            db.add_forum_group(uid, topic["forum_id"], topic["topic_id"], topic["title"])
        
        await callback_query.answer(f"✅ Selected all {len(all_topics)} topics")
        await forums_only_mode_callback(client, callback_query)
        
    except Exception as e:
        logger.error(f"Error in topics select all: {e}")
        await callback_query.answer("Error. Try again.", show_alert=True)

@pyro.on_callback_query(filters.regex("^topics_unselect_all$"))
async def topics_unselect_all_callback(client, callback_query):
    """Unselect all topics"""
    try:
        uid = callback_query.from_user.id
        db.db.forum_groups.delete_many({"user_id": uid})
        await callback_query.answer("✅ All topics unselected")
        await forums_only_mode_callback(client, callback_query)
    except Exception as e:
        logger.error(f"Error in topics unselect all: {e}")
        await callback_query.answer("Error. Try again.", show_alert=True)

@pyro.on_callback_query(filters.regex("^topics_page_"))
async def topics_page_callback(client, callback_query):
    """Handle topics pagination"""
    try:
        await forums_only_mode_callback(client, callback_query)
    except Exception as e:
        logger.error(f"Error in topics pagination: {e}")
        await callback_query.answer("Error. Try again.", show_alert=True)

@pyro.on_callback_query(filters.regex(r"^forums_page_(\d+)$"))
async def forums_page_callback(client, callback_query):
    """Handle forums pagination"""
    try:
        uid = callback_query.from_user.id
        page = int(callback_query.data.split("_")[-1])
        
        # Use cached forums - NO fetching!
        cached_data = db.get_cached_groups(uid)
        forum_groups = cached_data.get("forums", []) if cached_data else []
        
        if not forum_groups:
            await callback_query.answer(" No cached forums. Please go back and reload.", show_alert=True)
            return
        
        # Forums already loaded from cache, no fetching needed
        
        # Pagination setup
        items_per_page = 10
        start_idx = page * items_per_page
        end_idx = start_idx + items_per_page
        total_pages = (len(forum_groups) + items_per_page - 1) // items_per_page
        
        menu_text = (
            f"<b> FORUMS ONLY MODE</b>\n\n"
            f"Total Forum Groups: <b>{len(forum_groups)}</b>\n"
            f"Page: <b>{page + 1}/{total_pages}</b>\n\n"
            f"<i>Click on a forum to view and select topics.</i>"
        )
        
        buttons = [
            [InlineKeyboardButton("? Search Topics", callback_data="search_forum_topics")]
        ]
        
        # Add forum group buttons for current page
        for forum in forum_groups[start_idx:end_idx]:
            buttons.append([InlineKeyboardButton(
                f" {forum['title'][:30]}",
                callback_data=f"view_forum_topics_{forum['id']}"
            )])
        
        # Add pagination buttons
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("< Back", callback_data=f"forums_page_{page - 1}"))
        if end_idx < len(forum_groups):
            nav_buttons.append(InlineKeyboardButton(">", callback_data=f"forums_page_{page + 1}"))
        
        if nav_buttons:
            buttons.append(nav_buttons)
        
        buttons.append([InlineKeyboardButton("x Back to Groups Settings", callback_data="menu_groups")])
        
        await callback_query.message.edit_text(
            menu_text,
            reply_markup=InlineKeyboardMarkup(buttons),
            parse_mode=ParseMode.HTML
        )
        
    except Exception as e:
        logger.error(f"Error in forums_page callback: {e}")
        await callback_query.answer("Error loading page. Try again.", show_alert=True)

@pyro.on_callback_query(filters.regex("^view_forum_topics_(.+)$"))
async def view_forum_topics_callback(client, callback_query):
    """View topics in a specific forum group"""
    try:
        uid = callback_query.from_user.id
        forum_id = int(callback_query.data.split("_")[-1])
        await callback_query.answer("Loading topics...")
        
        # Get forum entity and fetch topics
        accounts = db.get_user_accounts(uid)
        forum_title = "Forum"
        topics = []
        
        for acc in accounts:
            tg_client = None
            try:
                tg_client = await get_telegram_client(acc["phone_number"], acc["session_string"])
                
                # Use PeerChannel instead of raw ID to avoid PeerUser confusion
                entity = await tg_client.get_entity(PeerChannel(abs(forum_id)))
                forum_title = entity.title
                
                # Fetch topics
                result = await tg_client(GetForumTopicsRequest(
                    channel=entity,
                    offset_date=0,
                    offset_id=0,
                    offset_topic=0,
                    limit=100
                ))
                
                for topic in result.topics:
                    if isinstance(topic, ForumTopic):
                        topics.append({
                            "id": topic.id,
                            "title": getattr(topic, 'title', f'Topic {topic.id}'),
                            "forum_id": forum_id
                        })
                
                await tg_client.disconnect()
                logger.info(f"✓ Loaded {len(topics)} topics from {forum_title}")
                break
            except Exception as e:
                logger.error(f"Error fetching topics: {e}")
                if tg_client:
                    try:
                        await tg_client.disconnect()
                    except:
                        pass
                continue
        
        if not topics:
            await callback_query.message.edit_text(
                f"<b> No Topics Found</b>\n\n"
                f"Forum: <b>{forum_title}</b>\n\n"
                f"No topics found in this forum.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("x Back to Forums", callback_data="forums_only_mode")]
                ]),
                parse_mode=ParseMode.HTML
            )
            return
        
        # Get selected forum groups
        selected_forum_ids = [f["id"] for f in db.get_forum_groups(uid)]
        is_forum_selected = forum_id in selected_forum_ids
        
        menu_text = (
            f"<b> {forum_title}</b>\n\n"
            f"Total Topics: <b>{len(topics)}</b>\n"
            f"Forum Status: {' Selected' if is_forum_selected else ' Not Selected'}\n\n"
            f"<i>Select topics to broadcast to:</i>"
        )
        
        buttons = [
            [
                InlineKeyboardButton("+ Select All Topics", callback_data=f"forum_select_all_{forum_id}"),
                InlineKeyboardButton("- Unselect All", callback_data=f"forum_unselect_all_{forum_id}")
            ],
            [InlineKeyboardButton("x Back to Forums", callback_data="forums_only_mode")]
        ]
        
        # Note: Topic selection would require database changes to store selected topics
        # For now, selecting forum = selecting all its topics
        
        await callback_query.message.edit_text(
            menu_text,
            reply_markup=InlineKeyboardMarkup(buttons),
            parse_mode=ParseMode.HTML
        )
        
    except Exception as e:
        logger.error(f"Error in view_forum_topics callback: {e}")
        await callback_query.answer("Error loading topics. Try again.", show_alert=True)

@pyro.on_callback_query(filters.regex("^menu_groups_mode$"))
async def menu_groups_mode_callback(client, callback_query):
    """Handle groups mode menu"""
    try:
        uid = callback_query.from_user.id
        await callback_query.answer()
        
        # Get current broadcast mode
        current_mode = db.get_broadcast_mode(uid)
        
        mode_display = {
            "groups_only": " Groups Only",
            "forums_only": " Topics Only",
            "both": " Both Groups & Topics"
        }
        
        menu_text = (
            f"<b> GROUPS MODE</b>\n\n"
            f"Current Mode: <b>{mode_display.get(current_mode, 'Both')}</b>👥\n\n"
            f"<b>Modes:</b>\n"
            f"• <b>Groups Only:</b> Broadcast to regular groups only\n"
            f"• <b>Topics Only:</b> Broadcast to topics only\n"
            f"• <b>Both:</b> Broadcast to groups and topics\n\n"
            f"<i>Select a mode below:</i>"
        )
        
        buttons = [
            [InlineKeyboardButton(
                f"{' ' if current_mode == 'groups_only' else ''} Groups Only 👥",
                callback_data="set_broadcast_mode_groups_only"
            )],
            [InlineKeyboardButton(
                f"{' ' if current_mode == 'forums_only' else ''} Topics Only 🗂️",
                callback_data="set_broadcast_mode_forums_only"
            )],
            [InlineKeyboardButton(
                f"{' ' if current_mode == 'both' else ''} Both Groups & Topics 🔀",
                callback_data="set_broadcast_mode_both"
            )],
            [InlineKeyboardButton("←", callback_data="menu_broadcast")]
        ]
        
        await callback_query.message.edit_text(
            menu_text,
            reply_markup=InlineKeyboardMarkup(buttons),
            parse_mode=ParseMode.HTML
        )
        
    except Exception as e:
        logger.error(f"Error in menu_groups_mode callback: {e}")
        await callback_query.answer("Error loading modes. Try again.", show_alert=True)

@pyro.on_callback_query(filters.regex("^set_broadcast_mode_(groups_only|forums_only|both)$"))
async def set_broadcast_mode_callback(client, callback_query):
    """Set broadcast mode (groups_only, forums_only, or both)"""
    try:
        uid = callback_query.from_user.id
        mode = callback_query.data.replace("set_broadcast_mode_", "")
        
        # Set the broadcast mode
        db.set_broadcast_mode(uid, mode)
        
        # FIX 8 & 9: Set optimal timing for Topics Only mode
        if mode == "forums_only":
            # Set cycle interval to 3600s (1 hour) for Topics Only
            db.set_user_ad_delay(uid, 3600)
            # Set group message delay to 10s for Topics Only
            if hasattr(db, 'set_user_group_msg_delay'):
                db.set_user_group_msg_delay(uid, 10)
            logger.info(f"[TOPICS MODE] Set cycle interval=3600s, message delay=10s for user {uid}")
        
        mode_names = {
            "groups_only": "Groups Only",
            "forums_only": "Topics Only",
            "both": "Both Groups & Topics"
        }
        
        await callback_query.answer(f"✅ Mode set to: {mode_names.get(mode, mode)}")
        
        # Refresh the menu to show updated selection
        await menu_groups_mode_callback(client, callback_query)
        
    except Exception as e:
        logger.error(f"Error in set_broadcast_mode callback: {e}")
        await callback_query.answer("Error setting mode. Try again.", show_alert=True)

@pyro.on_callback_query(filters.regex("^both_mode_groups$"))
async def both_mode_groups_callback(client, callback_query):
    """Show groups in both mode"""
    await groups_only_mode_callback(client, callback_query)

@pyro.on_callback_query(filters.regex("^both_mode_topics$"))
async def both_mode_topics_callback(client, callback_query):
    """Show topics in both mode"""
    try:
        uid = callback_query.from_user.id
        await callback_query.answer("Loading all topics...")
        
        # Fetch all forum groups and their topics
        accounts = db.get_user_accounts(uid)
        if not accounts:
            await callback_query.message.reply_text(
                " No accounts found. Please add an account first.",
                parse_mode=ParseMode.HTML
            )
            return
        
        all_topics = []
        for acc in accounts:
            tg_client = None
            try:
                tg_client = await get_telegram_client(acc["phone_number"], acc["session_string"])
                async for dialog in tg_client.iter_dialogs():
                    if dialog.is_group or dialog.is_channel:
                        entity = dialog.entity
                        is_forum = getattr(entity, 'forum', False)
                        if is_forum:
                            # Fetch topics for this forum
                            try:
                                result = await tg_client(GetForumTopicsRequest(
                                    channel=entity,
                                    offset_date=0,
                                    offset_id=0,
                                    offset_topic=0,
                                    limit=100
                                ))
                                
                                for topic in result.topics:
                                    if isinstance(topic, ForumTopic):
                                        all_topics.append({
                                            "id": topic.id,
                                            "title": getattr(topic, 'title', f'Topic {topic.id}'),
                                            "forum_id": entity.id,
                                            "forum_title": dialog.title
                                        })
                            except Exception as e:
                                logger.error(f"Error fetching topics from {dialog.title}: {e}")
                
                await tg_client.disconnect()
                logger.info(f"✓ Loaded {len(all_topics)} topics from all forums")
                break
            except Exception as e:
                logger.error(f"Error fetching forums: {e}")
                if tg_client:
                    try:
                        await tg_client.disconnect()
                    except:
                        pass
                continue
        
        if not all_topics:
            await callback_query.message.edit_text(
                "<b> No Topics Found</b>\n\n"
                "No topics found in your account.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("x Back", callback_data="both_groups_topics_mode")]
                ]),
                parse_mode=ParseMode.HTML
            )
            return
        
        # Pagination setup
        page = 0  # Default to first page
        items_per_page = 10
        start_idx = page * items_per_page
        end_idx = start_idx + items_per_page
        total_pages = (len(all_topics) + items_per_page - 1) // items_per_page
        
        menu_text = (
            f"<b> ALL FORUM TOPICS</b>\n\n"
            f"Total Topics: <b>{len(all_topics)}</b>\n"
            f"Page: <b>{page + 1}/{total_pages}</b>\n\n"
            f"<i>Topics from all forum groups:</i>"
        )
        
        buttons = [
            [
                InlineKeyboardButton("+ Select All", callback_data="topics_select_all"),
                InlineKeyboardButton("- Unselect All", callback_data="topics_unselect_all")
            ],
            [InlineKeyboardButton("? Search Topics", callback_data="search_all_topics")]
        ]
        
        # Show topics for current page
        for topic in all_topics[start_idx:end_idx]:
            buttons.append([InlineKeyboardButton(
                f" {topic['forum_title'][:15]} > {topic['title'][:20]}",
                callback_data=f"toggle_topic_{topic['forum_id']}_{topic['id']}"
            )])
        
        # Add pagination buttons
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("< Back", callback_data=f"topics_page_{page - 1}"))
        if end_idx < len(all_topics):
            nav_buttons.append(InlineKeyboardButton(">", callback_data=f"topics_page_{page + 1}"))
        
        if nav_buttons:
            buttons.append(nav_buttons)
        
        buttons.append([InlineKeyboardButton("x Back", callback_data="both_groups_topics_mode")])
        
        await callback_query.message.edit_text(
            menu_text,
            reply_markup=InlineKeyboardMarkup(buttons),
            parse_mode=ParseMode.HTML
        )
        
    except Exception as e:
        logger.error(f"Error in both_mode_topics callback: {e}")
        await callback_query.answer("Error loading topics. Try again.", show_alert=True)

@pyro.on_callback_query(filters.regex(r"^topics_page_(\d+)$"))
async def topics_page_callback(client, callback_query):
    """Handle topics pagination"""
    try:
        uid = callback_query.from_user.id
        page = int(callback_query.data.split("_")[-1])
        
        # Use cached topics - NO fetching!
        cached_data = db.get_cached_groups(uid)
        all_topics = cached_data.get("topics", []) if cached_data else []
        
        if not all_topics:
            await callback_query.answer(" No cached topics. Please go back and reload.", show_alert=True)
            return
        
        # Topics already loaded from cache, no fetching needed
        
        # Pagination setup
        items_per_page = 10
        start_idx = page * items_per_page
        end_idx = start_idx + items_per_page
        total_pages = (len(all_topics) + items_per_page - 1) // items_per_page
        
        menu_text = (
            f"<b> ALL FORUM TOPICS</b>\n\n"
            f"Total Topics: <b>{len(all_topics)}</b>\n"
            f"Page: <b>{page + 1}/{total_pages}</b>\n\n"
            f"<i>Topics from all forum groups:</i>"
        )
        
        buttons = [
            [
                InlineKeyboardButton("+ Select All", callback_data="topics_select_all"),
                InlineKeyboardButton("- Unselect All", callback_data="topics_unselect_all")
            ],
            [InlineKeyboardButton("? Search Topics", callback_data="search_all_topics")]
        ]
        
        # Show topics for current page
        for topic in all_topics[start_idx:end_idx]:
            buttons.append([InlineKeyboardButton(
                f" {topic['forum_title'][:15]} > {topic['title'][:20]}",
                callback_data=f"toggle_topic_{topic['forum_id']}_{topic['id']}"
            )])
        
        # Add pagination buttons
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("< Back", callback_data=f"topics_page_{page - 1}"))
        if end_idx < len(all_topics):
            nav_buttons.append(InlineKeyboardButton(">", callback_data=f"topics_page_{page + 1}"))
        
        if nav_buttons:
            buttons.append(nav_buttons)
        
        buttons.append([InlineKeyboardButton("x Back", callback_data="both_groups_topics_mode")])
        
        await callback_query.message.edit_text(
            menu_text,
            reply_markup=InlineKeyboardMarkup(buttons),
            parse_mode=ParseMode.HTML
        )
        
    except Exception as e:
        logger.error(f"Error in topics_page callback: {e}")
        await callback_query.answer("Error loading page. Try again.", show_alert=True)

@pyro.on_callback_query(filters.regex("^search_all_topics$"))
async def search_all_topics_callback(client, callback_query):
    """Handle search all topics"""
    try:
        uid = callback_query.from_user.id
        await callback_query.answer()
        
        # Set user state to wait for topic search keyword
        db.set_user_state(uid, "awaiting_topic_search")
        
        await callback_query.message.reply_text(
            "<b>🔍 Search Topics</b>\n\n"
            "Send a keyword to filter topics by name.\n\n"
            "<i>Example:</i> <code>general</code>\n\n"
            "Send /cancel to cancel search.",
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        logger.error(f"Error in search_all_topics callback: {e}")
        await callback_query.answer("Error. Try again.", show_alert=True)

@pyro.on_callback_query(filters.regex("^search_forum_topics$"))
async def search_forum_topics_callback(client, callback_query):
    """Handle search forum topics"""
    try:
        uid = callback_query.from_user.id
        await callback_query.answer()
        
        # Set user state to wait for topic search keyword
        db.set_user_state(uid, "awaiting_forum_topic_search")
        
        await callback_query.message.reply_text(
            "<b>🔍 Search Forum Topics</b>\n\n"
            "Send a keyword to filter topics across all forums.\n\n"
            "<i>Example:</i> <code>discussion</code>\n\n"
            "Send /cancel to cancel search.",
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        logger.error(f"Error in search_forum_topics callback: {e}")
        await callback_query.answer("Error. Try again.", show_alert=True)

@pyro.on_callback_query(filters.regex("^menu_ads_forward_mode$"))
async def menu_ads_forward_mode_callback(client, callback_query):
    """Handle ads forward mode menu"""
    try:
        uid = callback_query.from_user.id
        await callback_query.answer()
        
        # Get current message source
        post_link_data = db.get_user_post_link(uid)
        if post_link_data and post_link_data.get("message_source") == "post_link":
            current_mode = "post_link"
            post_link = post_link_data.get("post_link", "Not Set")
        else:
            current_mode = "saved_messages"
            post_link = "Not Set"
        
        # Build menu text - hide post link line when in saved messages mode
        menu_text = f"<b> ADS FORWARD MODE</b>\n\n"
        menu_text += f"Current Mode: <b>{' Post Link' if current_mode == 'post_link' else ' Saved Messages'}</b>\n"
        
        # Only show post link if in post_link mode
        if current_mode == 'post_link':
            menu_text += f"Post Link: <code>{post_link}</code>\n"
        
        menu_text += (
            f"\n<b>Modes:</b>\n"
            f"• <b>Saved Messages:</b> Forward from your Saved Messages\n"
            f"• <b>Post Link:</b> Forward from a specific Telegram post\n\n"
            f"<i>Select a mode below:</i>"
        )
        
        buttons = [
            [InlineKeyboardButton(
                f"{' ' if current_mode == 'saved_messages' else ''} Saved Messages",
                callback_data="set_forward_mode_saved_messages"
            )],
            [InlineKeyboardButton(
                f"{' ' if current_mode == 'post_link' else ''} Post Link",
                callback_data="set_forward_mode_post_link"
            )],
            [InlineKeyboardButton("←", callback_data="menu_broadcast")]
        ]
        
        await callback_query.message.edit_text(
            menu_text,
            reply_markup=InlineKeyboardMarkup(buttons),
            parse_mode=ParseMode.HTML
        )
        
    except Exception as e:
        logger.error(f"Error in menu_ads_forward_mode callback: {e}")
        await callback_query.answer("Error loading forward modes. Try again.", show_alert=True)

@pyro.on_callback_query(filters.regex("^set_forward_mode_(.+)$"))
async def set_forward_mode_callback(client, callback_query):
    """Set ads forward mode"""
    try:
        uid = callback_query.from_user.id
        mode = callback_query.data.replace("set_forward_mode_", "")
        
        if mode == "saved_messages":
            # Switch to saved messages - keep post link data, just change mode
            post_link_data = db.get_user_post_link(uid)
            if post_link_data:
                # Update only the message_source, preserve post_link data
                db.db.users.update_one(
                    {"user_id": uid},
                    {"$set": {"message_source": "saved_messages"}}
                )
            await callback_query.answer(" Switched to Saved Messages")
        elif mode == "post_link":
            # Check if post link is set
            post_link_data = db.get_user_post_link(uid)
            if post_link_data and post_link_data.get("post_link"):
                # Re-enable post link mode
                db.db.users.update_one(
                    {"user_id": uid},
                    {"$set": {"message_source": "post_link"}}
                )
                await callback_query.answer(" Switched to Post Link")
            else:
                await callback_query.answer(" No post link set! Set one first.", show_alert=True)
                # Redirect to post link management
                await callback_query.message.edit_text(
                    "<b> No Post Link Set</b>\n\n"
                    "Please set a post link first before using Post Link mode.",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("≈ Set Post Link", callback_data="menu_post_link")],
                        [InlineKeyboardButton("x Back", callback_data="menu_ads_forward_mode")]
                    ]),
                    parse_mode=ParseMode.HTML
                )
                return
        
        # Refresh menu
        await menu_ads_forward_mode_callback(client, callback_query)
        
    except Exception as e:
        logger.error(f"Error in set_forward_mode callback: {e}")
        await callback_query.answer("Error setting mode. Try again.", show_alert=True)

@pyro.on_callback_query(filters.regex("^menu_interval_management$"))
async def menu_interval_management_callback(client, callback_query):
    """Handle interval management menu"""
    try:
        uid = callback_query.from_user.id
        await callback_query.answer()
        
        # Get current settings
        user = db.get_user(uid)
        cycle_interval = user.get("ad_delay", 300)
        cycle_timeout = user.get("cycle_timeout", 900)
        group_msg_delay = user.get("group_msg_delay", 15)
        
        menu_text = (
            f"<b> INTERVAL MANAGEMENT</b>\n\n"
            f"<b>Current Settings:</b>\n"
            f"• Cycle Interval: <b>{cycle_interval}s</b>\n"
            f"• Cycle Timeout: <b>{cycle_timeout//60}min</b>\n"
            f"• Group Message Delay: <b>{group_msg_delay}s</b>\n\n"
            f"<b>ℹ️ About:</b>\n"
            f"• <b>Cycle Interval:</b> Time between broadcast cycles\n"
            f"• <b>Cycle Timeout:</b> Maximum time for one cycle\n"
            f"• <b>Group Message Delay:</b> Delay between messages to groups\n\n"
            f"<i>Click below to change settings:</i>"
        )
        
        buttons = [
            [InlineKeyboardButton("⏱ Set Cycle Interval", callback_data="set_ad_delay")],
            [InlineKeyboardButton("⏱ Set Cycle Timeout", callback_data="set_cycle_timeout")],
            [InlineKeyboardButton("⏱ Set Group Message Delay", callback_data="set_group_delay")],
            [InlineKeyboardButton("←", callback_data="menu_main")]
        ]
        
        await callback_query.message.edit_text(
            menu_text,
            reply_markup=InlineKeyboardMarkup(buttons),
            parse_mode=ParseMode.HTML
        )
        
    except Exception as e:
        logger.error(f"Error in menu_interval_management callback: {e}")
        await callback_query.answer("Error loading interval management menu. Try again.", show_alert=True)

@pyro.on_callback_query(filters.regex("^menu_saved_messages$"))
async def menu_saved_messages_callback(client, callback_query):
    """Handle saved message management menu"""
    try:
        uid = callback_query.from_user.id
        await callback_query.answer()
        
        # Get current saved messages count
        saved_msg_count = db.get_user_saved_messages_count(uid)
        
        menu_text = (
            f"<b> SAVED MESSAGE MANAGEMENT</b>\n\n"
            f"Current Messages Count: <b>{saved_msg_count}</b>\n\n"
            f"<b>ℹ️ About:</b>\n"
            f"The bot will rotate through the last <b>{saved_msg_count}</b> messages from your Saved Messages.\n\n"
            f"<i>Change the count below:</i>"
        )
        
        buttons = [
            [InlineKeyboardButton("★ Select Saved Messages Count", callback_data="select_saved_messages_count")],
            [InlineKeyboardButton("←", callback_data="menu_main")]
        ]
        
        await callback_query.message.edit_text(
            menu_text,
            reply_markup=InlineKeyboardMarkup(buttons),
            parse_mode=ParseMode.HTML
        )
        
    except Exception as e:
        logger.error(f"Error in menu_saved_messages callback: {e}")
        await callback_query.answer("Error loading saved messages menu. Try again.", show_alert=True)

@pyro.on_callback_query(filters.regex("^search_groups$"))
async def search_groups_callback(client, callback_query):
    """Handle search groups callback"""
    try:
        uid = callback_query.from_user.id
        await callback_query.answer()
        
        # Set user state to wait for search keyword
        db.set_user_state(uid, "awaiting_group_search")
        
        await callback_query.message.reply_text(
            "<b>🔍 Search Groups</b>\n\n"
            "Send a keyword to filter groups by name.\n\n"
            "<i>Example:</i> <code>crypto</code>\n\n"
            "Send /cancel to cancel search.",
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        logger.error(f"Error in search_groups callback: {e}")
        await callback_query.answer("Error initiating search. Try again.", show_alert=True)

@pyro.on_callback_query(filters.regex("^clear_search_filter$"))
async def clear_search_filter_callback(client, callback_query):
    """Handle clear search filter callback"""
    try:
        uid = callback_query.from_user.id
        await callback_query.answer("Search filter cleared")
        
        # Clear search filter
        db.clear_group_search_filter(uid)
        
        # Redirect back to group selection (would need to refresh UI)
        await callback_query.message.edit_text(
            "<b> Search Filter Cleared</b>\n\n"
            "Showing all groups again.",
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        logger.error(f"Error in clear_search_filter callback: {e}")
        await callback_query.answer("Error clearing filter. Try again.", show_alert=True)

@pyro.on_callback_query(filters.regex("^add_forums_only$"))
async def add_forums_only_callback(client, callback_query):
    """Handle add forums only callback"""
    try:
        uid = callback_query.from_user.id
        await callback_query.answer("Fetching groups and adding forums...")
        
        user = db.db.users.find_one({"user_id": uid})
        forum_only_mode = user.get("forum_only_mode", False) if user else False
        
        # Fetch fresh groups from user's accounts
        accounts = db.get_user_accounts(uid)
        if not accounts:
            await callback_query.message.reply_text(
                " No accounts found. Please add an account first.",
                parse_mode=ParseMode.HTML
            )
            return
        
        # Get groups from first account (fresh fetch)
        all_groups = []
        for acc in accounts:
            try:
                tg_client = await get_telegram_client(acc["phone_number"], acc["session_string"])
                async for dialog in tg_client.iter_dialogs():
                    if dialog.is_group or dialog.is_channel:
                        entity = dialog.entity
                        group_data = {
                            "id": entity.id,
                            "title": dialog.title,
                            "is_forum": getattr(entity, 'forum', False)
                        }
                        all_groups.append(group_data)
                break  # Use first account only
            except Exception as e:
                logger.error(f"Error fetching groups from account: {e}")
                continue
        
        if not all_groups:
            await callback_query.message.reply_text(
                " No groups found in your accounts.",
                parse_mode=ParseMode.HTML
            )
            return
        
        # Apply search filter if exists
        search_filter = db.get_group_search_filter(uid)
        filtered_groups = filter_groups_by_keyword(all_groups, search_filter)
        
        # Bulk add forums only
        added_count = bulk_select_forums_only(uid, filtered_groups, forum_only_mode)
        
        await callback_query.message.reply_text(
            f"<b> Added {added_count} Forum Groups</b>\n\n"
            f"Fetched fresh from your Telegram account.",
            parse_mode=ParseMode.HTML
        )
        
    except Exception as e:
        logger.error(f"Error in add_forums_only callback: {e}")
        await callback_query.answer("Error adding forums. Try again.", show_alert=True)

@pyro.on_callback_query(filters.regex("^add_all_groups_bulk$"))
async def add_all_groups_bulk_callback(client, callback_query):
    """Handle add all groups (bulk) callback"""
    try:
        uid = callback_query.from_user.id
        await callback_query.answer("Fetching groups and adding all...")
        
        user = db.db.users.find_one({"user_id": uid})
        forum_only_mode = user.get("forum_only_mode", False) if user else False
        
        # Fetch fresh groups from user's accounts
        accounts = db.get_user_accounts(uid)
        if not accounts:
            await callback_query.message.reply_text(
                " No accounts found. Please add an account first.",
                parse_mode=ParseMode.HTML
            )
            return
        
        # Get groups from first account (fresh fetch)
        all_groups = []
        for acc in accounts:
            try:
                tg_client = await get_telegram_client(acc["phone_number"], acc["session_string"])
                async for dialog in tg_client.iter_dialogs():
                    if dialog.is_group or dialog.is_channel:
                        entity = dialog.entity
                        group_data = {
                            "id": entity.id,
                            "title": dialog.title,
                            "is_forum": getattr(entity, 'forum', False)
                        }
                        all_groups.append(group_data)
                break  # Use first account only
            except Exception as e:
                logger.error(f"Error fetching groups from account: {e}")
                continue
        
        if not all_groups:
            await callback_query.message.reply_text(
                " No groups found in your accounts.",
                parse_mode=ParseMode.HTML
            )
            return
        
        # Apply search filter if exists
        search_filter = db.get_group_search_filter(uid)
        filtered_groups = filter_groups_by_keyword(all_groups, search_filter)
        
        # Bulk add all groups
        added_count = bulk_select_all_groups(uid, filtered_groups, forum_only_mode)
        
        filter_text = f" (filtered by '{search_filter}')" if search_filter else ""
        await callback_query.message.reply_text(
            f"<b> Added {added_count} Groups</b>{filter_text}\n\n"
            f"Fetched fresh from your Telegram account.",
            parse_mode=ParseMode.HTML
        )
        
    except Exception as e:
        logger.error(f"Error in add_all_groups_bulk callback: {e}")
        await callback_query.answer("Error adding groups. Try again.", show_alert=True)

@pyro.on_callback_query(filters.regex("^unselect_all_filtered$"))
async def unselect_all_filtered_callback(client, callback_query):
    """Handle unselect all filtered groups callback"""
    try:
        uid = callback_query.from_user.id
        await callback_query.answer("Fetching groups and removing filtered...")
        
        user = db.db.users.find_one({"user_id": uid})
        forum_only_mode = user.get("forum_only_mode", False) if user else False
        
        # Fetch fresh groups from user's accounts
        accounts = db.get_user_accounts(uid)
        if not accounts:
            await callback_query.message.reply_text(
                " No accounts found. Please add an account first.",
                parse_mode=ParseMode.HTML
            )
            return
        
        # Get groups from first account (fresh fetch)
        all_groups = []
        for acc in accounts:
            try:
                tg_client = await get_telegram_client(acc["phone_number"], acc["session_string"])
                async for dialog in tg_client.iter_dialogs():
                    if dialog.is_group or dialog.is_channel:
                        entity = dialog.entity
                        group_data = {
                            "id": entity.id,
                            "title": dialog.title,
                            "is_forum": getattr(entity, 'forum', False)
                        }
                        all_groups.append(group_data)
                break  # Use first account only
            except Exception as e:
                logger.error(f"Error fetching groups from account: {e}")
                continue
        
        if not all_groups:
            await callback_query.message.reply_text(
                " No groups found in your accounts.",
                parse_mode=ParseMode.HTML
            )
            return
        
        # Apply search filter if exists
        search_filter = db.get_group_search_filter(uid)
        filtered_groups = filter_groups_by_keyword(all_groups, search_filter)
        
        # Bulk unselect filtered groups
        removed_count = bulk_unselect_all(uid, filtered_groups, forum_only_mode)
        
        filter_text = f" (filtered by '{search_filter}')" if search_filter else ""
        await callback_query.message.reply_text(
            f"<b> Removed {removed_count} Groups</b>{filter_text}\n\n"
            f"Fetched fresh from your Telegram account.",
            parse_mode=ParseMode.HTML
        )
        
    except Exception as e:
        logger.error(f"Error in unselect_all_filtered callback: {e}")
        await callback_query.answer("Error removing groups. Try again.", show_alert=True)

@pyro.on_callback_query(filters.regex("^unselect_all_groups$"))
async def unselect_all_groups_callback(client, callback_query):
    """Handle unselect all groups callback"""
    try:
        uid = callback_query.from_user.id
        
        user = db.db.users.find_one({"user_id": uid})
        forum_only_mode = user.get("forum_only_mode", False) if user else False
        
        if forum_only_mode:
            selected_groups = db.get_forum_groups(uid) or []
        else:
            selected_groups = db.get_target_groups(uid) or []
        
        for group in selected_groups:
            if forum_only_mode:
                db.remove_forum_group(uid, group['group_id'])
            else:
                db.remove_target_group(uid, group['group_id'])
        
        await callback_query.answer("All groups unselected ", show_alert=True)

        callback_query.data = "groups_menu"
        await groups_menu_callback(client, callback_query)

    except Exception as e:
        logger.error(f"Error in unselect all groups callback: {e}")
        await callback_query.answer("Error unselecting groups. Try again.", show_alert=True)

# =======================================================
#  🔐 JOIN VERIFICATION FUNCTIONS
# =======================================================
@pyro.on_callback_query(filters.regex("joined_check"))
async def joined_check_callback(client, callback_query):
    """Handle joined check callback with instant Telethon verification using bot as admin"""
    try:
        uid = callback_query.from_user.id
        
        await callback_query.answer("🔍 Verifying your membership...", show_alert=False)
        
        logger.info(f"🔍 Starting instant join verification for user {uid}")
        
        if not telethon_bot.is_connected():
            await telethon_bot.connect()
            await telethon_bot.start(bot_token=config.BOT_TOKEN)
        
        is_joined = await verify_all_joins(
            telethon_bot,
            uid,
            config.MUST_JOIN_CHANNEL,
            config.MUSTJOIN_GROUP
        )
        
        if not is_joined:
            missing = []
            channel_check = await instant_join_check(telethon_bot, uid, config.MUST_JOIN_CHANNEL)
            group_check = await instant_join_check(telethon_bot, uid, config.MUSTJOIN_GROUP)
            
            if not channel_check:
                missing.append("📢 Channel")
            if not group_check:
                missing.append(" Group")
                
            msg = f" You haven't joined:\n\n{chr(10).join(missing)}\n\nPlease join and try again!"
            await callback_query.answer(msg, show_alert=True)
            logger.info(f" User {uid} failed join check: missing {', '.join(missing)}")
            return
        
        logger.info(f" User {uid} passed instant join verification!")
        await callback_query.answer(" Verification successful! Welcome!", show_alert=False)
        
        await callback_query.message.delete()
        
        from types import SimpleNamespace
        mock_message = SimpleNamespace(
            chat=callback_query.message.chat,
            from_user=callback_query.from_user,
            reply_photo=callback_query.message.reply_photo,
            reply_text=callback_query.message.reply_text
        )
        await start_command(client, mock_message)
        
    except Exception as e:
        logger.error(f"Error in joined check callback: {e}")
        await callback_query.answer(" Error verifying. Please try again.", show_alert=True)

@pyro.on_callback_query(filters.regex("back_to_start"))
async def back_to_start_callback(client, callback_query):
    """Handle back to start callback"""
    try:
        await callback_query.message.delete()
        await start_command(client, callback_query.message)
        logger.info(f"User {callback_query.from_user.id} went back to start")
    except Exception as e:
        logger.error(f"Error in back to start callback: {e}")

@pyro.on_callback_query(filters.regex("^menu_main$|^menu_broadcast$|^menu_login$|^menu_groups$|^menu_settings$"))
async def menu_callback(client, callback_query):
    """Handle all menu callbacks (cleaned, safe, and optimized)"""
    try:
        uid = callback_query.from_user.id
        menu_type = callback_query.data
        db.update_user_last_interaction(uid)

        if hasattr(menu_callback, 'preload_task'):
            try:
                menu_callback.preload_task.cancel()
            except Exception:
                pass

        accounts = db.get_user_accounts(uid) or []
        accounts_count = len(accounts)

        ad_msg_status = "Auto (From Saved Messages) "

        broadcast_state = db.get_broadcast_state(uid) or {}
        is_running = broadcast_state.get("running", False)
        broadcast_status = "Running " if is_running else "Stopped "

        current_delay = db.get_user_ad_delay(uid) or 600
        group_msg_delay = (
            db.get_user_group_msg_delay(uid)
            if hasattr(db, 'get_user_group_msg_delay')
            else 30
        )
        cycle_timeout = (
            db.get_user_cycle_timeout(uid)
            if hasattr(db, 'get_user_cycle_timeout')
            else 900
        )

        user = db.get_user(uid)
        account_limit = user.get('accounts_limit', 1) if user else 1
        forum_only_mode = user.get("forum_only_mode", False) if user else False
        groups_label = "Forum Groups" if forum_only_mode else "Target Groups"
        
        if forum_only_mode:
            target_groups_count = len(db.get_forum_groups(uid) or [])
        else:
            target_groups_count = len(db.get_target_groups(uid) or [])
        
        # Get broadcast mode and message source
        broadcast_mode = db.get_broadcast_mode(uid)
        broadcast_mode_display = {
            "groups_only": " Groups Only",
            "forums_only": " Topics Only",
            "both": " Both Groups & Topics"
        }
        
        # Get message source (post link or saved messages)
        post_link_data = db.get_user_post_link(uid)
        if post_link_data and post_link_data.get("message_source") == "post_link":
            message_source = " Post Link"
        else:
            message_source = " Saved Messages"
        
        # Calculate groups, forums, and topics count
        total_groups_count = 0
        groups_only_count = 0
        forums_only_count = 0
        topics_count = 0
        
        accounts = db.get_user_accounts(uid)
        if accounts:
            for acc in accounts:
                try:
                    tg_client = await get_telegram_client(acc["phone_number"], acc["session_string"])
                    async for dialog in tg_client.iter_dialogs():
                        if dialog.is_group or dialog.is_channel:
                            entity = dialog.entity
                            is_forum = getattr(entity, 'forum', False)
                            total_groups_count += 1
                            
                            if is_forum:
                                forums_only_count += 1
                                # Count topics in this forum
                                try:
                                    result = await tg_client(GetForumTopicsRequest(
                                        channel=entity,
                                        offset_date=0,
                                        offset_id=0,
                                        offset_topic=0,
                                        limit=100
                                    ))
                                    topics_count += len(result.topics)
                                except:
                                    pass
                            else:
                                groups_only_count += 1
                    
                    await tg_client.disconnect()
                    break
                except Exception as e:
                    logger.error(f"Error counting groups: {e}")
        
        status_info = (
            f"\n★ ACCOUNTS STATUS\n"
            f"  → Active Accounts: {accounts_count}/{account_limit}\n\n"
            f"★ BROADCAST STATUS\n"
            f"  → Message Mode: {message_source}\n"
            f"  → Broadcast State: {broadcast_status}\n"
            f"  → Cycle Interval: {current_delay}s\n"
            f"  → Cycle Timeout: {cycle_timeout//60}min\n"
            f"  → Message Delay: {group_msg_delay}s\n\n"
            f"★ GROUPS SETTINGS\n"
            f"  → Broadcast Mode: {broadcast_mode_display.get(broadcast_mode, 'Both')}\n"
            f"  → Total Groups: {total_groups_count}\n"
            f"  → Groups Only: {groups_only_count}\n"
            f"  → Forums Only: {forums_only_count}\n"
            f"  → Topics: {topics_count}"
        )

        if accounts_count == 0:
            account_button_text = " Host Account"
            account_button_callback = "host_account"
        else:
            account_button_text = " Logout Account"
            account_button_callback = "instant_logout"
        
        # Check if account is added
        accounts = db.get_user_accounts(uid)
        if accounts:
            account_button_text = "⇒  Manage Account"
        else:
            account_button_text = "⇒  Manage Account"
        
        # Check if Dashboard URL is available for Web Dashboard button
        from pyrogram.types import WebAppInfo
        dashboard_url = getattr(config, 'DASHBOARD_URL', '')
        
        main_menu = [
            [InlineKeyboardButton("⇒  Broadcast Menu", callback_data="menu_broadcast")],
            [InlineKeyboardButton(account_button_text, callback_data="menu_manage_account")],
            [InlineKeyboardButton("⇒  Groups Settings", callback_data="menu_groups")],
            [InlineKeyboardButton("⇒  Post Link Management", callback_data="menu_post_link")],
            [InlineKeyboardButton("⇒  Saved Message Management", callback_data="menu_saved_messages")],
            [InlineKeyboardButton("⏱ Interval Management", callback_data="menu_interval_management")],
        ]
        
        # Add Web Dashboard button right after interval management
        if dashboard_url and dashboard_url.startswith('http'):
            full_url = f"{dashboard_url}?user_id={uid}"
            main_menu.append([InlineKeyboardButton("📊 Web Dashboard", web_app=WebAppInfo(url=full_url))])
            logger.info(f"Dashboard button added with URL: {full_url}")
        else:
            logger.warning("Dashboard button not added - DASHBOARD_URL not set or invalid")

        schedule_enabled = db.db.users.find_one({"user_id": uid}, {"schedule_enabled": 1})
        schedule_enabled = schedule_enabled.get("schedule_enabled", False) if schedule_enabled else False
        
        broadcast_menu = [
            [
                InlineKeyboardButton("👥 Groups Mode", callback_data="menu_groups_mode"),
                InlineKeyboardButton("🔁 Ads Forward Mode", callback_data="menu_ads_forward_mode"),
            ],
            [
                InlineKeyboardButton(
                    "Start Broadcast 🚀" if not is_running else "Stop Broadcast 🛑",
                    callback_data="start_broadcast" if not is_running else "stop_broadcast",
                )
            ],
            [
                InlineKeyboardButton("📊 View Analytics", callback_data="view_analytics"),
                InlineKeyboardButton(f"🕛 Scheduled Ads {'' if schedule_enabled else ''}", callback_data="scheduled_ads")
            ],
            [InlineKeyboardButton("←", callback_data="menu_main")],
        ]

        login_menu = [
            [
                InlineKeyboardButton("+ Add Account", callback_data="host_account"),
            ],
            [InlineKeyboardButton("←", callback_data="menu_main")],
        ]

        groups_menu = [
            [InlineKeyboardButton("○ Groups Only Mode", callback_data="groups_only_mode")],
            [InlineKeyboardButton("◆ Topics Only Mode", callback_data="forums_only_mode")],
            [InlineKeyboardButton("● Both Groups & Topics", callback_data="both_groups_topics_mode")],
            [InlineKeyboardButton("↻ Refresh Groups Cache", callback_data="refresh_groups_cache")],
            [InlineKeyboardButton("←", callback_data="menu_main")],
        ]

        if menu_type == "menu_broadcast":
            caption = f"<b>BROADCAST MENU</b>\n{status_info}"
            buttons = broadcast_menu
        elif menu_type == "menu_login":
            caption = f"<b>ACCOUNT MENU</b>\n\n{status_info}"
            buttons = login_menu
        elif menu_type == "menu_groups":
            caption = f"<b>GROUPS MENU</b>\n\n{status_info}"
            buttons = groups_menu
        else:
            caption = ( 
                f"{status_info}\n\n"
            )
            buttons = main_menu

        try:
            await callback_query.message.edit_caption(
                caption=caption,
                reply_markup=kb(buttons),
                parse_mode=ParseMode.HTML,
            )
        except MessageNotModified:
            pass
        except Exception as edit_error:
            try:
                await callback_query.message.reply_photo(
                    photo=config.START_IMAGE,
                    caption=caption,
                    reply_markup=kb(buttons),
                    parse_mode=ParseMode.HTML,
                )
            except Exception as send_error:
                logger.error(f"Failed to edit or send menu: {edit_error} | {send_error}")
                await callback_query.answer("Error updating menu. Try again.", show_alert=True)
                return

        # REMOVED: Preload task - now using cache system instead
        # try:
        #     menu_callback.preload_task = asyncio.create_task(preload_user_groups(uid))
        #     menu_callback.preload_task.set_name(f"preload_groups_{uid}")
        #     menu_callback.preload_task.add_done_callback(
        #         lambda t: logger.info(f"Preload task completed for user {uid}")
        #     )
        # except Exception as preload_error:
        #     logger.error(f"Failed to start preload task: {preload_error}")

        logger.info(f"Menu '{menu_type}' displayed for user {uid}")

    except Exception as e:
        logger.error(f"Error in menu callback: {e}")
        await callback_query.answer("Error loading menu. Try again.", show_alert=True)

@pyro.on_callback_query(filters.regex("^set_ad_delay$"))
async def set_ad_delay_callback(client, callback_query):
    """Show preset delay options for broadcast interval"""
    try:
        uid = callback_query.from_user.id

        buttons = [
            [InlineKeyboardButton("⏱ 3 min (180s)", callback_data="delay_180"),
             InlineKeyboardButton("◆ 5 min (300s)", callback_data="delay_300")],
            [InlineKeyboardButton("⏱ 10 min (600s)", callback_data="delay_600"),
             InlineKeyboardButton("★ 20 min (1200s)", callback_data="delay_1200")],
            [InlineKeyboardButton("x Back", callback_data="menu_broadcast")]
        ]

        await callback_query.message.edit_caption(
            caption="""<b>Choose Broadcast Interval</b>

<b>How long should the bot wait between each full broadcast cycle?</b>

• <b>3 Minutes (180s)</b> - Very fast 🟠
• <b>5 Minutes (300s)</b> - Fast   
• <b>10 Minutes (600s)</b> - Balanced (Recommended) 
• <b>20 Minutes (1200s)</b> - Safe & Slow 

<i>Shorter interval = More frequent broadcasts but higher risk</i>""",
            parse_mode=ParseMode.HTML,
            reply_markup=kb(buttons)
        )

    except Exception as e:
        logger.error(f"Error in set_ad_delay_callback: {e}")
        await callback_query.answer("Error loading delay options. Try again.", show_alert=True)

@pyro.on_callback_query(filters.regex("^delay_"))
async def delay_option_selected(client, callback_query):
    """Handle preset delay button selection"""
    try:
        uid = callback_query.from_user.id
        delay = int(callback_query.data.split("_")[1])

        db.set_user_ad_delay(uid, delay)
        await callback_query.answer(f" Interval set to {delay}s", show_alert=True)

        await menu_callback(client, callback_query)

    except Exception as e:
        logger.error(f"Error setting broadcast delay: {e}")
        await callback_query.answer("Error setting delay. Try again.", show_alert=True)

@pyro.on_callback_query(filters.regex("host_account"))
async def host_account_callback(client, callback_query):
    """Handle host account callback - FREE FOR ALL - Smart API credentials management"""
    try:
        uid = callback_query.from_user.id
        user = db.get_user(uid)
        
        if not user:
            await callback_query.answer("Please restart with /start", show_alert=True)
            return
        
        accounts_count = db.get_user_accounts_count(uid)
        limit = user.get("accounts_limit", 1)
        
        if isinstance(limit, str):
            try:
                limit = int(limit)
            except (TypeError, ValueError):
                logger.error(f"Invalid accounts_limit for user {uid}: {limit}. Defaulting to 1")
                limit = 1
        
        logger.info(f"User {uid} accessing account hosting - {accounts_count}/{limit} accounts")
        
        if accounts_count >= limit:
            await callback_query.answer(
                f" Account limit reached! You have {accounts_count}/{limit} accounts.",
                show_alert=True
            )
            await callback_query.message.edit_caption(
                caption=f"<b> ACCOUNT LIMIT REACHED</b>\n\n"
                        f"<b>Current Accounts:</b> {accounts_count}/{limit}\n\n"
                        f"You have reached your account limit. Please remove an existing account before adding a new one.\n\n"
                        f"<i> Tip: Go to 'My Accounts' to manage your existing accounts.</i>",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("◉ My Accounts", callback_data="my_accounts")],
                    [InlineKeyboardButton("←", callback_data="menu_main")]
                ]),
                parse_mode=ParseMode.HTML
            )
            return

        credentials = db.get_user_api_credentials(uid)
        
        if credentials:
            logger.info(f"User {uid} already has API credentials, skipping API input")
            db.set_user_state(uid, "telethon_wait_phone")
            await callback_query.message.edit_caption(
                caption="<b> ADD NEW ACCOUNT</b>\n\n"
                        " API credentials already saved!\n\n"
                        "Please enter your <b>phone number</b> with country code:\n\n"
                        " <b>Example:</b> <code>+1234567890</code>\n\n"
                        "<i>The OTP will be sent to this number</i>",
                reply_markup=kb([
                    [InlineKeyboardButton("←", callback_data="menu_main")]
                ]),
                parse_mode=ParseMode.HTML
            )
            return
        
        logger.info(f"User {uid} has no API credentials, requesting them")
        db.set_user_state(uid, "waiting_api_id")
        await callback_query.message.edit_caption(
            caption="<b>🔑 API CREDENTIALS REQUIRED</b>\n\n"
                    "<b> Get your API credentials:</b>\n"
                    "1. Visit https://my.telegram.org\n"
                    "2. Login with your phone number\n"
                    "3. Go to 'API Development tools'\n"
                    "4. Create an app and get API ID & Hash\n\n"
                    "<b> Note:</b> You only need to do this ONCE.\n"
                    "After saving, you won't be asked again.\n\n"
                    "Now please enter your <b>API ID</b>:\n\n"
                    " <b>Example:</b> <code>12345678</code>",
            reply_markup=kb([
                [InlineKeyboardButton("←", callback_data="menu_main")]
            ]),
            parse_mode=ParseMode.HTML
        )
        return

    except Exception as e:
        logger.error(f"Error in host_account_callback: {e}")
        await callback_query.answer("Error processing request.", show_alert=True)

@pyro.on_callback_query(filters.regex("temp_api_start"))
async def temp_api_start_callback(client, callback_query):
    """Handle temporary API credentials start - asks for API ID"""
    try:
        uid = callback_query.from_user.id
        db.set_user_state(uid, "waiting_temp_api_id")
        
        await callback_query.message.edit_caption(
            caption="<b>🔑 STEP 1/2: API ID</b>\n\n"
                    "Enter your <b>API ID</b> (numbers only)\n\n"
                    "<b> Get it from:</b> https://my.telegram.org\n\n"
                    "<b>Example:</b> <code>12345678</code>",
            reply_markup=kb([
                [InlineKeyboardButton("x Cancel", callback_data="host_account")]
            ]),
            parse_mode=ParseMode.HTML
        )
        
    except Exception as e:
        logger.error(f"Error in temp_api_start: {e}")
        await callback_query.answer("Error starting API setup.", show_alert=True)

@pyro.on_callback_query(filters.regex(r"^instant_logout$"))
async def instant_logout_callback(client, callback_query):
    """Show confirmation before logging out (delete all accounts)."""
    try:
        uid = callback_query.from_user.id
        logger.info(f"🟠 Logout confirmation requested for user {uid}")

        await callback_query.message.edit_media(
            media=InputMediaPhoto(
                media=config.START_IMAGE,
                caption=(
                    "<b> CONFIRM LOGOUT</b>\n\n"
                    "<b>Are you sure you want to logout?</b>\n\n"
                    "• This will delete ALL your linked account(s) and clear stored sessions.\n"
                    "• This action <b>cannot</b> be undone.\n\n"
                    "<i>If you only want to logout from one account, manage accounts from 'My Accounts'.</i>"
                ),
                parse_mode=ParseMode.HTML
            ),
            reply_markup=kb([
                [InlineKeyboardButton("! Yes - Logout", callback_data="confirm_instant_logout_yes")],
                [InlineKeyboardButton("x Cancel", callback_data="menu_main")]
            ])
        )

        await callback_query.answer()

    except Exception as e:
        logger.error(f"Error showing logout confirmation for {uid}: {e}")
        await callback_query.answer(" Failed to open confirmation. Try again.", show_alert=True)

@pyro.on_callback_query(filters.regex(r"^confirm_instant_logout_yes$"))
async def confirm_instant_logout_callback(client, callback_query):
    """Perform the full logout after user confirmation."""
    try:
        uid = callback_query.from_user.id
        logger.info(f" CONFIRMED LOGOUT for user {uid}")

        await callback_query.answer(" Logging out...", show_alert=False)

        accounts = db.get_user_accounts(uid)

        if not accounts:
            await callback_query.answer("No accounts to logout!", show_alert=True)
            return

        deleted_count = 0
        for account in accounts:
            try:
                acc_id = str(account.get('_id'))
                if db.delete_user_account(uid, acc_id):
                    deleted_count += 1
                    logger.info(f"Deleted account {account.get('phone_number')} for user {uid}")
            except Exception as ex_del:
                logger.error(f"Error deleting account {acc_id} for user {uid}: {ex_del}")

        try:
            db.delete_user_fully(uid)
            # Delete groups cache when user logs out all accounts
            db.delete_groups_cache(uid)
            logger.info(f" Full cleanup executed for user {uid} - {deleted_count} account(s) removed, cache cleared")
        except Exception as e:
            logger.error(f"Error in full cleanup for user {uid}: {e}")

        username = callback_query.from_user.username or "N/A"
        first_name = callback_query.from_user.first_name or "User"
        try:
            db.create_user(uid, username, first_name)
        except Exception as e:
            logger.error(f"Failed to re-create user record for {uid}: {e}")

        try:
            await callback_query.message.delete()
        except Exception:
            pass

        try:
            await client.send_photo(
                chat_id=uid,
                photo=config.START_IMAGE,
                caption=(
                    f"<b> LOGOUT SUCCESSFUL</b>\n\n"
                    f"<b>All your data has been removed:</b>\n"
                    f"• {deleted_count} account(s) deleted\n"
                    f"• All sessions cleared\n"
                    f"• Broadcast data removed\n"
                    f"• Fresh start activated\n\n"
                    f"<i>You can now add a new account! </i>"
                ),
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("+ Host Account", callback_data="host_account")],
                    [InlineKeyboardButton("←", callback_data="menu_main")]
                ]),
                parse_mode=ParseMode.HTML
            )
        except Exception as e:
            logger.error(f"Failed to send logout success message to {uid}: {e}")

        await callback_query.answer(" Logged out successfully!", show_alert=False)
        logger.info(f" User {uid} logged out successfully - all data cleared")

    except Exception as e:
        logger.error(f"Error in confirm_instant_logout callback: {e}")
        await callback_query.answer(" Error during logout. Please try again.", show_alert=True)

@pyro.on_callback_query(filters.regex("^select_saved_messages_count$"))
async def select_saved_messages_count_callback(client, callback_query):
    """Ask user how many saved messages to use for rotation"""
    try:
        uid = callback_query.from_user.id
        current_count = db.get_user_saved_messages_count(uid)
        
        try:
            await callback_query.answer()
        except:
            pass
        
        try:
            await callback_query.message.edit_media(
                InputMediaPhoto(
                    media=config.START_IMAGE,
                    caption=f"""<b> SELECT SAVED MESSAGES COUNT</b>

<b>Current Setting:</b> Using <code>{current_count}</code> messages for rotation

<b>How it works:</b>
• Bot will use the first X messages from your Saved Messages
• Messages rotate per cycle (Cycle 1 → Msg 1, Cycle 2 → Msg 2, etc.)
• After the last message, rotation starts over

<b>Example:</b>
If you select 4 messages:
• Cycle 1: All groups get Message #1
• Cycle 2: All groups get Message #2
• Cycle 3: All groups get Message #3
• Cycle 4: All groups get Message #4
• Cycle 5: All groups get Message #1 (repeats)

<b> Enter a number (1-10):</b>
Reply with how many messages to use from your Saved Messages.""",
                    parse_mode=ParseMode.HTML
                ),
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("x Back", callback_data="menu_broadcast")]
                ])
            )
        except Exception as edit_error:
            if "MESSAGE_NOT_MODIFIED" not in str(edit_error):
                logger.error(f"Error editing message: {edit_error}")
        
        db.set_user_state(uid, "waiting_saved_messages_count")
        
    except Exception as e:
        logger.error(f"Error in select_saved_messages_count callback: {e}")
        try:
            await callback_query.answer("Error loading settings", show_alert=True)
        except:
            pass

@pyro.on_callback_query(filters.regex("set_api_credentials"))
async def set_api_credentials_callback(client, callback_query):
    """Handle set API credentials callback"""
    try:
        uid = callback_query.from_user.id
        db.set_user_state(uid, "waiting_api_id")
        
        await callback_query.message.edit_media(
            media=InputMediaPhoto(
                media=config.START_IMAGE,
                caption="<b>🔑 SET API CREDENTIALS - Step 1/2</b>\n\n"
                        "<b> Get your API ID:</b>\n"
                        "1. Go to https://my.telegram.org\n"
                        "2. Login with your phone number\n"
                        "3. Go to 'API Development tools'\n"
                        "4. Create a new application\n"
                        "5. Copy the <b>API ID</b> (numbers only)\n\n"
                        "<b> Send your API ID now:</b>\n"
                        "Example: 1234567",
                parse_mode=ParseMode.HTML
            ),
            reply_markup=kb([
                [InlineKeyboardButton("x Cancel", callback_data="host_account")]
            ])
        )
        logger.info(f"API credentials setup started for user {uid}")
        
    except Exception as e:
        logger.error(f"Error in set_api_credentials callback: {e}")
        await callback_query.answer("Error starting API setup. Try again.", show_alert=True)

@pyro.on_callback_query(filters.regex("quick_delay_"))
async def quick_delay_callback(client, callback_query):
    """Handle quick delay callback"""
    try:
        uid = callback_query.from_user.id
        delay = int(callback_query.data.split("_")[-1])
        
        try:
            db.set_user_ad_delay(uid, delay)
        except Exception as e:
            logger.error(f"Failed to set ad delay for user {uid}: {e}")
            await callback_query.answer("Error setting delay. Try again.", show_alert=True)
            return
        
        await callback_query.message.edit_caption(
            caption=f"""<b>✅ CYCLE INTERVAL UPDATED!</b>\n\n"""
                    f"<u>New Interval:</u> <code>{delay} seconds</code>\n\n"
                    f"Ready for broadcasting!",
            reply_markup=kb([[InlineKeyboardButton("←", callback_data="menu_main")]]),
            parse_mode=ParseMode.HTML
        )
        await send_dm_log(uid, f"<b> Broadcast interval updated:</b> {delay} seconds")
        db.set_user_state(uid, "")
        logger.info(f"Quick delay set to {delay}s for user {uid}")
        
    except Exception as e:
        logger.error(f"Error in quick_delay callback: {e}")
        await callback_query.answer("Error setting delay. Try again.", show_alert=True)

@pyro.on_callback_query(filters.regex("start_broadcast"))
async def start_broadcast_callback(client, callback_query):
    """Handle start broadcast callback"""
    try:
        uid = callback_query.from_user.id
        
        # Check if using post link mode and verify link is set
        post_link_data = db.get_user_post_link(uid)
        if post_link_data and post_link_data.get("message_source") == "post_link":
            # User is in post link mode - verify post link exists
            if not post_link_data.get("post_link") or not post_link_data.get("saved_msg_id"):
                await callback_query.answer(" Post link not set!", show_alert=True)
                await callback_query.message.reply_text(
                    "<b> Cannot Start Broadcast</b>\n\n"
                    "You are in <b>Post Link Mode</b> but no post link is set.\n\n"
                    "Please either:\n"
                    "• Set a post link in Post Link Management\n"
                    "• Or switch to Saved Messages mode",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("≈ Set Post Link", callback_data="menu_post_link")],
                        [InlineKeyboardButton("x Back", callback_data="menu_broadcast")]
                    ]),
                    parse_mode=ParseMode.HTML
                )
                return
        if db.get_broadcast_state(uid).get("running"):
            await callback_query.answer("Broadcast already running!", show_alert=True)
            return
        
        user_msg_count = db.get_user_saved_messages_count(uid)
        
        accounts = db.get_user_accounts(uid) or []
        if accounts:
            try:
                acc = accounts[0]
                session_encrypted = acc.get("session_string") or ""
                session_str = cipher_suite.decrypt(session_encrypted.encode()).decode()
                
                credentials = db.get_user_api_credentials(acc['user_id'])
                if credentials:
                    tg_client = TelegramClient(StringSession(session_str), credentials['api_id'], credentials['api_hash'])
                    await tg_client.start()
                    
                    saved_msgs_list = []
                    messages = await tg_client.get_messages("me", limit=20)
                    for msg in messages:
                        if msg.text or msg.media:
                            saved_msgs_list.append(msg)
                    
                    await tg_client.disconnect()
                    
                    if len(saved_msgs_list) < user_msg_count:
                        await callback_query.answer()
                        await callback_query.message.edit_media(
                            InputMediaPhoto(
                                media=config.START_IMAGE,
                                caption=f"""<b> NOT ENOUGH SAVED MESSAGES!</b>

<b>Selected Message Count:</b> <code>{user_msg_count}</code> messages
<b>Available in Saved Messages:</b> <code>{len(saved_msgs_list)}</code> messages

<b> Problem:</b>
You've selected to use {user_msg_count} messages for rotation, but you only have {len(saved_msgs_list)} message{'s' if len(saved_msgs_list) != 1 else ''} in your Telegram Saved Messages.

<b> Solution (choose one):</b>

<b>Option 1:</b> Add more messages to your Saved Messages
• Open Telegram "Saved Messages" chat
• Save at least {user_msg_count - len(saved_msgs_list)} more message{'s' if (user_msg_count - len(saved_msgs_list)) > 1 else ''}
• Return and start broadcast

<b>Option 2:</b> Reduce your message count setting
• Click "Select Saved Messages "
• Enter {len(saved_msgs_list)} or less
• Start broadcast

<i>Make sure you have enough messages before broadcasting!</i>""",
                                parse_mode=ParseMode.HTML
                            ),
                            reply_markup=InlineKeyboardMarkup([
                                [InlineKeyboardButton("★ Select Saved Messages", callback_data="select_saved_messages_count")],
                                [InlineKeyboardButton("x Back", callback_data="menu_broadcast")]
                            ])
                        )
                        return
            except Exception as e:
                logger.warning(f"Could not verify saved messages count for user {uid}: {e}")
        
        accounts = db.get_user_accounts(uid)
        if not accounts:
            await callback_query.answer("No accounts hosted yet!", show_alert=True)
            return
        
        if not db.get_logger_status(uid):
            try:
                await callback_query.message.edit_caption(
                    caption="<b> Logger bot not started yet!</b>\n\n"
                            f"Please start @{config.LOGGER_BOT_USERNAME.lstrip('@')} to receive Advertising logs.\n"
                            "<i>After starting, return here to begin Advertising.</i>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb([
                        [InlineKeyboardButton("+ Start Logger Bot", url=f"https://t.me/{config.LOGGER_BOT_USERNAME.lstrip('@')}")],
                        [InlineKeyboardButton("←", callback_data="menu_main")]
                    ])
                )
            except Exception as e:
                logger.error(f"Failed to edit logger bot message for {uid}: {e}")
                await callback_query.answer("Error: Please try again.", show_alert=True)
            return
        
        current_task = user_tasks.get(uid)
        if current_task:
            try:
                current_task.cancel()
                await current_task
                logger.info(f"Cancelled previous broadcast for {uid}")
            except Exception as e:
                logger.error(f"Failed to cancel previous broadcast task for {uid}: {e}")
            finally:
                if uid in user_tasks:
                    del user_tasks[uid]
        
        task = asyncio.create_task(run_broadcast(client, uid))
        user_tasks[uid] = task
        db.set_broadcast_state(uid, running=True)
        
        try:
            await callback_query.message.edit_caption(
                caption=""" <b>BROADCAST ON! </b>\n\n"""
                        """Your ads are now being sent to the groups your account is joined in.\n"""
                        f"""Logs will be sent to your DM via @{config.LOGGER_BOT_USERNAME.lstrip('@')}.</i>""",
                parse_mode=ParseMode.HTML,
                reply_markup=kb([[InlineKeyboardButton("←", callback_data="menu_main")]])
            )
            await callback_query.answer("Broadcast started! ", show_alert=True)
            logger.info(f"Broadcast started via callback for user {uid}")
        except Exception as e:
            logger.error(f"Failed to edit BROADCAST ON message for {uid}: {e}")
            try:
                await client.send_photo(
                    chat_id=uid,
                    photo=config.START_IMAGE,
                    caption="""<b>BROADCAST ON! </b>\n\n"""
                            """Your ads are now being sent to the groups your account is joined in.\n"""
                            f"""Logs will be sent to your DM via @{config.LOGGER_BOT_USERNAME.lstrip('@')}.""",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb([[InlineKeyboardButton("←", callback_data="menu_main")]])
                )
                await callback_query.answer("Broadcast started! ", show_alert=True)
                await send_dm_log(uid, "<b>Broadcast started! Logs will come here</b>")
                logger.info(f"Broadcast started via callback for user {uid} (fallback send)")
            except Exception as e2:
                logger.error(f"Failed to send fallback BROADCAST ON message for {uid}: {e2}")
                await callback_query.answer("Error starting broadcast. Please try again. 😔", show_alert=True)
                await send_dm_log(uid, f"<b> Failed to start broadcast:</b> {str(e2)} 😔")
                
    except Exception as e:
        logger.error(f"Error in start_broadcast callback for {uid}: {e}")
        await callback_query.answer("Error starting broadcast. Contact support. 😔", show_alert=True)
        await send_dm_log(uid, f"<b> Failed to start broadcast:</b> {str(e)} 😔")

@pyro.on_callback_query(filters.regex("stop_broadcast"))
async def stop_broadcast_callback(client, callback_query):
    """Handle stop broadcast callback"""
    try:
        uid = callback_query.from_user.id
        stopped = await stop_broadcast_task(uid)
        if not stopped:
            await callback_query.answer("No broadcast running!", show_alert=True)
            return
        
        await callback_query.answer("Broadcast stopped! ", show_alert=True)
        try:
            await callback_query.message.edit_caption(
                caption="""<b>BROADCAST STOPPED! </b>\n\n"""
                        """Your broadcast has been stopped.\n"""
                        """Check analytics for final stats.""",
                reply_markup=kb([[InlineKeyboardButton("←", callback_data="menu_main")]]),
                parse_mode=ParseMode.HTML
            )
        except Exception as e:
            logger.error(f"Failed to edit BROADCAST STOPPED message for {uid}: {e}")
            await client.send_photo(
                chat_id=uid,
                photo=config.START_IMAGE,
                caption="""<b>BROADCAST STOPPED!</b>\n\n"""
                        """Your broadcast has been stopped.\n"""
                        """Check analytics for final stats.""",
                parse_mode=ParseMode.HTML,
                reply_markup=kb([[InlineKeyboardButton("←", callback_data="menu_main")]])
            )
        await send_dm_log(uid, f"<b>Broadcast stopped!</b>")
        logger.info(f"Broadcast stopped via callback for user {uid}")
        
    except Exception as e:
        logger.error(f"Error in stop_broadcast callback: {e}")
        await callback_query.answer("Error stopping broadcast. Try again.", show_alert=True)

@pyro.on_callback_query(filters.regex("view_analytics"))
async def analytics_callback(client, callback_query):
    """Handle view analytics callback with detailed stats"""
    try:
        uid = callback_query.from_user.id
        await callback_query.answer()
        
        user_stats = db.get_user_analytics(uid) if hasattr(db, 'get_user_analytics') else {}
        if not user_stats:
            user_stats = db.get_user_stats(uid) if hasattr(db, 'get_user_stats') else {}
        if not user_stats:
            user_stats = {}
        
        accounts = db.get_user_accounts(uid) or []
        
        try:
            logger_failures = len(db.get_logger_failures(uid)) if hasattr(db, 'get_logger_failures') else 0
        except:
            logger_failures = 0
        
        total_sent = user_stats.get('total_sent', 0)
        total_failed = user_stats.get('total_failed', 0)
        total_messages = total_sent + total_failed
        success_rate = (total_sent / total_messages * 100) if total_messages > 0 else 0
        
        user = db.get_user(uid)
        account_limit = user.get('accounts_limit', 1) if user else 1
        active_accounts = len([a for a in accounts if a.get('is_active', False)])
        
        analytics_text = (
            f"<b> AZTECH ADS BOT ANALYTICS</b>\n\n"
            f"<b>📈 Broadcast Statistics:</b>\n"
            f"• Cycles Completed: <code>{user_stats.get('total_cycles', 0)}</code> \n"
            f"• Messages Sent: <code>{total_sent:,}</code> \n"
            f"• Failed Sends: <code>{total_failed:,}</code> \n"
            f"• Success Rate: <code>{success_rate:.1f}%</code> \n\n"
            f"<b>👤 Account Status:</b>\n"
            f"• Active Accounts: <code>{active_accounts}/{account_limit}</code> \n"
            f"• Logger Failures: <code>{logger_failures}</code> \n\n"
            f"<b> Settings:</b>\n"
            f"• Cycle Interval: <code>{db.get_user_ad_delay(uid)}s</code> \n\n"
            f"<i>Keep tracking your broadcast performance! </i>"
        )
        
        try:
            await callback_query.message.edit_caption(
                caption=analytics_text,
                reply_markup=kb([
                    [InlineKeyboardButton("↻ Refresh Analytics", callback_data="view_analytics")],
                    [InlineKeyboardButton("←", callback_data="menu_broadcast")]
                ]),
                parse_mode=ParseMode.HTML
            )
            logger.info(f"Analytics shown for user {uid}")
        except Exception as edit_error:
            error_msg = str(edit_error).lower()
            if "message is not modified" in error_msg or "same" in error_msg or "not modified" in error_msg:
                await callback_query.answer(" Analytics already up to date!", show_alert=False)
                logger.debug(f"Analytics content unchanged for user {uid}")
            else:
                raise edit_error
        
    except Exception as e:
        logger.error(f"Error in analytics callback: {e}")
        await callback_query.answer(" Error loading analytics. Try again.", show_alert=True)

@pyro.on_callback_query(filters.regex("detailed_report"))
async def detailed_report_callback(client, callback_query):
    """Handle detailed report callback"""
    try:
        uid = callback_query.from_user.id
        user_stats = db.get_user_analytics(uid)
        accounts = db.get_user_accounts(uid)
        logger_failures = db.get_logger_failures(uid)
        
        detailed_text = (
            f"<b>DETAILED ANALYTICS REPORT:</b>\n\n"
            f"<u>Date:</u> <i>{datetime.now().strftime('%d/%m/%y')}</i>\n"
            f"<b>User ID:</b> <code>{uid}</code>\n\n"
            "<b>Broadcast Stats:</b>\n"
            f"- <u>Total Sent:</u> <code>{user_stats.get('total_sent', 0)}</code>\n"
            f"- <i>Total Failed:</i> <b>{user_stats.get('total_failed', 0)}</b>\n"
            f"- <u>Total Broadcasts:</u> <code>{user_stats.get('total_broadcasts', 0)}</code>\n\n"
            "<b>Logger Stats:</b>\n"
            f"- <u>Logger Failures:</u> <code>{len(logger_failures)}</code>\n"
            f"- <i>Last Failure:</i> <b>{logger_failures[-1]['error'] if logger_failures else 'None'}</b>\n\n"
            "<b>Account Stats:</b>\n"
            f"- <i>Total Accounts:</i> <u>{len(accounts)}</u>\n"
            f"- <b>Active Accounts:</b> <code>{len([a for a in accounts if a['is_active']])}</code> \n"
            f"- <u>Inactive Accounts:</u> <i>{len([a for a in accounts if not a['is_active']])}</i> \n\n"
            f"<b>Current Delay:</b> <code>{db.get_user_ad_delay(uid)}s</code>"
        )
        
        await callback_query.message.edit_caption(
            caption=detailed_text,
            reply_markup=kb([
                [InlineKeyboardButton("x Back", callback_data="analytics")]
            ]),
            parse_mode=ParseMode.HTML
        )
        logger.info(f"Detailed report shown for user {uid}")
        
    except Exception as e:
        logger.error(f"Error in detailed_report callback: {e}")
        await callback_query.answer("Error loading detailed report. Try again.", show_alert=True)

# =======================================================
#  MESSAGE HANDLERS
# =======================================================

@pyro.on_message(filters.text & filters.private & filters.regex("^/cancel$"))
async def cancel_command(client, message):
    """Handle cancel command"""
    try:
        uid = message.from_user.id
        user_state = db.get_user_state(uid)
        
        if user_state == "awaiting_post_link":
            db.set_user_state(uid, "")
            await message.reply_text(
                "<b> Post Link Setup Cancelled</b>\n\n"
                "Post link setup has been cancelled.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("● Main Menu", callback_data="menu_main")]
                ]),
                parse_mode=ParseMode.HTML
            )
        elif user_state == "awaiting_group_search":
            db.set_user_state(uid, "")
            await message.reply_text(
                "<b> Search Cancelled</b>\n\n"
                "Search operation has been cancelled.",
                parse_mode=ParseMode.HTML
            )
        elif user_state in ["awaiting_topic_search", "awaiting_forum_topic_search"]:
            db.set_user_state(uid, "")
            await message.reply_text(
                "<b> Topic Search Cancelled</b>\n\n"
                "Topic search has been cancelled.",
                parse_mode=ParseMode.HTML
            )
        else:
            await message.reply_text(
                "No operation to cancel.",
                parse_mode=ParseMode.HTML
            )
    except Exception as e:
        logger.error(f"Error in cancel command: {e}")

@pyro.on_message((filters.text | filters.media) & filters.private & ~filters.command(["start", "bd", "stats", "stop", "leaderboard", "cancel"]))
async def handle_text_message(client, message):
    """Handle text messages for various states"""
    try:
        uid = message.from_user.id
        user_state = db.get_user_state(uid)
        
        # If user sends random text and state is stuck in account setup, clear it
        if user_state in ["waiting_api_id", "waiting_api_hash", "waiting_temp_api_id", "waiting_temp_api_hash"]:
            if message.text and len(message.text) < 10 and not message.text.isdigit():
                # User sent something like "hi" instead of API credentials - clear state
                db.set_user_state(uid, "")
                await message.reply(
                    " Account setup cancelled.\n\n"
                    "Use /start to return to main menu.",
                    parse_mode=ParseMode.HTML
                )
                return
        
        # Handle post link input
        if user_state == "awaiting_post_link" and message.text:
            post_link = message.text.strip()
            
            # Parse the post link
            parsed = parse_post_link(post_link)
            
            if not parsed:
                await message.reply_text(
                    "<b> Invalid Post Link</b>\n\n"
                    "Please send a valid Telegram post link.\n\n"
                    "<b>Examples:</b>\n"
                    "<code>https://t.me/channelname/123</code>\n"
                    "<code>t.me/channelname/123</code>\n"
                    "<code>https://t.me/c/1234567890/123</code>\n\n"
                    "<i>Send /cancel to cancel.</i>",
                    parse_mode=ParseMode.HTML
                )
                return
            
            from_peer, msg_id = parsed
            
            # Save post link to database
            success = db.set_user_post_link(uid, post_link, from_peer, msg_id)
            
            if success:
                db.set_user_state(uid, "")
                
                await message.reply_text(
                    f"<b> POST LINK SET SUCCESSFULLY</b>\n\n"
                    f"🔗 <b>Post Link:</b> <code>{post_link}</code>\n"
                    f" <b>Message ID:</b> <code>{msg_id}</code>\n"
                    f"📍 <b>From:</b> <code>{from_peer}</code>\n\n"
                    f"<b>Mode:</b>  Post Link\n\n"
                    f"<i>Your broadcasts will now forward this message to all groups!</i>",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("x Back to Post Link Menu", callback_data="menu_post_link")],
                        [InlineKeyboardButton("● Main Menu", callback_data="menu_main")]
                    ]),
                    parse_mode=ParseMode.HTML
                )
            else:
                await message.reply_text(
                    "<b> Error Saving Post Link</b>\n\n"
                    "Please try again.",
                    parse_mode=ParseMode.HTML
                )
            return
        
        # Handle topic search keyword
        if user_state in ["awaiting_topic_search", "awaiting_forum_topic_search"] and message.text:
            search_keyword = message.text.strip()
            
            # Save search filter (reuse group search filter)
            db.set_group_search_filter(uid, search_keyword)
            db.set_user_state(uid, "")
            
            # Fetch all topics and filter
            accounts = db.get_user_accounts(uid)
            all_topics = []
            
            if accounts:
                for acc in accounts:
                    try:
                        tg_client = await get_telegram_client(acc["phone_number"], acc["session_string"])
                        async for dialog in tg_client.iter_dialogs():
                            if dialog.is_group or dialog.is_channel:
                                entity = dialog.entity
                                is_forum = getattr(entity, 'forum', False)
                                if is_forum:
                                    try:
                                        result = await tg_client(GetForumTopicsRequest(
                                            channel=entity,
                                            offset_date=0,
                                            offset_id=0,
                                            offset_topic=0,
                                            limit=100
                                        ))
                                        
                                        for topic in result.topics:
                                            if isinstance(topic, ForumTopic):
                                                topic_title = getattr(topic, 'title', f'Topic {topic.id}')
                                                if search_keyword.lower() in topic_title.lower():
                                                    all_topics.append({
                                                        "id": topic.id,
                                                        "title": topic_title,
                                                        "forum_id": entity.id,
                                                        "forum_title": dialog.title
                                                    })
                                    except Exception as e:
                                        logger.error(f"Error fetching topics: {e}")
                        break
                    except Exception as e:
                        logger.error(f"Error fetching forums for search: {e}")
                        continue
            
            await message.reply_text(
                f"<b> Topic Search Results</b>\n\n"
                f"Keyword: <code>{search_keyword}</code>\n"
                f"Found: <b>{len(all_topics)}</b> topics\n\n"
                f"<i>Topics matching your search are ready.</i>",
                parse_mode=ParseMode.HTML
            )
            return
        
        # Handle group search keyword
        if user_state == "awaiting_group_search" and message.text:
            search_keyword = message.text.strip()
            
            # Save search filter
            db.set_group_search_filter(uid, search_keyword)
            db.set_user_state(uid, "")
            
            # Fetch fresh groups from user's accounts to show count
            accounts = db.get_user_accounts(uid)
            all_groups = []
            
            if accounts:
                for acc in accounts:
                    try:
                        tg_client = await get_telegram_client(acc["phone_number"], acc["session_string"])
                        async for dialog in tg_client.iter_dialogs():
                            if dialog.is_group or dialog.is_channel:
                                entity = dialog.entity
                                group_data = {
                                    "id": entity.id,
                                    "title": dialog.title,
                                    "is_forum": getattr(entity, 'forum', False)
                                }
                                all_groups.append(group_data)
                        break  # Use first account only
                    except Exception as e:
                        logger.error(f"Error fetching groups for search: {e}")
                        continue
            
            filtered_groups = filter_groups_by_keyword(all_groups, search_keyword)
            
            await message.reply_text(
                f"<b> Search Filter Applied</b>\n\n"
                f"Keyword: <code>{search_keyword}</code>\n"
                f"Found: <b>{len(filtered_groups)}</b> groups\n\n"
                f"<i>Use bulk action buttons to add/remove filtered groups.\n"
                f"Use /cancel or clear filter to reset.</i>",
                parse_mode=ParseMode.HTML
            )
            return
        
        user = db.db.users.find_one({"user_id": uid})
        
        if user:
            if user.get("waiting_for_schedule_start"):
                time_text = message.text.strip()
                
                if not re.match(r'^\d{1,2}:\d{2}\s?(AM|PM|am|pm)$', time_text):
                    await message.reply(
                        " Invalid format! Please use: HH:MM AM/PM\n"
                        "Example: 8:00 AM",
                        parse_mode=ParseMode.HTML
                    )
                    return
                
                db.db.users.update_one(
                    {"user_id": uid},
                    {
                        "$set": {"schedule_start_time": time_text.upper()},
                        "$unset": {"waiting_for_schedule_start": ""}
                    }
                )
                
                await message.reply(
                    f" Start time set to: <b>{time_text.upper()}</b>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("◷ Scheduled Ads", callback_data="scheduled_ads")]
                    ])
                )
                return
            
            elif user.get("waiting_for_schedule_end"):
                time_text = message.text.strip()
                
                if not re.match(r'^\d{1,2}:\d{2}\s?(AM|PM|am|pm)$', time_text):
                    await message.reply(
                        " Invalid format! Please use: HH:MM AM/PM\n"
                        "Example: 8:00 PM",
                        parse_mode=ParseMode.HTML
                    )
                    return
                
                db.db.users.update_one(
                    {"user_id": uid},
                    {
                        "$set": {"schedule_end_time": time_text.upper()},
                        "$unset": {"waiting_for_schedule_end": ""}
                    }
                )
                
                await message.reply(
                    f" End time set to: <b>{time_text.upper()}</b>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("◷ Scheduled Ads", callback_data="scheduled_ads")]
                    ])
                )
                return
        
        state = db.get_user_state(uid)
        text = message.text.strip()

        logger.info(f" Received message from {uid} | state='{state}' | text_length={len(text)}")

        if state == "waiting_temp_api_id":
            try:
                temp_api_id = int(message.text.strip())
                if temp_api_id <= 0:
                    raise ValueError("Invalid API ID")
                
                db.set_user_temp_data(uid, "temp_api_id", temp_api_id)
                db.set_user_state(uid, "waiting_temp_api_hash")
                
                await message.reply_text(
                    "<b>🔑 STEP 2/2: API HASH</b>\n\n"
                    " API ID received!\n\n"
                    "Now enter your <b>API Hash</b> (long string)\n\n"
                    "<b> Get it from:</b> https://my.telegram.org\n\n"
                    "<b>Example:</b> <code>abc123def456...</code>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb([
                        [InlineKeyboardButton("x Cancel", callback_data="host_account")]
                    ])
                )
                logger.info(f"Temp API ID received for user {uid}")
            except ValueError:
                await message.reply_text(
                    " <b>Invalid API ID</b>\n\n"
                    "Please send only numbers.\n"
                    "Example: 12345678",
                    parse_mode=ParseMode.HTML
                )
            return

        elif state == "waiting_temp_api_hash":
            temp_api_hash = message.text.strip()
            if len(temp_api_hash) < 10:
                await message.reply_text(
                    " <b>Invalid API Hash</b>\n\n"
                    "API Hash should be longer (usually 32+ characters).",
                    parse_mode=ParseMode.HTML
                )
                return
            
            temp_api_id = db.get_user_temp_data(uid, "temp_api_id")
            if not temp_api_id:
                await message.reply_text(
                    " <b>Session expired</b>\n\n"
                    "Please start over.",
                    parse_mode=ParseMode.HTML
                )
                return
            
            db.set_user_temp_data(uid, "temp_api_hash", temp_api_hash)
            db.set_user_state(uid, "telethon_wait_phone")
            
            await message.reply_text(
                " <b>API Credentials Received!</b>\n\n"
                "Now enter the <b>phone number</b> for the account.\n\n"
                "<b>Format:</b> <code>+1234567890</code>",
                parse_mode=ParseMode.HTML
            )
            logger.info(f"Temp API credentials received for user {uid}, ready for phone")
            return

        elif state == "waiting_api_id":
            try:
                api_id = int(message.text.strip())
                if api_id <= 0:
                    raise ValueError("Invalid API ID")
                
                db.set_user_temp_data(uid, "temp_api_id", api_id)
                db.set_user_state(uid, "waiting_api_hash")
                
                await message.reply_text(
                    "<b>🔑 SET API CREDENTIALS - Step 2/2</b>\n\n"
                    " API ID received successfully!\n\n"
                    "<b> Now send your API Hash:</b>\n"
                    "1. From the same page at my.telegram.org\n"
                    "2. Copy the <b>API Hash</b> (long string)\n"
                    "3. Paste it below\n\n"
                    "<b> Send your API Hash now:</b>\n"
                    "Example: abc123def456ghi789...",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb([
                        [InlineKeyboardButton("x Cancel", callback_data="host_account")]
                    ])
                )
                logger.info(f"API ID received for user {uid}")
            except ValueError:
                await message.reply_text(
                    " <b>Invalid API ID</b>\n\n"
                    "Please send only the numbers for your API ID.\n"
                    "Example: 1234567\n\n"
                    "Get it from: https://my.telegram.org",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb([
                        [InlineKeyboardButton("x Cancel", callback_data="host_account")]
                    ])
                )
            return

        elif state == "waiting_api_hash":
            api_hash = message.text.strip()
            if len(api_hash) < 10:
                await message.reply_text(
                    " <b>Invalid API Hash</b>\n\n"
                    "API Hash should be a longer string (usually 32+ characters).\n\n"
                    "Get it from: https://my.telegram.org",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb([
                        [InlineKeyboardButton("x Cancel", callback_data="host_account")]
                    ])
                )
                return
            
            temp_api_id = db.get_user_temp_data(uid, "temp_api_id")
            if temp_api_id:
                if db.store_user_api_credentials(uid, temp_api_id, api_hash):
                    db.clear_user_temp_data(uid, "temp_api_id")
                    db.set_user_state(uid, "normal")
                    
                    await message.reply_text(
                        " <b>API CREDENTIALS SAVED!</b>\n\n"
                        "Your API credentials have been stored securely.\n\n"
                        "<b> API ID:</b> " + str(temp_api_id) + "\n"
                        "<b> API Hash:</b> " + api_hash[:8] + "..." + "\n\n"
                        "You can now add accounts to the bot!",
                        parse_mode=ParseMode.HTML,
                        reply_markup=kb([
                            [InlineKeyboardButton("+ Add Account Now", callback_data="host_account")],
                            [InlineKeyboardButton("←", callback_data="menu_main")]
                        ])
                    )
                    logger.info(f"API credentials saved for user {uid}")
                else:
                    await message.reply_text(
                        " <b>Failed to save credentials</b>\n\n"
                        "Please try again or contact support.",
                        parse_mode=ParseMode.HTML
                    )
            else:
                await message.reply_text(
                    " <b>Session expired</b>\n\n"
                    "Please start over with API ID setup.",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb([
                        [InlineKeyboardButton("↻ Start Over", callback_data="set_api_credentials")]
                    ])
                )
            return

        elif state == "telethon_wait_otp":
            otp_code = message.text.strip()
            if not otp_code.isdigit() or len(otp_code) != 5:
                await message.reply_text(
                    " <b>Invalid OTP Code</b>\n\n"
                    "Please enter the 5-digit code sent to your phone.\n"
                    "Example: 12345",
                    parse_mode=ParseMode.HTML
                )
                return
            
            try:
                db.set_user_temp_data(uid, "otp_code", otp_code)
                db.set_user_state(uid, "normal")
                
                await message.reply_text(
                    " <b>OTP Received!</b>\n\n"
                    "Processing your account verification...\n"
                    "Please wait while we complete the setup.",
                    parse_mode=ParseMode.HTML
                )
                logger.info(f"OTP received for user {uid}")
                
            except Exception as e:
                logger.error(f"Error handling OTP for user {uid}: {e}")
                await message.reply_text(
                    " <b>Error Processing OTP</b>\n\n"
                    "Please try again or contact support.",
                    parse_mode=ParseMode.HTML
                )
            return

        elif state == "waiting_broadcast_delay":
            logger.info(f" Processing broadcast delay for user {uid}")
            try:
                delay = int(text)
                if delay < 120:
                    await message.reply(
                        f"<b> Invalid interval!</b>\n\n"
                        f"Minimum interval is 120 seconds.\nPlease enter a valid number",
                        parse_mode=ParseMode.HTML,
                        reply_markup=kb([[InlineKeyboardButton("x Back", callback_data="menu_main")]])
                    )
                    return
                if delay > 86400:
                    await message.reply(
                        f"<b> Invalid interval!</b>\n\n"
                        f"Maximum interval is 86400 seconds (24 hours).\nPlease enter a valid number",
                        parse_mode=ParseMode.HTML,
                        reply_markup=kb([[InlineKeyboardButton("←", callback_data="menu_main")]])
                    )
                    return

                db.set_user_ad_delay(uid, delay)
                db.set_user_state(uid, "")
                logger.info(f" Broadcast delay set for user {uid}: {delay}s")
                
                await message.reply(
                    f"<b>✅ CYCLE INTERVAL UPDATED!</b>\n\n"
                    f"<u>New Interval:</u> <code>{delay} seconds</code>\n\n"
                    f"Ready for broadcasting!",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb([[InlineKeyboardButton("● Dashboard", callback_data="menu_main")]])
                )
                await send_dm_log(uid, f"<b> Broadcast interval updated:</b> {delay} seconds")
                logger.info(f" Delay set for user {uid}: {delay}s")
            except ValueError:
                await message.reply(
                    f"<b> Invalid input!</b>\n\n"
                    f"<u>Please enter a number (in seconds).</u>\n<i>Example: <code>300</code> for 5 minutes.</i>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb([[InlineKeyboardButton("←", callback_data="menu_main")]])
                )
            except Exception as e:
                logger.error(f" Failed to set broadcast delay for {uid}: {e}")
                db.set_user_state(uid, "")
                await message.reply(
                    f"<b> Failed to set interval!</b>\n\n"
                    f"<u>Error:</u> <i>{str(e)}</i>\n"
                    f"<b>Contact:</b> <code>@{config.ADMIN_USERNAME}</code>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb([[InlineKeyboardButton("←", callback_data="menu_main")]])
                )
        
        elif state == "waiting_saved_messages_count":
            logger.info(f" Processing saved messages count for user {uid}")
            try:
                count = int(text)
                if count < 1:
                    await message.reply(
                        f"<b> Invalid count!</b>\n\n"
                        f"Minimum is 1 message.\nPlease enter a valid number",
                        parse_mode=ParseMode.HTML,
                        reply_markup=kb([[InlineKeyboardButton("x Back", callback_data="menu_broadcast")]])
                    )
                    return
                if count > 10:
                    await message.reply(
                        f"<b> Invalid count!</b>\n\n"
                        f"Maximum is 10 messages.\nPlease enter a valid number",
                        parse_mode=ParseMode.HTML,
                        reply_markup=kb([[InlineKeyboardButton("x Back", callback_data="menu_broadcast")]])
                    )
                    return

                db.set_user_saved_messages_count(uid, count)
                db.set_user_state(uid, "")
                logger.info(f" Saved messages count set for user {uid}: {count}")
                
                await message.reply(
                    f"<b>SAVED MESSAGES COUNT UPDATED! </b>\n\n"
                    f"<u>Messages to Use:</u> <code>{count}</code>\n\n"
                    f"<b>How it works:</b>\n"
                    f"• Bot will use first {count} message{'s' if count > 1 else ''} from your Saved Messages\n"
                    f"• Rotation: Cycle 1 → Msg 1, Cycle 2 → Msg 2, etc.\n"
                    f"• After message {count}, it loops back to message 1\n\n"
                    f"Ready for broadcasting!",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb([[InlineKeyboardButton(">> Broadcast Menu", callback_data="menu_broadcast")]])
                )
                await send_dm_log(uid, f"<b> Saved messages count updated:</b> {count} messages")
                logger.info(f" Saved messages count set for user {uid}: {count}")
            except ValueError:
                await message.reply(
                    f"<b> Invalid input!</b>\n\n"
                    f"<u>Please enter a number (1-10).</u>\n<i>Example: <code>3</code> for 3 messages.</i>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb([[InlineKeyboardButton("x Back", callback_data="menu_broadcast")]])
                )
            except Exception as e:
                logger.error(f" Failed to set saved messages count for {uid}: {e}")
                db.set_user_state(uid, "")
                await message.reply(
                    f"<b> Failed to set count!</b>\n\n"
                    f"<u>Error:</u> <i>{str(e)}</i>\n"
                    f"<b>Contact:</b> <code>@{config.ADMIN_USERNAME}</code>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb([[InlineKeyboardButton(">> Broadcast Menu", callback_data="menu_broadcast")]])
                )
            return

        elif state == "waiting_api_id":
            logger.info(f" Processing API ID for user {uid}")
            try:
                api_id = int(text.strip())
                db.set_temp_data(uid, "api_id", api_id)
                db.set_user_state(uid, "waiting_api_hash")
                await message.reply(
                    f" <b>API ID saved!</b>\n\n"
                    f"Now please enter your <b>API Hash</b>:\n\n"
                    f" <b>Example:</b> <code>abcd1234efgh5678...</code>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb([[InlineKeyboardButton("←", callback_data="menu_main")]])
                )
                return
            except ValueError:
                await message.reply(
                    f" <b>Invalid API ID!</b>\n\n"
                    f"Please enter a valid numeric API ID.\n\n"
                    f" <b>Example:</b> <code>12345678</code>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb([[InlineKeyboardButton("←", callback_data="menu_main")]])
                )
                return

        elif state == "waiting_api_hash":
            logger.info(f" Processing API Hash for user {uid}")
            api_hash = text.strip()
            if len(api_hash) < 10:
                await message.reply(
                    f" <b>Invalid API Hash!</b>\n\n"
                    f"API Hash should be longer than 10 characters.\n\n"
                    f" <b>Example:</b> <code>abcd1234efgh5678ijkl9012</code>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb([[InlineKeyboardButton("←", callback_data="menu_main")]])
                )
                return
            
            api_id = db.get_temp_data(uid, "api_id")
            if not api_id:
                await message.reply(
                    f" <b>Session expired!</b>\n\n"
                    f"Please start the account addition process again.",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb([[InlineKeyboardButton("←", callback_data="menu_main")]])
                )
                return
            
            db.store_user_api_credentials(uid, api_id, api_hash)
            db.set_temp_data(uid, "api_hash", api_hash)
            db.set_user_state(uid, "telethon_wait_phone")
            await message.reply(
                f" <b>API Credentials saved temporarily!</b>\n\n"
                f"Now please enter your <b>phone number</b> with country code:\n\n"
                f" <b>Example:</b> <code>+1234567890</code>",
                parse_mode=ParseMode.HTML,
                reply_markup=kb([[InlineKeyboardButton("←", callback_data="menu_main")]])
            )
            return

        elif state == "telethon_wait_phone":
            logger.info(f" Processing phone number for user {uid}")
            if not validate_phone_number(text):
                await message.reply(
                    f"<b> Invalid phone number!</b>\n\n"
                    f"<u>Please use international format.</u>\n"
                    f"<i>Example: <code>+1234567890</code></i>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb([[InlineKeyboardButton("←", callback_data="menu_main")]])
                )
                return
                
            status_msg = await message.reply(
                f"<b>⏳ Hold! We're trying to OTP...</b>\n\n"
                f"<u>Phone:</u> <code>{text}</code> \n"
                f"<i>Please wait a moment.</i> ",
                parse_mode=ParseMode.HTML
            )
            
            try:
                credentials = db.get_user_api_credentials(uid)
                
                if not credentials:
                    await message.reply(
                        f" <b>API credentials not found!</b>\n\n"
                        f"Please restart the account addition process.",
                        parse_mode=ParseMode.HTML,
                        reply_markup=kb([[InlineKeyboardButton("←", callback_data="menu_main")]])
                    )
                    return
                
                tg = TelegramClient(StringSession(), credentials['api_id'], credentials['api_hash'])
                await tg.connect()
                
                try:
                    sent_code = await tg.send_code_request(text)
                    session_str = tg.session.save()
                except Exception as api_error:
                    logger.error(f"Invalid API credentials for user {uid}: {api_error}")
                    db.delete_user_api_credentials(uid)
                    await status_msg.edit_caption(
                        f"<b> INVALID API CREDENTIALS!</b>\n\n"
                        f"<u>Error:</u> <i>{str(api_error)}</i>\n\n"
                        f"Your API ID or API Hash is incorrect.\n"
                        f"They have been removed from the database.\n\n"
                        f"<b>Please click 'Add Account' again and enter correct API credentials.</b>",
                        parse_mode=ParseMode.HTML,
                        reply_markup=kb([[InlineKeyboardButton("+ Add Account", callback_data="host_account")]])
                    )
                    db.set_user_state(uid, "")
                    await send_dm_log(uid, f"<b> Invalid API credentials removed. Please set correct ones.</b>")
                    try:
                        await tg.disconnect()
                    except:
                        pass
                    return

                temp_dict = {
                    "phone": text,
                    "session_str": session_str,
                    "phone_code_hash": sent_code.phone_code_hash,
                    "otp": ""
                }

                temp_json = json.dumps(temp_dict)
                temp_encrypted = cipher_suite.encrypt(temp_json.encode()).decode()
                db.set_temp_data(uid, "session", temp_encrypted)
                db.set_user_state(uid, "telethon_wait_otp")
                logger.info(f" OTP sent to {text} for user {uid}")

                base_caption = (
                    f"<b>OTP sent to <code>{text}</code>! </b>\n\n"
                    f"Enter the OTP using the keypad below\n"
                    f"<b>Current:</b> <code>_____</code>\n"
                    f"<b>Format:</b> <code>12345</code> (no spaces needed)\n"
                    f"<i>Valid for:</i> <u>{config.OTP_EXPIRY // 60} minutes</u>"
                )

                await status_msg.edit_caption(
                    base_caption,
                    parse_mode=ParseMode.HTML,
                    reply_markup=get_otp_keyboard()
                )
                await send_dm_log(uid, f"<b>OTP requested for phone number:</b> <code>{text}</code>")
            except PhoneNumberInvalidError:
                await status_msg.edit_caption(
                    f"<b> Invalid phone number! </b>\n\n"
                    f"<u>Please check the number and try again.</u>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb([[InlineKeyboardButton("←", callback_data="menu_main")]])
                )
            except Exception as e:
                logger.error(f"Failed to send OTP for {uid}: {e}")
                db.set_user_state(uid, "")
                await status_msg.edit_caption(
                    f"<b> Failed to send OTP!</b>\n\n"
                    f"<u>Error:</u> <i>{str(e)}</i>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb([[InlineKeyboardButton("←", callback_data="menu_main")]])
                )
                await send_dm_log(uid, f"<b> Failed to send OTP for phone:</b> {str(e)}")
            finally:
                try:
                    await tg.disconnect()
                except:
                    pass
            return

        elif state == "telethon_wait_password":
            logger.info(f" Processing 2FA password for user {uid}")
            temp_encrypted = db.get_temp_data(uid, "session")
            if not temp_encrypted:
                await message.reply(
                    f"<b> Session expired!</b>\n\n"
                    f"<u>Please restart the process.</u>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb([[InlineKeyboardButton("←", callback_data="menu_main")]])
                )
                db.set_user_state(uid, "")
                return

            try:
                temp_json = cipher_suite.decrypt(temp_encrypted.encode()).decode()
                temp_dict = json.loads(temp_json)
                phone = temp_dict["phone"]
                session_str = temp_dict["session_str"]
            except (json.JSONDecodeError, InvalidToken) as e:
                logger.error(f"Invalid temp data for user {uid} in 2FA: {e}")
                await message.reply(
                    f"<b> Corrupted session data!</b>\n\n"
                    f"<b>Please restart the process.</b>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb([[InlineKeyboardButton("←", callback_data="menu_main")]])
                )
                db.set_user_state(uid, "")
                db.delete_temp_data(uid, "session")
                return

            credentials = db.get_user_api_credentials(uid)
            
            if not credentials:
                await message.reply(
                    f" <b>API credentials not found!</b>\n\n"
                    f"Please restart the account addition process.",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb([[InlineKeyboardButton("←", callback_data="menu_main")]])
                )
                return
            
            tg = TelegramClient(StringSession(session_str), credentials['api_id'], credentials['api_hash'])
            try:
                await tg.connect()
                await tg.sign_in(password=text)
                session_encrypted = cipher_suite.encrypt(session_str.encode()).decode()
                db.add_user_account(uid, phone, session_encrypted)
                db.set_user_state(uid, "")
                db.delete_temp_data(uid, "session")
                logger.info(f" 2FA completed and account added for user {uid}")
                
                await message.reply(
                    f"<b>Account added! </b>\n\n"
                    f"<u>Phone:</u> <code>{phone}</code>\n"
                    "•Account is ready for broadcasting!\n\n\n"
                    "<b>Note: Your account is ready for broadcasting!</b>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb([[InlineKeyboardButton("● Dashboard", callback_data="menu_main")]])
                )
                await send_dm_log(uid, f"<b>Account added successfully :</b> <code>{phone}</code> ")
                
                # Fetch all groups and save to MongoDB cache
                await fetch_groups_after_account_add(uid)
                
                asyncio.create_task(auto_select_all_groups(uid, phone))
            except PasswordHashInvalidError:
                await message.reply(
                    f"<b> Invalid password!</b>\n\n"
                    f"<u>Please try again.</u>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb([[InlineKeyboardButton("←", callback_data="menu_main")]])
                )
            except Exception as e:
                logger.error(f"Failed to sign in with password for {uid}: {e}")
                db.set_user_state(uid, "")
                db.delete_temp_data(uid, "session")
                await message.reply(
                    f"<b> Login failed!</b>\n\n"
                    f"<u>Error:</u> <i>{str(e)}</i>\n"
                    f"<b>Contact:</b> <code>@{config.ADMIN_USERNAME}</code>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb([[InlineKeyboardButton("● Dashboard", callback_data="menu_main")]])
                )
                await send_dm_log(uid, f"<b>Account login failed:</b> {str(e)}")
            finally:
                try:
                    await tg.disconnect()
                except:
                    pass
            return
            return

        elif state:
            logger.warning(f" Unhandled state '{state}' for user {uid} with message: {text[:100]}")

        else:
            logger.info(f" Regular message from user {uid}: {text[:100]}")
            
    except Exception as e:
        logger.error(f"Error in handle_text_message: {e}")

# =======================================================
#  CYCLE TIMEOUT HANDLERS
# =======================================================

@pyro.on_callback_query(filters.regex("^set_cycle_timeout$"))
async def set_cycle_timeout_callback(client, callback_query):
    """Handle cycle timeout setting callback"""
    try:
        uid = callback_query.from_user.id
        user = db.get_user(uid)
        
        current_timeout = db.get_user_cycle_timeout(uid) if hasattr(db, 'get_user_cycle_timeout') else 600
        
        await callback_query.message.edit_caption(
            caption=f"""<b> BROADCAST CYCLE TIMEOUT</b>\n\n"""
                    f"<b>Current Timeout:</b> {current_timeout//60} minutes ⏱\n\n"
                    f"<i>Bot will pause for this duration after every 5 broadcast cycles to avoid account restrictions.</i>\n\n"
                    f"Select a timeout duration:",
            reply_markup=kb([
                [InlineKeyboardButton("⏱ 10 Minutes", callback_data="set_timeout_600"),
                 InlineKeyboardButton("⏱ 15 Minutes", callback_data="set_timeout_900")],
                [InlineKeyboardButton("⏱ 20 Minutes", callback_data="set_timeout_1200")],
                [InlineKeyboardButton("←", callback_data="menu_main")]
            ]),
            parse_mode=ParseMode.HTML
        )
        
    except Exception as e:
        logger.error(f"Error in set_cycle_timeout callback: {e}")
        await callback_query.answer("Error loading timeout settings. Try again.", show_alert=True)

@pyro.on_callback_query(filters.regex("^set_timeout_"))
async def set_specific_timeout_callback(client, callback_query):
    """Handle setting specific cycle timeout"""
    try:
        uid = callback_query.from_user.id
        timeout = int(callback_query.data.split("_")[-1])
        
        if hasattr(db, 'set_user_cycle_timeout'):
            db.set_user_cycle_timeout(uid, timeout)
        
        await callback_query.message.edit_caption(
            caption=f"""<b> CYCLE TIMEOUT UPDATED!</b>\n\n"""
                    f"<b>New Timeout:</b> {timeout//60} minutes\n\n"
                    f"<i>Your broadcast will now pause for {timeout//60} minutes after every 5 cycles.</i>",
            reply_markup=kb([[InlineKeyboardButton("←", callback_data="menu_main")]]),
            parse_mode=ParseMode.HTML
        )
        
        await send_dm_log(uid, f"<b> Cycle timeout updated to:</b> {timeout//60} minutes")
        
    except Exception as e:
        logger.error(f"Error in set_specific_timeout callback: {e}")
        await callback_query.answer("Error setting timeout. Try again.", show_alert=True)

# =======================================================
#  SCHEDULED ADS HANDLERS
# =======================================================

@pyro.on_callback_query(filters.regex("^scheduled_ads$"))
async def scheduled_ads_callback(client, callback_query):
    """Handle scheduled ads menu"""
    try:
        uid = callback_query.from_user.id
        
        user = db.db.users.find_one({"user_id": uid})
        schedule_enabled = user.get("schedule_enabled", False) if user else False
        schedule_start = user.get("schedule_start_time", "8:00 AM") if user else "8:00 AM"
        schedule_end = user.get("schedule_end_time", "8:00 PM") if user else "8:00 PM"
        
        status_emoji = " ON ✅" if schedule_enabled else " OFF ⛔"
        current_ist = get_ist_now().strftime('%I:%M %p')
        
        caption = (
            f"<b> SCHEDULED ADS (IST)</b>\n\n"
            f"<b>Status:</b> {status_emoji}\n"
            f"<b>Start Time:</b> {schedule_start} IST\n"
            f"<b>End Time:</b> {schedule_end} IST\n"
            f"<b>Current Time:</b> {current_ist} IST\n\n"
            f"<b>How it works:</b>\n"
            f"• Ads will ONLY run during the specified time\n"
            f"• Every day, same schedule (Indian Time)\n"
            f"• Automatically starts at start time\n"
            f"• Automatically stops at end time\n\n"
            f"<i>Example: 8:00 AM to 8:00 PM means ads run only during daytime.</i>"
        )
        
        buttons = [
            [
                InlineKeyboardButton(
                    "Turn ON ⏳" if not schedule_enabled else " Turn OFF ⛔",
                    callback_data="toggle_schedule"
                )
            ],
            [InlineKeyboardButton("▸ Set Start Time", callback_data="set_schedule_start")],
            [InlineKeyboardButton("◂ Set End Time", callback_data="set_schedule_end")],
            [InlineKeyboardButton("←", callback_data="menu_broadcast")]
        ]
        
        await callback_query.message.edit_media(
            InputMediaPhoto(
                media=config.START_IMAGE,
                caption=caption,
                parse_mode=ParseMode.HTML
            ),
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        
    except Exception as e:
        logger.error(f"Error in scheduled_ads: {e}")
        await callback_query.answer("Error occurred. Try again.", show_alert=True)

@pyro.on_callback_query(filters.regex("^toggle_schedule$"))
async def toggle_schedule_callback(client, callback_query):
    """Toggle schedule on/off"""
    try:
        uid = callback_query.from_user.id
        
        user = db.db.users.find_one({"user_id": uid})
        current_status = user.get("schedule_enabled", False) if user else False
        new_status = not current_status
        
        db.db.users.update_one(
            {"user_id": uid},
            {"$set": {"schedule_enabled": new_status}}
        )
        
        status_text = "ENABLED " if new_status else "DISABLED "
        await callback_query.answer(f"Schedule {status_text}", show_alert=True)
        
        await scheduled_ads_callback(client, callback_query)
        
    except Exception as e:
        logger.error(f"Error toggling schedule: {e}")
        await callback_query.answer("Error occurred. Try again.", show_alert=True)

@pyro.on_callback_query(filters.regex("^set_schedule_(start|end)$"))
async def set_schedule_time_callback(client, callback_query):
    """Set schedule start or end time"""
    try:
        uid = callback_query.from_user.id
        time_type = callback_query.data.split("_")[-1]
        
        time_label = "Start" if time_type == "start" else "End"
        
        caption = (
            f"<b> SET {time_label.upper()} TIME</b>\n\n"
            f"<b>Enter the {time_label.lower()} time in 12-hour format:</b>\n\n"
            f"<b>Examples:</b>\n"
            f"• <code>8:00 AM</code>\n"
            f"• <code>8:30 AM</code>\n"
            f"• <code>9:00 PM</code>\n"
            f"• <code>11:45 PM</code>\n\n"
            f"<i>Format: HH:MM AM/PM</i>"
        )
        
        await callback_query.message.edit_media(
            InputMediaPhoto(
                media=config.START_IMAGE,
                caption=caption,
                parse_mode=ParseMode.HTML
            ),
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("←", callback_data="scheduled_ads")]
            ])
        )
        
        db.db.users.update_one(
            {"user_id": uid},
            {"$set": {f"waiting_for_schedule_{time_type}": True}}
        )
        
    except Exception as e:
        logger.error(f"Error in set_schedule_time: {e}")
        await callback_query.answer("Error occurred. Try again.", show_alert=True)

# =======================================================
#  Main bot startup function
# =======================================================

async def start_bot_and_cleanup():
    """Main bot startup function with comprehensive initialization and cleanup."""
    
    required_dirs = ['sessions']
    for dir_name in required_dirs:
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)
            print(f" Created {dir_name}/ directory")
    
    try:
        await pyro.start()
        logger.info(" Main bot started successfully")

        await start_logger_bot()
        logger.info(" Logger bot connected successfully")

        global MAIN_LOOP
        try:
            MAIN_LOOP = asyncio.get_running_loop()
        except RuntimeError:
            MAIN_LOOP = None

        try:
            await preload_chat_cache(pyro)
        except Exception as e:
            logger.warning(f"Preload chat cache failed during startup: {e}")

        try:
            running_states = db.db.broadcast_states.update_many(
                {"running": True},
                {"$set": {"running": False, "paused": False, "updated_at": datetime.utcnow()}}
            )
            logger.info(f"[X] Stopped {running_states.modified_count} running broadcasts on startup.")
        except Exception as e:
            logger.error(f"Failed to stop running broadcasts: {e}")

        logger.info(" All systems ready! Bot is now operational.")
        await idle()

    except Exception as e:
        logger.error(f" Failed to start bot: {e}")

    finally:
        for uid, task in list(user_tasks.items()):
            try:
                task.cancel()
                logger.info(f" Cancelled broadcast task for user {uid}")
            except Exception as cancel_err:
                logger.warning(f"Failed to cancel task for {uid}: {cancel_err}")

        if db is not None and hasattr(db, 'close'):
            db.close()
            logger.info("Database connection closed")
        logger.info("Bot stopped gracefully")


if __name__ == "__main__":
    pyro.run(start_bot_and_cleanup())
