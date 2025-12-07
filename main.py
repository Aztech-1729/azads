import asyncio
import random
import string
import re
import json
import tempfile
from datetime import datetime, timedelta
from typing import Dict, Any, List, Tuple
from telethon import TelegramClient, functions, types, events
from telethon.sessions import StringSession
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
    UserDeactivatedError,
    UserDeactivatedBanError,
    AuthKeyUnregisteredError,
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
# Utils functions integrated directly (utils.py removed)

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

def format_broadcast_summary(sent: int, failed: int, duration: timedelta) -> str:
    """Format broadcast completion summary"""
    total = sent + failed
    success_rate = (sent / total * 100) if total > 0 else 0
    
    return (
        f"📊 <blockquote><b>BROADCAST SUMMARY</b></blockquote>\n\n"
        f"<blockquote>✅ <b>Sent:</b> {sent:,}</blockquote>\n"
        f"<blockquote>❌ <b>Failed:</b> {failed:,}</blockquote>\n"
        f"<blockquote>📈 <b>Success Rate:</b> {success_rate:.1f}%</blockquote>\n"
        f"<blockquote>⏰ <b>Duration:</b> {format_duration(duration)}</blockquote>\n"
        f"<blockquote>🎯 <b>Performance:</b> {generate_progress_bar(sent, total)}</blockquote>"
    )

def create_analytics_summary(analytics: Dict) -> str:
    """Create formatted analytics summary"""
    total_sent = analytics.get('total_sent', 0)
    total_failed = analytics.get('total_failed', 0)
    success_rate = calculate_success_rate(total_sent, total_failed)
    
    return (
        f"📊 <blockquote><b>PERFORMANCE ANALYTICS</b></blockquote>\n\n"
        f"<blockquote>📈 <b>Broadcasts:</b> {analytics.get('total_broadcasts', 0):,}</blockquote>\n"
        f"<blockquote>✅ <b>Sent:</b> {total_sent:,}</blockquote>\n"
        f"<blockquote>❌ <b>Failed:</b> {total_failed:,}</blockquote>\n"
        f"<blockquote>🎯 <b>Success Rate:</b> {success_rate:.1f}%</blockquote>\n"
        f"<blockquote>📱 <b>Accounts:</b> {analytics.get('total_accounts', 0)}</blockquote>"
    )

def format_error_message(error_type: str, context: str = "") -> str:
    """Format error messages consistently"""
    base_message = config.ERROR_MESSAGES.get(error_type, "❌ An error occurred")
    if context:
        return f"{base_message}\n\n<blockquote>🔍 <b>Context:</b> {context}</blockquote>"
    return base_message

def format_success_message(success_type: str, context: str = "") -> str:
    """Format success messages consistently"""
    base_message = config.SUCCESS_MESSAGES.get(success_type, "✅ Operation successful")
    if context:
        return f"{base_message}\n\n<blockquote>📋 <b>Details:</b> {context}</blockquote>"
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
import os
import logging
from cryptography.fernet import Fernet, InvalidToken
import threading
import time
import requests
from flask import Flask, jsonify
import sys
import io
import subprocess

# =======================================================
# REMOVED: AccountManager class - Not used in current broadcast system
# =======================================================

# =======================================================
# 🔍 ACCOUNT HEALTH MONITOR
# =======================================================

class AccountHealthMonitor:
    """Continuously monitors accounts for bans/freezes and auto-removes them"""
    
    def __init__(self):
        self.monitoring = {}
        self.banned_accounts = set()
    
    async def check_account_status(self, client, account_id, user_id):
        """Check if an account is still active"""
        try:
            # Try to get account info
            me = await client.get_me()
            if me:
                return True, "Active"
        except UserDeactivatedBanError:
            return False, "Account is banned/deactivated"
        except AuthKeyUnregisteredError:
            return False, "Account session expired"
        except UserDeactivatedError:
            return False, "Account is deactivated"
        except Exception as e:
            err_str = str(e).lower()
            if any(word in err_str for word in ['ban', 'deactivat', 'deleted', 'invalid']):
                return False, f"Account issue: {str(e)[:50]}"
            return True, "Unknown status"
    
    async def remove_banned_account(self, user_id, account_id, reason):
        """Remove banned account and all its data from database"""
        try:
            logger.warning(f"🚨 Auto-removing banned account {account_id} for user {user_id}: {reason}")
            
            # Delete account from database
            result = db.db.accounts.delete_one({
                'user_id': user_id,
                '_id': account_id
            })
            
            if result.deleted_count > 0:
                logger.info(f"✅ Removed banned account {account_id} from database")
                
                # Send notification to user
                try:
                    await send_dm_log(user_id,
                        f"<blockquote>🚨 <b>ACCOUNT REMOVED AUTOMATICALLY</b></blockquote>\n\n"
                        f"<b>Reason:</b> {reason}\n"
                        f"<b>Account ID:</b> <code>{str(account_id)[:8]}...</code>\n\n"
                        f"<b>Action Taken:</b>\n"
                        f"• Account removed from database\n"
                        f"• Broadcast continues with remaining accounts\n\n"
                        f"<i>⚠️ Please add a new account to continue broadcasting</i>"
                    )
                except Exception as e:
                    logger.error(f"Failed to send notification: {e}")
                
                return True
            return False
        except Exception as e:
            logger.error(f"Failed to remove banned account {account_id}: {e}")
            return False
    
    async def monitor_account(self, client, account_id, user_id):
        """Continuously monitor a single account"""
        while True:
            try:
                await asyncio.sleep(60)  # Check every 60 seconds
                
                # Skip if not in monitoring list
                if user_id not in self.monitoring or not self.monitoring[user_id]:
                    break
                
                # Check account status
                is_active, status = await self.check_account_status(client, account_id, user_id)
                
                if not is_active:
                    # Account is banned/frozen
                    self.banned_accounts.add(account_id)
                    
                    # Remove from database
                    await self.remove_banned_account(user_id, account_id, status)
                    
                    # Stop monitoring this account
                    break
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error monitoring account {account_id}: {e}")
                await asyncio.sleep(30)
    
    def start_monitoring(self, user_id):
        """Start monitoring all accounts for a user"""
        self.monitoring[user_id] = True
    
    def stop_monitoring(self, user_id):
        """Stop monitoring accounts for a user"""
        self.monitoring[user_id] = False
    
    def is_account_banned(self, account_id):
        """Check if account is marked as banned"""
        return account_id in self.banned_accounts

# Global account health monitor
account_monitor = AccountHealthMonitor()

# =======================================================
# 🚀 INITIALIZATION & CONFIGURATION
# =======================================================
# REMOVED: Duplicate imports already at top of file

# ✅ Force UTF-8 output for PowerShell and cmd (Windows fix)
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="ignore")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="ignore")

# ✅ Force Python to use UTF-8 mode for all IO
os.environ["PYTHONIOENCODING"] = "utf-8"

# =======================================================
# 🧠 LOGGING CONFIGURATION
# =======================================================
# Main log levels
logging.getLogger("__main__").setLevel(logging.INFO)
logging.getLogger("pyrogram").setLevel(logging.ERROR)
logging.getLogger("telethon").setLevel(logging.ERROR)

# 🧹 Show INFO level logs from database (to see Clear APIs progress)
db_logger = logging.getLogger("database")
db_logger.setLevel(logging.INFO)

# 🧱 Suppress noisy asyncio socket warnings
def _ignore_socket_warnings(loop, context):
    """Suppress harmless asyncio 'socket.send() raised exception' warnings."""
    msg = context.get("message", "")
    exc = context.get("exception")

    # Ignore low-level harmless network errors
    if isinstance(exc, OSError) or "socket.send" in msg:
        logging.getLogger("asyncio").debug(f"Ignored asyncio socket warning: {msg}")
        return

    # Let real exceptions show normally
    loop.default_exception_handler(context)

# Apply handler globally to current event loop
try:
    asyncio.get_event_loop().set_exception_handler(_ignore_socket_warnings)
except RuntimeError:
    pass

# =======================================================
# 🧩 OTHER GLOBALS & SETUP
# =======================================================
# Auto-reply runtime state (in-memory)

# Global main asyncio loop reference
MAIN_LOOP = None

# ✅ Initialize Flask app
app = Flask(__name__)

# Create logs directory and set up file + console logging
os.makedirs('logs', exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/AzTechAdsBot.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

print("AZTECH ADS Bot Started Successfully 🚀")

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
    
    # REMOVED: Blacklist management - sending to all groups without restrictions
    if not hasattr(db, 'clear_all_blacklisted_groups'):
        def clear_all_blacklisted_groups(self, user_id):
            """Clear all blacklisted groups for a user"""
            try:
                result = self.db.blacklisted_groups.delete_many({'user_id': user_id})
                return result.deleted_count
            except Exception as e:
                logger.error(f"Failed to clear blacklisted groups for user {user_id}: {e}")
                return 0
        setattr(db.__class__, 'clear_all_blacklisted_groups', clear_all_blacklisted_groups)
    
    if not hasattr(db, 'get_blacklisted_groups_count'):
        def get_blacklisted_groups_count(self, user_id):
            """Get count of blacklisted groups for a user"""
            try:
                return self.db.blacklisted_groups.count_documents({'user_id': user_id})
            except Exception as e:
                logger.error(f"Failed to count blacklisted groups for user {user_id}: {e}")
                return 0
        setattr(db.__class__, 'get_blacklisted_groups_count', get_blacklisted_groups_count)

# =======================================================
# 🚀 ULTRA-FAST GROUPS CACHE SYSTEM
# =======================================================

GROUPS_CACHE = {}
GROUPS_CACHE_TTL = 120  # 2 minutes cache (faster refresh)
QUICK_CACHE = {}  # Instant access cache for immediate responses

# Background task tracking
PRELOAD_TASKS = {}

async def ultra_fast_preload_groups(uid):
    """⚡ ULTRA-FAST group preloading with instant response + background loading"""
    try:
        # 🎯 INSTANT RESPONSE: Check if we have ANY cached data
        quick_cache = QUICK_CACHE.get(uid)
        if quick_cache and time.time() - quick_cache['timestamp'] < 30:  # 30 sec instant cache
            logger.info(f"🚀 INSTANT cache hit for user {uid} - {len(quick_cache['groups'])} groups")
            return quick_cache['groups']

        accounts = db.get_user_accounts(uid)
        if not accounts:
            return []

        # 🚀 PARALLEL ULTRA-FAST GROUP FETCHING
        async def lightning_fast_groups(acc):
            """Lightning-fast group fetching - only essentials"""
            tg_client = None
            try:
                session_str = cipher_suite.decrypt(acc['session_string'].encode()).decode()
                # Get user's API credentials
                credentials = db.get_user_api_credentials(acc['user_id'])
                if not credentials:
                    logger.error(f"No API credentials found for user {acc['user_id']}")
                    return []
                tg_client = TelegramClient(StringSession(session_str), credentials['api_id'], credentials['api_hash'])
                await asyncio.wait_for(tg_client.connect(), timeout=3)  # 3sec timeout
                
                groups = []
                # 🚀 SPEED BOOST: Limit to first 100 dialogs for instant response
                async for dialog in tg_client.iter_dialogs():
                    if dialog.is_group:
                        groups.append({
                            'id': dialog.id,
                            'title': dialog.title[:30],  # Truncate long titles for speed
                            'selected': True
                        })
                        
                        # Load all groups (no limit)
                            
                return groups
            except Exception as e:
                logger.warning(f"Fast fetch failed for {acc.get('phone_number', 'unknown')}: {e}")
                return []
            finally:
                if tg_client:
                    try:
                        await asyncio.wait_for(tg_client.disconnect(), timeout=1)
                    except Exception:
                        pass

        # 🚀 PARALLEL PROCESSING: All accounts simultaneously
        start_time = time.time()
        tasks = [lightning_fast_groups(acc) for acc in accounts]
        groups_lists = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Combine results
        all_groups = []
        seen_ids = set()
        for groups in groups_lists:
            if isinstance(groups, list):  # Skip exceptions
                for group in groups:
                    if group['id'] not in seen_ids:
                        seen_ids.add(group['id'])
                        all_groups.append(group)

        load_time = time.time() - start_time
        logger.info(f"⚡ ULTRA-FAST load completed in {load_time:.2f}s - {len(all_groups)} groups")

        # 🎯 DUAL CACHE: Quick cache for instant access + full cache
        QUICK_CACHE[uid] = {
            'groups': all_groups,
            'timestamp': time.time()
        }
        
        GROUPS_CACHE[uid] = {
            'groups': all_groups,
            'timestamp': time.time()
        }

        # 🚀 BACKGROUND TASK: Load remaining groups silently
        if uid not in PRELOAD_TASKS or PRELOAD_TASKS[uid].done():
            PRELOAD_TASKS[uid] = asyncio.create_task(background_full_load(uid, accounts))

        return all_groups

    except Exception as e:
        logger.error(f"Ultra-fast group loading failed for user {uid}: {e}")
        return []

async def background_full_load(uid, accounts):
    """🔄 Background task to load ALL groups without blocking UI"""
    try:
        await asyncio.sleep(2)  # Let UI respond first
        
        async def full_account_groups(acc):
            """Load ALL groups from account in background"""
            tg_client = None
            try:
                session_str = cipher_suite.decrypt(acc['session_string'].encode()).decode()
                # Get user's API credentials
                credentials = db.get_user_api_credentials(acc['user_id'])
                if not credentials:
                    logger.error(f"No API credentials found for user {acc['user_id']}")
                    return []
                tg_client = TelegramClient(StringSession(session_str), credentials['api_id'], credentials['api_hash'])
                await tg_client.connect()
                
                groups = []
                async for dialog in tg_client.iter_dialogs(limit=None):  # ALL groups
                    if dialog.is_group:
                        groups.append({
                            'id': dialog.id,
                            'title': dialog.title,
                            'selected': True
                        })
                return groups
            except Exception as e:
                logger.debug(f"Background load failed for {acc.get('phone_number', 'unknown')}: {e}")
                return []
            finally:
                if tg_client:
                    try:
                        await tg_client.disconnect()
                    except Exception:
                        pass

        # Load all groups in background
        tasks = [full_account_groups(acc) for acc in accounts]
        all_groups_lists = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Merge with existing cache
        full_groups = []
        seen_ids = set()
        
        for groups in all_groups_lists:
            if isinstance(groups, list):
                for group in groups:
                    if group['id'] not in seen_ids:
                        seen_ids.add(group['id'])
                        full_groups.append(group)

        # Update full cache
        GROUPS_CACHE[uid] = {
            'groups': full_groups,
            'timestamp': time.time()
        }
        
        logger.info(f"🔄 Background loaded {len(full_groups)} total groups for user {uid}")

    except Exception as e:
        logger.error(f"Background group loading failed for user {uid}: {e}")

def get_ultra_fast_groups(uid):
    """🚀 Get groups with ultra-fast priority: Quick cache > Full cache > None"""
    # Priority 1: Instant quick cache
    quick = QUICK_CACHE.get(uid)
    if quick and time.time() - quick['timestamp'] < 60:  # 1 min quick cache
        return quick['groups']
    
    # Priority 2: Full cache
    cache = GROUPS_CACHE.get(uid)
    if cache and time.time() - cache['timestamp'] < GROUPS_CACHE_TTL:
        return cache['groups']
    
    # Priority 3: Expired - clean up
    QUICK_CACHE.pop(uid, None)
    GROUPS_CACHE.pop(uid, None)
    return None

def invalidate_groups_cache(uid):
    """🗑️ Force invalidate cache for user"""
    QUICK_CACHE.pop(uid, None)
    GROUPS_CACHE.pop(uid, None)
    if uid in PRELOAD_TASKS:
        PRELOAD_TASKS[uid].cancel()
        PRELOAD_TASKS.pop(uid, None)

# Legacy compatibility
async def preload_user_groups(uid):
    """Legacy compatibility wrapper"""
    return await ultra_fast_preload_groups(uid)


async def auto_select_all_groups(uid, phone):
    """Auto-select all groups for a newly added account"""
    try:
        logger.info(f"Auto-selecting all groups for user {uid}, phone {phone}")
        
        # Get the newly added account
        accounts = db.get_user_accounts(uid)
        new_account = None
        for acc in accounts:
            if acc['phone_number'] == phone:
                new_account = acc
                break
        
        if not new_account:
            logger.warning(f"Could not find newly added account {phone} for user {uid}")
            return
        
        # Fetch all groups from this account
        try:
            session_str = cipher_suite.decrypt(new_account['session_string'].encode()).decode()
            
            # Get API credentials from database (already stored permanently)
            credentials = db.get_user_api_credentials(uid)
            
            if not credentials:
                logger.error(f"No API credentials found for user {uid}")
                return
            
            async with TelegramClient(StringSession(session_str), credentials['api_id'], credentials['api_hash']) as tg_client:
                # Get existing selected groups to avoid duplicates
                existing_groups = db.get_target_groups(uid) or []
                existing_ids = {g['group_id'] for g in existing_groups}
                
                # Add all groups from this account
                added_count = 0
                async for dialog in tg_client.iter_dialogs():
                    if dialog.is_group and dialog.id not in existing_ids:
                        try:
                            db.add_target_group(uid, dialog.id, dialog.title)
                            added_count += 1
                        except Exception as e:
                            logger.warning(f"Failed to add group {dialog.title}: {e}")
                
                logger.info(f"Auto-selected {added_count} groups for user {uid}")
                
        except Exception as e:
            logger.error(f"Error fetching groups for auto-selection: {e}")
        finally:
            # Clean up temp API credentials after auto-selection is complete
            db.delete_temp_data(uid, "api_id")
            db.delete_temp_data(uid, "api_hash")
            logger.info(f"Cleaned up temp API credentials for user {uid}")
            
    except Exception as e:
        logger.error(f"Error in auto_select_all_groups: {e}")

def get_cached_groups(uid):
    """Legacy compatibility wrapper"""
    return get_ultra_fast_groups(uid)

# =======================================================
# 🧠 DATABASE INITIALIZATION
# =======================================================
try:
    db = EnhancedDatabaseManager()
    logger.info("✅ Database initialized successfully.")
    
    # Add required group methods
    ensure_db_methods(db)
    logger.info("✅ Database methods initialized successfully.")

    # Auto-reply functionality removed

except Exception as e:
    logger.error(f"❌ Failed to initialize database: {e}. Exiting.")
    print("Bot failed to start due to database error. Check logs/AzTechAdsBot.log for details.")
    exit(1)

# =======================================================
# 🔧 ACCOUNT MANAGER INITIALIZATION
# =======================================================
# REMOVED: AccountManager initialization - not needed in current broadcast system
# =======================================================

# Admin check
ADMIN_IDS = config.ADMIN_IDS  # Use the list from config (both admins)
ALLOWED_BD_IDS = ADMIN_IDS + [6670166083]

def is_owner(uid):
    return uid in ALLOWED_BD_IDS

# ================= NO PREMIUM SYSTEM - ALL FEATURES FREE =================

def get_user_api_credentials_or_error(user_id):
    """Get user API credentials or return error message"""
    try:
        credentials = db.get_user_api_credentials(user_id)
        if not credentials:
            return None, f"❌ <b>API Credentials Required</b>\n\n" \
                        f"You need to set up your API credentials first.\n\n" \
                        f"📱 <b>Get your API credentials:</b>\n" \
                        f"1. Visit https://my.telegram.org\n" \
                        f"2. Login with your phone number\n" \
                        f"3. Go to 'API Development tools'\n" \
                        f"4. Create an app and get API ID & Hash\n\n" \
                        f"Then use the bot to add your first account!"
        return credentials, None
    except Exception as e:
        logger.error(f"Error getting API credentials for {user_id}: {e}")
        return None, "Error retrieving API credentials"

# Inline keyboard helper
def kb(rows):
    if not isinstance(rows, list) or not all(isinstance(row, list) for row in rows):
        logger.error("Invalid rows format for InlineKeyboardMarkup")
        raise ValueError("Rows must be a list of lists")
    return InlineKeyboardMarkup(rows)

# ✅ Safe: ensure a loop policy is available; actual MAIN_LOOP will be set in main()
try:
    asyncio.get_running_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

# ================================================
# 🔹 Initialize Pyrogram clients
# ================================================
pyro = PyroClient(
    "AzTechAdsBot",
    api_id=config.BOT_API_ID,
    api_hash=config.BOT_API_HASH,
    bot_token=config.BOT_TOKEN,
    workdir="./sessions",  # Store session files in sessions directory
    no_updates=False  # Enable updates
)

# Initialize Telethon bot client for join verification
# This client will be started when the bot starts and used for checking membership
telethon_bot = TelegramClient(
    'sessions/telethon_bot',
    config.BOT_API_ID,
    config.BOT_API_HASH
)

logger_client = PyroClient(
    "logger_bot",
    api_id=config.BOT_API_ID,
    api_hash=config.BOT_API_HASH,
    bot_token=config.LOGGER_BOT_TOKEN,
    workdir="./sessions",  # Store session files in sessions directory
    no_updates=False  # Enable updates
)

# Create sessions directory if it doesn't exist
os.makedirs("./sessions", exist_ok=True)

# ================================================
# 🧩 PRELOAD CHAT CACHE (Prevents PeerIdInvalid)
# ================================================
async def preload_chat_cache(client):
    """Preload chat info to avoid PeerIdInvalid after restart."""
    try:
        await client.get_chat(config.MUST_JOIN_CHANNEL)
        await client.get_chat(config.MUSTJOIN_GROUP)
        logger.info("✅ Chat cache preloaded successfully")
    except Exception as e:
        logger.warning(f"⚠️ Chat cache preload failed: {e}")

# Preload chat cache will be executed in the main() async startup to avoid
# calling run_until_complete at module import time.

# ================================================
# 🧠 In-memory storage
# ================================================
user_tasks = {}

# ================================================
# 🏥 LOGGER BOT HEALTH MONITORING
# ================================================
logger_bot_last_activity = datetime.now()

async def logger_bot_health_monitor():
    """Monitor logger bot health and restart if needed"""
    global logger_client, logger_bot_last_activity
    health_logger = logging.getLogger("LoggerHealth")
    
    while True:
        try:
            await asyncio.sleep(300)  # Check every 5 minutes
            
            # Check if logger bot has been inactive for too long (30 minutes)
            inactive_time = (datetime.now() - logger_bot_last_activity).total_seconds()
            
            if inactive_time > 1800:  # 30 minutes
                health_logger.warning("🏥 Logger bot appears inactive, performing health check...")
                
                try:
                    # Test logger bot with a simple API call
                    await asyncio.wait_for(logger_client.get_me(), timeout=10.0)
                    logger_bot_last_activity = datetime.now()
                    health_logger.info("✅ Logger bot health check passed")
                    
                except Exception as e:
                    health_logger.error(f"❌ Logger bot health check failed: {e}")
                    health_logger.info("🔄 Restarting logger bot...")
                    
                    try:
                        # Stop and restart logger bot
                        await logger_client.stop()
                        await asyncio.sleep(5)
                        await logger_client.start()
                        logger_bot_last_activity = datetime.now()
                        health_logger.info("✅ Logger bot restarted successfully")
                        
                    except Exception as restart_error:
                        health_logger.error(f"💥 Failed to restart logger bot: {restart_error}")
                        
        except Exception as e:
            health_logger.error(f"Health monitor error: {e}")
            await asyncio.sleep(60)

async def update_logger_activity():
    """Update logger bot activity timestamp"""
    global logger_bot_last_activity
    logger_bot_last_activity = datetime.now()


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

async def send_logger_message(user_id: int, text: str):
    """
    Send a short log message to the user's logger bot DM (the user who started the bot).
    Enhanced with retry logic and error handling to prevent crashes.
    """
    max_retries = 3
    retry_delay = 2
    
    for attempt in range(max_retries):
        try:
            if not db.get_logger_status(user_id):
                # User has not started the logger bot; just skip silently
                return
            
            # Add timeout to prevent hanging
            await asyncio.wait_for(
                logger_client.send_message(user_id, text, parse_mode=ParseMode.HTML),
                timeout=10.0
            )
            await update_logger_activity()  # Update activity timestamp
            return  # Success, exit retry loop
            
        except asyncio.TimeoutError:
            logger.warning(f"Logger message timeout for user {user_id} (attempt {attempt + 1})")
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay * (attempt + 1))
                continue
            db.log_logger_failure(user_id, "Timeout sending logger message")
            
        except PeerIdInvalid:
            # Peer invalid — record failure and attempt to notify user via main bot
            db.log_logger_failure(user_id, "PeerIdInvalid: User must start logger bot")
            try:
                # notify via main bot pyro if possible
                await pyro.send_message(
                    user_id,
                    "<b>⚠️ Logger bot not started!</b>\n\n"
                    f"Please start @{config.LOGGER_BOT_USERNAME} to receive log updates.",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb([[
                        InlineKeyboardButton("Start Logger Bot 📩", url=f"https://t.me/{config.LOGGER_BOT_USERNAME.lstrip('@')}")
                    ]])
                )
            except Exception:
                # silent fallback
                pass
            return  # Don't retry PeerIdInvalid
            
        except (TimeoutError, ConnectionError, OSError) as e:
            logger.warning(f"Network error sending logger message to {user_id} (attempt {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay * (attempt + 1))
                continue
            db.log_logger_failure(user_id, f"Network error: {str(e)}")
            
        except Exception as e:
            # record error and continue
            logger.error(f"Logger message error for user {user_id} (attempt {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
                continue
            db.log_logger_failure(user_id, str(e))


async def send_dm_log(user_id: int, log_message: str):
    """
    Send DM log to a specific user via their logger-bot DM.
    Enhanced with retry logic and error handling to prevent crashes.
    """
    max_retries = 3
    retry_delay = 2
    
    for attempt in range(max_retries):
        try:
            if not db.get_logger_status(user_id):
                # If user hasn't started logger bot, skip silently.
                return
            
            # Add timeout to prevent hanging
            await asyncio.wait_for(
                logger_client.send_message(user_id, log_message, parse_mode=ParseMode.HTML),
                timeout=10.0
            )
            await update_logger_activity()  # Update activity timestamp
            return  # Success, exit retry loop
            
        except asyncio.TimeoutError:
            logger.warning(f"DM log timeout for user {user_id} (attempt {attempt + 1})")
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay * (attempt + 1))
                continue
            db.log_logger_failure(user_id, "Timeout sending DM log")
            
        except PeerIdInvalid:
            db.log_logger_failure(user_id, "PeerIdInvalid: User must start logger bot")
            return  # Don't retry PeerIdInvalid
            
        except (TimeoutError, ConnectionError, OSError) as e:
            logger.warning(f"Network error sending DM log to {user_id} (attempt {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay * (attempt + 1))
                continue
            db.log_logger_failure(user_id, f"Network error: {str(e)}")
            
        except Exception as e:
            logger.error(f"DM log error for user {user_id} (attempt {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
                continue
            db.log_logger_failure(user_id, str(e))



# =======================================================
# 🔧 PROFILE UPDATE FUNCTION (Per-User Logger Integrated)
# =======================================================

# REMOVED: update_profile_info function - No bio or last name changes anymore

# REMOVED: Duplicate imports (time already imported, errors already at top)

JOIN_CACHE = {}  # simple in-memory cache

async def is_joined(client, uid, chat_id):
    """
    Fast + reliable join check using numeric chat IDs.
    Auto-resolves peers, caches results, and repairs PeerIdInvalid after restarts.
    """
    cache_key = f"{uid}:{chat_id}"
    if cache_key in JOIN_CACHE and (time.time() - JOIN_CACHE[cache_key]) < 300:
        # Use cached result if it’s still fresh (5 min)
        return True

    max_retries = 2
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Checking membership for user {uid} in chat_id {chat_id} (Attempt {attempt})")

            # ✅ Force Telegram to load this chat entity
            await client.resolve_peer(chat_id)

            member = await client.get_chat_member(chat_id, uid)
            logger.info(f"User {uid} is a member of chat_id {chat_id}")
            JOIN_CACHE[cache_key] = time.time()
            return True

        except UserNotParticipant:
            logger.info(f"User {uid} is not a member of chat_id {chat_id}")
            return False

        except (PeerIdInvalid, ChannelInvalidError) as e:
            logger.warning(f"Repairing peer cache for {chat_id}: {e}")
            try:
                chat = await client.get_chat(chat_id)
                member = await client.get_chat_member(chat.id, uid)
                logger.info(f"User {uid} is a member of chat_id {chat.id}")
                JOIN_CACHE[cache_key] = time.time()
                return True
            except Exception as e2:
                logger.error(f"Peer repair failed for {chat_id}: {e2}")

        except Exception as e:
            logger.error(f"Join check failed for {uid} in {chat_id}: {e}")

        await asyncio.sleep(1.5)

    logger.error(f"All retries failed for user {uid} in chat_id {chat_id}")
    return False

async def is_joined_all(client, uid):
    """Check if user has joined both required channels/groups using Pyrogram."""
    channel_joined = await is_joined(client, uid, config.MUST_JOIN_CHANNEL)
    await asyncio.sleep(0.5)
    group_joined = await is_joined(client, uid, config.MUSTJOIN_GROUP)
    logger.info(f"User {uid} - Channel (@{config.MUST_JOIN_CHANNEL}) joined: {channel_joined}, Group (@{config.MUSTJOIN_GROUP}) joined: {group_joined}")
    if not channel_joined:
        logger.info(f"User {uid} has not joined channel @{config.MUST_JOIN_CHANNEL}")
    if not group_joined:
        logger.info(f"User {uid} has not joined group @{config.MUSTJOIN_GROUP}")
    return channel_joined and group_joined

# =======================================================
# 🚀 TELETHON-BASED INSTANT JOIN VERIFICATION
# =======================================================

async def instant_join_check(bot_client, user_id, chat_username):
    """
    Instant join verification using bot's Telethon client as admin.
    Bot must be admin in the channel/group to check membership.
    Uses username instead of ID - cleaner and more maintainable.
    FAST and RELIABLE - no temporary client creation needed.
    """
    try:
        # Use bot's existing Telethon client
        # Bot is admin in channel/group, so it can check membership
        try:
            # Get the participant using username - if this succeeds, user is a member
            participant = await bot_client(functions.channels.GetParticipantRequest(
                channel=chat_username,  # Use username instead of ID
                participant=user_id
            ))
            
            if participant:
                logger.info(f"✅ User {user_id} is a member of @{chat_username}")
                return True
                
        except Exception as e:
            error_str = str(e).lower()
            if 'user not participant' in error_str or 'participant' in error_str or 'user_not_participant' in error_str:
                logger.info(f"❌ User {user_id} is NOT a member of @{chat_username}")
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
        
        # Check both in parallel for speed using bot's admin access
        channel_check, group_check = await asyncio.gather(
            instant_join_check(bot_client, user_id, channel_username),
            instant_join_check(bot_client, user_id, group_username),
            return_exceptions=True
        )
        
        # Handle exceptions
        channel_joined = channel_check if not isinstance(channel_check, Exception) else False
        group_joined = group_check if not isinstance(group_check, Exception) else False
        
        logger.info(f"📊 Verification result for {user_id}: @{channel_username}={channel_joined}, @{group_username}={group_joined}")
        
        return channel_joined and group_joined
        
    except Exception as e:
        logger.error(f"Error in verify_all_joins for user {user_id}: {e}")
        return False

async def validate_session(session_str, user_id=None):
    """Validate Telegram session string."""
    try:
        # For session validation, we'll use config temporarily
        # In a full implementation, you'd pass user_id and get their credentials
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

    # Reset cycle counter to 0 so next broadcast starts from first message
    try:
        db.reset_ad_cycle(uid)
        logger.info(f"🔄 Reset cycle counter to 0 for user {uid}")
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
        [InlineKeyboardButton("⌫", callback_data="otp_back"), InlineKeyboardButton("0", callback_data="otp_0"), InlineKeyboardButton("❌", callback_data="otp_cancel")],
        [InlineKeyboardButton("Show Code", url="tg://openmessage?user_id=777000")]
    ]
    return kb(rows)

# =======================================================
# ⏱️ GROUP MESSAGE DELAY HANDLERS
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
                caption=f"""<blockquote><b>⏱️ GROUP MESSAGE DELAY</b></blockquote>

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
                [InlineKeyboardButton("3 Seconds ⚡⚡", callback_data="group_delay_3"),
                 InlineKeyboardButton("5 Seconds ⚡", callback_data="group_delay_5")],
                [InlineKeyboardButton("10 Seconds 🔷", callback_data="group_delay_10"),
                 InlineKeyboardButton("15 Seconds ✅", callback_data="group_delay_15")],
                [InlineKeyboardButton("30 Seconds 🛡️", callback_data="group_delay_30")],
                [InlineKeyboardButton("Back 🔙", callback_data="menu_main")]
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
            caption=f"""<blockquote><b>✅ GROUP DELAY UPDATED!</b></blockquote>

<b>New Delay:</b> <code>{delay} seconds</code>
<i>This will be used for your next broadcast</i>""",
            reply_markup=kb([[InlineKeyboardButton("Back", callback_data="menu_main")]]),
            parse_mode=ParseMode.HTML
        )
        await callback_query.answer(f"Group message delay set to {delay}s ✨", show_alert=True)
        logger.info(f"Group delay set to {delay}s for user {uid}")
        
    except Exception as e:
        logger.error(f"Error in group_delay_select callback: {e}")
        await callback_query.answer("Error setting delay. Try again.", show_alert=True)

# =======================================================
# REMOVED: Blacklist handlers - no blacklist system
# =======================================================

# @pyro.on_callback_query(filters.regex("^clear_all_blacklisted$"))
async def clear_all_blacklisted_callback_DISABLED(client, callback_query):
    """🗑️ Clear all blacklisted groups for the user"""
    try:
        uid = callback_query.from_user.id
        
        # Get current count of blacklisted groups
        blacklisted_count = db.get_blacklisted_groups_count(uid)
        
        if blacklisted_count == 0:
            await callback_query.answer("❌ No blacklisted groups found!", show_alert=True)
            return
        
        # Show confirmation dialog
        await callback_query.message.edit_media(
            media=InputMediaPhoto(
                media=config.START_IMAGE,
                caption=f"""<blockquote><b>🗑️ CLEAR ALL BLACKLISTED GROUPS</b></blockquote>

<b>⚠️ Confirmation Required</b>

You have <code>{blacklisted_count}</code> blacklisted groups.

<i>Are you sure you want to clear ALL blacklisted groups?</i>
This action cannot be undone!

<b>What are blacklisted groups?</b>
Groups that were automatically blacklisted due to:
• Send message restrictions
• Flood wait errors  
• Permission issues
• Other broadcasting errors""",
                parse_mode=ParseMode.HTML
            ),
            reply_markup=kb([
                [InlineKeyboardButton("✅ Yes, Clear All", callback_data="confirm_clear_blacklisted"),
                 InlineKeyboardButton("❌ Cancel", callback_data="menu_groups")]
            ])
        )
        
        logger.info(f"Clear blacklisted groups confirmation shown for user {uid} - {blacklisted_count} groups")
        
    except Exception as e:
        logger.error(f"Error in clear_all_blacklisted callback: {e}")
        await callback_query.answer("❌ Error accessing blacklisted groups. Try again.", show_alert=True)

# @pyro.on_callback_query(filters.regex("^confirm_clear_blacklisted$"))
async def confirm_clear_blacklisted_callback_DISABLED(client, callback_query):
    """🗑️ Confirm and execute clearing all blacklisted groups"""
    try:
        uid = callback_query.from_user.id
        
        # Show processing message
        await callback_query.answer("🗑️ Clearing blacklisted groups...", show_alert=False)
        
        # Update UI to show processing
        await callback_query.message.edit_media(
            media=InputMediaPhoto(
                media=config.START_IMAGE,
                caption="<blockquote><b>🗑️ CLEARING BLACKLISTED GROUPS</b></blockquote>\n\n⚡ Processing...\n🔄 <i>Removing all blacklisted groups from database</i>",
                parse_mode=ParseMode.HTML
            ),
            reply_markup=kb([[InlineKeyboardButton("Please wait... ⏳", callback_data="noop")]])
        )
        
        # Clear all blacklisted groups from database
        start_time = time.time()
        deleted_count = db.clear_all_blacklisted_groups(uid)
        clear_time = time.time() - start_time
        
        # Success message
        await callback_query.message.edit_media(
            media=InputMediaPhoto(
                media=config.START_IMAGE,
                caption=f"""<blockquote><b>✅ BLACKLISTED GROUPS CLEARED!</b></blockquote>

<b>📊 Results:</b>
• Cleared Groups: <code>{deleted_count}</code>
• Processing Time: <code>{clear_time:.2f}s</code>

<i>🎯 All blacklisted groups have been removed from your database!</i>

<b>What this means:</b>
• Previously restricted groups can now be used again
• Broadcast will attempt to send to these groups
• Groups may get re-blacklisted if issues persist""",
                parse_mode=ParseMode.HTML
            ),
            reply_markup=kb([
                [InlineKeyboardButton("Back to Groups Menu 🔙", callback_data="menu_groups")]
            ])
        )
        
        # Show success popup
        await callback_query.answer(f"✅ Blacklisted groups cleared! ({deleted_count} groups)", show_alert=True)
        
        logger.info(f"✅ Cleared {deleted_count} blacklisted groups for user {uid} in {clear_time:.2f}s")
        
    except Exception as e:
        logger.error(f"Error in confirm_clear_blacklisted callback: {e}")
        await callback_query.answer("❌ Error clearing blacklisted groups. Please try again.", show_alert=True)
        
        try:
            await callback_query.message.edit_media(
                media=InputMediaPhoto(
                    media=config.START_IMAGE,
                    caption="<blockquote><b>❌ ERROR CLEARING BLACKLISTED GROUPS</b></blockquote>\n\nSomething went wrong. Please try again.",
                    parse_mode=ParseMode.HTML
                ),
                reply_markup=kb([
                    [InlineKeyboardButton("Try Again 🔄", callback_data="clear_all_blacklisted")],
                    [InlineKeyboardButton("Back 🔙", callback_data="menu_groups")]
                ])
            )
        except Exception:
            pass

# =======================================================



# =======================================================
# 📡 ULTRA-FAST GROUP ANALYSIS (2 seconds total)
# =======================================================

def generate_analysis_report(analysis_results, account_phone):
    """Generate a formatted analysis report"""
    report = f"<b>📱 {account_phone}</b>\n"
    report += f"• Total Groups: {analysis_results['total_groups']}\n"
    report += f"• Restricted: {analysis_results['total_restricted']}\n"
    report += f"• Slow Mode: {analysis_results['total_slow_mode']}\n"
    report += f"• Usable: {analysis_results['total_usable']}\n\n"
    
    # Add details about restrictions if any
    if analysis_results['total_restricted'] > 0:
        report += "<i>Restricted Groups:</i>\n"
        for group in analysis_results['restricted_groups'][:3]:  # Show first 3
            report += f"  - {group['title'][:20]}: {group['permission_info']}\n"
        if analysis_results['total_restricted'] > 3:
            report += f"  ... and {analysis_results['total_restricted'] - 3} more\n"
        report += "\n"
    
    return report

async def analyze_account_groups_fast(tg_client, account_phone, target_group_ids=None, skip_group_ids=None):
    """PRO MAX LEVEL group analysis - skips slow mode and high spam groups for maximum efficiency"""
    try:
        if skip_group_ids is None:
            skip_group_ids = []  # No IDs to skip when using usernames
            
        all_groups = []
        skipped_groups = []
        usable_groups = []
        
        # Get all dialogs with increased limit
        dialogs = await tg_client.get_dialogs(limit=500)
        
        for dialog in dialogs:
            if not dialog.is_group:
                continue
                
            # Skip protected groups
            if dialog.id in skip_group_ids:
                continue
                
            # If target groups specified, only analyze those
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
                
                # SKIP: Groups with slow mode enabled (no waiting)
                if hasattr(chat, 'slowmode_seconds') and chat.slowmode_seconds > 0:
                    logger.debug(f"Skipping slow mode group: {dialog.title} ({chat.slowmode_seconds}s)")
                    skipped_groups.append({'id': dialog.id, 'title': dialog.title, 'reason': 'SLOW_MODE'})
                    continue
                
                # SKIP: High spam/restricted groups (gigagroups with restrictions)
                if hasattr(chat, 'participants_count') and chat.participants_count > 200000:
                    # Very large groups often have strict spam filters
                    logger.debug(f"Skipping high spam risk group: {dialog.title} ({chat.participants_count} members)")
                    skipped_groups.append({'id': dialog.id, 'title': dialog.title, 'reason': 'HIGH_SPAM_RISK'})
                    continue
                
                # PRO: Store the entity to avoid peer resolution errors later
                try:
                    group_data['entity'] = await tg_client.get_entity(dialog.id)
                except Exception as entity_err:
                    logger.debug(f"Entity cache for {dialog.title}: {entity_err}")
                    group_data['entity'] = chat  # Use dialog entity as fallback
                
                # PRO: Detect forum groups (topics support)
                if hasattr(chat, 'forum') and chat.forum:
                    group_data['is_forum'] = True
                else:
                    group_data['is_forum'] = False
                
                # PRO: Check if it's a megagroup
                if hasattr(chat, 'megagroup'):
                    group_data['is_megagroup'] = chat.megagroup
                
                # Add to usable groups
                usable_groups.append(group_data)
                all_groups.append(group_data)
                
            except Exception as e:
                # Even if detailed analysis fails, still add to usable groups
                logger.debug(f"Detailed analysis skipped for {dialog.title}: {e}")
                group_data['entity'] = dialog.entity  # Use basic entity
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
# 🚀 RUN BROADCAST (Clean Logs + FloodWait Skip + Summary)
# =======================================================
# REMOVED: Duplicate imports (already imported at top)

async def run_broadcast(client, uid):
    """Run broadcast with clean logs, cycle-wise profile updates, FloodWait handling, and summary reports."""
    try:
        # Ensure we have a fresh DB object for safe concurrent runs
        global db
        db = EnhancedDatabaseManager()

        sent_count = 0
        failed_count = 0
        cycle_count = 0

        # Get settings
        delay = db.get_user_ad_delay(uid)
        group_msg_delay = db.get_user_group_msg_delay(uid)
        
        # Get current cycle for message rotation from Saved Messages
        current_cycle = db.get_current_ad_cycle(uid) if hasattr(db, 'get_current_ad_cycle') else db.get_ad_cycle(uid)
        
        # No rest period - continuous broadcasting
        broadcast_start_time = datetime.utcnow()
        cycle_timeout = db.get_user_cycle_timeout(uid) if hasattr(db, "get_user_cycle_timeout") else 900

        accounts = db.get_user_accounts(uid) or []
        target_groups = db.get_target_groups(uid) or []
        target_group_ids = [g["group_id"] for g in target_groups] if target_groups else []
        skip_group_ids = []  # No skip groups needed when using usernames

        if not target_groups:
            await client.send_message(uid,
                                     "<b>❌ No target groups selected!</b>\n\nPlease select target groups first from the Groups Menu.",
                                     parse_mode=ParseMode.HTML)
            return

        # ============================================================
        # 🔍 PRE-BROADCAST GROUP ANALYSIS & AUTO-FILTERING
        # ============================================================
        await send_dm_log(uid,
            "<blockquote><b>🔍 ANALYZING GROUPS</b></blockquote>\n\n"
            "Checking all selected groups for restrictions...\n"
            "This may take a moment ⏳"
        )
        
        logger.info(f"🔍 Starting INSTANT pre-broadcast group analysis for user {uid}")
        
        # Get Telethon client ONCE for all checks (much faster)
        if not accounts:
            logger.error("No accounts available for analysis")
            return
            
        first_account = accounts[0]
        session_encrypted = first_account.get("session_string")
        credentials = db.get_user_api_credentials(uid)
        
        # Decrypt session using cipher_suite
        from cryptography.fernet import Fernet
        cipher_suite = Fernet(config.ENCRYPTION_KEY.encode())
        session_str = cipher_suite.decrypt(session_encrypted.encode()).decode()
        
        # Create ONE persistent client for all checks
        tg_client = TelegramClient(
            StringSession(session_str),
            credentials['api_id'],
            credentials['api_hash']
        )
        await tg_client.connect()
        
        # Define async function to check a single group
        async def check_group(group):
            group_id = group.get("group_id")
            # Try multiple possible keys for group name
            group_title = group.get("title") or group.get("group_name") or group.get("name") or "Unknown Group"
            
            try:
                # Try to get the group entity
                group_entity = await tg_client.get_entity(group_id)
                
                # Get actual group title from Telegram
                if hasattr(group_entity, 'title'):
                    group_title = group_entity.title
                elif hasattr(group_entity, 'name'):
                    group_title = group_entity.name
                
                # Check permissions
                if hasattr(group_entity, 'default_banned_rights'):
                    rights = group_entity.default_banned_rights
                    if rights and rights.send_messages:
                        logger.warning(f"⚠️ Group '{group_title}' - No send permission")
                        return None, {
                            "id": group_id,
                            "title": group_title,
                            "reason": "No send permission"
                        }
                
                # Check if user is admin or can send
                if hasattr(group_entity, 'admin_rights'):
                    logger.info(f"✅ Group '{group_title}' - Usable (admin)")
                    return group, None
                else:
                    logger.info(f"✅ Group '{group_title}' - Usable")
                    return group, None
                    
            except ChatWriteForbiddenError:
                logger.warning(f"⚠️ Group '{group_title}' - Write forbidden")
                return None, {
                    "id": group_id,
                    "title": group_title,
                    "reason": "Write forbidden"
                }
                
            except ValueError:
                logger.warning(f"⚠️ Group '{group_title}' - Not a member")
                return None, {
                    "id": group_id,
                    "title": group_title,
                    "reason": "Not a member"
                }
                
            except Exception as e:
                logger.error(f"Error checking group '{group_title}': {e}")
                # Include in usable groups to avoid over-filtering
                return group, None
        
        # Check ALL groups in PARALLEL for instant results
        total_selected = len(target_groups)
        results = await asyncio.gather(*[check_group(group) for group in target_groups], return_exceptions=True)
        
        # Disconnect client after all checks
        await tg_client.disconnect()
        
        # Process results
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
        
        # Update target groups to only usable ones
        if restricted_groups:
            # Auto-unselect restricted groups from database
            # Clear all target groups and re-add only usable ones
            db.db.users.update_one(
                {"user_id": uid},
                {"$set": {"target_groups": usable_groups}}
            )
            target_groups = usable_groups
            target_group_ids = [g["group_id"] for g in target_groups] if target_groups else []
            
            # Send analysis report
            restricted_list = "\n".join([
                f"• {g['title']} - {g['reason']}"
                for g in restricted_groups[:10]  # Show max 10
            ])
            
            if len(restricted_groups) > 10:
                restricted_list += f"\n• ... and {len(restricted_groups) - 10} more"
            
            analysis_report = (
                f"<blockquote><b>📊 GROUP ANALYSIS COMPLETE</b></blockquote>\n\n"
                f"<b>Total Selected:</b> {total_selected}\n"
                f"<b>✅ Usable:</b> {len(usable_groups)}\n"
                f"<b>⚠️ Restricted:</b> {len(restricted_groups)}\n\n"
                f"<b>Restricted Groups (auto-unselected):</b>\n{restricted_list}\n\n"
                f"<i>Broadcasting will only send to {len(usable_groups)} usable groups.</i>"
            )
            
            await send_dm_log(uid, analysis_report)
            logger.info(f"📊 Analysis complete: {len(usable_groups)} usable, {len(restricted_groups)} restricted")
        else:
            await send_dm_log(uid,
                f"<blockquote><b>✅ ALL GROUPS USABLE</b></blockquote>\n\n"
                f"All {len(target_groups)} selected groups are ready for broadcasting!"
            )
            logger.info(f"✅ All {len(target_groups)} groups are usable")
        
        # Check if we have any usable groups left
        if not usable_groups:
            await send_dm_log(uid,
                "<b>❌ NO USABLE GROUPS</b>\n\n"
                "All selected groups have restrictions. Please check your groups and try again."
            )
            return

        # -------------------------
        # Initial broadcast start message (clean and simple)
        # -------------------------
        await send_dm_log(uid, f"<b>🚀 Broadcast Started!</b>\n\n📊 Target Groups: {len(target_groups)}\n💬 Messages will be forwarded from your Saved Messages")

        analysis_results = {}
        clients = {}
        usable_groups_map = {}

        # REMOVED: Blacklist check - sending to all groups without restrictions

        # Prepare clients + perform analysis in parallel
        analysis_tasks = []
        for acc in accounts:
            try:
                session_encrypted = acc.get("session_string") or ""
                session_str = cipher_suite.decrypt(session_encrypted.encode()).decode()
                if not await validate_session(session_str):
                    db.deactivate_account(acc["_id"])
                    continue

                # Get user's API credentials
                credentials = db.get_user_api_credentials(acc['user_id'])
                if not credentials:
                    logger.error(f"No API credentials found for user {acc['user_id']}")
                    continue
                
                tg_client = TelegramClient(StringSession(session_str), credentials['api_id'], credentials['api_hash'])
                await tg_client.start()
                clients[acc["_id"]] = tg_client

                # REMOVED: No profile updates (bio/last name) anymore

                # schedule group analysis (fast)
                task = analyze_account_groups_fast(tg_client, acc["phone_number"], target_group_ids, skip_group_ids)
                analysis_tasks.append((acc["_id"], task))

            except Exception as e:
                await send_dm_log(uid, f"<b>❌ Failed to start account {acc.get('phone_number','unknown')}:</b> {str(e)}")

        # Wait and collect results
        if analysis_tasks:
            completed = await asyncio.gather(*[t for _, t in analysis_tasks], return_exceptions=True)
            for (acc_id, _), res in zip(analysis_tasks, completed):
                if isinstance(res, Exception):
                    continue
                analysis_results[acc_id] = res
                filtered = res.get("usable_groups", [])  # No blacklist filtering
                usable_groups_map[acc_id] = filtered

        # Simple summary message
        total_usable = sum(len(v) for v in usable_groups_map.values())
        
        await send_dm_log(uid,
            f"<b>✅ Setup Complete</b>\n\n"
            f"📊 Accounts: {len(clients)}\n"
            f"🎯 Usable Groups: {total_usable}\n"
            f"⏱️ Cycle: {delay}s | Group Delay: {group_msg_delay}s\n\n"
            f"🚀 <b>Broadcasting now...</b>"
        )

        if total_usable == 0:
            await send_dm_log(uid, "<b>❌ No usable target groups found!</b>")
            for cl in clients.values():
                try:
                    await cl.disconnect()
                except:
                    pass
            return

        db.set_broadcast_state(uid, running=True)

        # Broadcast loop
        working_groups_map = {acc_id: groups.copy() for acc_id, groups in usable_groups_map.items()}
        # REMOVED: Blacklist tracking - all groups will receive messages
        last_message_time = {}
        
        # Temporary failed groups tracking (auto-unselect for 10 minutes)
        temp_failed_groups = {}  # {group_id: (fail_time, error_message)}
        TEMP_FAIL_DURATION = 600  # 10 minutes in seconds
        
        # Start account health monitoring
        account_monitor.start_monitoring(uid)
        monitoring_tasks = []
        for acc_id, tg_client in clients.items():
            task = asyncio.create_task(account_monitor.monitor_account(tg_client, acc_id, uid))
            monitoring_tasks.append(task)
        logger.info(f"🔍 Started monitoring {len(monitoring_tasks)} accounts for user {uid}")

        try:
            while db.get_broadcast_state(uid).get("running", False):
                cycle_count += 1

                # REMOVED: Profile update code - not used anymore

                # Cycle sending
                for acc in accounts:
                    acc_id = acc["_id"]
                    
                    # Check if account was banned during monitoring
                    if account_monitor.is_account_banned(acc_id):
                        logger.warning(f"⚠️ Skipping banned account {acc_id}")
                        working_groups_map.pop(acc_id, None)
                        clients.pop(acc_id, None)
                        continue
                    
                    tg_client = clients.get(acc_id)
                    if not tg_client:
                        continue
                    working_groups = working_groups_map.get(acc_id, [])
                    for group in working_groups[:]:
                        if not db.get_broadcast_state(uid).get("running", False):
                            raise asyncio.CancelledError("Stopped by user")

                        # REMOVED: Blacklist check - sending to ALL groups

                        # Fetch messages from user's Saved Messages chat dynamically
                        try:
                            # Get user's saved messages count setting
                            user_msg_count = db.get_user_saved_messages_count(uid)
                            
                            # Get messages from Saved Messages (fetch more than needed to ensure we have enough)
                            saved_msgs_list = []
                            messages = await tg_client.get_messages("me", limit=20)
                            
                            for msg in messages:
                                if msg.text or msg.media:  # Only messages with text or media
                                    saved_msgs_list.append(msg)
                            
                            if not saved_msgs_list:
                                logger.warning(f"No messages found in Saved Messages for user {uid}")
                                continue
                            
                            # FIXED: Reverse the list to use oldest messages first (1st, 2nd, 3rd order)
                            saved_msgs_list.reverse()  # Now oldest message is at index 0
                            
                            # Limit to user's selected count (default 3)
                            saved_msgs_list = saved_msgs_list[:user_msg_count]
                            
                            # Select message based on current cycle (one message per complete cycle)
                            # Cycle 1 -> Message 1, Cycle 2 -> Message 2, etc., then loops back
                            msg_index = current_cycle % len(saved_msgs_list)
                            current_saved_msg = saved_msgs_list[msg_index]
                            
                            logger.debug(f"Cycle {current_cycle + 1}: Using message {msg_index + 1} of {len(saved_msgs_list)} from Saved Messages")
                            
                        except Exception as e:
                            logger.error(f"Error fetching Saved Messages for user {uid}: {e}")
                            continue

                        # No slow mode handling - those groups are skipped during analysis
                        current_delay = group_msg_delay

                        try:
                            # Forward message with proper peer handling
                            # Get the group entity first to ensure valid peer
                            try:
                                group_entity = await tg_client.get_entity(group["id"])
                            except Exception as peer_err:
                                logger.warning(f"Failed to get entity for group {group['id']}: {peer_err}")
                                failed_count += 1
                                continue
                            
                            # Forward message from Saved Messages
                            await tg_client.forward_messages(
                                entity=group_entity,
                                messages=current_saved_msg,
                                from_peer="me"
                            )
                            sent_count += 1
                            db.increment_broadcast_stats(uid, True)
                            last_message_time[f"{acc['_id']}_{group['id']}"] = time.time()

                            # REPORT single-group send to user's logger via DM (only)
                            await send_dm_log(uid,
                                f"✅ <b>Forwarded to</b> {group.get('title','Unknown')}\n"
                                f"📱 Account: <code>{acc.get('phone_number')}</code>\n"
                                f"📨 Message: Saved Message #{(msg_index + 1)} (Cycle {current_cycle + 1})"
                            )

                            await asyncio.sleep(current_delay)

                        except FloodWait as e:
                            wait_time = int(getattr(e, "value", 0) or getattr(e, "x", 0) or 1)
                            # REMOVED: Blacklist addition - continuing without blocking
                            failed_count += 1
                            
                            # Send clean DM log for FloodWait
                            await send_dm_log(uid,
                                f"<blockquote>⏳ <b>Rate Limited</b></blockquote>\n\n"
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
                            
                            # Check if it's a permanent restriction
                            is_permanent = any(k in err_lower for k in ["banned", "forbidden", "kicked", "rights", "not enough", "restricted", "chat_write_forbidden"])
                            
                            if is_permanent:
                                # Permanently unselect - DON'T retry
                                failed_count += 1
                                
                                # Determine reason
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
                                    reason = "Access Denied"
                                
                                # Permanently remove from working groups
                                try:
                                    working_groups.remove(group)
                                except ValueError:
                                    pass
                                
                                await send_dm_log(uid,
                                    f"<b>🚫 Permanently Unselected</b>\n"
                                    f"<b>Group:</b> {group.get('title','Unknown')}\n"
                                    f"<b>Reason:</b> {reason}\n"
                                    f"<b>Action:</b> Group removed (no retry)"
                                )
                                logger.warning(f"Permanent error for group {group['id']}: {reason}")
                            else:
                                # Temporary error - retry after 10 minutes
                                temp_failed_groups[group["id"]] = (time.time(), error_msg)
                                failed_count += 1
                                
                                await send_dm_log(uid,
                                    f"<b>⏰ Temporarily Unselected</b>\n"
                                    f"<b>Group:</b> {group.get('title','Unknown')}\n"
                                    f"<b>Error:</b> {error_msg[:80]}\n"
                                    f"<b>Unselected for:</b> 10 minutes\n"
                                    f"<b>🔄 Auto-retry:</b> Broadcast will restart"
                                )
                                logger.warning(f"Temp error for group {group['id']}, retry in 10 min")
                            
                            continue

                        except Exception as e:
                            error_msg = str(e)
                            err = error_msg.lower()
                            
                            # Add to temporary failed groups (10-minute timeout)
                            temp_failed_groups[group["id"]] = (time.time(), error_msg)
                            
                            failed_count += 1
                            
                            # Determine clean reason for display
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
                            else:
                                reason = error_msg[:50]
                            
                            # Send detailed failure log with auto-unselect info
                            await send_dm_log(uid,
                                f"<b>❌ Send Failed - Group Temporarily Unselected</b>\n"
                                f"<b>Group:</b> {group.get('title','Unknown')}\n"
                                f"<b>Reason:</b> {reason}\n"
                                f"<b>⏰ Unselected for:</b> 10 minutes\n"
                                f"<b>🔄 Auto-retry:</b> After timeout expires"
                            )
                            
                            # Check if it's a permanent ban/restriction
                            is_permanent = any(k in err for k in ["banned", "forbidden", "kicked", "rights", "not enough"])
                            
                            if is_permanent:
                                # Remove from working_groups for this cycle
                                try:
                                    working_groups.remove(group)
                                except ValueError:
                                    pass
                            else:
                                # Temporary error - will retry
                                # REMOVED: Blacklist addition - continuing without blocking
                                
                                # Clean error reason
                                if "peer" in err:
                                    reason = "Invalid Peer"
                                elif "timeout" in err or "network" in err:
                                    reason = "Network Timeout"
                                elif "monoforum" in err or "reply" in err:
                                    reason = "Forum Error"
                                else:
                                    reason = str(e)[:40] + "..." if len(str(e)) > 40 else str(e)
                                
                                # Send clean DM log for temporary errors
                                await send_dm_log(uid,
                                    f"<blockquote>⚠️ <b>Temporary Error</b></blockquote>\n\n"
                                    f"<b>Group:</b> {group.get('title', 'Unknown')}\n"
                                    f"<b>Reason:</b> {reason}\n"
                                    f"<b>Action:</b> Will retry in next cycle\n\n"
                                    f"<i>Temporary issue. Retrying next cycle.</i>"
                                )
                                
                                logger.warning(f"Temporary error for group {group['id']}: {err[:80]}, will retry next cycle")
                            continue

                # No rest period - continuous broadcasting
                # End-of-cycle summary sent once per cycle
                # Update cycle counter for next message rotation
                if hasattr(db, 'increment_broadcast_cycle'):
                    db.increment_broadcast_cycle(uid)
                else:
                    db.update_ad_cycle(uid)
                
                # IMPORTANT: Update the local current_cycle variable from database
                current_cycle = db.get_current_ad_cycle(uid) if hasattr(db, 'get_current_ad_cycle') else db.get_ad_cycle(uid)
                logger.debug(f"Updated current_cycle to {current_cycle} for next iteration")
                
                # Get user's saved messages count for correct next message display
                user_msg_count = db.get_user_saved_messages_count(uid)
                next_msg_num = (current_cycle % user_msg_count) + 1
                
                await send_dm_log(uid,
                    f"<b>✅ Cycle {cycle_count} Completed</b>\n"
                    f"📤 Sent: {sent_count}\n"
                    f"❌ Failed: {failed_count}\n"
                    f"🕒 Next cycle in: {delay}s\n"
                    f"📨 Next message: #{next_msg_num} from Saved Messages"
                )

                # Safety cooldown every 5 cycles (adds extra timeout on top of regular delay)
                if cycle_count % 5 == 0:
                    logger.info(f"Cycle {cycle_count}: Adding safety cooldown of {cycle_timeout}s + regular delay of {delay}s")
                    await asyncio.sleep(cycle_timeout)

                # Regular cycle interval (used after every cycle)
                logger.info(f"Waiting {delay} seconds before next cycle for user {uid}")
                await asyncio.sleep(delay)

        except asyncio.CancelledError:
            # broadcast stopped by user
            raise

        finally:
            # Stop account monitoring
            account_monitor.stop_monitoring(uid)
            for task in monitoring_tasks:
                task.cancel()
            logger.info(f"🔍 Stopped monitoring accounts for user {uid}")
            
            # Cleanup clients
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
        
        # Stop monitoring on error
        account_monitor.stop_monitoring(uid)
        try:
            for task in monitoring_tasks:
                task.cancel()
        except:
            pass
        
        await send_dm_log(uid, f"<b>❌ Broadcast task failed:</b> {str(e)}")
        # notify admins minimally (no spam)
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

        # Create user record in DB
        db.create_user(uid, username, first_name)

        # All users are equal - no premium system
        if is_owner(uid):
            logger.info(f"Admin user {uid} accessing bot - same 1 account limit as regular users")
        else:
            # Regular user - everyone gets same status
            existing_status = db.get_user_status(uid)
            if not existing_status or existing_status.get("user_type") is None:
                # New user - set default free status
                db.set_user_status(uid, "free", 1, None)
                logger.info(f"New user {uid} set to default free status")
            else:
                # Existing user - preserve their current status
                current_type = existing_status.get("user_type", "free")
                current_limit = existing_status.get("accounts_limit", 1)
                logger.info(f"Preserving existing user_type '{current_type}' with {current_limit} accounts limit for user {uid}")

        # Update last interaction
        db.update_user_last_interaction(uid)

        # ✅ Force Join Check (if enabled)
        if config.ENABLE_FORCE_JOIN:
            if not await is_joined_all(client, uid):
                try:
                    await message.reply_photo(
                        photo=config.FORCE_JOIN_IMAGE,
                        caption=(
                            "<blockquote><b>🤖 WELCOME TO AZTECH ADS BOT</b></blockquote>\n\n"
                            "To unlock the full <b>AzTech Ads Bot</b> experience, please join our "
                            "official <b>channel</b> and <b>group</b> first!\n\n"
                            "<i>Tip:</i> Click the buttons below to join both. After joining, click "
                            "<b>‘Verify ✅’</b> to proceed.\n\n"
                            "Your <i>free automation journey</i> starts here 🚀"
                        ),
                        reply_markup=kb([
                            [InlineKeyboardButton("📢 Join Channel", url=config.MUST_JOIN_CHANNEL_URL)],
                            [InlineKeyboardButton("👥 Join Group", url=config.MUSTJOIN_GROUP_URL)],
                            [InlineKeyboardButton("✅ Verify", callback_data="joined_check")]
                        ]),
                        parse_mode=ParseMode.HTML
                    )
                    logger.info(f"Sent force join prompt to user {uid}")
                    return
                except Exception as e:
                    logger.error(f"Failed to send force join message to {uid}: {e}")
                    await message.reply(
                        "⚠️ Please join our official channel and group to continue.\n"
                        "If the buttons don’t work, contact support.",
                        parse_mode=ParseMode.HTML
                    )
                    return

        # ✅ If user already joined or force join is disabled
        await message.reply_photo(
            photo=config.START_IMAGE,
            caption=(
                "<blockquote><b>🤖 Welcome to AzTech Ads Bot [FREE]</b></blockquote>\n\n"
                "<b>The Future of Telegram Automation ⚙️</b>\n\n"
                "✨ <b>Powerful Features:</b>\n"
                "• 🚀 <b>Auto Ad Broadcasting</b> — Instantly promote your ads across multiple groups.\n"
                "• ⏱️ <b>Smart Time Intervals</b> — Schedule ads every 5m, 10m, or 20m.\n"
                "• 👥 <b>Target Group Selection</b> — Choose exactly where your ads go.\n"
                "• 📊 <b>Ad Analytics</b> — Track your ad performance in real time.\n\n"
                f"<blockquote><b>📢 Owner:</b> @{config.OWNER_USERNAME}</blockquote>\n"
                f"<blockquote><b>🔗 Updates:</b> @{config.UPDATES_CHANNEL}</blockquote>\n"
                f"<blockquote><b>💬 Support:</b> @{config.SUPPORT_USERNAME}</blockquote>\n\n"
                "<i>Start your first campaign and let AzTech Ads Bot handle the rest ⚡</i>"
            ),
            reply_markup=kb([
                [InlineKeyboardButton("Start Advertising 🚀", callback_data="menu_main")],
                [
                    InlineKeyboardButton("Updates 🔄", url=config.UPDATES_CHANNEL_URL),
                    InlineKeyboardButton("Support 💬", url=config.SUPPORT_GROUP_URL)
                ],
                [InlineKeyboardButton("How To Use 📖", url=config.GUIDE_URL)]
            ]),
            parse_mode=ParseMode.HTML
        )

        logger.info(f"Start command handled successfully for user {uid}")

    except Exception as e:
        logger.error(f"Error in /start command for {uid}: {e}")
        await message.reply(
            "⚠️ An unexpected error occurred while starting the bot.\n"
            "Please try again later or contact support.",
            parse_mode=ParseMode.HTML
        )

@pyro.on_message(filters.command("me"))
async def me_command(client, message):
    """Handle /me command - Show user details (NO PREMIUM SYSTEM)"""
    try:
        uid = message.from_user.id
        user = db.get_user(uid)
        
        if not user:
            await message.reply("You're not registered. Please /start first.", parse_mode=ParseMode.HTML)
            return
        
        accounts_count = db.get_user_accounts_count(uid)
        username = user.get('username', 'N/A')
        account_limit = user.get('accounts_limit', 1)
        
        # Simple status display - NO PREMIUM
        status_text = (
            f"<blockquote><b>🚀 AzTech Ads Bot [FREE]</b></blockquote>\n\n"
            f"<u>User ID:</u> <code>{uid}</code>\n"
            f"<b>Username:</b> <i>@{username}</i>\n"
            f"<b>Status:</b> ✅ Active User\n"
            f"<b>Hosted Accounts:</b> <u>{accounts_count} / {account_limit}</u>\n"
            f"<b>Logger Active:</b> {'Yes ✅' if db.get_logger_status(uid) else 'No ❌'}\n\n"
            "<b>📋 Your Features:</b>\n"
            " ✅ 1 Account hosting\n"
            " ✅ Full broadcasting system\n"
            " ✅ Smart group targeting\n"
            " ✅ Real-time analytics\n"
            " ✅ DM logging via logger bot\n"
            " ✅ Auto message rotation\n"
            " ✅ No restrictions\n\n"
            f"<blockquote><b>📢 Owner:</b> @{config.OWNER_USERNAME}</blockquote>\n"
            f"<blockquote><b>🔗 Updates:</b> @{config.UPDATES_CHANNEL}</blockquote>\n"
            f"<blockquote><b>💬 Support:</b> @{config.SUPPORT_USERNAME}</blockquote>\n\n"
            "<i>Enjoy your free automation experience! 🎉</i>"
        )
        
        status_buttons = [
            [InlineKeyboardButton("📊 Dashboard", callback_data="menu_main")],
            [
                InlineKeyboardButton("Updates 🔄", url=config.UPDATES_CHANNEL_URL),
                InlineKeyboardButton("Support 💬", url=config.SUPPORT_GROUP_URL)
            ]
        ]
        
        await message.reply_photo(
            photo=config.START_IMAGE,
            caption=status_text,
            reply_markup=InlineKeyboardMarkup(status_buttons),
            parse_mode=ParseMode.HTML
        )
        logger.info(f"/me command: user {uid} - Accounts: {accounts_count}/{account_limit}")
        
    except Exception as e:
        logger.error(f"Error in me command: {e}")
        await message.reply("Error getting user info. Please try again.")

@pyro.on_message(filters.command("stop"))
async def stop_command(client, message):
    """Handle /stop command"""
    try:
        uid = message.from_user.id
        stopped = await stop_broadcast_task(uid)
        if stopped:
            await message.reply("<blockquote><b>⏹️ Broadcast stopped!</b></blockquote>", parse_mode=ParseMode.HTML)
            await send_dm_log(uid, "<b>⏹️ Broadcast stopped!</b>")
            logger.info(f"Broadcast stopped via command for user {uid}")
        else:
            await message.reply("No broadcast running!", parse_mode=ParseMode.HTML)
            
    except Exception as e:
        logger.error(f"Error in stop command: {e}")
        await message.reply("Error stopping broadcast. Please try again.")

@pyro.on_message(filters.command("stats") & filters.user(ALLOWED_BD_IDS))
async def admin_stats_command(client, message):
    """Handle /stats command for admins"""
    try:
        stats = db.get_admin_stats()
        
        stats_text = (
            f"<blockquote><b>AzTech Ads Bot - ADMIN DASHBOARD</b></blockquote>\n\n"
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

# REMOVED: Duplicate /set command - using the better one at line 4449

@pyro.on_message(filters.command("stats") & ~filters.user(ALLOWED_BD_IDS))
async def non_admin_stats_command(client, message):
    """Handle /stats command for non-admins"""
    await message.reply(f"You Are Not Admin. Admin is @{config.ADMIN_USERNAME}")

@pyro.on_message(filters.command("bd") & filters.user(ALLOWED_BD_IDS))
async def admin_broadcast_command(client, message):
    """Handle /bd command for admins"""
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
            """<blockquote><b>📢 AzTech Ads Bot - ADMIN BROADCAST</b></blockquote>\n\n"""
            "<u>Status: Initializing...</u>",
            parse_mode=ParseMode.HTML
        )
        
        sent_count = 0
        failed_count = 0
        
        reply_msg = message.reply_to_message
        media = None
        caption = reply_msg.caption or reply_msg.text or ""
        
        if reply_msg.photo:
            media = reply_msg.photo.file_id
        elif reply_msg.document:
            media = reply_msg.document.file_id
        elif reply_msg.video:
            media = reply_msg.video.file_id
        
        for user in all_users:
            user_id = user['user_id']
            try:
                if media:
                    await client.send_photo(
                        chat_id=user_id,
                        photo=media,
                        caption=caption,
                        parse_mode=ParseMode.HTML
                    )
                else:
                    await client.send_message(
                        chat_id=user_id,
                        text=caption,
                        parse_mode=ParseMode.HTML
                    )
                sent_count += 1
            except PeerIdInvalid:
                logger.error(f"Failed to send broadcast to user {user_id}: PeerIdInvalid")
                failed_count += 1
            except FloodWait as e:
                logger.warning(f"Flood wait for user {user_id}: Wait {e.seconds} seconds")
                await asyncio.sleep(e.seconds)
                try:
                    if media:
                        await client.send_photo(user_id, photo=media, caption=caption, parse_mode=ParseMode.HTML)
                    else:
                        await client.send_message(user_id, text=caption, parse_mode=ParseMode.HTML)
                    sent_count += 1
                except Exception:
                    failed_count += 1
            except Exception as e:
                logger.error(f"Failed to send broadcast to user {user_id}: {e}")
                failed_count += 1
            
            if (sent_count + failed_count) % 10 == 0 or (sent_count + failed_count) == total_users:
                try:
                    await status_msg.edit_text(
                        f"""<blockquote><b>📢 AzTech Ads Bot - ADMIN BROADCAST</b></blockquote>\n\n"""
                        f"<u>Status: In Progress...</u> \n"
                        f"<b>Sent:</b> <code>{sent_count}/{total_users}</code>\n"
                        f"<i>Failed:</i> <u>{failed_count}</u>\n"
                        f"<blockquote>Progress: {generate_progress_bar(sent_count + failed_count, total_users)}</blockquote>",
                        parse_mode=ParseMode.HTML
                    )
                except Exception as e:
                    logger.error(f"Failed to update broadcast status: {e}")
            await asyncio.sleep(0.5)
        
        await status_msg.edit_text(
            f"""<blockquote><b>✅ AzTech Ads Bot - ADMIN BROADCAST COMPLETED</b></blockquote>\n\n"""
            f"<u>Sent:</u> <code>{sent_count}/{total_users}</code>\n"
            f"<b>Failed:</b> <i>{failed_count}</i> ⚠️\n"
            f"<blockquote>Success Rate: {generate_progress_bar(sent_count, total_users)} 💹</blockquote>",
            parse_mode=ParseMode.HTML
        )
        await send_dm_log(uid, f"<b>🏁 Admin broadcast completed:</b> Sent {sent_count}/{total_users}, Failed {failed_count} ✨")
        logger.info(f"Admin broadcast completed by {uid}")
        
    except Exception as e:
        logger.error(f"Error in admin broadcast command: {e}")
        await message.reply(f"Error during broadcast: {str(e)}", parse_mode=ParseMode.HTML)

@pyro.on_message(filters.command("bd") & ~filters.user(ALLOWED_BD_IDS))
async def non_admin_broadcast_command(client, message):
    """Handle /bd command for non-admins"""
    await message.reply("You Are Not Admin")

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
        f"<blockquote><b>OTP sent!✅</b></blockquote>\n\n"
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
            # Get API credentials from database
            credentials = db.get_user_api_credentials(uid)
            
            if not credentials:
                await callback_query.edit_message_text(
                    f"❌ <b>API credentials not found!</b>\n\n"
                    f"Please restart the account addition process.",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb([[InlineKeyboardButton("⬅️ Back", callback_data="menu_main")]])
                )
                return
            
            tg = TelegramClient(StringSession(session_str), credentials['api_id'], credentials['api_hash'])
            try:
                await tg.connect()
                await tg.sign_in(phone, code=otp, phone_code_hash=phone_code_hash)

                session_encrypted = cipher_suite.encrypt(session_str.encode()).decode()
                db.add_user_account(uid, phone, session_encrypted)

                await callback_query.message.edit_caption(
    f"<blockquote><b>Account Successfully added!✅</b></blockquote>\n\n"
    f"Phone: <code>{phone}</code>\n"
    "Your account is ready for broadcasting!\n"
    "<b>Note:</b> Your account is ready for broadcasting!",
    parse_mode=ParseMode.HTML,
    reply_markup=kb([[InlineKeyboardButton("Dashboard 🚪", callback_data="menu_main")]])
)

                await send_dm_log(uid, f"<b> Account added successfully:</b> <code>{phone}</code>✅")
                
                # Auto-select all groups from this account
                asyncio.create_task(auto_select_all_groups(uid, phone))
                
                db.set_user_state(uid, "")
                db.delete_temp_data(uid, "session")
                # Note: API credentials in temp (api_id, api_hash) will be cleaned up after auto-selection
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
                    base_caption + "\n\n<blockquote><b>🔐 2FA Detected!</b></blockquote>\n\n"
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
                    base_caption + "\n\n<b>❌ Invalid OTP! Try again.</b>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=get_otp_keyboard()
                )
                temp_dict["otp"] = ""
                temp_json = json.dumps(temp_dict)
                temp_encrypted = cipher_suite.encrypt(temp_json.encode()).decode()
                db.set_temp_data(uid, "session", temp_encrypted)
            except PhoneCodeExpiredError:
                await callback_query.message.edit_caption(
                    base_caption + "\n\n<b>❌ OTP expired! Please restart.</b>",
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
                    base_caption + f"\n\n<b>❌ Flood wait limit reached: Please wait {e.seconds}s and try again.</b>",
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
                    base_caption + f"\n\n<blockquote><b>❌ Login failed:</b>{str(e)}</blockquote>\n\n"
                    f"<b>Contact:</b> <code>@{config.ADMIN_USERNAME}</code>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=None
                )
                await send_dm_log(uid, f"<b>❌ Account login failed:</b> {str(e)}")
                db.set_user_state(uid, "")
                db.delete_temp_data(uid, "session")
                break
            finally:
                await tg.disconnect()

# =======================================================
# � GROUPS MENU SYSTEM
# =======================================================

@pyro.on_callback_query(filters.regex("^groups_menu"))
async def groups_menu_callback(client, callback_query):
    """Handle groups menu callback"""
    try:
        uid = callback_query.from_user.id
        # Fix page parsing
        try:
            page = int(callback_query.data.split("_")[-1]) if callback_query.data.count("_") > 1 else 1
        except ValueError:
            page = 1
        accounts = db.get_user_accounts(uid)
        
        if not accounts:
            await callback_query.answer("No accounts added yet!", show_alert=True)
            return

        # Show loading message
        await callback_query.message.edit_caption(
            caption="<b>⏳ Loading groups...</b>",
            parse_mode=ParseMode.HTML
        )

        # Get all groups from all accounts
        all_groups = []
        selected_groups = db.get_target_groups(uid) or []
        selected_group_ids = [g['group_id'] for g in selected_groups]
        
        # Use a faster approach with connection pooling
        async def get_account_groups(acc):
            try:
                session_str = cipher_suite.decrypt(acc['session_string'].encode()).decode()
                # Get user's API credentials
                credentials = db.get_user_api_credentials(uid)
                if not credentials:
                    logger.error(f"No API credentials found for user {uid}")
                    return []
                
                async with TelegramClient(StringSession(session_str), credentials['api_id'], credentials['api_hash']) as tg_client:
                    groups = []
                    async for dialog in tg_client.iter_dialogs(limit=None):
                        if dialog.is_group:
                            group_data = {
                                'id': dialog.id,
                                'title': dialog.title,
                                'selected': dialog.id in selected_group_ids  # Explicitly check if group is selected
                            }
                            groups.append(group_data)
                    return groups
            except Exception as e:
                logger.error(f"Failed to get groups for account {acc['phone_number']}: {e}")
                return []

        # Fetch groups from all accounts concurrently
        tasks = [get_account_groups(acc) for acc in accounts]
        groups_lists = await asyncio.gather(*tasks)
        
        # Merge groups from all accounts, removing duplicates
        seen_ids = set()
        for groups in groups_lists:
            for group in groups:
                if group['id'] not in seen_ids:
                    seen_ids.add(group['id'])
                    all_groups.append(group)

        # Pagination with larger pages
        items_per_page = 8  # Show more groups per page
        total_pages = (len(all_groups) + items_per_page - 1) // items_per_page
        start_idx = (page - 1) * items_per_page
        end_idx = start_idx + items_per_page
        current_groups = all_groups[start_idx:end_idx]

        # Get selection stats
        total_groups = len(all_groups)
        selected_count = sum(1 for g in all_groups if g['selected'])

        caption = f"<blockquote><b>BROADCAST GROUPS 👥</b></blockquote>\n\n"
        caption += f"<b>Selected:</b> {selected_count}/{total_groups} groups\n\n"
        caption += "<i>Click on groups to toggle selection:</i>\n"

        buttons = []
        # Show groups in two columns when possible
        group_pairs = [current_groups[i:i+2] for i in range(0, len(current_groups), 2)]
        selected_ids = [g['group_id'] for g in selected_groups]
        
        for pair in group_pairs:
            row = []
            for group in pair:
                # Check if group is in selected_ids
                status = "✅" if group['id'] in selected_ids else "❌"
                row.append(InlineKeyboardButton(
                    f"{group['title'][:20]} {status}",  # Limit title length
                    callback_data=f"toggle_group_{group['id']}"
                ))
            buttons.append(row)

        # Navigation buttons with page counter
        nav_buttons = []
        if page > 1:
            nav_buttons.append(InlineKeyboardButton("⬅️", callback_data=f"groups_menu_{page-1}"))
        nav_buttons.append(InlineKeyboardButton(f"{page}/{total_pages}", callback_data="noop"))
        if page < total_pages:
            nav_buttons.append(InlineKeyboardButton("➡️", callback_data=f"groups_menu_{page+1}"))
        if nav_buttons:
            buttons.append(nav_buttons)

        # Select All and Unselect All buttons
        buttons.append([
            InlineKeyboardButton("Select All ✅", callback_data="select_all_groups"),
            InlineKeyboardButton("Unselect All ❌", callback_data="unselect_all_groups")
        ])
        buttons.append([InlineKeyboardButton("Done ✅", callback_data="menu_main")])

        caption += f"\nPage {page}/{total_pages}"
        await callback_query.message.edit_caption(
            caption=caption,
            reply_markup=kb(buttons),
            parse_mode=ParseMode.HTML
        )
        
    except Exception as e:
        logger.error(f"Error in groups menu callback: {e}")
        await callback_query.answer("Error loading groups menu. Try again.", show_alert=True)

@pyro.on_callback_query(filters.regex("^toggle_group_"))
async def toggle_group_callback(client, callback_query):
    """Handle toggle group selection callback"""
    try:
        uid = callback_query.from_user.id
        group_id = int(callback_query.data.split("_")[2])
        
        # Get current selection state without fetching all groups
        group_state = db.get_target_group(uid, group_id)
        title = None
        
        # Get group title from the button text to avoid API calls
        for row in callback_query.message.reply_markup.inline_keyboard:
            for button in row:
                if button.callback_data == f"toggle_group_{group_id}":
                    title = button.text.split(" ")[0]  # Remove emoji
                    break
            if title:
                break

        if group_state:
            # Group is selected, remove it
            db.remove_target_group(uid, group_id)
            await callback_query.answer("❌ Removed from broadcast", show_alert=False)
        else:
            # Group is not selected, add it
            if title:
                db.add_target_group(uid, group_id, title)
                await callback_query.answer("✅ Added to broadcast", show_alert=False)
            else:
                # Fallback if title not found in button
                try:
                    # For temporary operations, use config credentials temporarily
                    async with TelegramClient(StringSession(), config.BOT_API_ID, config.BOT_API_HASH) as temp_client:
                        group = await temp_client.get_entity(group_id)
                        db.add_target_group(uid, group_id, group.title)
                except Exception as e:
                    logger.error(f"Error adding group {group_id}: {e}")
                    await callback_query.answer("Error adding group", show_alert=True)
                    return

        # Update only the specific button
        new_markup = list(callback_query.message.reply_markup.inline_keyboard)
        for i, row in enumerate(new_markup):
            for j, button in enumerate(row):
                if button.callback_data == f"toggle_group_{group_id}":
                    status = "❌" if group_state else "✅"
                    new_markup[i][j] = InlineKeyboardButton(
                        f"{title} {status}",
                        callback_data=f"toggle_group_{group_id}"
                    )

        # Update button immediately without reloading full menu
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
        selected_groups = db.get_target_groups(uid) or []
        all_groups = get_cached_groups(uid)
        
        if not all_groups:
            # If no cache, fetch groups
            all_groups = []
            accounts = db.get_user_accounts(uid)
            for acc in accounts:
                try:
                    session_str = cipher_suite.decrypt(acc['session_string'].encode()).decode()
                    # Get user's API credentials
                    credentials = db.get_user_api_credentials(acc['user_id'])
                    if not credentials:
                        logger.error(f"No API credentials found for user {acc['user_id']}")
                        continue
                    
                    async with TelegramClient(StringSession(session_str), credentials['api_id'], credentials['api_hash']) as tg_client:
                        async for dialog in tg_client.iter_dialogs():
                            if (dialog.is_group and 
                                dialog.entity.username != config.MUSTJOIN_GROUP and 
                                dialog.id not in [g['id'] for g in all_groups]):
                                all_groups.append({
                                    'id': dialog.id,
                                    'title': dialog.title
                                })
                except Exception as e:
                    logger.error(f"Error adding groups for account {acc['phone_number']}: {e}")
                    continue
        
        # First unselect any existing selections to avoid duplicates
        for group in selected_groups:
            db.remove_target_group(uid, group['group_id'])
        
        # Then add all groups
        for group in all_groups:
            try:
                db.add_target_group(uid, group['id'], group['title'])
            except Exception as e:
                logger.error(f"Error adding group {group['title']}: {e}")
                continue
        
        await callback_query.answer("All groups selected ✅", show_alert=True)

        # Refresh the groups menu
        await groups_menu_callback(client, callback_query)

    except Exception as e:
        logger.error(f"Error in select all groups callback: {e}")
        await callback_query.answer("Error selecting groups. Try again.", show_alert=True)

@pyro.on_callback_query(filters.regex("^unselect_all_groups$"))
async def unselect_all_groups_callback(client, callback_query):
    """Handle unselect all groups callback"""
    try:
        uid = callback_query.from_user.id
        selected_groups = db.get_target_groups(uid) or []
        
        # Delete groups individually
        for group in selected_groups:
            db.remove_target_group(uid, group['group_id'])
        
        await callback_query.answer("All groups unselected ❌", show_alert=True)

        # Refresh the groups menu
        await groups_menu_callback(client, callback_query)

    except Exception as e:
        logger.error(f"Error in unselect all groups callback: {e}")
        await callback_query.answer("Error unselecting groups. Try again.", show_alert=True)

# =======================================================
# �🔐 JOIN VERIFICATION FUNCTIONS
# =======================================================

async def instant_join_check(client, uid, chat_id):
    """Quick individual join check for a specific chat."""
    try:
        member = await client.get_chat_member(chat_id, uid)
        return True
    except UserNotParticipant:
        return False
    except Exception as e:
        logger.error(f"Join check error for user {uid} in {chat_id}: {e}")
        return False

async def verify_all_joins(client, uid, channel_id, group_id):
    """Verify user has joined both required groups."""
    try:
        # Verify channel membership
        channel_member = await client.get_chat_member(channel_id, uid)
        if not channel_member:
            return False

        # Verify group membership
        group_member = await client.get_chat_member(group_id, uid)
        if not group_member:
            return False

        return True
    except UserNotParticipant:
        return False
    except Exception as e:
        logger.error(f"Join verification error for user {uid}: {e}")
        return False

@pyro.on_callback_query(filters.regex("joined_check"))
async def joined_check_callback(client, callback_query):
    """Handle joined check callback with instant Telethon verification using bot as admin"""
    try:
        uid = callback_query.from_user.id
        
        # Show "checking..." message
        await callback_query.answer("🔍 Verifying your membership...", show_alert=False)
        
        logger.info(f"🔍 Starting instant join verification for user {uid}")
        
        # Ensure Telethon bot is connected
        if not telethon_bot.is_connected():
            await telethon_bot.connect()
            await telethon_bot.start(bot_token=config.BOT_TOKEN)
        
        # Use Telethon bot as admin to check membership using usernames
        is_joined = await verify_all_joins(
            telethon_bot,  # Use bot's Telethon client (bot is admin)
            uid,
            config.MUST_JOIN_CHANNEL,
            config.MUSTJOIN_GROUP
        )
        
        if not is_joined:
            missing = []
            # Quick individual checks to give specific feedback using bot's admin access
            channel_check = await instant_join_check(telethon_bot, uid, config.MUST_JOIN_CHANNEL)
            group_check = await instant_join_check(telethon_bot, uid, config.MUSTJOIN_GROUP)
            
            if not channel_check:
                missing.append("📢 Channel")
            if not group_check:
                missing.append("👥 Group")
                
            msg = f"❌ You haven't joined:\n\n{chr(10).join(missing)}\n\nPlease join and try again!"
            await callback_query.answer(msg, show_alert=True)
            logger.info(f"❌ User {uid} failed join check: missing {', '.join(missing)}")
            return
        
        # User has joined both - proceed to bot
        logger.info(f"✅ User {uid} passed instant join verification!")
        await callback_query.answer("✅ Verification successful! Welcome!", show_alert=False)
        
        # Delete force join message and show main menu
        await callback_query.message.delete()
        
        # Create a mock message object to call start_command
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
        await callback_query.answer("⚠️ Error verifying. Please try again.", show_alert=True)

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

        # Cancel any existing preload task
        if hasattr(menu_callback, 'preload_task'):
            try:
                menu_callback.preload_task.cancel()
            except Exception:
                pass

        # --- Fetch user data safely ---
        accounts = db.get_user_accounts(uid) or []
        accounts_count = len(accounts)


        # Ad message status - now using Saved Messages directly
        ad_msg_status = "Auto (From Saved Messages) ✅"

        # Broadcast state
        broadcast_state = db.get_broadcast_state(uid) or {}
        is_running = broadcast_state.get("running", False)
        broadcast_status = "Running 🚀" if is_running else "Stopped ❌"

        # Delays & settings
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

        # Target groups
        target_groups_count = len(db.get_target_groups(uid) or [])

        # --- Build status info text ---
        # Get user data for account limit
        user = db.get_user(uid)
        account_limit = user.get('accounts_limit', 1) if user else 1
        status_info = (
            f"Accounts Status 📱\n"
            f"• Active Accounts: {accounts_count}/{account_limit}\n"
            f"Broadcast Status 🚀\n"
            f"• Ad Message: {ad_msg_status}\n"
            f"• Broadcast State: {broadcast_status}\n"
            f"• Cycle Interval: {current_delay}s ⏭️\n"
            f"• Cycle Timeout: {cycle_timeout//60}min 🛑\n\n"
            f"Groups Settings ⚙️\n"
            f"• Message Delay: {group_msg_delay}s ⏰\n"
            f"• Target Groups: {target_groups_count} 👥"
        )

        # --- Menu Layouts ---
        # Dynamic button: Show "Host Account" if no accounts, "Logout Account" if has accounts
        if accounts_count == 0:
            account_button_text = "📱 Host Account"
            account_button_callback = "host_account"
        else:
            account_button_text = "🚪 Logout Account"
            account_button_callback = "instant_logout"
        
        main_menu = [
            [InlineKeyboardButton("Broadcast Menu 🚀", callback_data="menu_broadcast")],
            [
                InlineKeyboardButton(account_button_text, callback_data=account_button_callback),
                InlineKeyboardButton("Groups Settings ⚙️", callback_data="menu_groups"),
            ],
        ]

        broadcast_menu = [
            [
                InlineKeyboardButton("ℹ️ About Saved Messages", callback_data="info_saved_messages"),
                InlineKeyboardButton("Set Cycle Interval ⏳", callback_data="set_ad_delay"),
            ],
            [
                InlineKeyboardButton("Select Saved Messages 📝", callback_data="select_saved_messages_count"),
            ],
            [
                InlineKeyboardButton(
                    "Start Broadcast ▶️" if not is_running else "Stop Broadcast ⏹️",
                    callback_data="start_broadcast" if not is_running else "stop_broadcast",
                )
            ],
            [
                InlineKeyboardButton("⏳ Cycle Timeout", callback_data="set_cycle_timeout"),
                InlineKeyboardButton("📊 View Analytics", callback_data="view_analytics"),
            ],
            [InlineKeyboardButton("Back to Menu 🔙", callback_data="menu_main")],
        ]

        login_menu = [
            [
                InlineKeyboardButton("Add Account 📱", callback_data="host_account"),
                InlineKeyboardButton("My Accounts 🗂️", callback_data="view_accounts"),
            ],
            [InlineKeyboardButton("Back to Menu 🔙", callback_data="menu_main")],
        ]

        groups_menu = [
            [InlineKeyboardButton("Get All Your Groups Link 📂", callback_data="export_all_groups")],
            [InlineKeyboardButton("Select Target Groups 🔍", callback_data="groups_menu")],
            # REMOVED: Clear blacklist button - no blacklist system
            [
                InlineKeyboardButton("Group Message Delay ⏳", callback_data="set_group_delay"),
                InlineKeyboardButton("Cycle Timeout ⏳", callback_data="set_cycle_timeout"),
            ],
            [InlineKeyboardButton("Back to Menu 🔙", callback_data="menu_main")],
        ]

        # --- Select caption and buttons ---
        if menu_type == "menu_broadcast":
            caption = f"<blockquote><b>BROADCAST MENU</b></blockquote>\n\n{status_info}"
            buttons = broadcast_menu
        elif menu_type == "menu_login":
            caption = f"<blockquote><b>ACCOUNT MENU</b></blockquote>\n\n{status_info}"
            buttons = login_menu
        elif menu_type == "menu_groups":
            caption = f"<blockquote><b>GROUPS MENU</b></blockquote>\n\n{status_info}"
            buttons = groups_menu
        else:  # menu_main
            caption = (
                f"<blockquote><b>Welcome to AzTech Ads Bot [FREE]</b></blockquote>\n\n"
                f"{status_info}\n\n"
                f"<blockquote>Please choose an action below:</blockquote>"
            )
            buttons = main_menu

        # --- Update or send message ---
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

        # --- Preload groups in background ---
        try:
            menu_callback.preload_task = asyncio.create_task(preload_user_groups(uid))
            menu_callback.preload_task.set_name(f"preload_groups_{uid}")
            menu_callback.preload_task.add_done_callback(
                lambda t: logger.info(f"Preload task completed for user {uid}")
            )
        except Exception as preload_error:
            logger.error(f"Failed to start preload task: {preload_error}")

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
            [InlineKeyboardButton("3 min (180s) 🟠", callback_data="delay_180"),
             InlineKeyboardButton("5 min (300s) 🟡", callback_data="delay_300")],
            [InlineKeyboardButton("10 min (600s) 🟢", callback_data="delay_600"),
             InlineKeyboardButton("20 min (1200s) 🔵", callback_data="delay_1200")],
            [InlineKeyboardButton("Back 🔙", callback_data="menu_broadcast")]
        ]

        await callback_query.message.edit_caption(
            caption="""<b>Choose Broadcast Interval</b>

<b>How long should the bot wait between each full broadcast cycle?</b>

• <b>3 Minutes (180s)</b> - Very fast 🟠
• <b>5 Minutes (300s)</b> - Fast ⚡  
• <b>10 Minutes (600s)</b> - Balanced (Recommended) ✅
• <b>20 Minutes (1200s)</b> - Safe & Slow 🔵

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
        delay = int(callback_query.data.split("_")[1])  # Extract number from callback (e.g. delay_600)

        db.set_user_ad_delay(uid, delay)
        await callback_query.answer(f"✅ Interval set to {delay}s", show_alert=True)

        # Return to broadcast menu
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
        
        # Check account limit
        accounts_count = db.get_user_accounts_count(uid)
        limit = user.get("accounts_limit", 1)
        
        # Convert limit to integer if string
        if isinstance(limit, str):
            try:
                limit = int(limit)
            except (TypeError, ValueError):
                logger.error(f"Invalid accounts_limit for user {uid}: {limit}. Defaulting to 1")
                limit = 1
        
        logger.info(f"User {uid} accessing account hosting - {accounts_count}/{limit} accounts")
        
        # CHECK IF USER HAS REACHED ACCOUNT LIMIT
        if accounts_count >= limit:
            await callback_query.answer(
                f"❌ Account limit reached! You have {accounts_count}/{limit} accounts.",
                show_alert=True
            )
            await callback_query.message.edit_caption(
                caption=f"<blockquote><b>🚫 ACCOUNT LIMIT REACHED</b></blockquote>\n\n"
                        f"<b>Current Accounts:</b> {accounts_count}/{limit}\n\n"
                        f"You have reached your account limit. Please remove an existing account before adding a new one.\n\n"
                        f"<i>💡 Tip: Go to 'My Accounts' to manage your existing accounts.</i>",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("📊 My Accounts", callback_data="my_accounts")],
                    [InlineKeyboardButton("🔙 Back", callback_data="menu_main")]
                ]),
                parse_mode=ParseMode.HTML
            )
            return

        # Check if user already has API credentials
        credentials = db.get_user_api_credentials(uid)
        
        if credentials:
            # User already has API credentials - proceed directly to phone number
            logger.info(f"User {uid} already has API credentials, skipping API input")
            db.set_user_state(uid, "telethon_wait_phone")
            await callback_query.message.edit_caption(
                caption="<b>📱 ADD NEW ACCOUNT</b>\n\n"
                        "✅ API credentials already saved!\n\n"
                        "Please enter your <b>phone number</b> with country code:\n\n"
                        "📱 <b>Example:</b> <code>+1234567890</code>\n\n"
                        "<i>The OTP will be sent to this number</i>",
                reply_markup=kb([
                    [InlineKeyboardButton("🔙 Back", callback_data="menu_main")]
                ]),
                parse_mode=ParseMode.HTML
            )
            return
        
        # No API credentials found - ask for them
        logger.info(f"User {uid} has no API credentials, requesting them")
        db.set_user_state(uid, "waiting_api_id")
        await callback_query.message.edit_caption(
            caption="<b>🔑 API CREDENTIALS REQUIRED</b>\n\n"
                    "<b>📱 Get your API credentials:</b>\n"
                    "1. Visit https://my.telegram.org\n"
                    "2. Login with your phone number\n"
                    "3. Go to 'API Development tools'\n"
                    "4. Create an app and get API ID & Hash\n\n"
                    "<b>💡 Note:</b> You only need to do this ONCE.\n"
                    "After saving, you won't be asked again.\n\n"
                    "Now please enter your <b>API ID</b>:\n\n"
                    "📱 <b>Example:</b> <code>12345678</code>",
            reply_markup=kb([
                [InlineKeyboardButton("🔙 Back", callback_data="menu_main")]
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
                    "<b>📱 Get it from:</b> https://my.telegram.org\n\n"
                    "<b>Example:</b> <code>12345678</code>",
            reply_markup=kb([
                [InlineKeyboardButton("❌ Cancel", callback_data="host_account")]
            ]),
            parse_mode=ParseMode.HTML
        )
        
    except Exception as e:
        logger.error(f"Error in temp_api_start: {e}")
        await callback_query.answer("Error starting API setup.", show_alert=True)

@pyro.on_callback_query(filters.regex("view_accounts"))
async def view_accounts_callback(client, callback_query):
    """Handle view accounts callback"""
    try:
        uid = callback_query.from_user.id
        user = db.get_user(uid)
        accounts = db.get_user_accounts(uid)
        account_limit = user.get('accounts_limit', 1)

        if not accounts:
            await callback_query.message.edit_caption(
                caption=f"""<blockquote><b>NO ACCOUNTS HOSTED</b></blockquote>\n\n"""
                        f"📊 Account Limit: 0/{account_limit}\n\n"
                        f"""<b>Add your first account to start broadcasting!</b>""",
                reply_markup=kb([[InlineKeyboardButton("Add Account 📱", callback_data="host_account"),
                                InlineKeyboardButton("Back 🔙", callback_data="menu_main")]]),
                parse_mode=ParseMode.HTML
            )
            return
        
        account_limit = user.get('accounts_limit', 1)
        
        caption = "<blockquote><b>HOSTED ACCOUNTS</b></blockquote>\n\n"
        caption += f"<b>Account Limit:</b> {len(accounts)}/{account_limit}\n\n"
        buttons = []
        for i, acc in enumerate(accounts, 1):
            status = "Active ✅" if acc['is_active'] else "Inactive ❌"
            caption += f"{i}. <code>{acc['phone_number']}</code> - <i>{status}</i>\n"
            buttons.append([
                InlineKeyboardButton(f"{acc['phone_number']} ({status})", callback_data=f"view_acc_{acc['_id']}"),
                InlineKeyboardButton("Delete 🗑️", callback_data=f"delete_acc_{acc['_id']}")
            ])
        
        caption += "\n<blockquote>Choose an action:</blockquote>"
        buttons.append([InlineKeyboardButton("Add Account 📱", callback_data="host_account")])
        buttons.append([InlineKeyboardButton("Back 🔙", callback_data="menu_main")])

        await callback_query.message.edit_caption(
            caption=caption,
            reply_markup=kb(buttons),
            parse_mode=ParseMode.HTML
        )
        logger.info(f"View accounts shown for user {uid}")
        
    except Exception as e:
        logger.error(f"Error in view_accounts callback: {e}")
        await callback_query.answer("Error loading accounts. Try again.", show_alert=True)

@pyro.on_callback_query(filters.regex("clear_apis"))
async def clear_apis_callback(client, callback_query):
    """Handle clear APIs callback - Show confirmation"""
    try:
        uid = callback_query.from_user.id
        
        # Check if user has API credentials
        credentials = db.get_user_api_credentials(uid)
        if not credentials:
            await callback_query.answer("❌ You don't have any API credentials to clear!", show_alert=True)
            return
        
        try:
            await callback_query.message.edit_media(
                media=InputMediaPhoto(
                    media=config.START_IMAGE,
                    caption="<b>🗑️ CLEAR API CREDENTIALS</b>\n\n"
                            "⚠️ <b>WARNING:</b> This action will permanently delete your API credentials!\n\n"
                            "<b>This will:</b>\n"
                            "• Remove your API ID and Hash\n"
                            "• You'll need to re-add them to use the bot\n"
                            "• All your accounts will remain safe\n\n"
                            "Are you sure you want to continue?",
                    parse_mode=ParseMode.HTML
                ),
                reply_markup=kb([
                    [InlineKeyboardButton("✅ Yes, Clear APIs", callback_data="clear_apis_confirm")],
                    [InlineKeyboardButton("❌ Cancel", callback_data="view_accounts")]
                ])
            )
        except MessageNotModified:
            pass  # Ignore if message content is the same
        logger.info(f"Clear APIs confirmation shown for user {uid}")
        
    except Exception as e:
        logger.error(f"Error in clear_apis callback: {e}")
        await callback_query.answer("Error loading clear APIs. Try again.", show_alert=True)

@pyro.on_callback_query(filters.regex("clear_apis_confirm"))
async def clear_apis_confirm_callback(client, callback_query):
    """Handle clear APIs confirmation - Actually clear the APIs"""
    try:
        uid = callback_query.from_user.id
        
        # Check if user has API credentials before clearing
        credentials_before = db.get_user_api_credentials(uid)
        logger.info(f"User {uid} credentials before clearing: {bool(credentials_before)}")
        
        if not credentials_before:
            await callback_query.answer("❌ No API credentials found to clear!", show_alert=True)
            return
        
        # Show immediate feedback
        await callback_query.answer("🔄 Clearing API credentials...", show_alert=False)
        
        # Clear the API credentials completely - multiple attempts for reliability
        result = db.clear_user_api_credentials(uid)
        
        # Force second clearing to be absolutely sure
        if result:
            await asyncio.sleep(0.5)  # Small delay for DB writes
            db.clear_user_api_credentials(uid)  # Second attempt
        
        # Final verification after delay
        await asyncio.sleep(0.5)
        credentials_after = db.get_user_api_credentials(uid)
        has_api_after = db.has_user_api_credentials(uid)
        
        # Detailed logging
        logger.info(f"User {uid} - Clear result: {result}")
        logger.info(f"User {uid} - Credentials after: {credentials_after}")
        logger.info(f"User {uid} - Has API after: {has_api_after}")
        
        if not credentials_after and not has_api_after:
            try:
                await callback_query.message.edit_media(
                    media=InputMediaPhoto(
                        media=config.START_IMAGE,
                        caption="<b>✅ API CREDENTIALS CLEARED</b>\n\n"
                                "Your API credentials have been successfully deleted!\n\n"
                                "<b>What happened:</b>\n"
                                "• API ID and Hash completely removed\n"
                                "• Database cleared of all stored credentials\n"
                                "• Next account addition will ask for new APIs\n\n"
                                "<b>What's next:</b>\n"
                                "• Click 'Add Account' to set new credentials\n"
                                "• You'll be prompted for API ID and Hash\n\n"
                                "<i>✅ All API data cleared successfully!</i>",
                        parse_mode=ParseMode.HTML
                    ),
                    reply_markup=kb([
                        [InlineKeyboardButton("🔙 Back to Accounts", callback_data="view_accounts")]
                    ])
                )
                await callback_query.answer("✅ API credentials cleared successfully!")
            except MessageNotModified:
                await callback_query.answer("✅ API credentials cleared successfully!")
            logger.info(f"API credentials successfully cleared for user {uid}")
        else:
            logger.error(f"Failed to clear API credentials for user {uid}. Result: {result}, Credentials after: {bool(credentials_after)}")
            await callback_query.answer("❌ Failed to clear API credentials. Try again.", show_alert=True)
        
    except Exception as e:
        logger.error(f"Error in clear_apis_confirm callback for user {uid}: {e}")
        await callback_query.answer("❌ Error clearing APIs. Try again.", show_alert=True)
        # Also try to send a debug message
        try:
            await callback_query.message.reply_text(f"Debug: Clear APIs failed with error: {str(e)[:200]}")
        except:
            pass

@pyro.on_callback_query(filters.regex("instant_logout"))
async def instant_logout_callback(client, callback_query):
    """Instantly logout and delete all account data"""
    try:
        uid = callback_query.from_user.id
        
        # Show confirmation message
        await callback_query.answer("🔄 Logging out...", show_alert=False)
        
        # Get all user accounts
        accounts = db.get_user_accounts(uid)
        
        if not accounts:
            await callback_query.answer("No accounts to logout!", show_alert=True)
            return
        
        # Delete all accounts
        deleted_count = 0
        for account in accounts:
            try:
                acc_id = str(account.get('_id'))
                if db.delete_user_account(uid, acc_id):
                    deleted_count += 1
                    logger.info(f"Deleted account {account.get('phone_number')} for user {uid}")
            except Exception as e:
                logger.error(f"Error deleting account {acc_id}: {e}")
        
        # Full user data cleanup
        try:
            db.delete_user_fully(uid)
            logger.info(f"🧹 Full cleanup executed for user {uid} - {deleted_count} account(s) removed")
        except Exception as e:
            logger.error(f"Error in full cleanup for user {uid}: {e}")
        
        # Re-register user (fresh start)
        username = callback_query.from_user.username or "N/A"
        first_name = callback_query.from_user.first_name or "User"
        db.create_user(uid, username, first_name)
        
        # Show success message and return to main menu
        await callback_query.message.edit_media(
            InputMediaPhoto(
                media=config.START_IMAGE,
                caption=f"<blockquote><b>✅ LOGOUT SUCCESSFUL</b></blockquote>\n\n"
                        f"<b>All your data has been removed:</b>\n"
                        f"• {deleted_count} account(s) deleted\n"
                        f"• All sessions cleared\n"
                        f"• Broadcast data removed\n"
                        f"• Fresh start activated\n\n"
                        f"<i>You can now add a new account! 🚀</i>",
                parse_mode=ParseMode.HTML
            ),
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📱 Host Account", callback_data="host_account")],
                [InlineKeyboardButton("🔙 Main Menu", callback_data="menu_main")]
            ])
        )
        
        await callback_query.answer("✅ Logged out successfully!", show_alert=False)
        logger.info(f"✅ User {uid} logged out successfully - all data cleared")
        
    except Exception as e:
        logger.error(f"Error in instant_logout callback: {e}")
        await callback_query.answer("❌ Error during logout. Please try again.", show_alert=True)

@pyro.on_callback_query(filters.regex("delete_accounts"))
async def delete_accounts_callback(client, callback_query):
    """Handle delete accounts callback"""
    try:
        uid = callback_query.from_user.id
        accounts = db.get_user_accounts(uid)
        if not accounts:
            await callback_query.message.edit_caption(
                caption="""<blockquote><b>NO ACCOUNTS TO DELETE</b></blockquote>\n\n"""
                        """Add an account to start Advertising!""",
                reply_markup=kb([[InlineKeyboardButton("Add Account", callback_data="host_account"),
                                InlineKeyboardButton("Back", callback_data="menu_main")]]),
                parse_mode=ParseMode.HTML
            )
            return
        
        caption = "<blockquote><b>DELETE ACCOUNTS</b></blockquote>\n\n"
        buttons = []
        for i, acc in enumerate(accounts, 1):
            status = "Active ✅" if acc['is_active'] else "Inactive ❌"
            caption += f"{i}. <code>{acc['phone_number']}</code> - <i>{status}</i>\n"
            buttons.append([
                InlineKeyboardButton(f"{acc['phone_number']} ({status})", callback_data=f"view_acc_{acc['_id']}"),
                InlineKeyboardButton("Delete", callback_data=f"delete_acc_{acc['_id']}")
            ])
        
        caption += "\n<blockquote>Choose an account to delete:</blockquote>"
        buttons.append([InlineKeyboardButton("Back", callback_data="menu_main")])
        
        await callback_query.message.edit_caption(
            caption=caption,
            reply_markup=kb(buttons),
            parse_mode=ParseMode.HTML
        )
        logger.info(f"Delete accounts interface shown for user {uid}")
        
    except Exception as e:
        logger.error(f"Error in delete_accounts callback: {e}")
        await callback_query.answer("Error loading delete interface. Try again.", show_alert=True)

@pyro.on_callback_query(filters.regex("delete_acc_"))
async def delete_account_callback(client, callback_query):
    """Handle delete specific account callback — stops client, deletes account,
    and performs a full user cleanup if this was the last account."""
    uid = callback_query.from_user.id
    acc_id = callback_query.data.replace("delete_acc_", "")

    try:
        # REMOVED: AccountManager cleanup - not needed anymore

        # 2️⃣ Delete the account from DB
        deleted = False
        try:
            deleted = db.delete_user_account(uid, acc_id)
        except Exception as e:
            logger.error(f"❌ DB deletion failed for account {acc_id} (user {uid}): {e}")
            await callback_query.answer("Error deleting account. Try again.", show_alert=True)
            await send_dm_log(uid, f"<b>❌ Account deletion failed:</b> {str(e)}")
            return

        if not deleted:
            await callback_query.answer("⚠️ Account not found or already deleted.", show_alert=True)
            logger.warning(f"User {uid} attempted to delete non-existent account {acc_id}")
            return

        # 3️⃣ Check how many accounts remain
        remaining = db.get_user_accounts_count(uid)

        # 4️⃣ If no accounts left — full cleanup
        if remaining == 0:
            # REMOVED: AccountManager cleanup - not needed anymore

            try:
                db.delete_user_fully(uid)
                logger.info(f"🧹 Full cleanup executed for user {uid} after deleting last account {acc_id}")
            except Exception as e:
                logger.error(f"❌ Failed to fully delete user {uid}: {e}")

            await callback_query.message.edit_caption(
                caption=(
                    "<blockquote><b>🧹 Account & Data Removed</b></blockquote>\n\n"
                    "Your account was deleted, and since it was your <b>last account</b>, "
                    "all your related data (ads, groups, analytics, and sessions) "
                    "has been permanently removed from the system."
                ),
                reply_markup=kb([[InlineKeyboardButton("🏠 Back to Menu", callback_data="menu_main")]]),
                parse_mode=ParseMode.HTML
            )

            await send_dm_log(uid, f"<b>🧹 Cleanup complete:</b> User deleted last account {acc_id} — full purge done.")

        else:
            # Not last account — just show confirmation
            await callback_query.message.edit_caption(
                caption=(
                    "<blockquote><b>✅ Account Deleted</b></blockquote>\n\n"
                    f"Account removed successfully.\nYou still have <b>{remaining}</b> account(s) connected."
                ),
                reply_markup=kb([[InlineKeyboardButton("🔙 Back to Accounts", callback_data="delete_accounts")]]),
                parse_mode=ParseMode.HTML
            )

            await send_dm_log(uid, f"<b>✅ Account deleted:</b> {acc_id}")
            logger.info(f"User {uid} deleted account {acc_id}. Remaining accounts: {remaining}")

    except Exception as e:
        logger.error(f"❌ Unexpected error in delete_account_callback for user {uid}: {e}")
        try:
            await callback_query.answer("Error deleting account. Try again.", show_alert=True)
        except Exception:
            pass


@pyro.on_callback_query(filters.regex("view_acc_"))
async def view_account_callback(client, callback_query):
    """Handle view specific account callback"""
    try:
        uid = callback_query.from_user.id
        acc_id = callback_query.data.replace("view_acc_", "")
        accounts = db.get_user_accounts(uid)
        account = next((acc for acc in accounts if str(acc['_id']) == acc_id), None)
        if not account:
            await callback_query.answer(f"Custom settings for Accounts is not available in free version, DM @{config.ADMIN_USERNAME} to set custom settings for each account.", show_alert=True)
            return
        
        status = "Active ✅" if account['is_active'] else "Inactive ⭕"
        caption = (
            f"<blockquote><b>ACCOUNT DETAILS</b></blockquote>\n\n"
            f"Phone: <code>{account['phone_number']}</code>\n"
            f"<b>Status:</b> {status}\n\n"
            f"<blockquote>Choose an action:</blockquote>"
        )
        
        await callback_query.message.edit_caption(
            caption=caption,
            reply_markup=kb([
                [InlineKeyboardButton("Delete Account", callback_data=f"delete_acc_{acc_id}")],
                [InlineKeyboardButton("Back", callback_data="delete_accounts")]
            ]),
            parse_mode=ParseMode.HTML
        )
        logger.info(f"View account details for {account['phone_number']} by user {uid}")
        
    except Exception as e:
        logger.error(f"Error in view_account callback: {e}")
        await callback_query.answer("Error loading account details. Try again.", show_alert=True)

@pyro.on_callback_query(filters.regex("^info_saved_messages$"))
async def info_saved_messages_callback(client, callback_query):
    """Show comprehensive information about the new Saved Messages system"""
    try:
        uid = callback_query.from_user.id
        current_count = db.get_user_saved_messages_count(uid)
        
        await callback_query.message.edit_media(
            media=InputMediaPhoto(
                media=config.START_IMAGE,
                caption=f"""<blockquote><b>📨 SAVED MESSAGES SYSTEM</b></blockquote>

<b>🎯 How It Works:</b>
Bot uses messages <b>directly from your Telegram Saved Messages</b> - no setup needed!

<b>📋 Message Rotation:</b>
• Currently using: <b>{current_count} messages</b> for rotation
• Cycle 1 → 1st message from Saved Messages
• Cycle 2 → 2nd message from Saved Messages  
• Cycle 3 → 3rd message (if using 3+ messages)
• After last message → Back to 1st (repeats automatically)

<b>✅ Benefits:</b>
• ✨ Emojis work perfectly
• 📸 All media supported (photos, videos, etc.)
• 🔄 Automatic rotation
• ⚡ Real-time updates

<b>💡 How to Use:</b>
<b>1.</b> Open Telegram "Saved Messages" chat
<b>2.</b> Save your ad messages there
<b>3.</b> Click "Select Saved Messages 📝" to choose how many to use
<b>4.</b> Click "Start Broadcasting"
<b>5.</b> Bot forwards them automatically!

<b>🔥 Tips:</b>
• Use emojis ✨
• Mix text and media 📸
• Add/delete messages anytime 🔄

<blockquote><b>💎 Just save messages and broadcast!</b></blockquote>""",
                parse_mode=ParseMode.HTML
            ),
            reply_markup=kb([
                [InlineKeyboardButton("🚀 Start Broadcasting Now", callback_data="start_broadcast")],
                [InlineKeyboardButton("⬅️ Back to Menu", callback_data="menu_main")]
            ])
        )
        await callback_query.answer()
        logger.info(f"Saved Messages info displayed for user {callback_query.from_user.id}")
        
    except Exception as e:
        logger.error(f"Error in info_saved_messages callback: {e}")
        await callback_query.answer("Error loading info. Try again.", show_alert=True)

@pyro.on_callback_query(filters.regex("^select_saved_messages_count$"))
async def select_saved_messages_count_callback(client, callback_query):
    """Ask user how many saved messages to use for rotation"""
    try:
        await callback_query.answer()
        uid = callback_query.from_user.id
        current_count = db.get_user_saved_messages_count(uid)
        
        await callback_query.message.edit_media(
            InputMediaPhoto(
                media=config.START_IMAGE,
                caption=f"""<blockquote><b>📝 SELECT SAVED MESSAGES COUNT</b></blockquote>

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

<b>📨 Enter a number (1-10):</b>
Reply with how many messages to use from your Saved Messages.""",
                parse_mode=ParseMode.HTML
            ),
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Back 🔙", callback_data="menu_broadcast")]
            ])
        )
        
        # Set user state to wait for input
        db.set_user_state(uid, "waiting_saved_messages_count")
        
    except Exception as e:
        logger.error(f"Error in select_saved_messages_count callback: {e}")
        await callback_query.answer("Error loading settings", show_alert=True)

# REMOVED: Old saved messages management system - replaced with direct Saved Messages integration

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
                        "<b>📱 Get your API ID:</b>\n"
                        "1. Go to https://my.telegram.org\n"
                        "2. Login with your phone number\n"
                        "3. Go to 'API Development tools'\n"
                        "4. Create a new application\n"
                        "5. Copy the <b>API ID</b> (numbers only)\n\n"
                        "<b>💬 Send your API ID now:</b>\n"
                        "Example: 1234567",
                parse_mode=ParseMode.HTML
            ),
            reply_markup=kb([
                [InlineKeyboardButton("❌ Cancel", callback_data="host_account")]
            ])
        )
        logger.info(f"API credentials setup started for user {uid}")
        
    except Exception as e:
        logger.error(f"Error in set_api_credentials callback: {e}")
        await callback_query.answer("Error starting API setup. Try again.", show_alert=True)

@pyro.on_callback_query(filters.regex("set_delay|set_time_interval"))
async def set_delay_callback(client, callback_query):
    """Handle set delay callback"""
    try:
        uid = callback_query.from_user.id
        current_delay = db.get_user_ad_delay(uid) or 600  # Default to 10 minutes
        user = db.get_user(uid)
        user_type = user.get('user_type', 'free')
        
        min_delay = 300  # 5 minutes for all users
        
        await callback_query.message.edit_media(
            media=InputMediaPhoto(
                media=config.START_IMAGE,
                caption=f"""<blockquote><b>⏱️ SET BROADCAST CYCLE INTERVAL 🚀</b></blockquote>\n\n"""
                        f"<u>Current Interval:</u> <code>{current_delay} seconds</code>\n"
                        f"<u>Account Type:</u> {user_type.upper()}\n\n"
                        f"<b>Recommended Intervals:</b>\n"
                        f"•{min_delay}s - Safe (Recommended) ✅\n"
                        f"•{min_delay * 2}s - Conservative �\n"
                        f"•{min_delay * 4}s - Ultra Safe �\n\n"
                        f"<blockquote>To set custom time interval send a number (in seconds):\n"
                        f"Minimum allowed: {min_delay}s for {user_type.upper()} users\n"
                        f"(Note: Short intervals may risk account restrictions)</blockquote>",
                parse_mode=ParseMode.HTML
            ),
            reply_markup=kb([
                [InlineKeyboardButton("20min 🟢", callback_data="quick_delay_1200"),
                 InlineKeyboardButton("5min 🔴", callback_data="quick_delay_300"),
                 InlineKeyboardButton("10min 🟡", callback_data="quick_delay_600")],
                [InlineKeyboardButton("Back 🔙", callback_data="menu_main")]
            ])
        )
        db.set_user_state(uid, "waiting_broadcast_delay")
        logger.info(f"Set delay interface shown for user {uid}")
        
    except Exception as e:
        logger.error(f"Error in set_delay callback: {e}")
        await callback_query.answer("Error loading delay setup. Try again.", show_alert=True)

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
        
        if delay >= 1200:
            mode = "Conservative"
        elif delay >= 600:
            mode = "Balanced"
        elif delay >= 300:
            mode = "Aggressive"
        else:
            mode = "Custom"
        
        await callback_query.message.edit_caption(
            caption=f"""<blockquote><b>CYCLE INTERVAL UPDATED!</b></blockquote>\n\n"""
                    f"<u>New Interval:</u> <code>{delay} seconds</code> \n"
                    f"<b>Mode:</b> <i>{mode}</i>\n\n"
                    f"<blockquote>Ready for broadcasting!</blockquote>",
            reply_markup=kb([[InlineKeyboardButton("Back", callback_data="menu_main")]]),
            parse_mode=ParseMode.HTML
        )
        await send_dm_log(uid, f"<b> Broadcast interval updated:</b> {delay} seconds ({mode})")
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
        if db.get_broadcast_state(uid).get("running"):
            await callback_query.answer("Broadcast already running!", show_alert=True)
            return
        
        # Check if user has enough saved messages for their selected count
        user_msg_count = db.get_user_saved_messages_count(uid)
        
        # Get one of user's accounts to check Saved Messages
        accounts = db.get_user_accounts(uid) or []
        if accounts:
            try:
                # Try to connect and check saved messages
                acc = accounts[0]
                session_encrypted = acc.get("session_string") or ""
                session_str = cipher_suite.decrypt(session_encrypted.encode()).decode()
                
                # Get user's API credentials
                credentials = db.get_user_api_credentials(acc['user_id'])
                if credentials:
                    tg_client = TelegramClient(StringSession(session_str), credentials['api_id'], credentials['api_hash'])
                    await tg_client.start()
                    
                    # Count available saved messages
                    saved_msgs_list = []
                    messages = await tg_client.get_messages("me", limit=20)
                    for msg in messages:
                        if msg.text or msg.media:
                            saved_msgs_list.append(msg)
                    
                    await tg_client.disconnect()
                    
                    # Check if user has enough messages
                    if len(saved_msgs_list) < user_msg_count:
                        await callback_query.answer()
                        await callback_query.message.edit_media(
                            InputMediaPhoto(
                                media=config.START_IMAGE,
                                caption=f"""<blockquote><b>⚠️ NOT ENOUGH SAVED MESSAGES!</b></blockquote>

<b>Selected Message Count:</b> <code>{user_msg_count}</code> messages
<b>Available in Saved Messages:</b> <code>{len(saved_msgs_list)}</code> messages

<b>❌ Problem:</b>
You've selected to use {user_msg_count} messages for rotation, but you only have {len(saved_msgs_list)} message{'s' if len(saved_msgs_list) != 1 else ''} in your Telegram Saved Messages.

<b>✅ Solution (choose one):</b>

<b>Option 1:</b> Add more messages to your Saved Messages
• Open Telegram "Saved Messages" chat
• Save at least {user_msg_count - len(saved_msgs_list)} more message{'s' if (user_msg_count - len(saved_msgs_list)) > 1 else ''}
• Return and start broadcast

<b>Option 2:</b> Reduce your message count setting
• Click "Select Saved Messages 📝"
• Enter {len(saved_msgs_list)} or less
• Start broadcast

<blockquote><i>Make sure you have enough messages before broadcasting!</i></blockquote>""",
                                parse_mode=ParseMode.HTML
                            ),
                            reply_markup=InlineKeyboardMarkup([
                                [InlineKeyboardButton("Select Saved Messages 📝", callback_data="select_saved_messages_count")],
                                [InlineKeyboardButton("Back 🔙", callback_data="menu_broadcast")]
                            ])
                        )
                        return
            except Exception as e:
                logger.warning(f"Could not verify saved messages count for user {uid}: {e}")
                # Continue anyway - let the broadcast handle it
        
        # Messages will be fetched from Saved Messages during broadcast - no pre-check needed
        
        accounts = db.get_user_accounts(uid)
        if not accounts:
            await callback_query.answer("No accounts hosted yet!", show_alert=True)
            return
        
        if not db.get_logger_status(uid):
            try:
                await callback_query.message.edit_caption(
                    caption="<b>⚠️ Logger bot not started yet!</b>\n\n"
                            f"Please start @{config.LOGGER_BOT_USERNAME.lstrip('@')} to receive Advertising logs.\n"
                            "<i>After starting, return here to begin Advertising.</i>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb([
                        [InlineKeyboardButton("Start Logger Bot 📩", url=f"https://t.me/{config.LOGGER_BOT_USERNAME.lstrip('@')}")],
                        [InlineKeyboardButton("Back", callback_data="menu_main")]
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
                caption="""<blockquote> <b>BROADCAST ON! 🚀</b></blockquote>\n\n"""
                        """Your ads are now being sent to the groups your account is joined in.\n"""
                        f"""Logs will be sent to your DM via @{config.LOGGER_BOT_USERNAME.lstrip('@')}.</i>""",
                parse_mode=ParseMode.HTML,
                reply_markup=kb([[InlineKeyboardButton("Back", callback_data="menu_main")]])
            )
            await callback_query.answer("Broadcast started! ▶️", show_alert=True)
            logger.info(f"Broadcast started via callback for user {uid}")
        except Exception as e:
            logger.error(f"Failed to edit BROADCAST ON message for {uid}: {e}")
            try:
                await client.send_photo(
                    chat_id=uid,
                    photo=config.START_IMAGE,
                    caption="""<blockquote><b>BROADCAST ON! 🚀</b></blockquote>\n\n"""
                            """Your ads are now being sent to the groups your account is joined in.\n"""
                            f"""Logs will be sent to your DM via @{config.LOGGER_BOT_USERNAME.lstrip('@')}.""",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb([[InlineKeyboardButton("Back 🔙", callback_data="menu_main")]])
                )
                await callback_query.answer("Broadcast started! 🚀", show_alert=True)
                await send_dm_log(uid, "<b>Broadcast started! Logs will come here</b>")
                logger.info(f"Broadcast started via callback for user {uid} (fallback send)")
            except Exception as e2:
                logger.error(f"Failed to send fallback BROADCAST ON message for {uid}: {e2}")
                await callback_query.answer("Error starting broadcast. Please try again. 😔", show_alert=True)
                await send_dm_log(uid, f"<b>❌ Failed to start broadcast:</b> {str(e2)} 😔")
                
    except Exception as e:
        logger.error(f"Error in start_broadcast callback for {uid}: {e}")
        await callback_query.answer("Error starting broadcast. Contact support. 😔", show_alert=True)
        await send_dm_log(uid, f"<b>❌ Failed to start broadcast:</b> {str(e)} 😔")

@pyro.on_callback_query(filters.regex("stop_broadcast"))
async def stop_broadcast_callback(client, callback_query):
    """Handle stop broadcast callback"""
    try:
        uid = callback_query.from_user.id
        stopped = await stop_broadcast_task(uid)
        if not stopped:
            await callback_query.answer("No broadcast running!", show_alert=True)
            return
        
        await callback_query.answer("Broadcast stopped! ⏸️", show_alert=True)
        try:
            await callback_query.message.edit_caption(
                caption="""<blockquote><b>BROADCAST STOPPED! ✨</b></blockquote>\n\n"""
                        """Your broadcast has been stopped.\n"""
                        """Check analytics for final stats.""",
                reply_markup=kb([[InlineKeyboardButton("Back", callback_data="menu_main")]]),
                parse_mode=ParseMode.HTML
            )
        except Exception as e:
            logger.error(f"Failed to edit BROADCAST STOPPED message for {uid}: {e}")
            await client.send_photo(
                chat_id=uid,
                photo=config.START_IMAGE,
                caption="""<blockquote><b>BROADCAST STOPPED!</b></blockquote>\n\n"""
                        """Your broadcast has been stopped.\n"""
                        """Check analytics for final stats.""",
                parse_mode=ParseMode.HTML,
                reply_markup=kb([[InlineKeyboardButton("Back", callback_data="menu_main")]])
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
        
        # Get user stats with null checks
        user_stats = db.get_user_analytics(uid) if hasattr(db, 'get_user_analytics') else {}
        if not user_stats:
            user_stats = db.get_user_stats(uid) if hasattr(db, 'get_user_stats') else {}
        if not user_stats:
            user_stats = {}
        
        accounts = db.get_user_accounts(uid) or []
        
        # Get logger failures with null check
        try:
            logger_failures = len(db.get_logger_failures(uid)) if hasattr(db, 'get_logger_failures') else 0
        except:
            logger_failures = 0
        
        # Calculate success rate
        total_sent = user_stats.get('total_sent', 0)
        total_failed = user_stats.get('total_failed', 0)
        total_messages = total_sent + total_failed
        success_rate = (total_sent / total_messages * 100) if total_messages > 0 else 0
        
        # Get user account limit
        user = db.get_user(uid)
        account_limit = user.get('accounts_limit', 1) if user else 1
        active_accounts = len([a for a in accounts if a.get('is_active', False)])
        
        analytics_text = (
            f"<blockquote><b>📊 AZTECH ADS BOT ANALYTICS</b></blockquote>\n\n"
            f"<b>📈 Broadcast Statistics:</b>\n"
            f"• Cycles Completed: <code>{user_stats.get('total_cycles', 0)}</code> 🔄\n"
            f"• Messages Sent: <code>{total_sent:,}</code> ✅\n"
            f"• Failed Sends: <code>{total_failed:,}</code> ❌\n"
            f"• Success Rate: <code>{success_rate:.1f}%</code> 📊\n\n"
            f"<b>👤 Account Status:</b>\n"
            f"• Active Accounts: <code>{active_accounts}/{account_limit}</code> 📱\n"
            f"• Logger Failures: <code>{logger_failures}</code> 📝\n\n"
            f"<b>⚙️ Settings:</b>\n"
            f"• Cycle Interval: <code>{db.get_user_ad_delay(uid)}s</code> ⏰\n\n"
            f"<i>Keep tracking your broadcast performance! 🚀</i>"
        )
        
        try:
            await callback_query.message.edit_caption(
                caption=analytics_text,
                reply_markup=kb([
                    [InlineKeyboardButton("🔄 Refresh Analytics", callback_data="view_analytics")],
                    [InlineKeyboardButton("🔙 Back to Menu", callback_data="menu_broadcast")]
                ]),
                parse_mode=ParseMode.HTML
            )
            logger.info(f"Analytics shown for user {uid}")
        except Exception as edit_error:
            # If content is same, just answer callback (no error to user)
            error_msg = str(edit_error).lower()
            if "message is not modified" in error_msg or "same" in error_msg or "not modified" in error_msg:
                await callback_query.answer("✅ Analytics already up to date!", show_alert=False)
                logger.debug(f"Analytics content unchanged for user {uid}")
            else:
                # Real error, re-raise
                raise edit_error
        
    except Exception as e:
        logger.error(f"Error in analytics callback: {e}")
        await callback_query.answer("❌ Error loading analytics. Try again.", show_alert=True)

@pyro.on_callback_query(filters.regex("detailed_report"))
async def detailed_report_callback(client, callback_query):
    """Handle detailed report callback"""
    try:
        uid = callback_query.from_user.id
        user_stats = db.get_user_analytics(uid)
        accounts = db.get_user_accounts(uid)
        logger_failures = db.get_logger_failures(uid)
        
        detailed_text = (
            f"<blockquote><b>DETAILED ANALYTICS REPORT:</b></blockquote>\n\n"
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
            f"- <b>Active Accounts:</b> <code>{len([a for a in accounts if a['is_active']])}</code> 🟢\n"
            f"- <u>Inactive Accounts:</u> <i>{len([a for a in accounts if not a['is_active']])}</i> 🔴\n\n"
            f"<blockquote><b>Current Delay:</b> <code>{db.get_user_ad_delay(uid)}s</code></blockquote>"
        )
        
        await callback_query.message.edit_caption(
            caption=detailed_text,
            reply_markup=kb([
                [InlineKeyboardButton("Back", callback_data="analytics")]
            ]),
            parse_mode=ParseMode.HTML
        )
        logger.info(f"Detailed report shown for user {uid}")
        
    except Exception as e:
        logger.error(f"Error in detailed_report callback: {e}")
        await callback_query.answer("Error loading detailed report. Try again.", show_alert=True)

# =======================================================

# =======================================================
# 💬 MESSAGE HANDLERS
# =======================================================

@pyro.on_message((filters.text | filters.media) & filters.private & ~filters.command(["start", "bd", "me", "stats", "stop"]))
async def handle_text_message(client, message):
    """Handle text messages for various states"""
    try:
        uid = message.from_user.id
        state = db.get_user_state(uid)
        text = message.text.strip()

        logger.info(f"📩 Received message from {uid} | state='{state}' | text_length={len(text)}")

        # ✅ 1️⃣ AUTO REPLY MESSAGE SETTING
# Auto-reply functionality removed
        if False:  # Disabled auto-reply check
            account_id = state.split(":", 1)[1]
            
            # Get account details including phone number
            accounts = db.get_user_accounts(uid)
            account = next((acc for acc in accounts if str(acc['_id']) == account_id), None)
            
            if not account:
                await message.reply("❌ Account not found!", parse_mode=ParseMode.HTML)
                return
                
            phone_number = account['phone_number']
            
            logger.info(f"🔧 Processing auto-reply message for user {uid}, account {phone_number}")
            
            try:
                db.set_auto_reply(uid, account_id, text, enabled=True)
                db.set_user_state(uid, "")
                logger.info(f"✅ Auto-reply message saved for user {uid}, account {phone_number}")
                
                await message.reply(
                    f"<blockquote><b>✅ Auto-reply message saved!</b></blockquote>\n\n"
                    f"<b>📱 Phone:</b> <code>{phone_number}</code>\n"
                    f"<b>🆔 Account ID:</b> <code>{account_id}</code>\n\n"
                    f"<code>{text}</code>\n\n"
                    f"<i>Auto-reply enabled for this account.</i>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb([[InlineKeyboardButton("⬅️ Back", callback_data=f"auto_reply:{account_id}")]])
                )
                
                # Log to logger bot
                await send_logger_message(
                    f"✍️ <b>Auto-Reply Message Updated</b>\n"
                    f"📱 Account: <code>{phone_number}</code>\n"
                    f"🆔 ID: <code>{account_id}</code>\n"
                    f"👤 User: <code>{uid}</code>\n"
                    f"💬 Message: <code>{text[:100]}{'...' if len(text) > 100 else ''}</code>"
                )
                
                logger.info(f"✅ Auto-reply message set for user {uid}, account {phone_number}")
            except Exception as e:
                logger.error(f"❌ Failed to set auto-reply for {uid}:{account_id}: {e}")
                await message.reply(
                    f"<b>❌ Failed to save auto-reply!</b>\n\n<i>{str(e)}</i>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb([[InlineKeyboardButton("⬅️ Back", callback_data=f"auto_reply:{account_id}")]])
                )
            return


        # REMOVED: waiting_saved_msg state - no longer needed with direct Saved Messages system

        elif state == "waiting_temp_api_id":
            # Handle temporary API ID input (not stored permanently)
            try:
                temp_api_id = int(message.text.strip())
                if temp_api_id <= 0:
                    raise ValueError("Invalid API ID")
                
                # Store temporarily in session
                db.set_user_temp_data(uid, "temp_api_id", temp_api_id)
                db.set_user_state(uid, "waiting_temp_api_hash")
                
                await message.reply_text(
                    "<b>🔑 STEP 2/2: API HASH</b>\n\n"
                    "✅ API ID received!\n\n"
                    "Now enter your <b>API Hash</b> (long string)\n\n"
                    "<b>📱 Get it from:</b> https://my.telegram.org\n\n"
                    "<b>Example:</b> <code>abc123def456...</code>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb([
                        [InlineKeyboardButton("❌ Cancel", callback_data="host_account")]
                    ])
                )
                logger.info(f"Temp API ID received for user {uid}")
            except ValueError:
                await message.reply_text(
                    "❌ <b>Invalid API ID</b>\n\n"
                    "Please send only numbers.\n"
                    "Example: 12345678",
                    parse_mode=ParseMode.HTML
                )
            return

        elif state == "waiting_temp_api_hash":
            # Handle temporary API Hash input
            temp_api_hash = message.text.strip()
            if len(temp_api_hash) < 10:
                await message.reply_text(
                    "❌ <b>Invalid API Hash</b>\n\n"
                    "API Hash should be longer (usually 32+ characters).",
                    parse_mode=ParseMode.HTML
                )
                return
            
            # Get stored temp API ID
            temp_api_id = db.get_user_temp_data(uid, "temp_api_id")
            if not temp_api_id:
                await message.reply_text(
                    "❌ <b>Session expired</b>\n\n"
                    "Please start over.",
                    parse_mode=ParseMode.HTML
                )
                return
            
            # Store both temporarily for phone number entry
            db.set_user_temp_data(uid, "temp_api_hash", temp_api_hash)
            db.set_user_state(uid, "telethon_wait_phone")
            
            await message.reply_text(
                "✅ <b>API Credentials Received!</b>\n\n"
                "Now enter the <b>phone number</b> for the account.\n\n"
                "<b>Format:</b> <code>+1234567890</code>",
                parse_mode=ParseMode.HTML
            )
            logger.info(f"Temp API credentials received for user {uid}, ready for phone")
            return

        elif state == "waiting_api_id":
            # Handle API ID input
            try:
                api_id = int(message.text.strip())
                if api_id <= 0:
                    raise ValueError("Invalid API ID")
                
                # Store temporarily and ask for API Hash
                db.set_user_temp_data(uid, "temp_api_id", api_id)
                db.set_user_state(uid, "waiting_api_hash")
                
                await message.reply_text(
                    "<b>🔑 SET API CREDENTIALS - Step 2/2</b>\n\n"
                    "✅ API ID received successfully!\n\n"
                    "<b>📱 Now send your API Hash:</b>\n"
                    "1. From the same page at my.telegram.org\n"
                    "2. Copy the <b>API Hash</b> (long string)\n"
                    "3. Paste it below\n\n"
                    "<b>💬 Send your API Hash now:</b>\n"
                    "Example: abc123def456ghi789...",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb([
                        [InlineKeyboardButton("❌ Cancel", callback_data="host_account")]
                    ])
                )
                logger.info(f"API ID received for user {uid}")
            except ValueError:
                await message.reply_text(
                    "❌ <b>Invalid API ID</b>\n\n"
                    "Please send only the numbers for your API ID.\n"
                    "Example: 1234567\n\n"
                    "Get it from: https://my.telegram.org",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb([
                        [InlineKeyboardButton("❌ Cancel", callback_data="host_account")]
                    ])
                )
            return

        elif state == "waiting_api_hash":
            # Handle API Hash input
            api_hash = message.text.strip()
            if len(api_hash) < 10:  # Basic validation
                await message.reply_text(
                    "❌ <b>Invalid API Hash</b>\n\n"
                    "API Hash should be a longer string (usually 32+ characters).\n\n"
                    "Get it from: https://my.telegram.org",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb([
                        [InlineKeyboardButton("❌ Cancel", callback_data="host_account")]
                    ])
                )
                return
            
            # Get stored API ID and save both
            temp_api_id = db.get_user_temp_data(uid, "temp_api_id")
            if temp_api_id:
                # Store API credentials
                if db.store_user_api_credentials(uid, temp_api_id, api_hash):
                    # Clean up temp data
                    db.clear_user_temp_data(uid, "temp_api_id")
                    db.set_user_state(uid, "normal")
                    
                    await message.reply_text(
                        "✅ <b>API CREDENTIALS SAVED!</b>\n\n"
                        "Your API credentials have been stored securely.\n\n"
                        "<b>✅ API ID:</b> " + str(temp_api_id) + "\n"
                        "<b>✅ API Hash:</b> " + api_hash[:8] + "..." + "\n\n"
                        "You can now add accounts to the bot!",
                        parse_mode=ParseMode.HTML,
                        reply_markup=kb([
                            [InlineKeyboardButton("📱 Add Account Now", callback_data="host_account")],
                            [InlineKeyboardButton("🔙 Main Menu", callback_data="menu_main")]
                        ])
                    )
                    logger.info(f"API credentials saved for user {uid}")
                else:
                    await message.reply_text(
                        "❌ <b>Failed to save credentials</b>\n\n"
                        "Please try again or contact support.",
                        parse_mode=ParseMode.HTML
                    )
            else:
                await message.reply_text(
                    "❌ <b>Session expired</b>\n\n"
                    "Please start over with API ID setup.",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb([
                        [InlineKeyboardButton("🔄 Start Over", callback_data="set_api_credentials")]
                    ])
                )
            return

        elif state == "telethon_wait_otp":
            # Handle OTP verification for Telethon
            otp_code = message.text.strip()
            if not otp_code.isdigit() or len(otp_code) != 5:
                await message.reply_text(
                    "❌ <b>Invalid OTP Code</b>\n\n"
                    "Please enter the 5-digit code sent to your phone.\n"
                    "Example: 12345",
                    parse_mode=ParseMode.HTML
                )
                return
            
            try:
                # Store OTP for verification process
                db.set_user_temp_data(uid, "otp_code", otp_code)
                db.set_user_state(uid, "normal")
                
                await message.reply_text(
                    "✅ <b>OTP Received!</b>\n\n"
                    "Processing your account verification...\n"
                    "Please wait while we complete the setup.",
                    parse_mode=ParseMode.HTML
                )
                logger.info(f"OTP received for user {uid}")
                
                # Note: The actual OTP verification will be handled by the Telethon client
                # This just stores the OTP for the process to continue
                
            except Exception as e:
                logger.error(f"Error handling OTP for user {uid}: {e}")
                await message.reply_text(
                    "❌ <b>Error Processing OTP</b>\n\n"
                    "Please try again or contact support.",
                    parse_mode=ParseMode.HTML
                )
            return

        elif state == "waiting_broadcast_delay":
            logger.info(f"🔧 Processing broadcast delay for user {uid}")
            try:
                delay = int(text)
                if delay < 120:
                    await message.reply(
                        f"<blockquote><b>❌ Invalid interval!</b></blockquote>\n\n"
                        f"Minimum interval is 120 seconds.\nPlease enter a valid number",
                        parse_mode=ParseMode.HTML,
                        reply_markup=kb([[InlineKeyboardButton("Back", callback_data="menu_main")]])
                    )
                    return
                if delay > 86400:
                    await message.reply(
                        f"<blockquote><b>❌ Invalid interval!</b></blockquote>\n\n"
                        f"Maximum interval is 86400 seconds (24 hours).\nPlease enter a valid number",
                        parse_mode=ParseMode.HTML,
                        reply_markup=kb([[InlineKeyboardButton("Back", callback_data="menu_main")]])
                    )
                    return

                db.set_user_ad_delay(uid, delay)
                db.set_user_state(uid, "")
                logger.info(f"✅ Broadcast delay set for user {uid}: {delay}s")
                
                if delay >= 1200:
                    mode = "Conservative"
                elif delay >= 600:
                    mode = "Balanced"
                elif delay >= 300:
                    mode = "Aggressive"
                else:
                    mode = "Custom"
                await message.reply(
                    f"<blockquote><b>CYCLE INTERVAL UPDATED! ✅</b></blockquote>\n\n"
                    f"<u>New Interval:</u> <code>{delay} seconds</code>\n"
                    f"<b>Mode:</b> <i>{mode}</i>\n\n"
                    f"<blockquote>Ready for broadcasting!</blockquote>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb([[InlineKeyboardButton("Dashboard 🚪", callback_data="menu_main")]])
                )
                await send_dm_log(uid, f"<b>⏱️ Broadcast interval updated:</b> {delay} seconds ({mode})")
                logger.info(f"⏱️ Delay set for user {uid}: {delay}s")
            except ValueError:
                await message.reply(
                    f"<blockquote><b>❌ Invalid input!</b></blockquote>\n\n"
                    f"<u>Please enter a number (in seconds).</u>\n<i>Example: <code>300</code> for 5 minutes.</i>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb([[InlineKeyboardButton("Back", callback_data="menu_main")]])
                )
            except Exception as e:
                logger.error(f"❌ Failed to set broadcast delay for {uid}: {e}")
                db.set_user_state(uid, "")
                await message.reply(
                    f"<blockquote><b>❌ Failed to set interval!</b></blockquote>\n\n"
                    f"<u>Error:</u> <i>{str(e)}</i>\n"
                    f"<b>Contact:</b> <code>@{config.ADMIN_USERNAME}</code>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb([[InlineKeyboardButton("Dashboard", callback_data="menu_main")]])
                )
        
        elif state == "waiting_saved_messages_count":
            logger.info(f"🔧 Processing saved messages count for user {uid}")
            try:
                count = int(text)
                if count < 1:
                    await message.reply(
                        f"<blockquote><b>❌ Invalid count!</b></blockquote>\n\n"
                        f"Minimum is 1 message.\nPlease enter a valid number",
                        parse_mode=ParseMode.HTML,
                        reply_markup=kb([[InlineKeyboardButton("Back", callback_data="menu_broadcast")]])
                    )
                    return
                if count > 10:
                    await message.reply(
                        f"<blockquote><b>❌ Invalid count!</b></blockquote>\n\n"
                        f"Maximum is 10 messages.\nPlease enter a valid number",
                        parse_mode=ParseMode.HTML,
                        reply_markup=kb([[InlineKeyboardButton("Back", callback_data="menu_broadcast")]])
                    )
                    return

                db.set_user_saved_messages_count(uid, count)
                db.set_user_state(uid, "")
                logger.info(f"✅ Saved messages count set for user {uid}: {count}")
                
                await message.reply(
                    f"<blockquote><b>SAVED MESSAGES COUNT UPDATED! ✅</b></blockquote>\n\n"
                    f"<u>Messages to Use:</u> <code>{count}</code>\n\n"
                    f"<b>How it works:</b>\n"
                    f"• Bot will use first {count} message{'s' if count > 1 else ''} from your Saved Messages\n"
                    f"• Rotation: Cycle 1 → Msg 1, Cycle 2 → Msg 2, etc.\n"
                    f"• After message {count}, it loops back to message 1\n\n"
                    f"<blockquote>Ready for broadcasting!</blockquote>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb([[InlineKeyboardButton("Broadcast Menu 🚀", callback_data="menu_broadcast")]])
                )
                await send_dm_log(uid, f"<b>📝 Saved messages count updated:</b> {count} messages")
                logger.info(f"📝 Saved messages count set for user {uid}: {count}")
            except ValueError:
                await message.reply(
                    f"<blockquote><b>❌ Invalid input!</b></blockquote>\n\n"
                    f"<u>Please enter a number (1-10).</u>\n<i>Example: <code>3</code> for 3 messages.</i>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb([[InlineKeyboardButton("Back", callback_data="menu_broadcast")]])
                )
            except Exception as e:
                logger.error(f"❌ Failed to set saved messages count for {uid}: {e}")
                db.set_user_state(uid, "")
                await message.reply(
                    f"<blockquote><b>❌ Failed to set count!</b></blockquote>\n\n"
                    f"<u>Error:</u> <i>{str(e)}</i>\n"
                    f"<b>Contact:</b> <code>@{config.ADMIN_USERNAME}</code>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb([[InlineKeyboardButton("Broadcast Menu", callback_data="menu_broadcast")]])
                )
            return

        # ✅ 1️⃣ API ID INPUT
        elif state == "waiting_api_id":
            logger.info(f"🔧 Processing API ID for user {uid}")
            try:
                api_id = int(text.strip())
                db.set_temp_data(uid, "api_id", api_id)
                db.set_user_state(uid, "waiting_api_hash")
                await message.reply(
                    f"✅ <b>API ID saved!</b>\n\n"
                    f"Now please enter your <b>API Hash</b>:\n\n"
                    f"📱 <b>Example:</b> <code>abcd1234efgh5678...</code>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb([[InlineKeyboardButton("⬅️ Back", callback_data="menu_main")]])
                )
                return
            except ValueError:
                await message.reply(
                    f"❌ <b>Invalid API ID!</b>\n\n"
                    f"Please enter a valid numeric API ID.\n\n"
                    f"📱 <b>Example:</b> <code>12345678</code>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb([[InlineKeyboardButton("⬅️ Back", callback_data="menu_main")]])
                )
                return

        # ✅ 2️⃣ API HASH INPUT
        elif state == "waiting_api_hash":
            logger.info(f"🔧 Processing API Hash for user {uid}")
            api_hash = text.strip()
            if len(api_hash) < 10:
                await message.reply(
                    f"❌ <b>Invalid API Hash!</b>\n\n"
                    f"API Hash should be longer than 10 characters.\n\n"
                    f"📱 <b>Example:</b> <code>abcd1234efgh5678ijkl9012</code>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb([[InlineKeyboardButton("⬅️ Back", callback_data="menu_main")]])
                )
                return
            
            api_id = db.get_temp_data(uid, "api_id")
            if not api_id:
                await message.reply(
                    f"❌ <b>Session expired!</b>\n\n"
                    f"Please start the account addition process again.",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb([[InlineKeyboardButton("⬅️ Back", callback_data="menu_main")]])
                )
                return
            
            # Store API credentials in database (needed for broadcasting and session management)
            db.store_user_api_credentials(uid, api_id, api_hash)
            db.set_temp_data(uid, "api_hash", api_hash)  # Keep in temp for immediate use
            db.set_user_state(uid, "telethon_wait_phone")
            await message.reply(
                f"✅ <b>API Credentials saved temporarily!</b>\n\n"
                f"Now please enter your <b>phone number</b> with country code:\n\n"
                f"📱 <b>Example:</b> <code>+1234567890</code>",
                parse_mode=ParseMode.HTML,
                reply_markup=kb([[InlineKeyboardButton("⬅️ Back", callback_data="menu_main")]])
            )
            return

        # ✅ 3️⃣ PHONE NUMBER INPUT
        elif state == "telethon_wait_phone":
            logger.info(f"🔧 Processing phone number for user {uid}")
            if not validate_phone_number(text):
                await message.reply(
                    f"<blockquote><b>❌ Invalid phone number!</b></blockquote>\n\n"
                    f"<u>Please use international format.</u>\n"
                    f"<i>Example: <code>+1234567890</code></i>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb([[InlineKeyboardButton("Back", callback_data="menu_main")]])
                )
                return
                
            status_msg = await message.reply(
                f"<blockquote><b>⏳ Hold! We're trying to OTP...</b></blockquote>\n\n"
                f"<u>Phone:</u> <code>{text}</code> \n"
                f"<i>Please wait a moment.</i> ",
                parse_mode=ParseMode.HTML
            )
            
            try:
                # Get API credentials from database
                credentials = db.get_user_api_credentials(uid)
                
                if not credentials:
                    await message.reply(
                        f"❌ <b>API credentials not found!</b>\n\n"
                        f"Please restart the account addition process.",
                        parse_mode=ParseMode.HTML,
                        reply_markup=kb([[InlineKeyboardButton("⬅️ Back", callback_data="menu_main")]])
                    )
                    return
                
                # Try to send OTP with API credentials
                tg = TelegramClient(StringSession(), credentials['api_id'], credentials['api_hash'])
                await tg.connect()
                
                try:
                    sent_code = await tg.send_code_request(text)
                    session_str = tg.session.save()
                except Exception as api_error:
                    # API credentials are wrong - delete them from database
                    logger.error(f"Invalid API credentials for user {uid}: {api_error}")
                    db.delete_user_api_credentials(uid)
                    await status_msg.edit_caption(
                        f"<blockquote><b>❌ INVALID API CREDENTIALS!</b></blockquote>\n\n"
                        f"<u>Error:</u> <i>{str(api_error)}</i>\n\n"
                        f"Your API ID or API Hash is incorrect.\n"
                        f"They have been removed from the database.\n\n"
                        f"<b>Please click 'Add Account' again and enter correct API credentials.</b>",
                        parse_mode=ParseMode.HTML,
                        reply_markup=kb([[InlineKeyboardButton("Add Account", callback_data="host_account")]])
                    )
                    db.set_user_state(uid, "")
                    await send_dm_log(uid, f"<b>❌ Invalid API credentials removed. Please set correct ones.</b>")
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
                logger.info(f"✅ OTP sent to {text} for user {uid}")

                base_caption = (
                    f"<blockquote><b>OTP sent to <code>{text}</code>! ✅</b></blockquote>\n\n"
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
                    f"<blockquote><b>❌ Invalid phone number! </b></blockquote>\n\n"
                    f"<u>Please check the number and try again.</u>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb([[InlineKeyboardButton("Back", callback_data="menu_main")]])
                )
            except Exception as e:
                logger.error(f"Failed to send OTP for {uid}: {e}")
                db.set_user_state(uid, "")
                await status_msg.edit_caption(
                    f"<blockquote><b>❌ Failed to send OTP!</b></blockquote>\n\n"
                    f"<u>Error:</u> <i>{str(e)}</i>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb([[InlineKeyboardButton("Back", callback_data="menu_main")]])
                )
                await send_dm_log(uid, f"<b>❌ Failed to send OTP for phone:</b> {str(e)}")
            finally:
                try:
                    await tg.disconnect()
                except:
                    pass
            return

        # ✅ 5️⃣ 2FA PASSWORD INPUT
        elif state == "telethon_wait_password":
            logger.info(f"🔧 Processing 2FA password for user {uid}")
            temp_encrypted = db.get_temp_data(uid, "session")
            if not temp_encrypted:
                await message.reply(
                    f"<blockquote><b>❌ Session expired!</b></blockquote>\n\n"
                    f"<u>Please restart the process.</u>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb([[InlineKeyboardButton("Back", callback_data="menu_main")]])
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
                    f"<blockquote><b>❌ Corrupted session data!</b></blockquote>\n\n"
                    f"<b>Please restart the process.</b>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb([[InlineKeyboardButton("Back", callback_data="menu_main")]])
                )
                db.set_user_state(uid, "")
                db.delete_temp_data(uid, "session")
                return

            # Get API credentials from database
            credentials = db.get_user_api_credentials(uid)
            
            if not credentials:
                await message.reply(
                    f"❌ <b>API credentials not found!</b>\n\n"
                    f"Please restart the account addition process.",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb([[InlineKeyboardButton("⬅️ Back", callback_data="menu_main")]])
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
                logger.info(f"✅ 2FA completed and account added for user {uid}")
                
                await message.reply(
                    f"<blockquote><b>Account added!✅ </b></blockquote>\n\n"
                    f"<u>Phone:</u> <code>{phone}</code>\n"
                    "•Account is ready for broadcasting!\n\n\n"
                    "<b>Note: Your account is ready for broadcasting!</b>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb([[InlineKeyboardButton("Dashboard", callback_data="menu_main")]])
                )
                await send_dm_log(uid, f"<b>Account added successfully ✅:</b> <code>{phone}</code> ✨")
                
                # Auto-select all groups from this account
                asyncio.create_task(auto_select_all_groups(uid, phone))
                # Note: API credentials in temp (api_id, api_hash) will be cleaned up after auto-selection
            except PasswordHashInvalidError:
                await message.reply(
                    f"<blockquote><b>⚠️ Invalid password!</b></blockquote>\n\n"
                    f"<u>Please try again.</u>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb([[InlineKeyboardButton("Back 🔙", callback_data="menu_main")]])
                )
            except Exception as e:
                logger.error(f"Failed to sign in with password for {uid}: {e}")
                db.set_user_state(uid, "")
                db.delete_temp_data(uid, "session")
                await message.reply(
                    f"<blockquote><b>❌ Login failed!</b></blockquote>\n\n"
                    f"<u>Error:</u> <i>{str(e)}</i>\n"
                    f"<b>Contact:</b> <code>@{config.ADMIN_USERNAME}</code>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb([[InlineKeyboardButton("Dashboard 🚪", callback_data="menu_main")]])
                )
                await send_dm_log(uid, f"<b>Account login failed:❌</b> {str(e)}")
            finally:
                try:
                    await tg.disconnect()
                except:
                    pass
            return

        # ✅ 6️⃣ AD MESSAGE STATES (adding / editing)
        elif state == "adding_ad":
            try:
                ad_text = text.strip()
                ad_id = db.add_user_ad_message(uid, ad_text)
                db.set_user_state(uid, "")
                success_msg = await message.reply_text(
                    "✅ Ad added successfully.\nOpen the Ad menu to view, edit, or delete it.",
                    parse_mode=ParseMode.HTML
                )
                # Auto delete the success message after 1 second and redirect to ad menu
                await asyncio.sleep(1)
                try:
                    await success_msg.delete()
                except:
                    pass
                
                # Auto redirect to ads menu
                try:
                    # Create a fake callback query to trigger ads menu
                    from types import SimpleNamespace
                    fake_cq = SimpleNamespace()
                    fake_cq.from_user = message.from_user
                    fake_cq.message = message
                    fake_cq.answer = lambda *args, **kwargs: asyncio.create_task(asyncio.sleep(0))
                    await open_ads_menu(client, fake_cq)
                except Exception as e:
                    logger.error(f"Failed to auto-redirect to ads menu: {e}")
                
                logger.info(f"✅ Added new ad for {uid} (len={len(ad_text)})")
            except ValueError as ve:
                db.set_user_state(uid, "")
                await message.reply_text(str(ve))
            except Exception as e:
                db.set_user_state(uid, "")
                logger.error(f"Error adding ad for {uid}: {e}")
                await message.reply_text("❌ Failed to add ad. Try again later.")
            return

        elif state and state.startswith("editing_ad:"):
            try:
                aid = state.split(":", 1)[1]
                new_text = text.strip()
                ok = db.update_user_ad_message(uid, aid, new_text)
                db.set_user_state(uid, "")
                if ok:
                    success_msg = await message.reply_text(
                        "✅ Ad updated successfully.\nCheck it in the Ad menu.",
                        parse_mode=ParseMode.HTML
                    )
                    # Auto delete the success message after 1 second and redirect to ad menu
                    await asyncio.sleep(1)
                    try:
                        await success_msg.delete()
                    except:
                        pass
                    
                    # Auto redirect to ads menu
                    try:
                        # Create a fake callback query to trigger ads menu
                        from types import SimpleNamespace
                        fake_cq = SimpleNamespace()
                        fake_cq.from_user = message.from_user
                        fake_cq.message = message
                        fake_cq.answer = lambda *args, **kwargs: asyncio.create_task(asyncio.sleep(0))
                        await open_ads_menu(client, fake_cq)
                    except Exception as e:
                        logger.error(f"Failed to auto-redirect to ads menu: {e}")
                    
                    logger.info(f"✅ Updated ad {aid} for user {uid}")
                else:
                    await message.reply_text("❌ Update failed (maybe ad was deleted).", parse_mode=ParseMode.HTML)
            except Exception as e:
                db.set_user_state(uid, "")
                logger.error(f"Error editing ad for {uid}: {e}")
                await message.reply_text("❌ Failed to update ad. Try again later.")
            return

        # ✅ 7️⃣ UNHANDLED STATE - Log for debugging
        elif state:
            logger.warning(f"⚠️ Unhandled state '{state}' for user {uid} with message: {text[:100]}")

        # ✅ 8️⃣ NO STATE - Regular message (not in any waiting state)
        else:
            logger.info(f"💬 Regular message from user {uid}: {text[:100]}")
            
    except Exception as e:
        logger.error(f"Error in handle_text_message: {e}")

# =======================================================
# ⏰ CYCLE TIMEOUT HANDLERS
# =======================================================

@pyro.on_callback_query(filters.regex("^export_all_groups$"))
async def handle_export_all_groups(client, callback_query):
    """Export all user's groups to a .txt file"""
    uid = callback_query.from_user.id
    
    try:
        await callback_query.answer("🔍 Scanning your groups...", show_alert=False)
        
        status_msg = await callback_query.message.reply_text(
            "🔍 <b>Scanning Your Groups...</b>\n\n"
            "⏳ Please wait while I fetch all your groups from linked accounts...",
            parse_mode=ParseMode.HTML
        )
        
        # Get all user accounts
        accounts = db.get_user_accounts(uid)
        if not accounts:
            await status_msg.edit_text(
                "❌ <b>No Accounts Found</b>\n\n"
                "Please add accounts first to export groups.",
                parse_mode=ParseMode.HTML
            )
            return
        
        all_groups = []
        
        # Scan each account
        for account in accounts:
            phone = account["phone_number"]
            try:
                # Get decrypted session
                session_string = account["session_string"]
                decrypted_session = cipher_suite.decrypt(session_string.encode()).decode()
                
                # Create Telegram client
                from telethon import TelegramClient, functions
                from telethon.sessions import StringSession
                
                client_session = TelegramClient(
                    StringSession(decrypted_session),
                    config.BOT_API_ID,
                    config.BOT_API_HASH
                )
                
                await client_session.connect()
                
                # Get dialogs (chats)
                async for dialog in client_session.iter_dialogs():
                    if dialog.is_group or dialog.is_channel:
                        try:
                            # Generate link
                            if dialog.entity.username:
                                link = f"https://t.me/{dialog.entity.username}"
                            else:
                                # Try to export invite link for private groups
                                try:
                                    invite = await client_session(functions.messages.ExportChatInviteRequest(dialog.id))
                                    link = invite.link
                                except:
                                    link = f"Private Group (ID: {dialog.id})"
                            
                            all_groups.append({
                                "title": dialog.title,
                                "link": link,
                                "phone": phone
                            })
                        except:
                            continue
                
                await client_session.disconnect()
                
            except Exception as e:
                logger.error(f"Error scanning {phone}: {e}")
                continue
        
        if not all_groups:
            await status_msg.edit_text(
                "ℹ️ <b>No Groups Found</b>\n\n"
                "You don't have any groups in your linked accounts.",
                parse_mode=ParseMode.HTML
            )
            return
        
        # Remove duplicates based on link
        unique_groups = {g["link"]: g for g in all_groups}.values()
        
        # Create export file with format: {count}_GROUPS.txt
        total_groups = len(all_groups)
        filename = f"{total_groups}_GROUPS.txt"
        filepath = os.path.join(tempfile.gettempdir(), filename)
        
        with open(filepath, "w", encoding="utf-8") as f:
            f.write("╔══════════════════════════════════════════════════════╗\n")
            f.write("║         AZTECH ADS BOT - MY GROUPS EXPORT           ║\n")
            f.write("╚══════════════════════════════════════════════════════╝\n\n")
            
            # Write only group links with count
            for i, group in enumerate(unique_groups, 1):
                f.write(f"{i}. {group['link']}\n")
        
        # Send file
        await client.send_document(
            chat_id=uid,
            document=filepath,
            caption=(
                f"📂 <b>Your Groups Export</b>\n\n"
                f"✅ Total Groups: <b>{len(unique_groups)}</b>\n"
                f"📱 From Accounts: <b>{len(accounts)}</b>\n"
                f"📅 Exported: <b>{datetime.now().strftime('%Y-%m-%d %H:%M')}</b>"
            ),
            parse_mode=ParseMode.HTML
        )
        
        # Cleanup
        os.remove(filepath)
        
        await status_msg.edit_text(
            f"✅ <b>Export Complete!</b>\n\n"
            f"📂 Exported <b>{len(unique_groups)}</b> groups to file.\n"
            f"Check the file below! 📂",
            parse_mode=ParseMode.HTML
        )
        
    except Exception as e:
        logger.error(f"Error exporting groups: {e}")
        await callback_query.message.reply_text(
            f"❌ <b>Export Failed</b>\n\n"
            f"An error occurred while exporting groups.\n"
            f"Please try again later.",
            parse_mode=ParseMode.HTML
        )


@pyro.on_callback_query(filters.regex("^set_cycle_timeout$"))
async def set_cycle_timeout_callback(client, callback_query):
    """Handle cycle timeout setting callback"""
    try:
        uid = callback_query.from_user.id
        user = db.get_user(uid)
        
        # All users can use cycle timeout - no premium restrictions
        current_timeout = db.get_user_cycle_timeout(uid) if hasattr(db, 'get_user_cycle_timeout') else 600  # Default 10 min
        
        await callback_query.message.edit_caption(
            caption=f"""<blockquote><b>⏰ BROADCAST CYCLE TIMEOUT</b></blockquote>\n\n"""
                    f"<b>Current Timeout:</b> {current_timeout//60} minutes\n\n"
                    f"<i>Bot will pause for this duration after every 5 broadcast cycles to avoid account restrictions.</i>\n\n"
                    f"<blockquote>Select a timeout duration:</blockquote>",
            reply_markup=kb([
                [InlineKeyboardButton("10 Minutes 🟢", callback_data="set_timeout_600"),
                 InlineKeyboardButton("15 Minutes 🟢", callback_data="set_timeout_900")],
                [InlineKeyboardButton("20 Minutes 🟢", callback_data="set_timeout_1200")],
                [InlineKeyboardButton("Back", callback_data="menu_main")]
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
        
        # Save timeout in database
        if hasattr(db, 'set_user_cycle_timeout'):
            db.set_user_cycle_timeout(uid, timeout)
        
        await callback_query.message.edit_caption(
            caption=f"""<blockquote><b>✅ CYCLE TIMEOUT UPDATED!</b></blockquote>\n\n"""
                    f"<b>New Timeout:</b> {timeout//60} minutes\n\n"
                    f"<i>Your broadcast will now pause for {timeout//60} minutes after every 5 cycles.</i>",
            reply_markup=kb([[InlineKeyboardButton("Back", callback_data="menu_main")]]),
            parse_mode=ParseMode.HTML
        )
        
        await send_dm_log(uid, f"<b>⏰ Cycle timeout updated to:</b> {timeout//60} minutes")
        
    except Exception as e:
        logger.error(f"Error in set_specific_timeout callback: {e}")
        await callback_query.answer("Error setting timeout. Try again.", show_alert=True)

# =======================================================
# 📡 LOGGER BOT HANDLERS
# =======================================================

@logger_client.on_message(filters.command(["start"]))
async def logger_start_command(client, message):
    """Handle logger bot start command"""
    try:
        uid = message.from_user.id
        username = message.from_user.username or "Unknown"
        first_name = message.from_user.first_name or "User"
        
        db.create_user(uid, username, first_name)
        if is_owner(uid):
            db.db.users.update_one({"user_id": uid}, {"$set": {"accounts_limit": 1}})
        db.set_logger_status(uid, is_active=True)
        await message.reply(
            f"<b>Welcome to AzTech Ads Bot Logger! 🚀</b>\n\n"
            f"Logs for your ad broadcasts will be sent here.\n"
            f"Start the main bot (@{config.BOT_USERNAME.lstrip('@')}) to begin broadcasting! 🌟",
            parse_mode=ParseMode.HTML
        )
        logger.info(f"Logger bot started by user {uid}")
        
    except Exception as e:
        logger.error(f"Error in logger_start_command: {e}")

# =======================================================
# 👑 ADMIN COMMANDS
# =======================================================


# -------------------- AD MESSAGES INLINE MENU --------------------

async def open_ads_menu(client, callback_query):
    """Open the ads management menu"""
    try:
        uid = callback_query.from_user.id
        ads = db.get_user_ad_messages(uid) or []
        
        caption = f"📢 <b>Ad Messages Manager</b>\n\n"
        caption += f"You have <b>{len(ads)}/{db.MAX_ADS_PER_USER}</b> ad messages.\n\n"
        
        if ads:
            caption += "<b>Your Ads:</b>\n"
            for idx, ad in enumerate(ads, 1):
                preview = ad.get('message', '')[:50]
                if len(ad.get('message', '')) > 50:
                    preview += "..."
                caption += f"{idx}. {preview}\n"
        else:
            caption += "<i>No ads created yet. Click 'Add New' to create one.</i>"
        
        buttons = []
        if len(ads) < db.MAX_ADS_PER_USER:
            buttons.append([InlineKeyboardButton("➕ Add New Ad", callback_data="ad_add_new")])
        
        if ads:
            for ad in ads:
                aid = str(ad["_id"])
                buttons.append([
                    InlineKeyboardButton(f"✏️ Edit", callback_data=f"ad_edit_{aid}"),
                    InlineKeyboardButton(f"🗑️ Delete", callback_data=f"ad_del_{aid}")
                ])
        
        buttons.append([InlineKeyboardButton("⬅️ Back", callback_data="menu_main")])
        
        await callback_query.answer()
        if hasattr(callback_query.message, 'edit_caption'):
            await callback_query.message.edit_caption(
                caption=caption,
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(buttons)
            )
        else:
            await callback_query.message.edit_text(
                text=caption,
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(buttons)
            )
    except Exception as e:
        logger.error(f"open_ads_menu error: {e}")
        await callback_query.answer("Error opening ads menu.", show_alert=True)



@pyro.on_callback_query(filters.regex(r"^ad_add_new$"))
async def ad_add_new(client, callback_query):
    try:
        uid = callback_query.from_user.id
        ads = db.get_user_ad_messages(uid) or []
        if len(ads) >= db.MAX_ADS_PER_USER:
            await callback_query.answer(f"Limit reached ({db.MAX_ADS_PER_USER}). Delete an ad to add a new one.", show_alert=True)
            return
        db.set_user_state(uid, "adding_ad")
        await callback_query.answer()
        await callback_query.message.edit_caption(
            caption="✍️ Send your new ad message now (HTML allowed). After sending, I'll confirm.",
            reply_markup=kb([[InlineKeyboardButton("❌ Cancel", callback_data="menu_ads")]]),
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        logger.error(f"ad_add_new error: {e}")
        await callback_query.answer("Could not enter add mode.", show_alert=True)


@pyro.on_callback_query(filters.regex(r"^ad_edit_(.+)$"))
async def ad_edit(client, callback_query):
    try:
        uid = callback_query.from_user.id
        aid = callback_query.matches[0].group(1)
        ads = db.get_user_ad_messages(uid) or []
        if not any(str(a["_id"]) == aid for a in ads):
            await callback_query.answer("Ad not found.", show_alert=True)
            return
        db.set_user_state(uid, f"editing_ad:{aid}")
        await callback_query.answer()
        await callback_query.message.edit_caption(
            caption="✏️ Send the new text for this ad. It will replace the existing message.",
            reply_markup=kb([[InlineKeyboardButton("❌ Cancel", callback_data="menu_ads")]]),
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        logger.error(f"ad_edit error: {e}")
        await callback_query.answer("Could not enter edit mode.", show_alert=True)


@pyro.on_callback_query(filters.regex(r"^ad_del_(.+)$"))
async def ad_delete(client, callback_query):
    try:
        uid = callback_query.from_user.id
        aid = callback_query.matches[0].group(1)
        ok = db.delete_user_ad_message(uid, aid)
        await callback_query.answer("Deleted ✅" if ok else "Delete failed", show_alert=True)
        # refresh menu
        await open_ads_menu(client, callback_query)
    except Exception as e:
        logger.error(f"ad_delete error: {e}")
        await callback_query.answer("Error deleting ad.", show_alert=True)




# =======================================================
# 🌐 FLASK + MAIN BOT STARTUP (STAY IDLE UNTIL ENABLED)
# =======================================================
# REMOVED: Duplicate imports (all already imported at top)

# =======================================================
# 🌐 FLASK ROUTES
# =======================================================
@app.route("/")
def home():
    """Simple route to confirm the bot is alive."""
    return jsonify({"message": "✅ AzTech Ads Bot is alive and running.", "status": "active"})


@app.route("/health")
def health_check():
    """Health check endpoint for uptime monitoring."""
    try:
        pyro_status = "✅ Running" if pyro.is_connected else "❌ Not Connected"
        db_status = "✅ Connected" if db else "❌ Not Connected"
        return jsonify({
            "status": "ok",
            "bot": "AzTech Ads Bot",
            "pyro_client": pyro_status,
            "database": db_status,
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }), 200
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e),
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }), 500


def run_flask():
    """Start tiny web server for Render health check."""
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, use_reloader=False)


# =======================================================
# 🧹 AUTO CLEANUP THREAD FOR TEMP BLACKLIST
# =======================================================
# REMOVED: cleanup_temp_blacklist function - temp blacklist system removed


# =======================================================
# 🚀 MAIN BOT STARTUP FUNCTION
# =======================================================
async def main():
    """Main bot startup function."""
    try:
        await pyro.start()
        logger.info("✅ Main bot started successfully")

        await logger_client.start()
        logger.info("✅ Logger bot started successfully")

        # Get main event loop
        global MAIN_LOOP
        try:
            MAIN_LOOP = asyncio.get_running_loop()
        except RuntimeError:
            MAIN_LOOP = None

        # Preload caches
        try:
            await preload_chat_cache(pyro)
        except Exception as e:
            logger.warning(f"Preload chat cache failed during startup: {e}")

        # 🛑 Stop all running broadcasts on startup (safe shutdown resume)
        try:
            running_states = db.db.broadcast_states.update_many(
                {"running": True},
                {"$set": {"running": False, "paused": False, "updated_at": datetime.utcnow()}}
            )
            logger.info(f"🛑 Stopped {running_states.modified_count} running broadcasts on startup.")
        except Exception as e:
            logger.error(f"Failed to stop running broadcasts: {e}")


        logger.info("🚀 All systems ready! Bot is now operational.")
        await idle()

    except Exception as e:
        logger.error(f"❌ Failed to start bot: {e}")

    finally:
        # Cancel all active user broadcast tasks gracefully
        for uid, task in list(user_tasks.items()):
            try:
                task.cancel()
                logger.info(f"🧹 Cancelled broadcast task for user {uid}")
            except Exception as cancel_err:
                logger.warning(f"Failed to cancel task for {uid}: {cancel_err}")

        # Close DB connection
        if db is not None and hasattr(db, 'close'):
            db.close()
            logger.info("Database connection closed")
        logger.info("Bot stopped gracefully")


# =======================================================
# 🚀 ENTRY POINT
# =======================================================
if __name__ == "__main__":
# subprocess already imported at top

    # 🌐 Start Flask health server
    threading.Thread(target=run_flask, daemon=True).start()
    logger.info("✅ Flask server started")

    # REMOVED: Temp blacklist cleanup - temp blacklist system removed


    # 🚀 Start the main Pyrogram bot
    pyro.run(main())


