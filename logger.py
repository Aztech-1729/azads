"""
AzTech Ads Bot - Logger Bot Module
Handles DM logging and health monitoring
"""

import asyncio
import logging
from datetime import datetime
from pyrogram import Client as PyroClient, filters
from pyrogram.enums import ParseMode

import config
from database import EnhancedDatabaseManager

# Initialize database
db = EnhancedDatabaseManager()

# Setup logger
logger = logging.getLogger(__name__)

# Import needed for send_logger_message function
from pyrogram.errors import PeerIdInvalid
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup

# Helper function for keyboard markup
def kb(rows):
    """Helper function to create inline keyboard markup"""
    return InlineKeyboardMarkup(rows)

# Initialize logger bot client
logger_client = PyroClient(
    "logger_bot",
    api_id=config.BOT_API_ID,
    api_hash=config.BOT_API_HASH,
    bot_token=config.LOGGER_BOT_TOKEN,
    workdir="./sessions"
)

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
            
            inactive_time = (datetime.now() - logger_bot_last_activity).total_seconds()
            
            if inactive_time > 1800:  # 30 minutes
                health_logger.warning("🏥 Logger bot appears inactive, performing health check...")
                
                try:
                    await asyncio.wait_for(logger_client.get_me(), timeout=10.0)
                    logger_bot_last_activity = datetime.now()
                    health_logger.info("[OK] Logger bot health check passed")
                    
                except Exception as e:
                    health_logger.error(f" Logger bot health check failed: {e}")
                    health_logger.info(" Restarting logger bot...")
                    
                    try:
                        await logger_client.stop()
                        await asyncio.sleep(5)
                        await logger_client.start()
                        logger_bot_last_activity = datetime.now()
                        health_logger.info("[OK] Logger bot restarted successfully")
                        
                    except Exception as restart_error:
                        health_logger.error(f"💥 Failed to restart logger bot: {restart_error}")
                        
        except Exception as e:
            health_logger.error(f"Health monitor error: {e}")
            await asyncio.sleep(60)

async def update_logger_activity():
    """Update logger bot activity timestamp"""
    global logger_bot_last_activity
    logger_bot_last_activity = datetime.now()

# ================================================
# BROADCAST LOGGING FUNCTIONS
# ================================================

async def send_logger_message(user_id: int, text: str, pyro_client=None):
    """
    Send a short log message to the user's logger bot DM (the user who started the bot).
    Enhanced with retry logic and error handling to prevent crashes.
    """
    max_retries = 3
    retry_delay = 2
    
    for attempt in range(max_retries):
        try:
            if not db.get_logger_status(user_id):
                return
            
            await asyncio.wait_for(
                logger_client.send_message(user_id, text, parse_mode=ParseMode.HTML),
                timeout=10.0
            )
            await update_logger_activity()
            return
            
        except asyncio.TimeoutError:
            logger.warning(f"Logger message timeout for user {user_id} (attempt {attempt + 1})")
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay * (attempt + 1))
                continue
            db.log_logger_failure(user_id, "Timeout sending logger message")
            
        except PeerIdInvalid:
            db.log_logger_failure(user_id, "PeerIdInvalid: User must start logger bot")
            if pyro_client:
                try:
                    await pyro_client.send_message(
                        user_id,
                        "<b> Logger bot not started!</b>\n\n"
                        f"Please start @{config.LOGGER_BOT_USERNAME} to receive log updates.",
                        parse_mode=ParseMode.HTML,
                        reply_markup=kb([[
                            InlineKeyboardButton("+ Start Logger Bot", url=f"https://t.me/{config.LOGGER_BOT_USERNAME.lstrip('@')}")
                        ]])
                    )
                except Exception:
                    pass
            return
            
        except (TimeoutError, ConnectionError, OSError) as e:
            logger.warning(f"Network error sending logger message to {user_id} (attempt {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay * (attempt + 1))
                continue
            db.log_logger_failure(user_id, f"Network error: {str(e)}")
            
        except Exception as e:
            logger.error(f"Logger message error for user {user_id} (attempt {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
                continue
            db.log_logger_failure(user_id, str(e))

async def send_dm_log(user_id: int, log_message: str):
    """
    Send DM log to a specific user via their logger-bot DM.
    Enhanced with retry logic and error handling to prevent crashes.
    Handles: Broadcast Started, Analyzing Groups, Sent to Group, etc.
    """
    max_retries = 3
    retry_delay = 2
    
    for attempt in range(max_retries):
        try:
            if not db.get_logger_status(user_id):
                return
            
            await asyncio.wait_for(
                logger_client.send_message(user_id, log_message, parse_mode=ParseMode.HTML),
                timeout=10.0
            )
            await update_logger_activity()
            return
            
        except asyncio.TimeoutError:
            logger.warning(f"DM log timeout for user {user_id} (attempt {attempt + 1})")
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay * (attempt + 1))
                continue
            db.log_logger_failure(user_id, "Timeout sending DM log")
            
        except PeerIdInvalid:
            db.log_logger_failure(user_id, "PeerIdInvalid: User must start logger bot")
            return
            
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

# ================================================
# LOGGER BOT COMMAND HANDLERS
# ================================================

@logger_client.on_message(filters.command(["start"]))
async def logger_start_command(client, message):
    """Handle logger bot start command"""
    try:
        uid = message.from_user.id
        username = message.from_user.username or "Unknown"
        first_name = message.from_user.first_name or "User"
        
        logger.info(f"[LOGGER BOT] Received /start from user {uid}")
        
        db.create_user(uid, username, first_name)
        db.set_logger_status(uid, is_active=True)
        
        await message.reply(
            f"<b>✨ Welcome to AzTech Ads Bot Logger! 📊</b>\n\n"
            f"Logs for your ad broadcasts will be sent here.\n"
            f"Start the main bot (@aztechadsbot) to begin broadcasting! 🚀",
            parse_mode=ParseMode.HTML
        )
        logger.info(f"[LOGGER BOT] Successfully replied to user {uid}")
        
    except Exception as e:
        logger.error(f"[LOGGER BOT] Error in logger_start_command: {e}", exc_info=True)
        # Try to send a simple message without HTML if there's an error
        try:
            await message.reply("Welcome to AzTech Ads Bot Logger!\n\nLogs will be sent here.")
        except Exception as e2:
            logger.error(f"[LOGGER BOT] Failed to send fallback message: {e2}")

# ================================================
# LOGGER BOT LIFECYCLE
# ================================================

async def start_logger_bot():
    """Start the logger bot"""
    try:
        await logger_client.start()
        logger.info(" Logger bot started successfully")
        
        # Start health monitor in background
        asyncio.create_task(logger_bot_health_monitor())
        logger.info(" Logger bot health monitor started")
        
    except Exception as e:
        logger.error(f" Failed to start logger bot: {e}")
        raise

async def stop_logger_bot():
    """Stop the logger bot"""
    try:
        await logger_client.stop()
        logger.info(" Logger bot stopped successfully")
    except Exception as e:
        logger.error(f" Failed to stop logger bot: {e}")

# ================================================
# PRE-BROADCAST ANALYSIS LOGGING
# ================================================

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

# Export functions for use in main.py
__all__ = [
    'logger_client', 
    'start_logger_bot', 
    'stop_logger_bot', 
    'update_logger_activity',
    'send_logger_message',
    'send_dm_log',
    'send_analysis_start',
    'send_analysis_complete',
    'send_broadcast_started',
    'send_setup_complete'
]
