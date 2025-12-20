import os

BOT_TOKEN = os.getenv("BOT_TOKEN", "8407662292:AAGWpWd_vwVRhkCeyiyJV87hqO0qVoY6SyQ")
LOGGER_BOT_TOKEN = os.getenv("LOGGER_BOT_TOKEN", "8392058083:AAGr60K_t1eFaLhAqg4-92vIRG2xgr1sTlQ")
BOT_USERNAME = os.getenv("BOT_USERNAME", "aztechadsbot")
BOT_NAME = os.getenv("BOT_NAME", "AZTECH ADS BOT [FREE]")
LOGGER_BOT_USERNAME = os.getenv("LOGGER_BOT_USERNAME", "aztechloggersbot")

# Telegram API Configuration
BOT_API_ID = int(os.getenv("BOT_API_ID", "34866733"))  # Replace with your actual API ID
BOT_API_HASH = os.getenv("BOT_API_HASH", "dff1913909bde847b8755ab5e248dbc6")  # Replace with your actual API hash

# Social Media & Contact Information
OWNER_USERNAME = "AzTechDeveloper"
UPDATES_CHANNEL = "AzTechsHub"
SUPPORT_USERNAME = "AzTechDeveloper"

# URLs for social links
UPDATES_CHANNEL_URL = f"https://t.me/{UPDATES_CHANNEL}"
SUPPORT_GROUP_URL = "https://t.me/AzTechDeveloper"
GUIDE_URL = "https://t.me/AzTechsGC"

# Web Dashboard URL (change this to your VPS domain or IP when deployed)
DASHBOARD_URL = os.getenv("DASHBOARD_URL", "https://aztechadsbot.rf.gd")  # HTTPS for Telegram Web App

# Admin Configuration - Multiple admins supported
ADMIN_ID = 6670166083  # Primary admin user ID
ADMIN_IDS = [6670166083]  # Both admin IDs (primary + alt)

# OTP Configuration
OTP_EXPIRY = 300  # 5 minutes in seconds
ADMIN_USERNAME = "AzTechDeveloper"

# Image URLs #must change 
START_IMAGE = "https://i.postimg.cc/02NHXL9H/wmremove-transformed.png" 
BROADCAST_IMAGE = "https://i.postimg.cc/02NHXL9H/wmremove-transformed.png"
FORCE_JOIN_IMAGE = "https://i.postimg.cc/02NHXL9H/wmremove-transformed.png"

# Force Join Settings
ENABLE_FORCE_JOIN = True
MUST_JOIN_CHANNEL = "aztechshub"  # Channel username (without @)
MUSTJOIN_GROUP = "AzTechsGC"       # Group username (without @)
MUST_JOIN_CHANNEL_URL = f"https://t.me/{MUST_JOIN_CHANNEL}"
MUSTJOIN_GROUP_URL = f"https://t.me/{MUSTJOIN_GROUP}"

# Encryption Key (use env var in production; fallback kept for local dev)
ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY", "RnVa0xtPfK1pm3qu_POAvFI9qkSyISKFShE37_JSQ2w=")

# Database Configuration
MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb+srv://aztech:adpmz786@cluster0.mhuaw3q.mongodb.net/AdsBot_db?retryWrites=true&w=majority"
)
DB_NAME = "AdsBot_db"

# Broadcast Settings
DEFAULT_DELAY = 300
MIN_DELAY = 60
MAX_DELAY = 3600

# OTP Settings
OTP_LENGTH = 5
OTP_EXPIRY = 300

# Logging Configuration
LOG_LEVEL = "INFO"

# Feature Toggles
ENABLE_FORCE_JOIN = True
ENABLE_OTP_VERIFICATION = True
ENABLE_BROADCASTING = True
ENABLE_ANALYTICS = True

# Success Messages
SUCCESS_MESSAGES = {
    "account_added": "Account added successfully!",
    "otp_sent": "OTP sent to your phone number!",
    "broadcast_started": "Broadcast started successfully!",
    "broadcast_completed": "Broadcast completed successfully!",
    "accounts_deleted": "All accounts deleted successfully!"  # Added for delete all accounts
}

# Error Messages
ERROR_MESSAGES = {
    "account_limit": "You have reached your account limit. Please contact support for assistance.",
    "invalid_phone": "Invalid phone number format! Use +1234567890",
    "otp_expired": "OTP has expired. Please restart hosting.",
    "invalid_otp": "Invalid OTP. Please try again.",
    "login_failed": "Failed to login to Telegram account!",
    "no_groups": "No groups found in your account!",
    "no_messages": "No messages found in Saved Messages!",
    "broadcast_limit": "Daily broadcast limit reached! Contact @AzTechDeveloper for assistance.",
    "unauthorized": "You are not authorized to perform this action!",
    "force_join_required": "Join required channels to access this feature!"
}

# Session Storage
SESSION_STORAGE_PATH = "sessions/"