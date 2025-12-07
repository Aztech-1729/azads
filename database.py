import sys
import logging
from datetime import datetime, timedelta
import pymongo
from pymongo.errors import ConnectionFailure, OperationFailure
import config
from bson.objectid import ObjectId
import time
import json
import os
import requests

# âœ… Ensure emojis (âœ…, âŒ, ðŸ§¹) display correctly on Windows
try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass

# âœ… Ensure the logs directory exists (fix for Render)
os.makedirs("logs", exist_ok=True)

# âœ… Logging setup - INFO only (clean logs)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/AzTechAdsBot.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class EnhancedDatabaseManager:
    def __init__(self):
        self.client = None
        self.db = None
        self._init_db()  # ðŸš€ CRITICAL FIX: Initialize database connection on creation
        # Initialize collections after database connection
        self.users = self.db.users if self.db is not None else None
        self.accounts = self.db.accounts if self.db is not None else None
        self.premium_users = self.db.premium_users if self.db is not None else None
        self._load_persistent_globals()

    def add_temp_blacklist(self, user_id, group_id, reason="FloodWait", duration=3600):
        """Temporarily blacklist a group for a specific user."""
        try:
            expires_at = datetime.utcnow() + timedelta(seconds=duration)
            self.db.temp_blacklist.update_one(
                {"user_id": user_id, "group_id": group_id},
                {
                    "$set": {
                        "user_id": user_id,
                        "group_id": group_id,
                        "reason": reason,
                        "expires_at": expires_at,
                        "created_at": datetime.utcnow()
                    }
                },
                upsert=True
            )
            logger.info(f"ðŸ•’ Group {group_id} temporarily blacklisted for user {user_id} ({reason}, {duration}s)")
        except Exception as e:
            logger.error(f"âŒ Failed to add temp blacklist for group {group_id}: {e}")

    def is_temp_blacklisted(self, user_id, group_id):
        """Check if a group is temporarily blacklisted."""
        try:
            doc = self.db.temp_blacklist.find_one({"user_id": user_id, "group_id": group_id})
            if not doc:
                return False
            if datetime.utcnow() > doc.get("expires_at", datetime.utcnow()):
                self.db.temp_blacklist.delete_one({"_id": doc["_id"]})  # Cleanup expired entries
                return False
            return True
        except Exception as e:
            logger.error(f"Failed to check temp blacklist for {group_id}: {e}")
            return False

    def add_blacklisted_group(self, user_id, group_id, group_title, reason):
        """Add a group to permanent blacklist"""
        self.db.blacklisted_groups.update_one(
            {
                'user_id': user_id,
                'group_id': group_id
            },
            {
                '$set': {
                    'user_id': user_id,
                    'group_id': group_id,
                    'title': group_title,
                    'reason': reason,
                    'blacklisted_at': datetime.utcnow()
                }
            },
            upsert=True
        )

    def get_blacklisted_groups(self, user_id):
        """Get all blacklisted groups for a user"""
        return list(self.db.blacklisted_groups.find({'user_id': user_id}))

    def is_group_blacklisted(self, user_id, group_id):
        """Check if a group is blacklisted for a user"""
        return self.db.blacklisted_groups.find_one({
            'user_id': user_id,
            'group_id': group_id
        }) is not None

    def _init_db(self):
        """Initialize MongoDB connection with exponential backoff retries and robust index handling."""
        max_retries = 5
        retry_delay = 2
        last_error = None

        # MongoDB client options with improved settings for better reliability
        client_options = {
            'serverSelectionTimeoutMS': 30000,
            'connectTimeoutMS': 20000,
            'socketTimeoutMS': 20000,
            'retryWrites': True,
            'retryReads': True,
            'maxPoolSize': 50,
            'minPoolSize': 10,
            'maxIdleTimeMS': 10000,
            'waitQueueTimeoutMS': 10000,
            'tlsAllowInvalidCertificates': True,  # Added for potential SSL issues
            'w': 'majority',  # Ensure write acknowledgment
            'journal': True,  # Enable journaling for durability
            'appName': 'AzTechAdsBot'  # Custom app name for monitoring
        }

        for attempt in range(max_retries):
            try:
                logger.info(f"MongoDB connection attempt {attempt + 1}/{max_retries}")
                
                # Parse connection string to extract host and port
                if "mongodb+srv://" in config.MONGO_URI:
                    logger.info("Using MongoDB Atlas connection string")
                else:
                    logger.info("Using standard MongoDB connection string")
                
                self.client = pymongo.MongoClient(config.MONGO_URI, **client_options)
                
                # Test connection with increased timeout
                self.client.admin.command("ping", socketTimeoutMS=10000)
                logger.info("MongoDB server ping successful")
                
                self.db = self.client[config.DB_NAME]
                logger.info(f"Connected to database: {config.DB_NAME}")
                
                # Test database access
                collections = self.db.list_collection_names()
                logger.info(f"Database access verified - Collections: {collections}")
                
                # Helper to safely create or verify indexes
                def ensure_index(collection, key, **kwargs):
                    index_key = key if isinstance(key, list) else [(key, pymongo.ASCENDING)]
                    index_name = "_".join(f"{k}_{v}" for k, v in index_key)
                    index_retry_delay = 1
                    for index_attempt in range(3):
                        try:
                            existing_indexes = collection.index_information()
                            if index_name in existing_indexes:
                                existing_unique = existing_indexes[index_name].get("unique", False)
                                desired_unique = kwargs.get("unique", False)
                                if existing_unique != desired_unique:
                                    collection.drop_index(index_name)
                                    logger.info(f"Dropped conflicting index {index_name} on {collection.name}")
                                else:
                                    logger.info(f"Index {index_name} on {collection.name} already exists with correct specs")
                                    return
                            collection.create_index(key, name=index_name, **kwargs)
                            logger.info(f"Created index {index_name} on {collection.name}")
                            return
                        except OperationFailure as e:
                            logger.error(f"Failed to create index {index_name} on {collection.name} (attempt {index_attempt + 1}): {e}")
                            if index_attempt < 2:
                                time.sleep(index_retry_delay)
                                index_retry_delay *= 2
                            else:
                                raise

                # âœ… Create necessary indexes
                ensure_index(self.db.users, "user_id", unique=True)
                ensure_index(self.db.accounts, [("user_id", pymongo.ASCENDING), ("phone_number", pymongo.ASCENDING)])
                ensure_index(self.db.ad_messages, "user_id")
                ensure_index(self.db.ad_delays, "user_id", unique=True)
                ensure_index(self.db.broadcast_states, "user_id", unique=True)
                ensure_index(self.db.target_groups, [("user_id", pymongo.ASCENDING), ("group_id", pymongo.ASCENDING)])
                ensure_index(self.db.analytics, "user_id", unique=True)
                ensure_index(self.db.broadcast_logs, "user_id")
                ensure_index(self.db.broadcast_activity, "user_id")
                ensure_index(self.db.temp_data, [("user_id", pymongo.ASCENDING), ("key", pymongo.ASCENDING)], unique=True)
                ensure_index(self.db.logger_status, "user_id", unique=True)
                ensure_index(self.db.logger_failures, "user_id")
                ensure_index(self.db.premium_users, "user_id", unique=True)
# Auto-reply indexes removed
                
                # ðŸ†• Ensure group_msg_delays collection has index
                ensure_index(self.db.group_msg_delays, "user_id", unique=True)

                # ðŸ†• Ensure ad_pointers index for rotation pointer (one-per-user)
                ensure_index(self.db.ad_pointers, "user_id", unique=True)
                
                logger.info("âœ… All database indexes ensured successfully")
                return

            except ConnectionFailure as e:
                logger.error(f"MongoDB connection attempt {attempt + 1}/{max_retries} failed: {e}")
                last_error = e
                if attempt < max_retries - 1:
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    retry_delay *= 2
                else:
                    logger.error("Max retries reached for MongoDB connection. Check MONGO_URI in config.py.")
                    raise last_error
            except OperationFailure as e:
                logger.error(f"Failed to initialize MongoDB: {e}. Ensure MONGO_URI credentials and database name are correct.")
                if "bad auth" in str(e).lower():
                    logger.error("Authentication failed. Verify username, password, and database name in MONGO_URI.")
                raise
            except Exception as e:
                logger.error(f"Unexpected error during MongoDB init: {e}")
                raise

    def _load_persistent_globals(self):
        """Load persistent user data like ad messages, delays, broadcast states from DB."""
        try:
            # Test if collections exist and are accessible
            collections_to_check = ['ad_messages', 'ad_delays', 'broadcast_states', 'logger_status']
            for collection_name in collections_to_check:
                if hasattr(self.db, collection_name):
                    count = getattr(self.db, collection_name).count_documents({})
                    logger.info(f"âœ… {collection_name}: {count} documents")
                else:
                    logger.warning(f"âš ï¸ Collection {collection_name} not found")
                    
        except Exception as e:
            logger.error(f"Failed to load persistent globals: {e}")

    # ================= USER MANAGEMENT =================

    def create_user(self, user_id, username, first_name):
        """Create or update a user with fixed 5-account limit and vouch tracking."""
        try:
            self.db.users.update_one(
                {"user_id": user_id},
                {
                    "$set": {
                        "username": username or "Unknown",
                        "first_name": first_name or "User",
                        "last_interaction": datetime.utcnow()
                    },
                    "$setOnInsert": {
                        "created_at": datetime.utcnow(),
                        "accounts_limit": 1,  # All users get exactly 1 account
                        "has_joined_vouch": False,
                        "state": "",
                        "user_id": user_id,
                        "user_type": "free"  # Default to free user
                    }
                },
                upsert=True
            )
            logger.info(f"User created/updated: {user_id}")
        except Exception as e:
            logger.error(f"Failed to create user {user_id}: {e}")
            raise

    def get_user(self, user_id):
        """Fetch user data."""
        try:
            user = self.db.users.find_one({"user_id": user_id})
            return user if user else None
        except Exception as e:
            logger.error(f"Failed to get user {user_id}: {e}")
            return None

    def update_user_last_interaction(self, user_id):
        """Update user's last interaction timestamp."""
        try:
            self.db.users.update_one(
                {"user_id": user_id},
                {"$set": {"last_interaction": datetime.utcnow()}}
            )
        except Exception as e:
            logger.error(f"Failed to update last interaction for {user_id}: {e}")
            raise

    def set_user_state(self, user_id, state):
        """Set user state for conversation flow."""
        try:
            self.db.users.update_one(
                {"user_id": user_id},
                {"$set": {"state": state, "updated_at": datetime.utcnow()}}
            )
        except Exception as e:
            logger.error(f"Failed to set user state for {user_id}: {e}")
            raise

    def get_user_state(self, user_id):
        """Get user state."""
        try:
            user = self.db.users.find_one({"user_id": user_id}, {"state": 1})
            return user.get("state", "") if user else ""
        except Exception as e:
            logger.error(f"Failed to get user state for {user_id}: {e}")
            return ""

    def has_vouch_sent(self, user_id):
        """Check if vouch message has been sent for a user."""
        try:
            user = self.db.users.find_one({"user_id": user_id}, {"has_joined_vouch": 1})
            return user.get("has_joined_vouch", False) if user else False
        except Exception as e:
            logger.error(f"Failed to check vouch status for {user_id}: {e}")
            return False

    def set_vouch_sent(self, user_id):
        """Mark vouch message as sent for a user."""
        try:
            self.db.users.update_one(
                {"user_id": user_id},
                {"$set": {"has_joined_vouch": True}}
            )
        except Exception as e:
            logger.error(f"Failed to set vouch sent for {user_id}: {e}")
            raise

    # ================= ACCOUNT MANAGEMENT =================

    def get_user_accounts(self, user_id):
        """Fetch all accounts for a user."""
        try:
            return list(self.db.accounts.find({"user_id": user_id}))
        except Exception as e:
            logger.error(f"Failed to get accounts for {user_id}: {e}")
            return []

    def get_all_user_accounts(self):
        """Fetch all user accounts."""
        try:
            return list(self.db.accounts.find())
        except Exception as e:
            logger.error(f"Failed to fetch all user accounts: {e}")
            return []

    def get_user_accounts_count(self, user_id):
        """Count user's accounts."""
        try:
            return self.db.accounts.count_documents({"user_id": user_id})
        except Exception as e:
            logger.error(f"Failed to count accounts for {user_id}: {e}")
            return 0

    def add_user_account(self, user_id, phone_number, session_string, **kwargs):
        """Add a user account with dynamic limit enforcement."""
        try:
            user = self.get_user(user_id)
            if not user:
                logger.warning(f"User {user_id} not found")
                return False
            
            accounts_count = self.get_user_accounts_count(user_id)
            limit = user.get("accounts_limit", 5)
            if isinstance(limit, str) and limit.lower() == "unlimited":
                limit = 999  # Or float('inf')
            else:
                try:
                    limit = int(limit)
                except (TypeError, ValueError):
                    logger.error(f"Invalid accounts_limit for user {user_id}: {limit}. Defaulting to 5")
                    limit = 5
            
            if accounts_count >= limit:
                logger.warning(f"Account limit exceeded for {user_id}: {accounts_count}/{limit}")
                return False
            
            first_name = kwargs.get('first_name', '')
            last_name = kwargs.get('last_name', '')
            self.db.accounts.insert_one({
                "user_id": user_id,
                "phone_number": phone_number,
                "session_string": session_string,
                "first_name": first_name,
                "last_name": last_name,
                "is_active": True,
                "created_at": datetime.utcnow()
            })
            logger.info(f"Account added for user {user_id}: {phone_number}")
            return True
        except Exception as e:
            logger.error(f"Failed to add account for {user_id}: {e}")
            return False

    def delete_user_account(self, user_id, account_id):
        """Delete a user account by user_id and account_id."""
        try:
            result = self.db.accounts.delete_one({"user_id": user_id, "_id": ObjectId(account_id)})
            if result.deleted_count > 0:
                logger.info(f"Account {account_id} deleted for user {user_id}")
                return True
            else:
                logger.warning(f"No account found with ID {account_id} for user {user_id}")
                return False
        except Exception as e:
            logger.error(f"Failed to delete account {account_id} for user {user_id}: {e}")
            raise

    def delete_all_user_accounts(self, user_id):
        """Delete all accounts for a user."""
        try:
            result = self.db.accounts.delete_many({"user_id": user_id})
            deleted_count = result.deleted_count
            logger.info(f"Deleted {deleted_count} accounts for user {user_id}")
            return deleted_count
        except Exception as e:
            logger.error(f"Failed to delete all accounts for {user_id}: {e}")
            raise

    def deactivate_account(self, account_id):
        """Deactivate an account."""
        try:
            self.db.accounts.update_one(
                {"_id": ObjectId(account_id)},
                {"$set": {"is_active": False, "updated_at": datetime.utcnow()}}
            )
            logger.info(f"Deactivated account {account_id}")
        except Exception as e:
            logger.error(f"Failed to deactivate account {account_id}: {e}")
            raise

    # ================= AD MESSAGE MANAGEMENT =================
    # (supports up to MAX_ADS_PER_USER ads per user, CRUD + rotation pointer)

    MAX_ADS_PER_USER = 5

    # OLD AD MESSAGE FUNCTIONS REMOVED - NOW USING SAVED MESSAGES SYSTEM

    # ================= AD DELAY MANAGEMENT =================

    def get_user_ad_delay(self, user_id):
        """Get user's ad delay."""
        try:
            doc = self.db.ad_delays.find_one({"user_id": user_id}, {"delay": 1})
            return doc.get("delay", 300) if doc else 300
        except Exception as e:
            logger.error(f"Failed to get ad delay for {user_id}: {e}")
            return 300
            
    def get_user_group_msg_delay(self, user_id):
        """Get user's group message delay. Default is 15 seconds."""
        try:
            doc = self.db.group_msg_delays.find_one({"user_id": user_id}, {"delay": 1})
            return doc.get("delay", 15) if doc else 15  # Default to 15 seconds
        except Exception as e:
            logger.error(f"Failed to get group message delay for {user_id}: {e}")
            return 15  # Default to 15 seconds
            
    def set_user_group_msg_delay(self, user_id, delay):
        """Set user's group message delay."""
        try:
            self.db.group_msg_delays.update_one(
                {"user_id": user_id},
                {"$set": {"delay": delay, "updated_at": datetime.utcnow()}},
                upsert=True
            )
            logger.info(f"Group msg delay set to {delay}s for user {user_id}")
        except Exception as e:
            logger.error(f"Failed to set group msg delay for {user_id}: {e}")
            raise

    # ================= CYCLE TIMEOUT MANAGEMENT =================

    def get_user_cycle_timeout(self, user_id):
        """Get user's cycle timeout in seconds. Default: 10 minutes (600s)."""
        try:
            doc = self.db.cycle_timeouts.find_one({"user_id": user_id}, {"timeout": 1})
            return doc.get("timeout", 600) if doc else 600
        except Exception as e:
            logger.error(f"Failed to get cycle timeout for {user_id}: {e}")
            return 600

    def set_user_cycle_timeout(self, user_id, timeout):
        """Set user's cycle timeout in seconds."""
        try:
            self.db.cycle_timeouts.update_one(
                {"user_id": user_id},
                {"$set": {
                    "timeout": timeout,
                    "updated_at": datetime.utcnow()
                }},
                upsert=True
            )
            logger.info(f"Cycle timeout set to {timeout}s for user {user_id}")
        except Exception as e:
            logger.error(f"Failed to set cycle timeout for {user_id}: {e}")
            raise

    def get_user_saved_messages_count(self, user_id):
        """Get the number of saved messages to use for rotation"""
        try:
            user = self.db.users.find_one({"user_id": user_id}, {"saved_messages_count": 1})
            return user.get("saved_messages_count", 3) if user else 3  # Default to 3 messages
        except Exception as e:
            logger.error(f"Failed to get saved messages count for {user_id}: {e}")
            return 3
    
    def reset_ad_cycle(self, user_id):
        """Reset ad cycle index to 0 (start from first message)"""
        try:
            self.db.users.update_one(
                {"user_id": user_id},
                {"$set": {"ad_cycle_index": 0}},
                upsert=True
            )
            logger.info(f"Reset ad cycle to 0 for user {user_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to reset ad cycle for {user_id}: {e}")
            return False

    def set_user_saved_messages_count(self, user_id, count):
        """Set the number of saved messages to use for rotation"""
        try:
            self.db.users.update_one(
                {"user_id": user_id},
                {
                    "$set": {
                        "saved_messages_count": count,
                        "updated_at": datetime.utcnow()
                    }
                },
                upsert=True
            )
            logger.info(f"Saved messages count set to {count} for user {user_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to set saved messages count for {user_id}: {e}")
            return False

    # ================= BROADCAST MANAGEMENT =================
    def set_user_ad_delay(self, user_id, delay):
        """Set user's ad delay."""
        try:
            self.db.ad_delays.update_one(
                {"user_id": user_id},
                {"$set": {"delay": delay, "updated_at": datetime.utcnow()}},
                upsert=True
            )
            logger.info(f"Ad delay set for {user_id}: {delay}s")
        except Exception as e:
            logger.error(f"Failed to set ad delay for {user_id}: {e}")
            raise

    # ================= BROADCAST MANAGEMENT =================

    def get_broadcast_state(self, user_id):
        """Get user's broadcast state."""
        try:
            doc = self.db.broadcast_states.find_one({"user_id": user_id}, {"running": 1, "paused": 1})
            return doc if doc else {"running": False, "paused": False}
        except Exception as e:
            logger.error(f"Failed to get broadcast state for {user_id}: {e}")
            return {"running": False, "paused": False}

    def set_broadcast_state(self, user_id, running=False, paused=False):
        """Set user's broadcast state."""
        try:
            self.db.broadcast_states.update_one(
                {"user_id": user_id},
                {"$set": {"running": running, "paused": paused, "updated_at": datetime.utcnow()}},
                upsert=True
            )
            logger.info(f"Broadcast state updated for {user_id}: running={running}, paused={paused}")
        except Exception as e:
            logger.error(f"Failed to set broadcast state for {user_id}: {e}")
            raise

    def increment_broadcast_cycle(self, user_id):
        """Increment the broadcast cycle count for a user and update cycle index for message rotation."""
        try:
            # Increment analytics
            self.db.analytics.update_one(
                {"user_id": user_id},
                {
                    "$inc": {"total_cycles": 1},
                    "$set": {"updated_at": datetime.utcnow()}
                },
                upsert=True
            )
            
            # Also update the cycle index for message rotation
            self.update_ad_cycle(user_id)
            
            logger.info(f"Incremented broadcast cycle for user {user_id}")
        except Exception as e:
            logger.error(f"Failed to increment broadcast cycle for {user_id}: {e}")
            raise

    # ================= TARGET GROUPS MANAGEMENT =================

    def get_target_groups(self, user_id):
        """Fetch user's target groups."""
        try:
            return list(self.db.target_groups.find({"user_id": user_id}))
        except Exception as e:
            logger.error(f"Failed to get target groups for {user_id}: {e}")
            return []

    def add_target_group(self, user_id, group_id, group_name):
        """Add a target group for a user."""
        try:
            self.db.target_groups.update_one(
                {"user_id": user_id, "group_id": group_id},
                {
                    "$set": {
                        "group_name": group_name,
                        "created_at": datetime.utcnow(),
                        "updated_at": datetime.utcnow()
                    }
                },
                upsert=True
            )
            logger.info(f"Target group {group_name} added for user {user_id}")
        except Exception as e:
            logger.error(f"Failed to add target group for {user_id}: {e}")
            raise

    # ================= ANALYTICS & STATISTICS =================

    def get_user_analytics(self, user_id):
        """Fetch analytics for a user."""
        try:
            stats = self.db.analytics.find_one({"user_id": user_id})
            return stats if stats else {
                "total_broadcasts": 0,
                "total_sent": 0,
                "total_failed": 0,
                "total_cycles": 0,
                "vouch_successes": 0,
                "vouch_failures": 0
            }
        except Exception as e:
            logger.error(f"Failed to get analytics for {user_id}: {e}")
            return {
                "total_broadcasts": 0,
                "total_sent": 0,
                "total_failed": 0,
                "total_cycles": 0,
                "vouch_successes": 0,
                "vouch_failures": 0
            }

    def increment_broadcast_stats(self, user_id, success, group_id=None, account_id=None):
        """Increment broadcast stats for a user, optionally tracking group and account stats."""
        try:
            update = {
                "$inc": {
                    "total_sent" if success else "total_failed": 1,
                    "total_broadcasts": 1
                },
                "$set": {"updated_at": datetime.utcnow()}
            }
            if group_id:
                update["$inc"][f"groups.{group_id}.sent" if success else f"groups.{group_id}.failed"] = 1
            if account_id:
                update["$inc"][f"accounts.{account_id}.sent" if success else f"accounts.{account_id}.failed"] = 1
            self.db.analytics.update_one(
                {"user_id": user_id},
                update,
                upsert=True
            )
            logger.info(f"Updated broadcast stats for user {user_id}: {'success' if success else 'failure'}")
        except Exception as e:
            logger.error(f"Failed to update broadcast stats for {user_id}: {e}")
            raise

    def increment_vouch_success(self, channel_id):
        """Increment vouch success count."""
        try:
            self.db.analytics.update_one(
                {"channel_id": channel_id},
                {
                    "$inc": {"vouch_successes": 1},
                    "$set": {"updated_at": datetime.utcnow()}
                },
                upsert=True
            )
            logger.info(f"Incremented vouch success for channel {channel_id}")
        except Exception as e:
            logger.error(f"Failed to increment vouch success for {channel_id}: {e}")
            raise

    def increment_vouch_failure(self, channel_id, error):
        """Increment vouch failure count."""
        try:
            self.db.analytics.update_one(
                {"channel_id": channel_id},
                {
                    "$inc": {"vouch_failures": 1},
                    "$set": {"updated_at": datetime.utcnow(), "last_error": str(error)}
                },
                upsert=True
            )
            logger.info(f"Incremented vouch failure for channel {channel_id}: {error}")
        except Exception as e:
            logger.error(f"Failed to increment vouch failure for {channel_id}: {e}")
            raise

    # ================= LOGGING =================

    def log_broadcast(self, user_id, message, accounts_count, groups_count, sent_count, failed_count, status):
        """Log a broadcast event."""
        try:
            self.db.broadcast_logs.insert_one({
                "user_id": user_id,
                "message": message,
                "accounts_count": accounts_count,
                "groups_count": groups_count,
                "sent_count": sent_count,
                "failed_count": failed_count,
                "status": status,
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow()
            })
            logger.info(f"Broadcast logged for user {user_id}: {status}")
        except Exception as e:
            logger.error(f"Failed to log broadcast for {user_id}: {e}")
            raise

    def update_broadcast_log(self, user_id, sent_count, failed_count, status):
        """Update broadcast log."""
        try:
            self.db.broadcast_logs.update_one(
                {"user_id": user_id, "status": "running"},
                {
                    "$set": {
                        "sent_count": sent_count,
                        "failed_count": failed_count,
                        "status": status,
                        "updated_at": datetime.utcnow()
                    }
                }
            )
            logger.info(f"Broadcast log updated for user {user_id}: {status}")
        except Exception as e:
            logger.error(f"Failed to update broadcast log for {user_id}: {e}")
            raise

    def log_broadcast_activity(self, user_id, sent_count, failed_count):
        """Log broadcast activity."""
        try:
            self.db.broadcast_activity.insert_one({
                "user_id": user_id,
                "sent_count": sent_count,
                "failed_count": failed_count,
                "timestamp": datetime.utcnow()
            })
            logger.info(f"Broadcast activity logged for user {user_id}")
        except Exception as e:
            logger.error(f"Failed to log broadcast activity for {user_id}: {e}")
            raise

    # ================= LOGGER BOT MANAGEMENT =================

    def get_logger_status(self, user_id):
        """Check if user has started the logger bot."""
        try:
            doc = self.db.logger_status.find_one({"user_id": user_id}, {"is_active": 1})
            return doc.get("is_active", False) if doc else False
        except Exception as e:
            logger.error(f"Failed to get logger status for {user_id}: {e}")
            return False

    def set_logger_status(self, user_id, is_active=True):
        """Mark if user has started the logger bot."""
        try:
            self.db.logger_status.update_one(
                {"user_id": user_id},
                {"$set": {"is_active": is_active, "updated_at": datetime.utcnow()}},
                upsert=True
            )
            logger.info(f"Logger status set for {user_id}: is_active={is_active}")
        except Exception as e:
            logger.error(f"Failed to set logger status for {user_id}: {e}")
            raise

    def log_logger_failure(self, user_id, error):
        """Log a failure when sending a DM via logger bot."""
        try:
            self.db.logger_failures.insert_one({
                "user_id": user_id,
                "error": str(error),
                "timestamp": datetime.utcnow()
            })
            logger.info(f"Logged logger failure for user {user_id}: {error}")
        except Exception as e:
            logger.error(f"Failed to log logger failure for {user_id}: {e}")
            raise

    def get_logger_failures(self, user_id):
        """Fetch logger failure stats for a user."""
        try:
            return list(self.db.logger_failures.find({"user_id": user_id}))
        except Exception as e:
            logger.error(f"Failed to get logger failures for {user_id}: {e}")
            return []

    # ================= USER STATUS MANAGEMENT =================

    def get_user_status(self, user_id):
        """Get user status information including user_type and accounts_limit"""
        try:
            user = self.db.users.find_one({"user_id": user_id})
            if user:
                return {
                    "user_type": user.get("user_type", "free"),
                    "accounts_limit": user.get("accounts_limit", 1),
                    "premium_until": user.get("premium_until", None)
                }
            return None
        except Exception as e:
            logger.error(f"Failed to get user status for {user_id}: {e}")
            return None

    def set_user_status(self, user_id, user_type="free", accounts_limit=None, premium_until=None):
        """Set user status with proper type and limits"""
        try:
            if accounts_limit is None:
                accounts_limit = 1  # All users get exactly 1 account
                
            update_data = {
                "user_type": user_type,
                "accounts_limit": accounts_limit,
                "premium_until": premium_until,
                "updated_at": datetime.utcnow()
            }
            
            self.db.users.update_one(
                {"user_id": user_id},
                {"$set": update_data}
            )
            logger.info(f"User status updated for {user_id}: {user_type} with {accounts_limit} accounts limit")
            return True
        except Exception as e:
            logger.error(f"Failed to set user status for {user_id}: {e}")
            return False

    def is_user_premium(self, user_id):
        """Check if user has premium status"""
        try:
            user = self.db.users.find_one({"user_id": user_id})
            if user:
                return user.get("user_type", "free") == "premium"
            return False
        except Exception as e:
            logger.error(f"Failed to check premium status for {user_id}: {e}")
            return False

    # ================= API CREDENTIALS MANAGEMENT =================

    def store_user_api_credentials(self, user_id, api_id, api_hash):
        """Store user's API ID and Hash securely"""
        try:
            # Ensure user exists first, then update credentials
            self.db.users.update_one(
                {"user_id": user_id},
                {
                    "$set": {
                        "api_id": int(api_id),  # Ensure it's an integer
                        "api_hash": str(api_hash),  # Ensure it's a string
                        "credentials_updated_at": datetime.utcnow()
                    }
                },
                upsert=True  # Create user document if it doesn't exist
            )
            logger.info(f"API credentials stored for user {user_id}: api_id={api_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to store API credentials for {user_id}: {e}")
            return False
    
    def delete_user_api_credentials(self, user_id):
        """Delete user's API credentials from database"""
        try:
            self.db.users.update_one(
                {"user_id": user_id},
                {
                    "$unset": {
                        "api_id": "",
                        "api_hash": "",
                        "credentials_updated_at": ""
                    }
                }
            )
            logger.info(f"API credentials deleted for user {user_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete API credentials for {user_id}: {e}")
            return False

    def get_user_api_credentials(self, user_id):
        """Get user's API credentials"""
        try:
            user = self.db.users.find_one({"user_id": user_id}, {"api_id": 1, "api_hash": 1})
            if user and "api_id" in user and "api_hash" in user:
                return {
                    "api_id": user["api_id"],
                    "api_hash": user["api_hash"]
                }
            return None
        except Exception as e:
            logger.error(f"Failed to get API credentials for {user_id}: {e}")
            return None

    def has_user_api_credentials(self, user_id):
        """Check if user has stored API credentials"""
        try:
            user = self.db.users.find_one({"user_id": user_id}, {"api_id": 1, "api_hash": 1})
            return user and "api_id" in user and "api_hash" in user
        except Exception as e:
            logger.error(f"Failed to check API credentials for {user_id}: {e}")
            return False

    def clear_user_api_credentials(self, user_id):
        """Clear user's API credentials completely from MongoDB - SIMPLIFIED AND DIRECT"""
        try:
            logger.info(f"ðŸ”„ Starting API credentials clearing for user {user_id}")
            
            # First, check if user exists
            user_before = self.db.users.find_one({"user_id": user_id})
            if not user_before:
                logger.warning(f"âŒ User {user_id} not found in database")
                return False
                
            has_api_before = "api_id" in user_before or "api_hash" in user_before
            logger.info(f"ðŸ“Š User {user_id} before clearing - has api_id: {'api_id' in user_before}, has api_hash: {'api_hash' in user_before}")
            
            if not has_api_before:
                logger.info(f"â„¹ï¸ User {user_id} has no API credentials to clear")
                return True  # Nothing to clear, consider it success
            
            # DIRECT MongoDB $unset operation
            logger.info(f"ðŸ—‘ï¸ Executing MongoDB $unset for user {user_id}")
            result = self.db.users.update_one(
                {"user_id": user_id},
                {
                    "$unset": {
                        "api_id": 1,
                        "api_hash": 1,
                        "credentials_updated_at": 1,
                        "api_credentials_set": 1,
                        "last_api_check": 1
                    }
                }
            )
            
            logger.info(f"ðŸ“ MongoDB update result: matched={result.matched_count}, modified={result.modified_count}")
            
            # Immediate verification
            user_after = self.db.users.find_one({"user_id": user_id})
            has_api_id = "api_id" in user_after if user_after else False
            has_api_hash = "api_hash" in user_after if user_after else False
            
            logger.info(f"âœ… After clearing - api_id exists: {has_api_id}, api_hash exists: {has_api_hash}")
            
            # Success if both fields are gone
            if not has_api_id and not has_api_hash:
                logger.info(f"âœ… API credentials successfully cleared for user {user_id}")
                return True
            else:
                logger.error(f"âŒ API credentials still present for user {user_id}: api_id={has_api_id}, api_hash={has_api_hash}")
                return False
                
        except Exception as e:
            logger.error(f"ðŸ’¥ Exception in clear_user_api_credentials for {user_id}: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return False

    def set_user_temp_data(self, user_id, key, value):
        """Store temporary data for user (like temp API ID)"""
        try:
            result = self.db.users.update_one(
                {"user_id": user_id},
                {
                    "$set": {
                        f"temp_data.{key}": value,
                        f"temp_data.{key}_timestamp": datetime.now()
                    }
                },
                upsert=True
            )
            return result.acknowledged
        except Exception as e:
            logger.error(f"Failed to set temp data for {user_id}: {e}")
            return False

    def get_user_temp_data(self, user_id, key):
        """Get temporary data for user"""
        try:
            user = self.db.users.find_one({"user_id": user_id})
            if not user or "temp_data" not in user:
                return None
            
            temp_data = user["temp_data"]
            if key not in temp_data:
                return None
                
            # Check if data is not too old (30 minutes)
            timestamp_key = f"{key}_timestamp"
            if timestamp_key in temp_data:
                timestamp = temp_data[timestamp_key]
                if (datetime.now() - timestamp).total_seconds() > 1800:  # 30 minutes
                    self.clear_user_temp_data(user_id, key)
                    return None
                    
            return temp_data[key]
        except Exception as e:
            logger.error(f"Failed to get temp data for {user_id}: {e}")
            return None

    def clear_user_temp_data(self, user_id, key):
        """Clear specific temporary data for user"""
        try:
            result = self.db.users.update_one(
                {"user_id": user_id},
                {
                    "$unset": {
                        f"temp_data.{key}": "",
                        f"temp_data.{key}_timestamp": ""
                    }
                }
            )
            return result.acknowledged
        except Exception as e:
            logger.error(f"Failed to clear temp data for {user_id}: {e}")
            return False

    def add_saved_message(self, user_id, message_id, message_text=""):
        """Add a saved message for the user"""
        try:
            saved_messages = self.get_saved_messages(user_id)
            if len(saved_messages) >= 3:
                return False  # Maximum 3 saved messages
            
            message_data = {
                "message_id": message_id,
                "message_text": message_text,
                "added_at": datetime.now()
            }
            
            result = self.db.users.update_one(
                {"user_id": user_id},
                {"$push": {"saved_messages": message_data}},
                upsert=True
            )
            return result.modified_count > 0
        except Exception as e:
            logger.error(f"Failed to add saved message for {user_id}: {e}")
            return False

    def get_saved_messages(self, user_id):
        """Get all saved messages for a user"""
        try:
            user = self.db.users.find_one({"user_id": user_id}, {"saved_messages": 1})
            return user.get("saved_messages", []) if user else []
        except Exception as e:
            logger.error(f"Failed to get saved messages for {user_id}: {e}")
            return []

    def clear_saved_messages(self, user_id):
        """Clear all saved messages for a user"""
        try:
            result = self.db.users.update_one(
                {"user_id": user_id},
                {"$unset": {"saved_messages": ""}}
            )
            return result.modified_count > 0
        except Exception as e:
            logger.error(f"Failed to clear saved messages for {user_id}: {e}")
            return False

    def get_current_ad_cycle(self, user_id):
        """Get current ad cycle index for rotation"""
        try:
            user = self.db.users.find_one({"user_id": user_id}, {"ad_cycle_index": 1})
            return user.get("ad_cycle_index", 0) if user else 0
        except Exception as e:
            logger.error(f"Failed to get ad cycle for {user_id}: {e}")
            return 0

    def update_ad_cycle(self, user_id):
        """Update ad cycle index for next message rotation"""
        try:
            # Use the user's selected saved messages count instead of stored messages
            user_msg_count = self.get_user_saved_messages_count(user_id)
            if user_msg_count == 0:
                return 0
            
            current_cycle = self.get_current_ad_cycle(user_id)
            next_cycle = (current_cycle + 1) % user_msg_count
            
            self.db.users.update_one(
                {"user_id": user_id},
                {"$set": {"ad_cycle_index": next_cycle}},
                upsert=True
            )
            logger.info(f"Updated ad cycle for user {user_id}: {current_cycle} -> {next_cycle} (out of {user_msg_count} messages)")
            return next_cycle
        except Exception as e:
            logger.error(f"Failed to update ad cycle for {user_id}: {e}")
            return 0

    # ================= TEMPORARY DATA MANAGEMENT =================

    def set_temp_data(self, user_id, key, value):
        """Store temporary key-value data for user (e.g., during login flow)."""
        try:
            self.db.temp_data.update_one(
                {"user_id": user_id, "key": key},
                {"$set": {"value": value, "updated_at": datetime.utcnow()}},
                upsert=True
            )
            logger.info(f"Set temp data for {user_id} [{key}] = {value}")
        except Exception as e:
            logger.error(f"Failed to set temp data for {user_id}: {e}")

    def get_temp_data(self, user_id, key=None):
        """Get temporary data for user from temp_data collection."""
        try:
            query = {"user_id": user_id}
            if key:
                query["key"] = key
            data = list(self.db.temp_data.find(query, {"_id": 0}))
            if not data:
                return None
            if key:
                return data[0].get("value")
            return data[-1].get("value")
        except Exception as e:
            logger.error(f"Failed to get temp data for {user_id}: {e}")
            return None

    def delete_temp_data(self, user_id, key=None):
        """Delete temporary data for a user."""
        try:
            query = {"user_id": user_id}
            if key:
                query["key"] = key
            result = self.db.temp_data.delete_many(query)
            logger.info(f"Deleted {result.deleted_count} temp data entries for {user_id}")
            return result.deleted_count
        except Exception as e:
            logger.error(f"Failed to delete temp data for {user_id}: {e}")
            return 0


    # ================= ADMIN FUNCTIONS =================

    def get_all_users(self, page=0, limit=0):
        """Fetch all users with optional pagination (limit=0 for all users)."""
        try:
            if limit == 0:
                return list(self.db.users.find({}))
            skip = page * limit
            return list(self.db.users.find({}).skip(skip).limit(limit))
        except Exception as e:
            logger.error(f"Failed to get all users: {e}")
            return []

    def get_admin_stats(self):
        """Fetch admin statistics with aggregated analytics across all users."""
        try:
            total_users = self.db.users.count_documents({})
            logger.info(f"Total users fetched: {total_users}")
            total_accounts = self.db.accounts.count_documents({})
            logger.info(f"Total accounts fetched: {total_accounts}")
            
            # Aggregate user analytics
            analytics_pipeline = [
                {
                    "$group": {
                        "_id": None,
                        "total_sent": {"$sum": "$total_sent"},
                        "total_failed": {"$sum": "$total_failed"},
                        "total_broadcasts": {"$sum": "$total_broadcasts"}
                    }
                }
            ]
            analytics_result = list(self.db.analytics.aggregate(analytics_pipeline))
            analytics_stats = analytics_result[0] if analytics_result else {
                "total_sent": 0,
                "total_failed": 0,
                "total_broadcasts": 0
            }
            logger.info(f"Analytics stats: {analytics_stats}")

            # Aggregate vouch stats
            vouch_pipeline = [
                {
                    "$group": {
                        "_id": None,
                        "vouch_successes": {"$sum": "$vouch_successes"},
                        "vouch_failures": {"$sum": "$vouch_failures"}
                    }
                }
            ]
            vouch_result = list(self.db.analytics.aggregate(vouch_pipeline))
            vouch_stats = vouch_result[0] if vouch_result else {
                "vouch_successes": 0,
                "vouch_failures": 0
            }
            logger.info(f"Vouch stats: {vouch_stats}")

            active_logger_users = self.db.logger_status.count_documents({"is_active": True})
            logger.info(f"Active logger users: {active_logger_users}")

            return {
                "total_users": total_users,
                "total_forwards": analytics_stats["total_sent"],
                "total_accounts": total_accounts,
                "active_logger_users": active_logger_users,
                "vouch_successes": vouch_stats["vouch_successes"],
                "vouch_failures": vouch_stats["vouch_failures"],
                "total_broadcasts": analytics_stats["total_broadcasts"],
                "total_failed": analytics_stats["total_failed"]
            }
        except Exception as e:
            logger.error(f"Failed to get admin stats: {e}")
            return {
                "total_users": 0,
                "total_forwards": 0,
                "total_accounts": 0,
                "active_logger_users": 0,
                "vouch_successes": 0,
                "vouch_failures": 0,
                "total_broadcasts": 0,
                "total_failed": 0
            }
    # ================= AUTO REPLIES MANAGEMENT =================
    
    def reset_all_auto_replies(self):
        """Reset/clear all auto replies from the database."""
        try:
            result = self.db.auto_replies.delete_many({})
            logger.info(f"âœ… Reset all auto replies - {result.deleted_count} entries cleared")
            return result.deleted_count
        except Exception as e:
            logger.error(f"âŒ Failed to reset auto replies: {e}")
            return 0

    # ================= USER FULL CLEANUP =================

    def delete_user_fully(self, user_id):
        """
        Delete all data related to a specific user from the database.
        Called when the user deletes their last account or manually requests deletion.
        """
        try:
            collections = [
                "users", "accounts", "ad_messages", "ad_pointers",
                "ad_delays", "group_msg_delays", "cycle_timeouts",
                "broadcast_states", "broadcast_logs", "broadcast_activity",
                "blacklisted_groups", "temp_blacklist", "analytics",
                "auto_replies", "target_groups", "logger_status",
                "logger_failures", "temp_data"
            ]
            deleted_total = 0
            for coll in collections:
                col = getattr(self.db, coll, None)
                # âœ… FIXED: Explicitly check if collection exists
                if col is not None:
                    result = col.delete_many({"user_id": user_id})
                    if result.deleted_count > 0:
                        logger.info(f"ðŸ§¹ Deleted {result.deleted_count} from {coll} for user {user_id}")
                        deleted_total += result.deleted_count

            if deleted_total == 0:
                logger.info(f"â„¹ï¸ No user data found to delete for user {user_id}")

            logger.info(f"âœ… Full cleanup completed for user {user_id} â€” total {deleted_total} docs removed.")
            return True

        except Exception as e:
            logger.error(f"âŒ Failed to fully delete user {user_id}: {e}")
            return False

# Module-level function for backward compatibility
def reset_all_auto_replies():
    """Module-level function to reset all auto replies."""
    try:
        db_manager = EnhancedDatabaseManager()
        return db_manager.reset_all_auto_replies()
    except Exception as e:
        logger.error(f"âŒ Failed to reset auto replies: {e}")
        return 0

    def close(self):
        """Close MongoDB connection."""
        try:
            if self.client is not None:
                self.client.close()
                logger.info("MongoDB connection closed")
        except Exception as e:
            logger.error(f"Failed to close MongoDB connection: {e}")
            raise

