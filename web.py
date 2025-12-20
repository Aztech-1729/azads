"""
AzTech Ads Bot - Professional Web Dashboard
Provides user authentication, profile display, and analytics tracking
Includes bot analytics integration
"""

import os
import sys
import hmac
import hashlib
import time
import logging
import requests
from datetime import datetime, timedelta
from functools import wraps
from flask import Flask, render_template, request, jsonify, session, redirect, url_for
from flask_cors import CORS
from database import EnhancedDatabaseManager
import config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = os.urandom(24)  # Generate random secret key for sessions
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(days=7)

# Enable CORS for frontend hosted on different domain
CORS(app, supports_credentials=True, origins=['http://aztechadsbot.rf.gd', 'https://aztechadsbot.rf.gd'])

# Initialize database
db = EnhancedDatabaseManager()

# Telegram Bot Token (for widget validation)
BOT_TOKEN = config.BOT_TOKEN


def verify_telegram_auth(auth_data):
    """Verify Telegram login widget authentication"""
    check_hash = auth_data.get('hash')
    if not check_hash:
        return False
    
    # Remove hash from data
    auth_data_copy = {k: v for k, v in auth_data.items() if k != 'hash'}
    
    # Create data check string
    data_check_arr = [f"{k}={v}" for k, v in sorted(auth_data_copy.items())]
    data_check_string = '\n'.join(data_check_arr)
    
    # Create secret key from bot token
    secret_key = hashlib.sha256(BOT_TOKEN.encode()).digest()
    
    # Calculate hash
    calculated_hash = hmac.new(
        secret_key,
        data_check_string.encode(),
        hashlib.sha256
    ).hexdigest()
    
    # Check if hash matches and data is not too old (24 hours)
    if calculated_hash != check_hash:
        return False
    
    auth_date = int(auth_data.get('auth_date', 0))
    if time.time() - auth_date > 86400:
        return False
    
    return True


def login_required(f):
    """Decorator to require login for routes"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function


@app.route('/')
def index():
    """Home page - redirect to dashboard with user_id from URL parameter"""
    # Get user_id from URL parameter (sent from bot)
    user_id = request.args.get('user_id')
    
    if user_id:
        # Store user_id in session
        user_id = int(user_id)
        user = db.get_user(user_id)
        
        if user:
            session['user_id'] = user_id
            session['username'] = user.get('username', '')
            session['first_name'] = user.get('first_name', 'User')
            session['photo_url'] = f"https://api.telegram.org/file/bot{config.BOT_TOKEN}/photos/{user_id}.jpg"
            session.permanent = True
            return redirect(url_for('dashboard'))
    
    # If no user_id or invalid, show error
    return "Access denied. Please open from Telegram bot.", 403


@app.route('/login')
def login():
    """Login page with Telegram widget"""
    # Read HTML file and replace bot_username
    with open('login.html', 'r', encoding='utf-8') as f:
        html_content = f.read()
    html_content = html_content.replace('{{ bot_username }}', config.BOT_USERNAME)
    return html_content


@app.route('/auth/telegram', methods=['POST'])
def telegram_auth():
    """Handle Telegram authentication"""
    try:
        auth_data = request.json
        
        if not verify_telegram_auth(auth_data):
            return jsonify({'success': False, 'error': 'Authentication failed'}), 401
        
        user_id = int(auth_data.get('id'))
        username = auth_data.get('username', '')
        first_name = auth_data.get('first_name', '')
        
        # Create or update user in database
        db.create_user(user_id, username, first_name)
        
        # Store in session
        session['user_id'] = user_id
        session['username'] = username
        session['first_name'] = first_name
        session['photo_url'] = auth_data.get('photo_url', '')
        session.permanent = True
        
        return jsonify({'success': True, 'redirect': url_for('dashboard')})
    
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/dashboard')
@login_required
def dashboard():
    """Main dashboard page"""
    user_id = session.get('user_id')
    
    # Get user data
    user = db.get_user(user_id)
    accounts = db.get_user_accounts(user_id)
    analytics = db.get_user_analytics(user_id)
    target_groups = db.get_target_groups(user_id)
    forum_groups = db.get_forum_groups(user_id)
    broadcast_state = db.get_broadcast_state(user_id)
    
    # Calculate total groups
    total_groups = len(target_groups) + len(forum_groups)
    
    # Get user status
    user_status = db.get_user_status(user_id)
    accounts_limit = user_status.get('accounts_limit', 1) if user_status else 1
    
    # Read HTML file
    with open('dashboard.html', 'r', encoding='utf-8') as f:
        html_content = f.read()
    
    # Replace template variables
    html_content = html_content.replace('{{ first_name }}', session.get('first_name', 'User'))
    html_content = html_content.replace('{{ username or \'No username\' }}', session.get('username') or 'No username')
    html_content = html_content.replace('{{ photo_url }}', session.get('photo_url', ''))
    html_content = html_content.replace('{{ analytics.total_cycles or 0 }}', str(analytics.get('total_cycles', 0)))
    html_content = html_content.replace('{{ analytics.total_sent or 0 }}', str(analytics.get('total_sent', 0)))
    html_content = html_content.replace('{{ analytics.total_failed or 0 }}', str(analytics.get('total_failed', 0)))
    html_content = html_content.replace('{{ total_groups or 0 }}', str(total_groups))
    html_content = html_content.replace('{{ accounts_count }}', str(len(accounts)))
    html_content = html_content.replace('{{ accounts_limit }}', str(accounts_limit))
    
    # Build accounts table
    if accounts:
        accounts_html = '<div class="table-responsive"><table class="table table-hover"><thead><tr><th>Phone Number</th><th>Name</th><th>Status</th><th>Added</th></tr></thead><tbody>'
        for account in accounts:
            status_badge = '<span class="badge bg-success">Active</span>' if account.get('is_active', True) else '<span class="badge bg-danger">Inactive</span>'
            created_date = account.get('created_at').strftime('%Y-%m-%d') if account.get('created_at') else 'N/A'
            accounts_html += f'<tr><td>{account.get("phone_number")}</td><td>{account.get("first_name", "")} {account.get("last_name", "")}</td><td>{status_badge}</td><td>{created_date}</td></tr>'
        accounts_html += '</tbody></table></div>'
    else:
        accounts_html = '<div class="text-center py-4"><i class="fas fa-user-plus fa-3x text-muted mb-3"></i><p class="text-muted">No accounts added yet. Add your first account via the bot!</p></div>'
    
    # Replace accounts section
    html_content = html_content.replace('{% if accounts %}', '').replace('{% else %}', '').replace('{% endif %}', '').replace('{% for account in accounts %}', '').replace('{% endfor %}', '')
    
    # Build broadcast status
    if broadcast_state.get('running'):
        if broadcast_state.get('paused'):
            status_badge = '<span class="badge bg-warning fs-5"><i class="fas fa-pause"></i> Paused</span>'
        else:
            status_badge = '<span class="badge bg-success fs-5"><i class="fas fa-play"></i> Running</span>'
    else:
        status_badge = '<span class="badge bg-secondary fs-5"><i class="fas fa-stop"></i> Stopped</span>'
    
    # Replace broadcast status
    html_content = html_content.replace('{% if broadcast_state.running %}', '').replace('{% if broadcast_state.paused %}', '').replace('{% else %}', '').replace('{% endif %}', '')
    
    # Simple replacements for remaining template logic
    import re
    html_content = re.sub(r'{%.*?%}', '', html_content)
    
    return html_content


@app.route('/api/analytics')
@login_required
def get_analytics():
    """API endpoint to get real-time analytics"""
    user_id = session.get('user_id')
    
    analytics = db.get_user_analytics(user_id)
    accounts = db.get_user_accounts(user_id)
    target_groups = db.get_target_groups(user_id)
    forum_groups = db.get_forum_groups(user_id)
    broadcast_state = db.get_broadcast_state(user_id)
    
    return jsonify({
        'success': True,
        'data': {
            'total_cycles': analytics.get('total_cycles', 0),
            'total_sent': analytics.get('total_sent', 0),
            'total_failed': analytics.get('total_failed', 0),
            'total_broadcasts': analytics.get('total_broadcasts', 0),
            'accounts_count': len(accounts),
            'groups_count': len(target_groups) + len(forum_groups),
            'broadcast_running': broadcast_state.get('running', False),
            'broadcast_paused': broadcast_state.get('paused', False)
        }
    })


@app.route('/api/accounts')
@login_required
def get_accounts():
    """API endpoint to get user accounts"""
    user_id = session.get('user_id')
    accounts = db.get_user_accounts(user_id)
    
    # Format accounts data
    accounts_data = []
    for acc in accounts:
        accounts_data.append({
            'id': str(acc.get('_id')),
            'phone_number': acc.get('phone_number'),
            'first_name': acc.get('first_name', ''),
            'last_name': acc.get('last_name', ''),
            'is_active': acc.get('is_active', True),
            'created_at': acc.get('created_at').isoformat() if acc.get('created_at') else None
        })
    
    return jsonify({
        'success': True,
        'accounts': accounts_data
    })


@app.route('/api/groups')
@login_required
def get_groups():
    """API endpoint to get user groups"""
    user_id = session.get('user_id')
    
    target_groups = db.get_target_groups(user_id)
    forum_groups = db.get_forum_groups(user_id)
    
    # Format groups data
    groups_data = {
        'regular_groups': [
            {
                'id': g.get('group_id'),
                'name': g.get('group_name'),
                'type': 'regular'
            }
            for g in target_groups
        ],
        'forum_groups': [
            {
                'id': g.get('group_id'),
                'name': g.get('group_name'),
                'type': 'forum',
                'topics_count': len(g.get('topics', []))
            }
            for g in forum_groups
        ]
    }
    
    return jsonify({
        'success': True,
        'groups': groups_data
    })


@app.route('/logout')
def logout():
    """Logout user"""
    session.clear()
    return redirect(url_for('login'))


# API endpoint for bot to update analytics
@app.route('/api/bot/update_analytics', methods=['POST'])
def bot_update_analytics():
    """API endpoint for bot to update user analytics"""
    try:
        data = request.json
        
        # Verify API key or bot token
        api_key = request.headers.get('X-API-Key')
        if api_key != config.BOT_TOKEN:
            return jsonify({'success': False, 'error': 'Unauthorized'}), 401
        
        user_id = data.get('user_id')
        action = data.get('action')  # 'increment_cycle', 'increment_sent', 'increment_failed'
        
        if action == 'increment_cycle':
            db.increment_broadcast_cycle(user_id)
        elif action == 'increment_sent':
            db.increment_broadcast_stats(user_id, success=True)
        elif action == 'increment_failed':
            db.increment_broadcast_stats(user_id, success=False)
        
        return jsonify({'success': True})
    
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


def main():
    """Start the web dashboard"""
    try:
        logger.info("=" * 60)
        logger.info("AzTech Ads Bot - Web Dashboard")
        logger.info("=" * 60)
        logger.info("")
        logger.info("Starting dashboard server...")
        logger.info("Dashboard will be available at:")
        logger.info("  - Local: http://localhost:5000")
        logger.info("  - Network: http://0.0.0.0:5000")
        logger.info("")
        logger.info("Press CTRL+C to stop the server")
        logger.info("=" * 60)
        
        # Start Flask application
        app.run(
            host='0.0.0.0',
            port=5000,
            debug=False,  # Set to False in production
            threaded=True
        )
        
    except KeyboardInterrupt:
        logger.info("\nShutting down dashboard server...")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Failed to start dashboard: {e}")
        sys.exit(1)


# ============================================================================
# Bot Analytics Integration
# ============================================================================

class DashboardAnalytics:
    """Handle analytics reporting to the web dashboard"""
    
    def __init__(self, dashboard_url="http://localhost:5000"):
        self.dashboard_url = dashboard_url
        self.api_endpoint = f"{dashboard_url}/api/bot/update_analytics"
        self.headers = {
            "X-API-Key": BOT_TOKEN,
            "Content-Type": "application/json"
        }
    
    def send_update(self, user_id, action):
        """
        Send analytics update to dashboard
        
        Args:
            user_id: Telegram user ID
            action: Type of action ('increment_cycle', 'increment_sent', 'increment_failed')
        """
        try:
            payload = {
                "user_id": user_id,
                "action": action
            }
            
            response = requests.post(
                self.api_endpoint,
                json=payload,
                headers=self.headers,
                timeout=5
            )
            
            if response.status_code == 200:
                logger.debug(f"Analytics updated for user {user_id}: {action}")
                return True
            else:
                logger.warning(f"Failed to update analytics: {response.status_code}")
                return False
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Error sending analytics update: {e}")
            return False
    
    def increment_cycle(self, user_id):
        """Increment broadcast cycle count"""
        return self.send_update(user_id, "increment_cycle")
    
    def increment_sent(self, user_id):
        """Increment successful ads sent count"""
        return self.send_update(user_id, "increment_sent")
    
    def increment_failed(self, user_id):
        """Increment failed ads count"""
        return self.send_update(user_id, "increment_failed")


# Global dashboard analytics instance
dashboard_analytics = DashboardAnalytics()


def notify_cycle_complete(user_id):
    """Notify dashboard that a broadcast cycle completed"""
    dashboard_analytics.increment_cycle(user_id)


def notify_message_sent(user_id):
    """Notify dashboard that a message was sent successfully"""
    dashboard_analytics.increment_sent(user_id)


def notify_message_failed(user_id):
    """Notify dashboard that a message failed to send"""
    dashboard_analytics.increment_failed(user_id)


if __name__ == '__main__':
    main()
