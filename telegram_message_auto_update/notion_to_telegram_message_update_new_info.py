"""
═══════════════════════════════════════════════════════════════════════════════
                        TELEGRAM MESSAGE UPDATER
                    Sync Notion Changes to Telegram Posts
═══════════════════════════════════════════════════════════════════════════════

WHAT THIS SCRIPT DOES:
----------------------
This script synchronizes changes from Notion back to existing Telegram messages:

1. FINDS events in Notion that have a telegram_message_id
2. CHECKS if Notion was edited more recently than last Telegram update
3. COMPARES current Telegram message with new Notion data
4. UPDATES Telegram messages that have changed
5. STORES message data in JSON cache for comparison
6. UPDATES timestamp_telegram in Notion after successful sync

KEY FEATURES:
-------------
• Change Detection: Only updates when Notion data is newer
• Smart Comparison: Compares formatted message text to avoid unnecessary updates
• JSON Cache: Maintains exact copy of what's on Telegram
• Error Handling: Gracefully handles API failures without data loss
• Test Mode: Dry run option to preview changes without updating
• Channel Selection: Test on test channel before going live

NOTION FIELDS REQUIRED:
-----------------------
- telegram_message_id: Number field with Telegram message ID
- telegram_test_channel_id: Number field for test channel ID
- timestamp_telegram: Date field for last sync time
- last_notion_edited_time: Formula/date field with last edit time
- All event fields from telegram_event_scheduler.py

USAGE:
------
Run the script:
    python telegram_message_updater.py

Options:
    --test           Dry run mode (no actual updates)
    --live           Use live channel
    --auto           Skip all confirmations (for automation)
    --clean-session  Delete existing session file before starting

═══════════════════════════════════════════════════════════════════════════════
"""

import asyncio
import json
import os
import requests
import io
import logging
import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Dict, Optional, List, Tuple
from dataclasses import dataclass, asdict
from telethon import TelegramClient
from telethon.tl.functions.messages import EditMessageRequest
from notion_client import Client
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# ═══════════════════════════════════════════════════════════════
# LOGGING CONFIGURATION FOR REPLIT
# ═══════════════════════════════════════════════════════════════

# Get script directory for log file
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(SCRIPT_DIR, 'updater_run.log')

# Configure logging to both file and console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, mode='a', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

# Custom print function that logs and prints
def log_print(message, level="INFO"):
    """Print and log a message with timestamp"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    full_message = f"[{timestamp}] {message}"
    
    # Print to console (for manual runs)
    print(message)
    
    # Log to file (for scheduled runs)
    if level == "ERROR":
        logger.error(message)
    elif level == "WARNING":
        logger.warning(message)
    else:
        logger.info(message)
    
    # Force flush to ensure immediate writing
    sys.stdout.flush()
    for handler in logger.handlers:
        if isinstance(handler, logging.FileHandler):
            handler.flush()

# Log startup
log_print("=" * 50)
log_print("SCRIPT STARTED - Telegram Message Updater")
log_print(f"Environment: {'REPLIT' if 'REPL_ID' in os.environ else 'LOCAL'}")
log_print(f"Working directory: {os.getcwd()}")
log_print(f"Script directory: {SCRIPT_DIR}")
log_print(f"Log file: {LOG_FILE}")
log_print("=" * 50)

# ═══════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════

# Notion Configuration
NOTION_TOKEN = os.getenv('NOTION_TOKEN')
if not NOTION_TOKEN:
    raise ValueError("NOTION_TOKEN not found in .env file")

NOTION_MASTER_DB_ID = os.getenv('NOTION_MASTER_DB_ID', '1f5b2c11515b801ebd95cd423b72eb55')

# Telegram Configuration
TELEGRAM_API_ID = os.getenv('TELEGRAM_API_ID')
TELEGRAM_API_HASH = os.getenv('TELEGRAM_API_HASH')

if not TELEGRAM_API_ID or not TELEGRAM_API_HASH:
    raise ValueError("Telegram credentials not found in .env file")

api_id = int(TELEGRAM_API_ID)
api_hash = TELEGRAM_API_HASH

# Channel Configuration
TELEGRAM_LIVE_CHANNEL = os.getenv('TELEGRAM_LIVE_CHANNEL', 'raveinbelgium')
TELEGRAM_TEST_CHANNEL = os.getenv('TELEGRAM_TEST_CHANNEL', 'testchannel1234123434')

# Cache Configuration
CACHE_FILE = os.path.join(SCRIPT_DIR, 'telegram_messages_cache.json')

# Session Configuration - use environment-specific session to avoid conflicts
def get_session_file():
    """Get environment-specific session file to avoid conflicts between local and Replit"""
    if 'REPL_ID' in os.environ:
        # Replit environment - use persistent storage
        home_dir = os.path.expanduser("~")
        session_dir = os.path.join(home_dir, ".telegram_sessions")
        if not os.path.exists(session_dir):
            os.makedirs(session_dir, mode=0o700)
        session_name = "replit_updater_session"
    else:
        # Local environment
        session_dir = SCRIPT_DIR
        session_name = "local_updater_session"
    
    return os.path.join(session_dir, session_name)

SESSION_FILE = get_session_file()
log_print(f"📱 Using session file: {SESSION_FILE}")

TIMEZONE = ZoneInfo(os.getenv('TIMEZONE', 'Europe/Brussels'))

# Initialize Notion client
notion = Client(auth=NOTION_TOKEN)


# ═══════════════════════════════════════════════════════════════
# DATA STRUCTURES
# ═══════════════════════════════════════════════════════════════

@dataclass
class TelegramMessage:
    """Represents a Telegram message's content with structured data"""
    message_id: int
    channel: str
    text: str  # Formatted HTML text for Telegram
    image_url: Optional[str]
    last_updated: str
    notion_id: str

    # Structured event data (mirrors Notion fields)
    event_data: Optional[dict] = None

    def to_dict(self) -> dict:
        """Convert to dictionary with clean structure"""
        return {
            "message_id": self.message_id,
            "channel": self.channel,
            "notion_id": self.notion_id,
            "last_updated": self.last_updated,
            "formatted_text": self.text,  # Keep formatted version separate
            "image_url": self.image_url,
            "event": self.event_data or {}  # Clean event fields
        }

    @classmethod
    def from_dict(cls, data: dict):
        """Create from dictionary, handling both old and new formats"""
        # Handle old format
        if "text" in data and "event" not in data:
            return cls(
                message_id=data["message_id"],
                channel=data["channel"],
                text=data["text"],
                image_url=data.get("image_url"),
                last_updated=data["last_updated"],
                notion_id=data["notion_id"],
                event_data=None
            )

        # New format
        return cls(
            message_id=data["message_id"],
            channel=data["channel"],
            text=data.get("formatted_text", ""),
            image_url=data.get("image_url"),
            last_updated=data["last_updated"],
            notion_id=data["notion_id"],
            event_data=data.get("event", {})
        )


class MessageCache:
    """Manages the JSON cache of Telegram messages"""

    def __init__(self, cache_file: str = None):
        self.cache_file = cache_file or CACHE_FILE
        self.messages: Dict[str, TelegramMessage] = {}  # Changed to use string keys
        self.load()

    def load(self):
        """Load cache from JSON file"""
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    for key, msg_data in data.items():
                        self.messages[key] = TelegramMessage.from_dict(msg_data)
                log_print(f"📦 Loaded {len(self.messages)} cached messages")
            except Exception as e:
                log_print(f"⚠️  Cache load error: {e}. Starting fresh.", "WARNING")
                self.messages = {}

    def save(self):
        """Save cache to JSON file with clean structure"""
        try:
            data = {
                key: msg.to_dict()
                for key, msg in self.messages.items()
            }
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            log_print(f"💾 Saved {len(self.messages)} messages to cache")
        except Exception as e:
            log_print(f"❌ Cache save error: {e}", "ERROR")

    def get(self, channel: str, message_id: int) -> Optional[TelegramMessage]:
        """Get a cached message using channel:id as key"""
        key = f"{channel}:{message_id}"
        return self.messages.get(key)

    def update(self, message: TelegramMessage):
        """Update or add a message to cache"""
        key = f"{message.channel}:{message.message_id}"
        self.messages[key] = message


# ═══════════════════════════════════════════════════════════════
# NOTION DATA FETCHING
# ═══════════════════════════════════════════════════════════════

def safe_get_text(rich_text_list: list) -> str:
    """Safely extract plain text from Notion rich text field"""
    if rich_text_list and len(rich_text_list) > 0:
        return rich_text_list[0].get("plain_text", "")
    return ""


def safe_get_url(prop: dict) -> str:
    """Safely extract URL from Notion property"""
    return prop.get("url") or ""


def parse_notion_date(date_str: str) -> Optional[datetime]:
    """Parse Notion date string to datetime"""
    if not date_str:
        return None
    try:
        # Handle both date and datetime formats
        if 'T' in date_str:
            return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        else:
            return datetime.strptime(date_str, '%Y-%m-%d')
    except:
        return None


def fetch_events_with_telegram_ids(channel: str) -> List[dict]:
    """Fetch all events that have telegram_message_id or telegram_test_channel_id from Notion"""
    events = []
    has_more = True
    cursor = None

    log_print("Querying Notion database...")

    # Get today's date for filtering
    today = datetime.now().date()

    # Determine which field to check based on channel
    if channel == TELEGRAM_TEST_CHANNEL:
        id_field = "telegram_test_channel_id"
        print(f"Looking for events with {id_field}...")
    else:
        id_field = "telegram_message_id"
        print(f"Looking for events with {id_field}...")

    while has_more:
        # Query with pagination - filter by the appropriate ID field
        params = {
            "database_id": NOTION_MASTER_DB_ID,
            "filter": {
                "and": [
                    {
                        "property": id_field,
                        "number": {"is_not_empty": True}
                    },
                    {
                        "property": "event_date",
                        "date": {"on_or_after": today.isoformat()}
                    }
                ]
            }
        }
        if cursor:
            params["start_cursor"] = cursor

        response = notion.databases.query(**params)

        # Process each event
        for item in response["results"]:
            p = item["properties"]

            # Get the appropriate telegram ID based on channel
            if channel == TELEGRAM_TEST_CHANNEL:
                telegram_id = p.get("telegram_test_channel_id", {}).get("number")
            else:
                telegram_id = p.get("telegram_message_id", {}).get("number")

            if not telegram_id:
                continue

            # Get title
            title_items = p.get("title", {}).get("title", [])
            if not title_items:
                print(f"⚠️  Skipping event with telegram_id {telegram_id} – no title")
                continue
            title = title_items[0]["plain_text"]

            try:
                # Get timestamps
                timestamp_telegram_prop = p.get("timestamp_telegram", {})
                if timestamp_telegram_prop:
                    date_prop = timestamp_telegram_prop.get("date")
                    timestamp_telegram = date_prop.get("start") if date_prop else None
                else:
                    timestamp_telegram = None
                last_edited = item.get("last_edited_time")

                # Skip if we can't determine if update is needed
                if not last_edited:
                    print(f"⚠️  Skipping '{title}' – no last_edited_time")
                    continue

                # Get event data - store raw Notion values
                event_date_prop = p.get("event_date", {})
                event_date = event_date_prop.get("date", {}).get("start") if event_date_prop.get("date") else None

                # Skip past events
                if event_date:
                    event_date_obj = datetime.strptime(event_date, '%Y-%m-%d').date()
                    if event_date_obj < today:
                        print(f"⚠️  Skipping '{title}' – event date in the past")
                        continue

                until_date_prop = p.get("until_date", {})
                until_date = until_date_prop.get("date", {}).get("start") if until_date_prop.get("date") else None

                location = safe_get_text(p.get("event_location", {}).get("rich_text", []))
                start_time = safe_get_text(p.get("start_time", {}).get("rich_text", []))
                lineup_rt = p.get("raw_lineup", {}).get("rich_text", [])
                lineup = lineup_rt[0]["plain_text"] if lineup_rt else ""

                # Get URLs
                event_url = safe_get_url(p.get("event_url", {}))
                fb_url = safe_get_url(p.get("facebook_event_url", {}))
                swap_url = safe_get_url(p.get("ticketswap_url", {}))
                image_url = safe_get_url(p.get("socials_img_url", {}))
                ig_url = safe_get_url(p.get("ig_post_url", {}))

                events.append({
                    "id": item["id"],
                    "title": title,
                    "telegram_message_id": int(telegram_id),
                    "timestamp_telegram": timestamp_telegram,
                    "last_edited_time": last_edited,
                    # Raw Notion values
                    "event_date": event_date,  # Keep as YYYY-MM-DD
                    "until_date": until_date,   # Keep as YYYY-MM-DD or None
                    "event_location": location,  # Match Notion field name
                    "start_time": start_time,
                    "raw_lineup": lineup,  # Match Notion field name
                    "event_url": event_url,
                    "facebook_event_url": fb_url,  # Match Notion field name
                    "ticketswap_url": swap_url,    # Match Notion field name
                    "ig_post_url": ig_url,         # Match Notion field name
                    "socials_img_url": image_url,  # Match Notion field name
                    # Keep simplified versions for backward compatibility
                    "date": event_date,
                    "location": location,
                    "lineup": lineup,
                    "fb_url": fb_url,
                    "swap_url": swap_url,
                    "ig_url": ig_url,
                    "image_url": image_url
                })
            except Exception as e:
                print(f"❌ Error processing '{title}': {e}")
                continue

        has_more = response.get("has_more", False)
        cursor = response.get("next_cursor")

    return events


def update_notion_timestamp(event_id: str):
    """Update timestamp_telegram in Notion after successful sync"""
    try:
        notion.pages.update(
            page_id=event_id,
            properties={
                "timestamp_telegram": {
                    "date": {"start": datetime.now(TIMEZONE).isoformat()}
                }
            }
        )
        return True
    except Exception as e:
        print(f"❌ Failed to update Notion timestamp: {e}")
        return False


# ═══════════════════════════════════════════════════════════════
# MESSAGE FORMATTING
# ═══════════════════════════════════════════════════════════════

def format_event_date(date_str: str, until_str: Optional[str]) -> str:
    """Format event date, handling multi-day events"""
    if not date_str:
        return "DATE TBA"

    start_date = datetime.strptime(date_str, '%Y-%m-%d')

    if until_str:
        until_date = datetime.strptime(until_str, '%Y-%m-%d')
        if start_date.month == until_date.month:
            return f"{start_date.day}-{until_date.day} {start_date.strftime('%b').upper()}"
        else:
            return f"{start_date.day} {start_date.strftime('%b').upper()} - {until_date.day} {until_date.strftime('%b').upper()}"
    else:
        return f"{start_date.day} {start_date.strftime('%b').upper()}"


def build_message_text(event: dict) -> str:
    """Build the Telegram message text with proper formatting"""
    # Handle date formatting
    formatted_date = format_event_date(event.get('date') or event.get('event_date'),
                                       event.get('until_date'))

    # Build links section - dynamically include only non-empty URLs
    links = []

    # Facebook link
    fb_url = event.get('fb_url') or event.get('facebook_event_url')
    if fb_url and fb_url.strip():
        links.append(f"<a href='{fb_url}'>Facebook</a>")

    # Tickets/Event URL
    event_url = event.get('event_url')
    if event_url and event_url.strip():
        links.append(f"<a href='{event_url}'>Tickets</a>")

    # Ticketswap URL - IMPORTANT: Check both field names
    swap_url = event.get('swap_url') or event.get('ticketswap_url')
    if swap_url and swap_url.strip():
        links.append(f"<a href='{swap_url}'>Ticketswap</a>")

    # Build the message
    title = event.get('title', 'Event')
    location = event.get('location') or event.get('event_location', 'TBA')
    start_time = event.get('start_time', 'TBA')
    lineup = event.get('lineup') or event.get('raw_lineup', '')

    msg_text = f"<b>{formatted_date} | {title}</b>\n"
    msg_text += f"{location} • Starts at {start_time}\n\n"

    # Only add lineup if it exists and isn't empty
    if lineup and lineup.strip() and lineup != "Lineup TBA":
        msg_text += f"Lineup: {lineup}\n\n"

    # Add main links if any exist
    if links:
        links_text = " | ".join(links)
        msg_text += links_text

    # Instagram gets special treatment - separate line at the bottom with share-like icon and IG link
    ig_url = event.get('ig_url') or event.get('ig_post_url')
    if ig_url and ig_url.strip():
        if links:  # Add spacing if there were other links
            msg_text += "\n\n"
        msg_text += f"<a href='{ig_url}'>↗ IG</a>"

    return msg_text


# ═══════════════════════════════════════════════════════════════
# TELEGRAM OPERATIONS
# ═══════════════════════════════════════════════════════════════

async def get_telegram_message(client: TelegramClient, channel: str, message_id: int) -> Optional[str]:
    """Fetch current message text from Telegram"""
    try:
        message = await client.get_messages(channel, ids=message_id)
        if message:
            return message.text or message.caption
        return None
    except Exception as e:
        print(f"❌ Error fetching message {message_id}: {e}")
        return None


async def update_telegram_message(
    client: TelegramClient,
    channel: str,
    message_id: int,
    new_text: str
) -> bool:
    """Update a Telegram message with new text only
    
    Note: Telegram does not allow updating images in existing messages.
    Only the text/caption can be modified.
    """
    try:
        # Get the channel entity
        channel_entity = await client.get_entity(channel)

        # Edit the message (text only)
        await client.edit_message(
            channel_entity,
            message_id,
            text=new_text,
            parse_mode='html',
            link_preview=False
        )

        return True
    except Exception as e:
        # Let the caller handle specific errors
        if "Content of the message was not modified" in str(e):
            raise e  # Re-raise for special handling
        print(f"❌ Error updating message {message_id}: {e}")
        return False


async def check_needs_update(event: dict, cache: MessageCache, channel: str) -> Tuple[bool, str]:
    """
    Check if an event needs updating
    Returns: (needs_update, reason)
    """
    # First check if we have this in cache
    cached_msg = cache.get(channel, event['telegram_message_id'])
    if not cached_msg:
        # Always need to sync if not in cache, regardless of timestamps
        return True, "Not in cache (rebuilding)"

    # Check if Notion was edited after last Telegram update
    last_edited = parse_notion_date(event['last_edited_time'])
    timestamp_telegram = parse_notion_date(event.get('timestamp_telegram'))

    if not last_edited:
        return False, "No last_edited_time"

    # If never synced to Telegram, needs update
    if not timestamp_telegram:
        return True, "Never synced to Telegram"

    # If Notion not edited since last sync, skip
    if last_edited <= timestamp_telegram:
        return False, "No changes since last sync"

    # Compare message text
    new_text = build_message_text(event)
    if cached_msg.text != new_text:
        return True, "Message content changed"

    # Check if image changed
    if event.get('image_url') != cached_msg.image_url:
        return True, "Image URL changed"

    return False, "No changes detected"


def compare_and_show_changes(current_text: str, new_text: str, event: dict = None) -> bool:
    """Compare texts and show what changed"""
    current_lines = current_text.strip().split('\n') if current_text else []
    new_lines = new_text.strip().split('\n') if new_text else []

    has_changes = False

    # Check for specific link changes if event data provided
    if event:
        # Extract current links from the message
        current_has_fb = 'Facebook</a>' in current_text
        current_has_tickets = 'Tickets</a>' in current_text
        current_has_swap = 'Ticketswap</a>' in current_text
        current_has_ig = '↗ IG</a>' in current_text

        # Check new links - being careful with field names
        new_has_fb = bool(event.get('fb_url') or event.get('facebook_event_url'))
        new_has_tickets = bool(event.get('event_url'))
        new_has_swap = bool(event.get('swap_url') or event.get('ticketswap_url'))
        new_has_ig = bool(event.get('ig_url') or event.get('ig_post_url'))

        # Track if any link changes occurred
        link_changes = []

        # Check each link for changes
        if current_has_fb and not new_has_fb:
            link_changes.append("   ➖ Removed Facebook link")
            has_changes = True
        elif not current_has_fb and new_has_fb:
            link_changes.append("   ➕ Added Facebook link")
            has_changes = True

        if current_has_tickets and not new_has_tickets:
            link_changes.append("   ➖ Removed Tickets link")
            has_changes = True
        elif not current_has_tickets and new_has_tickets:
            link_changes.append("   ➕ Added Tickets link")
            has_changes = True

        if current_has_swap and not new_has_swap:
            link_changes.append("   ➖ Removed Ticketswap link")
            has_changes = True
        elif not current_has_swap and new_has_swap:
            link_changes.append("   ➕ Added Ticketswap link")
            has_changes = True

        if current_has_ig and not new_has_ig:
            link_changes.append("   ➖ Removed Instagram link")
            has_changes = True
        elif not current_has_ig and new_has_ig:
            link_changes.append("   ➕ Added Instagram link (↗ IG)")
            has_changes = True

        # Show link changes summary if any occurred
        if link_changes:
            print("   🔗 Link changes:")
            for change in link_changes:
                print(change)

        # Also check if URLs themselves changed (not just added/removed)
        if current_has_fb and new_has_fb:
            # Extract actual URLs to compare
            import re
            current_fb_match = re.search(r'href=[\'"]([^\'"]*facebook[^\'"]*)[\'"]', current_text)
            new_fb_url = event.get('fb_url') or event.get('facebook_event_url')
            if current_fb_match and current_fb_match.group(1) != new_fb_url:
                print("   🔄 Facebook URL changed")
                has_changes = True

        # Check Ticketswap URL specifically
        if current_has_swap and new_has_swap:
            import re
            current_swap_match = re.search(r'href=[\'"]([^\'"]*ticketswap[^\'"]*)[\'"]', current_text)
            new_swap_url = event.get('swap_url') or event.get('ticketswap_url')
            if current_swap_match and current_swap_match.group(1) != new_swap_url:
                print("   🔄 Ticketswap URL changed")
                has_changes = True

        # Check Instagram URL specifically - handle both missing and changed URLs
        new_ig_url = event.get('ig_url') or event.get('ig_post_url')
        if new_ig_url:  # If we have an IG URL in Notion
            if not current_has_ig:
                # IG URL exists in Notion but missing from Telegram message
                print("   ➕ Instagram URL missing from message - will add it")
                has_changes = True
            else:
                # Both have IG, check if URL changed
                import re
                current_ig_match = re.search(r'href=[\'"]([^\'"]+)[\'"]>↗ IG</a>', current_text)
                if current_ig_match and current_ig_match.group(1) != new_ig_url:
                    print("   🔄 Instagram URL changed")
                    print(f"      From: {current_ig_match.group(1)}")
                    print(f"      To:   {new_ig_url}")
                    has_changes = True

    # Check for other non-link changes
    title_changed = False
    lineup_changed = False
    location_changed = False

    for i, (curr, new) in enumerate(zip(current_lines, new_lines)):
        if curr != new:
            if i == 0 and not title_changed:  # Title/date line
                print(f"   📅 Title/Date changed")
                print(f"      From: {curr[:60]}...")
                print(f"      To:   {new[:60]}...")
                title_changed = True
                has_changes = True
            elif ("Lineup:" in curr or "Lineup:" in new) and not lineup_changed:
                if "Lineup:" not in curr and "Lineup:" in new:
                    print(f"   ➕ Added lineup: {new[:60]}...")
                elif "Lineup:" in curr and "Lineup:" not in new:
                    print(f"   ➖ Removed lineup")
                else:
                    print(f"   🎵 Lineup changed")
                    print(f"      From: {curr[:60]}...")
                    print(f"      To:   {new[:60]}...")
                lineup_changed = True
                has_changes = True
            elif i == 1 and not location_changed:  # Location/time line
                # Only show if it's actually different and not a link line
                if "href=" not in curr and "href=" not in new:
                    print(f"   📍 Location/Time changed")
                    print(f"      From: {curr}")
                    print(f"      To:   {new}")
                    location_changed = True
                    has_changes = True

    # Check for structural changes
    if len(new_lines) > len(current_lines) and not has_changes:
        print(f"   ➕ Added {len(new_lines) - len(current_lines)} lines")
        has_changes = True
    elif len(current_lines) > len(new_lines) and not has_changes:
        print(f"   ➖ Removed {len(current_lines) - len(new_lines)} lines")
        has_changes = True

    return has_changes


# ═══════════════════════════════════════════════════════════════
# MAIN SYNC LOGIC
# ═══════════════════════════════════════════════════════════════

async def sync_events(channel: str, test_mode: bool = False):
    """Main sync function"""
    print("\n📊 FETCHING EVENTS FROM NOTION")
    print("=" * 50)

    events = fetch_events_with_telegram_ids(channel)
    if not events:
        if channel == TELEGRAM_TEST_CHANNEL:
            print("❌ No events found with telegram_test_channel_id")
        else:
            print("❌ No events found with telegram_message_id")
        return

    print(f"✅ Found {len(events)} events with Telegram IDs for {channel}")

    # Load cache
    cache = MessageCache()

    # Statistics
    stats = {
        "checked": 0,
        "updated": 0,
        "skipped": 0,
        "errors": 0,
        "identical": 0
    }

    print("\n🔄 CHECKING FOR UPDATES")
    print("=" * 50)

    # Check if we should clean the session
    import sys
    if '--clean-session' in sys.argv:
        session_path = f"{SESSION_FILE}.session"
        if os.path.exists(session_path):
            os.remove(session_path)
            print(f"🗑️  Deleted old session file: {session_path}")
    
    # Try to connect with auto-retry for session conflicts
    max_retries = 2
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            async with TelegramClient(SESSION_FILE, api_id, api_hash) as client:
                # If we get here, connection succeeded
                log_print("✅ Connected to Telegram successfully")
                
                for event in events:
                    stats["checked"] += 1

                    # Check if update needed
                    needs_update, reason = await check_needs_update(event, cache, channel)

                    if not needs_update:
                        print(f"⏭️  {event['title'][:30]}... - {reason}")
                        stats["skipped"] += 1
                        continue

                    print(f"\n📝 {event['title']}")
                    print(f"   Message ID: {event['telegram_message_id']}")
                    print(f"   Reason: {reason}")

                    if test_mode:
                        print("   [TEST MODE] Would update this message")
                        stats["updated"] += 1
                        continue

                    # Get current message from Telegram
                    current_text = await get_telegram_message(client, channel, event['telegram_message_id'])
                    if current_text is None:
                        print(f"   ❌ Message not found on Telegram")
                        stats["errors"] += 1
                        continue

                    # Build new message text
                    new_text = build_message_text(event)

                    # Show detailed changes (but not if we're just rebuilding cache)
                    if reason == "Not in cache (rebuilding)":
                        # When rebuilding cache, don't show spurious changes
                        has_changes = False
                    else:
                        has_changes = compare_and_show_changes(current_text, new_text, event)

                    # Normalize text for comparison (handle markdown vs HTML bold formats)
                    def normalize_for_comparison(text):
                        """Normalize text to handle both markdown and HTML bold formats"""
                        if not text:
                            return ""
                        # Convert markdown bold to HTML bold for comparison
                        import re
                        normalized = text.strip()
                        # Convert **text** to <b>text</b>
                        normalized = re.sub(r'\*\*([^*]+)\*\*', r'<b>\1</b>', normalized)
                        return normalized
                    
                    # Compare normalized versions for actual update decision
                    current_normalized = normalize_for_comparison(current_text)
                    new_normalized = normalize_for_comparison(new_text)

                    # Check if actually different
                    if current_normalized == new_normalized:
                        if reason == "Not in cache (rebuilding)":
                            print("   ✅ Rebuilding cache entry")
                        else:
                            print("   ✅ Content identical")
                        stats["identical"] += 1

                        # Always update cache (important for rebuilding)
                        cache.update(TelegramMessage(
                        message_id=event['telegram_message_id'],
                        channel=channel,
                        text=new_text,
                        image_url=event.get('socials_img_url'),
                        last_updated=datetime.now(TIMEZONE).isoformat(),
                        notion_id=event['id'],
                        event_data={
                            # Match exact Notion field names
                            "title": event.get('title', ''),
                            "event_date": event.get('event_date'),  # YYYY-MM-DD format
                            "until_date": event.get('until_date'),   # YYYY-MM-DD or null
                            "event_location": event.get('event_location', ''),
                            "start_time": event.get('start_time', ''),
                            "raw_lineup": event.get('raw_lineup', ''),
                            "event_url": event.get('event_url'),
                            "facebook_event_url": event.get('facebook_event_url'),
                            "ticketswap_url": event.get('ticketswap_url'),
                            "ig_post_url": event.get('ig_post_url'),
                            "socials_img_url": event.get('socials_img_url')
                        }
                    ))

                        # Only update Notion timestamp if we're doing actual sync, not cache rebuild
                        if reason != "Not in cache (rebuilding)":
                            update_notion_timestamp(event['id'])
                        continue

                    if has_changes or (reason == "Not in cache (rebuilding)" and current_normalized != new_normalized):
                        if reason != "Not in cache (rebuilding)":
                            print("   🔄 Applying changes...")

                    # Try to update the message (text only - images cannot be updated)
                    try:
                        success = await update_telegram_message(
                        client,
                        channel,
                        event['telegram_message_id'],
                        new_text
                        # Images cannot be updated in existing Telegram messages
                    )

                        if success:
                            print("   ✅ Updated successfully")
                            stats["updated"] += 1

                            # Update cache
                            cache.update(TelegramMessage(
                            message_id=event['telegram_message_id'],
                            channel=channel,
                            text=new_text,
                            image_url=event.get('socials_img_url'),
                            last_updated=datetime.now(TIMEZONE).isoformat(),
                            notion_id=event['id'],
                            event_data={
                                # Match exact Notion field names
                                "title": event.get('title', ''),
                                "event_date": event.get('event_date'),  # YYYY-MM-DD format
                                "until_date": event.get('until_date'),   # YYYY-MM-DD or null
                                "event_location": event.get('event_location', ''),
                                "start_time": event.get('start_time', ''),
                                "raw_lineup": event.get('raw_lineup', ''),
                                "event_url": event.get('event_url'),
                                "facebook_event_url": event.get('facebook_event_url'),
                                "ticketswap_url": event.get('ticketswap_url'),
                                "ig_post_url": event.get('ig_post_url'),
                                "socials_img_url": event.get('socials_img_url')
                            }
                        ))

                            # Update Notion timestamp
                            update_notion_timestamp(event['id'])
                        else:
                            print("   ❌ Update failed")
                            stats["errors"] += 1

                    except Exception as e:
                        if "Content of the message was not modified" in str(e):
                            print("   ℹ️  Message already has this content, updating cache")
                            stats["identical"] += 1

                            # Update cache anyway
                            cache.update(TelegramMessage(
                            message_id=event['telegram_message_id'],
                            channel=channel,
                            text=new_text,
                            image_url=event.get('socials_img_url'),
                            last_updated=datetime.now(TIMEZONE).isoformat(),
                            notion_id=event['id'],
                            event_data={
                                # Match exact Notion field names
                                "title": event.get('title', ''),
                                "event_date": event.get('event_date'),  # YYYY-MM-DD format
                                "until_date": event.get('until_date'),   # YYYY-MM-DD or null
                                "event_location": event.get('event_location', ''),
                                "start_time": event.get('start_time', ''),
                                "raw_lineup": event.get('raw_lineup', ''),
                                "event_url": event.get('event_url'),
                                "facebook_event_url": event.get('facebook_event_url'),
                                "ticketswap_url": event.get('ticketswap_url'),
                                "ig_post_url": event.get('ig_post_url'),
                                "socials_img_url": event.get('socials_img_url')
                            }
                        ))

                            # Update Notion timestamp
                            update_notion_timestamp(event['id'])
                        else:
                            print(f"   ❌ Error: {e}")
                            stats["errors"] += 1
                
                # Successfully completed all events (end of for loop)
                break  # Exit retry loop
                
        except Exception as e:
            if "AuthKeyDuplicatedError" in str(e.__class__.__name__):
                log_print("\n⚠️  Session conflict detected. Cleaning up...", "WARNING")
                # Try to delete the session and retry
                session_path = f"{SESSION_FILE}.session"
                if os.path.exists(session_path):
                    os.remove(session_path)
                    log_print(f"🗑️  Deleted corrupted session: {session_path}")
                
                retry_count += 1
                if retry_count < max_retries:
                    log_print(f"🔄 Retrying connection (attempt {retry_count + 1}/{max_retries})...")
                    await asyncio.sleep(2)  # Wait before retry
                    continue
                else:
                    log_print("\n❌ Max retries reached. Session conflict persists.", "ERROR")
                    log_print("💡 Try running with --clean-session flag")
                    return
            else:
                log_print(f"\n❌ Unexpected error: {e}", "ERROR")
                return

    # Save cache
    if not test_mode:
        cache.save()

    # Print summary
    print("\n📊 SUMMARY")
    print("=" * 50)
    print(f"✅ Updated: {stats['updated']}")
    print(f"ℹ️  Already identical: {stats['identical']}")
    print(f"⏭️  Skipped: {stats['skipped']}")
    print(f"❌ Errors: {stats['errors']}")
    print(f"📋 Total checked: {stats['checked']}")


# ═══════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════════

async def main():
    """Main entry point"""
    import sys

    print("=" * 50)
    print("      TELEGRAM MESSAGE UPDATER")
    print("=" * 50)

    # Parse command line arguments
    test_mode = '--test' in sys.argv
    use_live = '--live' in sys.argv
    auto_mode = '--auto' in sys.argv

    if test_mode:
        print("\n🧪 TEST MODE - no actual updates")

    # Determine channel
    if use_live:
        channel = TELEGRAM_LIVE_CHANNEL
        print(f"\n📡 Using LIVE channel: {channel}")
        print("   This is where your actual messages are posted")
        if not test_mode and not auto_mode:
            confirm = input("⚠️  Are you sure you want to update LIVE messages? (yes/no): ")
            if confirm.lower() not in ['yes', 'y']:
                print("❌ Cancelled")
                return
    else:
        # Default to live channel for reading (since that's where messages are)
        if auto_mode:
            # In auto mode, default to live channel
            channel = TELEGRAM_LIVE_CHANNEL
            print(f"\n📡 Auto mode: Using LIVE channel: {channel}")
        else:
            # Manual mode - ask for channel selection
            print(f"\n📡 Channel selection:")
            print(f"   1. LIVE channel ({TELEGRAM_LIVE_CHANNEL}) - where your messages are")
            print(f"   2. TEST channel ({TELEGRAM_TEST_CHANNEL}) - for testing")

            choice = input("\nSelect channel (1/2) [default: 1]: ").strip() or "1"

            if choice == "1":
                channel = TELEGRAM_LIVE_CHANNEL
                print(f"\n✅ Using LIVE channel: {channel}")
                if not test_mode:
                    confirm = input("⚠️  This will update LIVE messages. Continue? (yes/no): ")
                    if confirm.lower() not in ['yes', 'y']:
                        print("❌ Cancelled")
                        return
            elif choice == "2":
                channel = TELEGRAM_TEST_CHANNEL
                print(f"\n✅ Using TEST channel: {channel}")
                print("⚠️  Note: Messages might not exist in test channel")
            else:
                print("❌ Invalid choice")
                return

    # Run sync
    await sync_events(channel, test_mode)


if __name__ == "__main__":
    asyncio.run(main())