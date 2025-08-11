"""
═══════════════════════════════════════════════════════════════════════════════
                        TELEGRAM EVENT SCHEDULER
                    Automated Event Publishing System
═══════════════════════════════════════════════════════════════════════════════

WHAT THIS SCRIPT DOES:
----------------------
This script automates the process of publishing events from Notion to Telegram:

1. FETCHES events from your Notion database that are tagged with 'readyfortg'
2. SCHEDULES them with human-like posting patterns
3. POSTS to both test and live channels when using --live
4. UPDATES Notion to mark events as published and prevent duplicates

KEY FEATURES:
-------------
• Human-Like Posting: Mimics manual posting with bursts, gaps, and natural timing
  - Sometimes posts 2-3 events quickly (burst)
  - Sometimes spaces them out steadily
  - Sometimes includes longer gaps
  - Adds random variance to avoid predictable patterns

• Smart Urgency System:
  - TODAY events: Posted ASAP with 1-2 minute intervals
  - THIS WEEK events: Posted with 2-5 minute intervals
  - FUTURE events: Posted with natural human patterns

• Optimal Time Windows:
  - Weekdays: Morning (8:30-9:30), Lunch (12:30-13:30), Evening (19:00-21:00)
  - Friday: Morning (9:00-11:00), Afternoon (14:00-16:00), Prime (18:00-21:00)
  - Weekend: Different patterns for Saturday and Sunday

• Command Line Options:
  - --auto: Skip all confirmations for automation
  - --single: Select a single event to post
  - --dry-run: Preview scheduling without posting
  - --live: Use live channel (also posts to test)
  - --test: Use test channel only
  - --continue: Continue scheduling in same window from last run
  - --reset-state: Clear the scheduling state and start fresh

• Dual Channel Posting: When using live channel, also posts to test channel

• State Tracking (JSON):
  - Tracks scheduling sessions in telegram_schedule_state.json
  - Groups events naturally in the same time window
  - Use --continue to append new events to existing schedule
  - Prevents scattering when marking events ready one by one

• Safety Features:
  - Only updates Notion after successful upload
  - Dry run mode for testing
  - Shows preview of scheduling pattern

REQUIREMENTS:
-------------
The script needs these fields in your Notion database:
- title: Event name
- event_date: Date of the event
- until_date: (Optional) End date for multi-day events
- event_location: Venue name
- start_time: When the event starts
- raw_lineup: Artists/DJs performing
- socials_img_url: Event poster/flyer URL
- event_url: Ticket purchase link
- facebook_event_url: Facebook event link
- ticketswap_url: Ticketswap resale link
- data_tags: Must include 'readyfortg' to be processed
- priority: Number field (1 = high priority)
- published_on_telegram: Checkbox to track published events
- telegram_scheduled_at: Date field to store scheduling time

ENVIRONMENT VARIABLES (.env):
------------------------------
Required:
- NOTION_TOKEN: Your Notion integration token
- NOTION_MASTER_DB_ID: Your Notion database ID
- TELEGRAM_API_ID: Your Telegram API ID
- TELEGRAM_API_HASH: Your Telegram API hash

Optional:
- TELEGRAM_LIVE_CHANNEL: Production channel name
- TELEGRAM_TEST_CHANNEL: Test channel for dry runs
- TIMEZONE: Default 'Europe/Brussels'

USAGE:
------
Run the script:
    python telegram_event_scheduler.py [options]

Options:
    --auto      Skip all confirmations
    --single    Post single event selection
    --dry-run   Preview without posting
    --live      Use live channel (also posts to test)
    --test      Use test channel only

Examples:
    python telegram_event_scheduler.py --live --auto
    python telegram_event_scheduler.py --single
    python telegram_event_scheduler.py --dry-run
    python telegram_event_scheduler.py --continue --auto
    python telegram_event_scheduler.py --reset-state

═══════════════════════════════════════════════════════════════════════════════
"""

import asyncio
import random
import os
import requests
import io
import argparse
import json
from pathlib import Path
from datetime import datetime, timedelta, time
from zoneinfo import ZoneInfo
from telethon import TelegramClient
from notion_client import Client
from dotenv import load_dotenv
from typing import List, Tuple, Dict, Optional

# Load environment variables
load_dotenv()

# ═══════════════════════════════════════════════════════════════
# CONFIGURATION FROM ENVIRONMENT
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
    raise ValueError(
        "\n❌ Telegram credentials not found!\n"
        "Please add to your .env file:\n"
        "  TELEGRAM_API_ID=your_api_id\n"
        "  TELEGRAM_API_HASH=your_api_hash\n"
    )

api_id = int(TELEGRAM_API_ID)
api_hash = TELEGRAM_API_HASH

# Channel Configuration
TELEGRAM_LIVE_CHANNEL = os.getenv('TELEGRAM_LIVE_CHANNEL', 'raveinbelgium')
TELEGRAM_TEST_CHANNEL = os.getenv('TELEGRAM_TEST_CHANNEL', 'testchannel1234123434')

# Scheduler Configuration
TIMEZONE = ZoneInfo(os.getenv('TIMEZONE', 'Europe/Brussels'))
MAX_SCHEDULED = int(os.getenv('MAX_SCHEDULED', '1000'))

# Human-like posting patterns
MIN_SPACING_SECONDS = int(os.getenv('MIN_SPACING_SECONDS', '60'))
MAX_SPACING_MINUTES = int(os.getenv('MAX_SPACING_MINUTES', '15'))
BURST_CHANCE = float(os.getenv('BURST_CHANCE', '0.3'))
BURST_SIZE_MIN = int(os.getenv('BURST_SIZE_MIN', '2'))
BURST_SIZE_MAX = int(os.getenv('BURST_SIZE_MAX', '3'))
BURST_SPACING_MAX = int(os.getenv('BURST_SPACING_MAX', '180'))

# Urgency thresholds
URGENT_TODAY_HOURS = int(os.getenv('URGENT_TODAY_HOURS', '24'))
URGENT_WEEK_DAYS = int(os.getenv('URGENT_WEEK_DAYS', '7'))

# Initialize Notion client
notion = Client(auth=NOTION_TOKEN)

# Get script directory for storing files
SCRIPT_DIR = Path(__file__).parent

# Scheduling state file (in script directory)
SCHEDULE_STATE_FILE = SCRIPT_DIR / "telegram_schedule_state.json"


# ═══════════════════════════════════════════════════════════════
# SCHEDULING STATE MANAGEMENT
# ═══════════════════════════════════════════════════════════════

def load_schedule_state() -> dict:
    """Load the current scheduling state from JSON file"""
    if SCHEDULE_STATE_FILE.exists():
        try:
            with open(SCHEDULE_STATE_FILE, 'r') as f:
                return json.load(f)
        except json.JSONDecodeError:
            print("⚠️  Schedule state file corrupted, starting fresh")
    
    # Default state structure
    return {
        "last_updated": None,
        "current_window": None,
        "current_day": None,
        "last_scheduled_time": None,
        "events_in_window": [],
        "daily_count": 0,
        "total_scheduled_today": 0
    }


def save_schedule_state(state: dict):
    """Save the scheduling state to JSON file"""
    state["last_updated"] = datetime.now(TIMEZONE).isoformat()
    with open(SCHEDULE_STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2, default=str)


def get_current_window() -> Optional[Tuple[str, datetime, datetime]]:
    """Get the current time window we're in, if any"""
    now = datetime.now(TIMEZONE)
    windows = get_optimal_windows(now.weekday())
    
    for window_name, start_time, end_time in windows:
        window_start = datetime.combine(now.date(), start_time, TIMEZONE)
        window_end = datetime.combine(now.date(), end_time, TIMEZONE)
        
        # Check if we're currently in this window (with 30 min buffer before)
        if window_start - timedelta(minutes=30) <= now <= window_end:
            return (window_name, window_start, window_end)
    
    return None


def should_continue_in_window(state: dict) -> bool:
    """Check if we should continue scheduling in the same window"""
    if not state["last_scheduled_time"]:
        return False
    
    last_time = datetime.fromisoformat(state["last_scheduled_time"])
    now = datetime.now(TIMEZONE)
    
    # If more than 2 hours passed, start fresh
    if (now - last_time).total_seconds() > 7200:
        return False
    
    # Check if we're still in the same window
    current = get_current_window()
    if not current:
        return False
    
    window_name, window_start, window_end = current
    
    # Check if the last scheduled time was in this window
    if window_start <= last_time <= window_end:
        return True
    
    return False


# ═══════════════════════════════════════════════════════════════
# TIME WINDOWS AND PATTERNS
# ═══════════════════════════════════════════════════════════════

def get_optimal_windows(day_of_week: int) -> List[Tuple[str, time, time]]:
    """Get optimal posting windows based on day of week (0=Monday)"""
    if day_of_week < 4:  # Monday-Thursday
        return [
            ("MORNING", time(8, 30), time(9, 30)),
            ("LUNCH", time(12, 30), time(13, 30)),
            ("EVENING", time(19, 0), time(21, 0))
        ]
    elif day_of_week == 4:  # Friday
        return [
            ("MORNING", time(9, 0), time(11, 0)),
            ("AFTERNOON", time(14, 0), time(16, 0)),
            ("PRIME", time(18, 0), time(21, 0))
        ]
    elif day_of_week == 5:  # Saturday
        return [
            ("MIDDAY", time(11, 0), time(13, 0)),
            ("EVENING", time(17, 0), time(19, 0))
        ]
    else:  # Sunday
        return [
            ("MIDDAY", time(12, 0), time(14, 0)),
            ("EVENING", time(18, 0), time(20, 0))
        ]


def calculate_urgency(event_date_str: str) -> str:
    """Calculate event urgency: 'today', 'this_week', or 'future'"""
    event_date = datetime.strptime(event_date_str, '%Y-%m-%d').date()
    today = datetime.now(TIMEZONE).date()
    days_until = (event_date - today).days
    
    if days_until <= 0:
        return 'today'
    elif days_until <= URGENT_WEEK_DAYS:
        return 'this_week'
    else:
        return 'future'


def generate_human_posting_times(events: List[Dict], window_start: datetime, window_end: datetime) -> List[Tuple[Dict, datetime]]:
    """Generate human-like posting times for events within a window"""
    if not events:
        return []
    
    result = []
    
    # Group events by urgency
    urgent_today = [e for e in events if calculate_urgency(e['date']) == 'today']
    urgent_week = [e for e in events if calculate_urgency(e['date']) == 'this_week']
    regular = [e for e in events if calculate_urgency(e['date']) == 'future']
    
    # Process urgent events first with minimal spacing
    current_time = window_start
    
    # Today's events: 1-2 minute intervals
    for event in urgent_today:
        if current_time >= window_end:
            break
        result.append((event, current_time))
        spacing = random.randint(60, 120)  # 1-2 minutes
        current_time += timedelta(seconds=spacing)
    
    # This week's events: 2-5 minute intervals
    for event in urgent_week:
        if current_time >= window_end:
            break
        result.append((event, current_time))
        spacing = random.randint(120, 300)  # 2-5 minutes
        current_time += timedelta(seconds=spacing)
    
    # Regular events: human-like patterns with bursts and gaps
    remaining = regular.copy()
    
    while remaining and current_time < window_end:
        # Decide on pattern
        if random.random() < BURST_CHANCE and len(remaining) >= BURST_SIZE_MIN:
            # Burst pattern: 2-3 posts close together
            burst_size = min(random.randint(BURST_SIZE_MIN, BURST_SIZE_MAX), len(remaining))
            for _ in range(burst_size):
                if not remaining or current_time >= window_end:
                    break
                event = remaining.pop(0)
                result.append((event, current_time))
                # Small spacing within burst (30-180 seconds)
                spacing = random.randint(30, BURST_SPACING_MAX)
                current_time += timedelta(seconds=spacing)
            
            # Add gap after burst (5-15 minutes)
            if remaining:
                gap = random.randint(300, MAX_SPACING_MINUTES * 60)
                current_time += timedelta(seconds=gap)
        else:
            # Regular spacing (3-10 minutes)
            if not remaining:
                break
            event = remaining.pop(0)
            
            # 20% chance to post on round time
            if random.random() < 0.2:
                # Round to nearest 5 minutes
                minutes = current_time.minute
                round_minutes = ((minutes + 2) // 5) * 5
                if round_minutes == 60:
                    current_time = current_time.replace(minute=0) + timedelta(hours=1)
                else:
                    current_time = current_time.replace(minute=round_minutes)
            
            result.append((event, current_time))
            
            # Add variance to spacing
            base_spacing = random.randint(180, 600)  # 3-10 minutes
            variance = random.randint(-60, 120)  # -1 to +2 minutes variance
            spacing = max(MIN_SPACING_SECONDS, base_spacing + variance)
            current_time += timedelta(seconds=spacing)
    
    return result


def calculate_daily_limit(total_events: int, has_urgent: bool) -> int:
    """Calculate how many events to post per day"""
    if has_urgent:
        return 10  # Higher limit for urgent events
    elif total_events < 10:
        return 2
    elif total_events < 25:
        return 3
    elif total_events < 50:
        return 5
    else:
        return 7


def should_skip_today(events_remaining: int, urgent_count: int) -> bool:
    """Determine if we should skip posting today for natural appearance"""
    if urgent_count > 0:
        return False  # Never skip if urgent events
    if events_remaining < 5:
        return False  # Don't skip for small batches
    
    # 20% chance to skip a day for natural appearance
    return random.random() < 0.2


# ═══════════════════════════════════════════════════════════════
# NOTION DATA FETCHING
# ═══════════════════════════════════════════════════════════════

def safe_get_url(prop: dict) -> str:
    """Safely extract URL from Notion property"""
    return prop.get("url") or ""


def fetch_ready_events() -> list[dict]:
    """Fetch all events tagged with 'readyfortg' from Notion"""
    events = []
    has_more = True
    cursor = None

    while has_more:
        # Query database with pagination
        if cursor:
            response = notion.databases.query(
                database_id=NOTION_MASTER_DB_ID,
                start_cursor=cursor,
                filter={"property": "data_tags", "multi_select": {"contains": "readyfortg"}}
            )
        else:
            response = notion.databases.query(
                database_id=NOTION_MASTER_DB_ID,
                filter={"property": "data_tags", "multi_select": {"contains": "readyfortg"}}
            )

        # Process each page
        for item in response["results"]:
            p = item["properties"]

            # Get event title
            title_items = p.get("title", {}).get("title", [])
            if not title_items:
                continue
            title = title_items[0]["plain_text"]

            # Skip if already published
            if p.get("published_on_telegram", {}).get("checkbox"):
                print(f"⚠️  Skipping '{title}' – already published")
                continue

            # Check for required fields
            if not p.get("event_date", {}).get("date"):
                print(f"⚠️  Skipping '{title}' – missing event date")
                continue

            location_items = p.get("event_location", {}).get("rich_text", [])
            if not location_items:
                print(f"⚠️  Skipping '{title}' – missing location")
                continue

            start_time_items = p.get("start_time", {}).get("rich_text", [])
            if not start_time_items:
                print(f"⚠️  Skipping '{title}' – missing start time")
                continue

            image_url = safe_get_url(p.get("socials_img_url", {}))
            if not image_url:
                print(f"⚠️  Skipping '{title}' – missing image URL")
                continue

            # Extract data
            priority = p.get("priority", {}).get("number") or 0
            lineup_rt = p.get("raw_lineup", {}).get("rich_text", [])
            lineup = lineup_rt[0]["plain_text"] if lineup_rt else "Lineup TBA"
            tags = [tag["name"] for tag in p.get("data_tags", {}).get("multi_select", [])]

            # Safely get until_date
            until_date_field = p.get("until_date")
            until_date = None
            if until_date_field and until_date_field.get("date"):
                until_date = until_date_field["date"].get("start")

            events.append({
                "id": item["id"],
                "title": title,
                "date": p["event_date"]["date"]["start"],
                "until_date": until_date,
                "start_time": start_time_items[0]["plain_text"],
                "location": location_items[0]["plain_text"],
                "lineup": lineup,
                "event_url": safe_get_url(p.get("event_url", {})),
                "fb_url": safe_get_url(p.get("facebook_event_url", {})),
                "swap_url": safe_get_url(p.get("ticketswap_url", {})),
                "image_url": image_url,
                "priority": priority,
                "tags": tags
            })

        has_more = response.get("has_more", False)
        cursor = response.get("next_cursor")

    # Sort by priority (1 = high priority) then by date
    return sorted(events, key=lambda e: (e["priority"] != 1, e["date"]))


def count_scheduled_events() -> int:
    """Count how many events are already scheduled in Notion"""
    response = notion.databases.query(
        database_id=NOTION_MASTER_DB_ID,
        filter={"property": "telegram_scheduled_at", "date": {"is_not_empty": True}}
    )
    return len(response["results"])


def update_notion_after_scheduling(event_id: str, scheduled_time: datetime, tags: list[str]):
    """Update Notion entry after successfully scheduling to Telegram"""
    # Update tags: remove readyfortg, add postedontg
    updated_tags = [
        {"name": "postedontg"} if tag.lower() == "readyfortg" else {"name": tag}
        for tag in tags
    ]

    notion.pages.update(
        page_id=event_id,
        properties={
            "telegram_scheduled_at": {"date": {"start": scheduled_time.isoformat()}},
            "published_on_telegram": {"checkbox": True},
            "data_tags": {"multi_select": updated_tags},
            "data_status": {"multi_select": [{"name": "processed"}]}
        }
    )


# ═══════════════════════════════════════════════════════════════
# TELEGRAM SCHEDULING
# ═══════════════════════════════════════════════════════════════

def format_event_date(date_str: str, until_str: str | None) -> str:
    """Format event date, handling multi-day events"""
    start_date = datetime.strptime(date_str, '%Y-%m-%d')

    if until_str:
        until_date = datetime.strptime(until_str, '%Y-%m-%d')
        if start_date.month == until_date.month:
            # Same month: "6-8 SEP"
            return f"{start_date.day}-{until_date.day} {start_date.strftime('%b').upper()}"
        else:
            # Different months: "30 SEP - 2 OCT"
            return f"{start_date.day} {start_date.strftime('%b').upper()} - {until_date.day} {until_date.strftime('%b').upper()}"
    else:
        # Single day: "6 SEP"
        return f"{start_date.day} {start_date.strftime('%b').upper()}"


def build_message_text(event: dict) -> str:
    """Build the Telegram message text with proper formatting"""
    formatted_date = format_event_date(event['date'], event.get('until_date'))

    # Build links section
    links = []
    if event.get('fb_url'):
        links.append(f"<a href='{event['fb_url']}'>Facebook</a>")
    if event.get('event_url'):
        links.append(f"<a href='{event['event_url']}'>Tickets</a>")
    if event.get('swap_url'):
        links.append(f"<a href='{event['swap_url']}'>Ticketswap</a>")

    links_text = " | ".join(links) if links else ""

    # Build the message
    msg_text = f"<b>{formatted_date} | {event['title']}</b>\n"
    msg_text += f"{event['location']} • Starts at {event['start_time']}\n\n"

    if event['lineup'] and event['lineup'] != "Lineup TBA":
        msg_text += f"Lineup: {event['lineup']}\n\n"

    if links_text:
        msg_text += links_text

    return msg_text


async def schedule_single_event(client: TelegramClient, event: dict, scheduled_time: datetime, channels: List[str], dry_run: bool = False) -> bool:
    """Schedule a single event to be sent at the specified time to multiple channels"""
    if dry_run:
        print(f"   🧪 [DRY RUN] Would schedule: {event['title']} at {scheduled_time.strftime('%H:%M')}")
        return True
    
    try:
        # Build message text
        msg_text = build_message_text(event)

        # Download image
        img_response = requests.get(event['image_url'], timeout=15)
        if img_response.status_code != 200:
            print(f"❌ Failed to download image for '{event['title']}'")
            return False

        # Track successful uploads
        successful_channels = []
        
        # Schedule to each channel
        for channel in channels:
            try:
                # Prepare image for sending (need fresh BytesIO for each send)
                img_bytes = io.BytesIO(img_response.content)
                img_bytes.name = "event.jpg"
                
                # Schedule the message (always silent)
                await client.send_file(
                    channel,
                    img_bytes,
                    caption=msg_text,
                    schedule=scheduled_time,
                    silent=True,  # Always send silently
                    parse_mode='html',
                    link_preview=False
                )
                successful_channels.append(channel)
            except Exception as e:
                print(f"      ⚠️ Failed to post to {channel}: {e}")
        
        # Only update Notion if at least one channel succeeded
        if successful_channels:
            update_notion_after_scheduling(event["id"], scheduled_time, event["tags"])
            print(f"   ✅ {event['title']} → {', '.join(successful_channels)}")
            return True
        else:
            print(f"   ❌ Failed to post to any channel: {event['title']}")
            return False

    except Exception as e:
        print(f"   ❌ Failed: {event['title']} - {e}")
        return False


async def schedule_all_events(channels: List[str], dry_run: bool = False, single_mode: bool = False, continue_mode: bool = False):
    """Main scheduling logic with human-like posting patterns"""
    print("\n📊 FETCHING EVENTS FROM NOTION")
    print("=" * 50)

    events = fetch_ready_events()
    if not events:
        print("❌ No events found with 'readyfortg' tag.")
        return
    
    # Single event mode
    if single_mode:
        print(f"\n🎯 SINGLE EVENT MODE")
        print("-" * 50)
        for i, event in enumerate(events[:20], 1):  # Show max 20
            date_str = format_event_date(event['date'], event.get('until_date'))
            urgency = calculate_urgency(event['date'])
            urgency_emoji = "🔥" if urgency == 'today' else "⚡" if urgency == 'this_week' else "📅"
            print(f"{i:2}. {urgency_emoji} {date_str} | {event['title'][:40]}")
        
        try:
            choice = int(input("\nSelect event number (0 to cancel): "))
            if choice == 0:
                print("❌ Cancelled")
                return
            if choice < 1 or choice > len(events):
                print("❌ Invalid selection")
                return
            
            events = [events[choice - 1]]  # Continue with just this event
            print(f"\n✅ Selected: {events[0]['title']}")
        except (ValueError, KeyError):
            print("❌ Invalid input")
            return

    # Check scheduling limit
    scheduled_count = count_scheduled_events()
    if scheduled_count >= MAX_SCHEDULED:
        print(f"⚠️  Scheduled limit reached ({MAX_SCHEDULED}).")
        return

    print(f"\n✅ Found {len(events)} events to schedule")
    print(f"   Already scheduled: {scheduled_count}")
    
    # Count urgency levels
    urgent_today = [e for e in events if calculate_urgency(e['date']) == 'today']
    urgent_week = [e for e in events if calculate_urgency(e['date']) == 'this_week']
    regular = [e for e in events if calculate_urgency(e['date']) == 'future']
    
    if urgent_today:
        print(f"   🔥 TODAY: {len(urgent_today)} events")
    if urgent_week:
        print(f"   ⚡ THIS WEEK: {len(urgent_week)} events")
    if regular:
        print(f"   📅 FUTURE: {len(regular)} events")

    print("\n📅 SCHEDULING TO TELEGRAM")
    print("=" * 50)
    print(f"📡 Channels: {', '.join(channels)}")
    print(f"🕐 Timezone: {TIMEZONE}")
    print(f"🤖 Mode: {'DRY RUN' if dry_run else 'LIVE'}")
    print(f"🔇 Silent mode: ENABLED")

    # Load scheduling state
    state = load_schedule_state()
    
    # Check if we should continue from previous session
    if continue_mode and should_continue_in_window(state):
        print(f"\n📂 CONTINUING FROM PREVIOUS SESSION")
        print(f"   Last scheduled: {state['last_scheduled_time']}")
        print(f"   Events in window: {len(state['events_in_window'])}")
        print(f"   Continuing in same window...")
    else:
        # Reset state for new session
        state = {
            "last_updated": None,
            "current_window": None,
            "current_day": str(datetime.now(TIMEZONE).date()),
            "last_scheduled_time": None,
            "events_in_window": [],
            "daily_count": 0,
            "total_scheduled_today": 0
        }
        if continue_mode:
            print("\n📂 Starting fresh session (too much time passed or different window)")

    total_scheduled = 0
    day_offset = 0
    events_remaining = events.copy()
    
    async with TelegramClient(str(SCRIPT_DIR / 'scheduler_session'), api_id, api_hash) as client:
        await client.start()
        
        while events_remaining and scheduled_count < MAX_SCHEDULED:
            current_day = datetime.now(TIMEZONE).date() + timedelta(days=day_offset)
            
            # Check if we should skip today
            urgent_count = len([e for e in events_remaining if calculate_urgency(e['date']) in ['today', 'this_week']])
            if should_skip_today(len(events_remaining), urgent_count) and day_offset > 0:
                print(f"\n⏭️ Skipping {current_day.strftime('%A, %Y-%m-%d')} for natural appearance")
                day_offset += 1
                continue
            
            # Get optimal windows for this day
            windows = get_optimal_windows(current_day.weekday())
            
            # Calculate daily limit
            has_urgent = urgent_count > 0
            daily_limit = calculate_daily_limit(len(events_remaining), has_urgent)
            daily_posted = 0
            
            print(f"\n📆 {current_day.strftime('%A, %Y-%m-%d')}")
            print(f"   Daily limit: {daily_limit} events")
            
            for window_name, window_start, window_end in windows:
                if not events_remaining or daily_posted >= daily_limit:
                    break
                    
                window_start_dt = datetime.combine(current_day, window_start, TIMEZONE)
                window_end_dt = datetime.combine(current_day, window_end, TIMEZONE)
                
                # Skip past windows
                if datetime.now(TIMEZONE) >= window_end_dt:
                    continue
                
                # Ensure we start at least 2 minutes in the future
                if window_start_dt < datetime.now(TIMEZONE) + timedelta(minutes=2):
                    window_start_dt = datetime.now(TIMEZONE) + timedelta(minutes=2)
                
                # Take events for this window
                window_events = events_remaining[:min(daily_limit - daily_posted, len(events_remaining))]
                if not window_events:
                    continue
                
                # Check if we're continuing in the same window
                if continue_mode and state["current_window"] == window_name and state["last_scheduled_time"]:
                    # Continue from last scheduled time
                    last_time = datetime.fromisoformat(state["last_scheduled_time"])
                    if last_time > window_start_dt:
                        window_start_dt = last_time + timedelta(minutes=random.randint(2, 5))
                
                # Generate human-like posting times
                scheduled_posts = generate_human_posting_times(window_events, window_start_dt, window_end_dt)
                
                if scheduled_posts:
                    print(f"\n   📍 {window_name} window ({window_start.strftime('%H:%M')}-{window_end.strftime('%H:%M')})")
                    
                    # Update state for this window
                    state["current_window"] = window_name
                    state["current_day"] = str(current_day)
                    
                    for event, scheduled_time in scheduled_posts:
                        if scheduled_count >= MAX_SCHEDULED:
                            print(f"\n⚠️  Reached schedule limit ({MAX_SCHEDULED})")
                            save_schedule_state(state)
                            return
                        
                        success = await schedule_single_event(client, event, scheduled_time, channels, dry_run)
                        if success:
                            total_scheduled += 1
                            scheduled_count += 1
                            daily_posted += 1
                            events_remaining.remove(event)
                            
                            # Update state tracking
                            state["last_scheduled_time"] = scheduled_time.isoformat()
                            state["events_in_window"].append({
                                "event_id": event["id"],
                                "title": event["title"],
                                "scheduled_at": scheduled_time.isoformat()
                            })
                            state["daily_count"] = daily_posted
                            state["total_scheduled_today"] += 1
                            
                            # Save state after each successful scheduling
                            if not dry_run:
                                save_schedule_state(state)
                        
                        # Small delay between scheduling operations
                        if not dry_run:
                            await asyncio.sleep(0.5)
            
            day_offset += 1
            
            # Don't schedule more than 7 days ahead
            if day_offset > 7:
                print("\n⚠️  Reached 7-day scheduling limit")
                break
    
    print(f"\n✅ COMPLETE! Scheduled {total_scheduled} events")
    if events_remaining:
        print(f"   ⚠️  {len(events_remaining)} events not scheduled (limits reached)")


# ═══════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════════

async def main():
    """Main entry point with argument parsing"""
    parser = argparse.ArgumentParser(
        description="Telegram Event Scheduler - Post events with human-like patterns",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python telegram_event_scheduler.py --live --auto
  python telegram_event_scheduler.py --single
  python telegram_event_scheduler.py --dry-run
  python telegram_event_scheduler.py --test
        """
    )
    
    parser.add_argument('--auto', action='store_true', help='Skip all confirmations')
    parser.add_argument('--single', action='store_true', help='Select single event to post')
    parser.add_argument('--dry-run', action='store_true', help='Preview without posting')
    parser.add_argument('--continue', dest='continue_mode', action='store_true', 
                       help='Continue scheduling in the same window as previous run')
    parser.add_argument('--reset-state', action='store_true', 
                       help='Reset the scheduling state and start fresh')
    
    channel_group = parser.add_mutually_exclusive_group()
    channel_group.add_argument('--live', action='store_true', help='Use live channel (also posts to test)')
    channel_group.add_argument('--test', action='store_true', help='Use test channel only')
    
    args = parser.parse_args()
    
    print("=" * 70)
    print("      🤖 TELEGRAM EVENT SCHEDULER")
    print("      Human-Like Posting Patterns")
    print("=" * 70)
    
    # Handle state reset
    if args.reset_state:
        if SCHEDULE_STATE_FILE.exists():
            SCHEDULE_STATE_FILE.unlink()
            print("\n✅ Scheduling state has been reset")
        else:
            print("\n📭 No state file to reset")
        return
    
    # Determine channels
    if args.live:
        channels = [TELEGRAM_LIVE_CHANNEL, TELEGRAM_TEST_CHANNEL]
        print(f"\n📡 Using LIVE + TEST channels")
        
        if not args.auto and not args.dry_run:
            confirm = input(f"⚠️  Will post to LIVE channel ({TELEGRAM_LIVE_CHANNEL}). Continue? (yes/no): ").strip().lower()
            if confirm not in ['yes', 'y']:
                print("❌ Cancelled")
                return
    elif args.test:
        channels = [TELEGRAM_TEST_CHANNEL]
        print(f"\n📝 Using TEST channel only: {TELEGRAM_TEST_CHANNEL}")
    else:
        # Interactive mode
        print(f"\n📡 Available channels:")
        print(f"   1. Test only ({TELEGRAM_TEST_CHANNEL})")
        print(f"   2. Live + Test ({TELEGRAM_LIVE_CHANNEL} + {TELEGRAM_TEST_CHANNEL})")
        
        if not args.auto:
            choice = input("\nSelect channel option (1/2): ").strip()
            
            if choice == "2":
                channels = [TELEGRAM_LIVE_CHANNEL, TELEGRAM_TEST_CHANNEL]
                if not args.dry_run:
                    confirm = input(f"\n⚠️  Will post to LIVE channel. Continue? (yes/no): ").strip().lower()
                    if confirm not in ['yes', 'y']:
                        print("❌ Cancelled")
                        return
            else:
                channels = [TELEGRAM_TEST_CHANNEL]
        else:
            # Default to test in auto mode if not specified
            channels = [TELEGRAM_TEST_CHANNEL]
            print("   Using test channel (default for auto mode)")
    
    # Run scheduler
    await schedule_all_events(channels, args.dry_run, args.single, args.continue_mode)


if __name__ == "__main__":
    asyncio.run(main())