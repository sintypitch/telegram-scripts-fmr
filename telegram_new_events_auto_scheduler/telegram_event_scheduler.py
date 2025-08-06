"""
═══════════════════════════════════════════════════════════════════════════════
                        TELEGRAM EVENT SCHEDULER
                    Automated Event Publishing System
═══════════════════════════════════════════════════════════════════════════════

WHAT THIS SCRIPT DOES:
----------------------
This script automates the process of publishing events from Notion to Telegram:

1. FETCHES events from your Notion database that are tagged with 'readyfortg'
2. SCHEDULES them as silent Telegram posts distributed across time windows
3. UPDATES Notion to mark events as published and prevent duplicates

KEY FEATURES:
-------------
• Smart Scheduling: Distributes events across 3 daily time windows
  - PRIMARY (18:00-20:00): Priority events get prime time
  - SECONDARY (11:00-13:00): Additional scheduling window
  - OVERFLOW (16:00-16:30): Catch-up window for remaining events

• Priority System: Events with priority=1 get scheduled first in prime slots

• Batch Processing: Schedules up to 5 events at the same random time within
  each window to appear natural and avoid spam detection

• Silent Delivery: All messages sent with silent=True (no notification sounds)

• Duplicate Prevention: Checks Notion's 'published_on_telegram' field to avoid
  re-posting events that have already been scheduled

• Multi-day Events: Properly formats date ranges (e.g., "6-8 SEP")

• Safety Features:
  - Requires confirmation before posting to live channels
  - Shows preview of what will be scheduled
  - Updates Notion tags from 'readyfortg' to 'postedontg'

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
- DEFAULT_CHANNEL: 'test' or 'live'
- TIMEZONE: Default 'Europe/Brussels'
- BATCH_SIZE: Events per time window (default 5)
- MAX_SCHEDULED: Maximum total scheduled posts (default 1000)
- PRIMARY_WINDOW_START/END: Prime time window (default 18:00-20:00)
- SECONDARY_WINDOW_START/END: Secondary window (default 11:00-13:00)
- OVERFLOW_WINDOW_START/END: Overflow window (default 16:00-16:30)

USAGE:
------
Run the script:
    python telegram_event_scheduler.py

The script will:
1. Show how many events are ready to schedule
2. Ask which channel to use (test/live)
3. Require confirmation for live channel
4. Schedule all events across multiple days
5. Report success/failure for each event
6. Update Notion to mark events as published

═══════════════════════════════════════════════════════════════════════════════
"""

import asyncio
import random
import os
import requests
import io
from datetime import datetime, timedelta, time
from zoneinfo import ZoneInfo
from telethon import TelegramClient
from notion_client import Client
from dotenv import load_dotenv

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
        "  TELEGRAM_API_ID=27679914\n"
        "  TELEGRAM_API_HASH=15f69f65a61d63b292b132eb530dd56f\n"
    )

api_id = int(TELEGRAM_API_ID)
api_hash = TELEGRAM_API_HASH

# Channel Configuration
TELEGRAM_LIVE_CHANNEL = os.getenv('TELEGRAM_LIVE_CHANNEL', 'raveinbelgium')
TELEGRAM_TEST_CHANNEL = os.getenv('TELEGRAM_TEST_CHANNEL', 'testchannel1234123434')
DEFAULT_CHANNEL = os.getenv('DEFAULT_CHANNEL', 'test')

# Scheduler Configuration
TIMEZONE = ZoneInfo(os.getenv('TIMEZONE', 'Europe/Brussels'))
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '5'))
MAX_SCHEDULED = int(os.getenv('MAX_SCHEDULED', '1000'))

# Parse window times from env or use defaults
def parse_time(time_str, default):
    if time_str:
        h, m = map(int, time_str.split(':'))
        return time(h, m)
    return default

PRIMARY_START = parse_time(os.getenv('PRIMARY_WINDOW_START'), time(18, 0))
PRIMARY_END = parse_time(os.getenv('PRIMARY_WINDOW_END'), time(20, 0))
SECONDARY_START = parse_time(os.getenv('SECONDARY_WINDOW_START'), time(11, 0))
SECONDARY_END = parse_time(os.getenv('SECONDARY_WINDOW_END'), time(13, 0))
OVERFLOW_START = parse_time(os.getenv('OVERFLOW_WINDOW_START'), time(16, 0))
OVERFLOW_END = parse_time(os.getenv('OVERFLOW_WINDOW_END'), time(16, 30))

# Initialize Notion client
notion = Client(auth=NOTION_TOKEN)


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
            until_date = p.get("until_date", {}).get("date", {}).get("start")

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


async def schedule_single_event(client: TelegramClient, event: dict, scheduled_time: datetime, channel: str) -> bool:
    """Schedule a single event to be sent at the specified time"""
    try:
        # Build message text
        msg_text = build_message_text(event)

        # Download image
        img_response = requests.get(event['image_url'], timeout=15)
        if img_response.status_code != 200:
            print(f"❌ Failed to download image for '{event['title']}'")
            return False

        # Prepare image for sending
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

        # Update Notion
        update_notion_after_scheduling(event["id"], scheduled_time, event["tags"])

        print(f"   ✅ {event['title']}")
        return True

    except Exception as e:
        print(f"   ❌ Failed: {event['title']} - {e}")
        return False


async def schedule_all_events(channel: str):
    """Main scheduling logic - distributes events across time windows"""
    print("\n📊 FETCHING EVENTS FROM NOTION")
    print("=" * 50)

    events = fetch_ready_events()
    if not events:
        print("❌ No events found with 'readyfortg' tag.")
        return

    # Check scheduling limit
    scheduled_count = count_scheduled_events()
    if scheduled_count >= MAX_SCHEDULED:
        print(f"⚠️  Scheduled limit reached ({MAX_SCHEDULED}).")
        return

    print(f"✅ Found {len(events)} events to schedule")
    print(f"   Already scheduled: {scheduled_count}")
    print(f"   Available slots: {MAX_SCHEDULED - scheduled_count}")

    # Separate priority vs regular events
    priority_events = [e for e in events if e["priority"] == 1]
    regular_events = [e for e in events if e["priority"] != 1]

    print(f"   Priority events: {len(priority_events)}")
    print(f"   Regular events: {len(regular_events)}")

    print("\n📅 SCHEDULING TO TELEGRAM")
    print("=" * 50)
    print(f"📡 Channel: {channel}")
    print(f"🕐 Timezone: {TIMEZONE}")
    print(f"📦 Batch size: {BATCH_SIZE}")
    print(f"🔇 Silent mode: ENABLED")

    idx_priority = 0
    idx_regular = 0
    day_offset = 0
    total_scheduled = 0

    async with TelegramClient('scheduler_session', api_id, api_hash) as client:
        while (idx_priority < len(priority_events) or idx_regular < len(regular_events)) and scheduled_count < MAX_SCHEDULED:
            current_day = datetime.now(TIMEZONE).date() + timedelta(days=day_offset)

            # Define windows for this day
            daily_windows = [
                ("PRIMARY", PRIMARY_START, PRIMARY_END),
                ("SECONDARY", SECONDARY_START, SECONDARY_END),
                ("OVERFLOW", OVERFLOW_START, OVERFLOW_END)
            ]

            for window_name, window_start, window_end in daily_windows:
                window_start_dt = datetime.combine(current_day, window_start, TIMEZONE)
                window_end_dt = datetime.combine(current_day, window_end, TIMEZONE)

                # Skip past windows
                if datetime.now(TIMEZONE) >= window_end_dt:
                    continue

                batch = []

                # PRIMARY window prioritizes priority events
                if window_name == "PRIMARY":
                    remaining_priority = len(priority_events) - idx_priority

                    # Skip only if no events left at all
                    if remaining_priority == 0 and idx_regular >= len(regular_events):
                        continue

                    # Fill with priority events first
                    num_priority = min(BATCH_SIZE, remaining_priority)
                    batch.extend(priority_events[idx_priority:idx_priority + num_priority])
                    idx_priority += num_priority

                    # Fill remaining slots with regular events
                    if len(batch) < BATCH_SIZE and idx_regular < len(regular_events):
                        num_regular = BATCH_SIZE - len(batch)
                        batch.extend(regular_events[idx_regular:idx_regular + num_regular])
                        idx_regular += num_regular

                else:  # SECONDARY & OVERFLOW - chronological order
                    while len(batch) < BATCH_SIZE:
                        if idx_priority < len(priority_events):
                            batch.append(priority_events[idx_priority])
                            idx_priority += 1
                        elif idx_regular < len(regular_events):
                            batch.append(regular_events[idx_regular])
                            idx_regular += 1
                        else:
                            break

                if not batch:
                    continue

                # Check scheduling limit
                if scheduled_count + len(batch) > MAX_SCHEDULED:
                    print(f"\n⚠️  Would exceed schedule limit. Stopping.")
                    return

                # Schedule at random time within window
                random_seconds = random.randint(0, int((window_end_dt - window_start_dt).total_seconds()))
                scheduled_time = window_start_dt + timedelta(seconds=random_seconds)

                print(f"\n📍 {window_name} window on {current_day.strftime('%Y-%m-%d')} at {scheduled_time.strftime('%H:%M')}")

                # Schedule each event in the batch
                for event in batch:
                    success = await schedule_single_event(client, event, scheduled_time, channel)
                    if success:
                        total_scheduled += 1

                scheduled_count += len(batch)

            day_offset += 1  # Move to next day

    print(f"\n✅ COMPLETE! Scheduled {total_scheduled} events")


# ═══════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════════

async def main():
    """Main entry point for the scheduler"""
    print("=" * 50)
    print("      TELEGRAM EVENT SCHEDULER")
    print("=" * 50)

    # Determine which channel to use
    if DEFAULT_CHANNEL == 'test':
        default_channel = TELEGRAM_TEST_CHANNEL
        default_name = "TEST"
    else:
        default_channel = TELEGRAM_LIVE_CHANNEL
        default_name = "LIVE"

    print(f"\n📡 Available channels:")
    print(f"   1. Test ({TELEGRAM_TEST_CHANNEL})")
    print(f"   2. Live ({TELEGRAM_LIVE_CHANNEL})")
    print(f"   3. Use default from .env ({default_name}: {default_channel})")

    choice = input("\nSelect channel (1/2/3): ").strip()

    if choice == "1":
        selected_channel = TELEGRAM_TEST_CHANNEL
        print(f"\n📌 Using TEST channel: {TELEGRAM_TEST_CHANNEL}")
    elif choice == "2":
        selected_channel = TELEGRAM_LIVE_CHANNEL
        confirm = input(f"\n⚠️  LIVE channel selected ({TELEGRAM_LIVE_CHANNEL}). Are you sure? (yes/no): ").strip().lower()
        if confirm not in ['yes', 'y']:
            print("❌ Cancelled.")
            return
        print(f"\n📌 Using LIVE channel: {TELEGRAM_LIVE_CHANNEL}")
    elif choice == "3":
        selected_channel = default_channel
        if default_name == "LIVE":
            confirm = input(f"\n⚠️  Default is LIVE channel ({default_channel}). Continue? (yes/no): ").strip().lower()
            if confirm not in ['yes', 'y']:
                print("❌ Cancelled.")
                return
        print(f"\n📌 Using {default_name} channel: {default_channel}")
    else:
        print("❌ Invalid choice.")
        return

    # Start scheduling
    await schedule_all_events(selected_channel)


if __name__ == "__main__":
    asyncio.run(main())