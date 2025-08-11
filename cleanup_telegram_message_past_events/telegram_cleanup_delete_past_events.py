"""
═══════════════════════════════════════════════════════════════════════════════
                      TELEGRAM CHANNEL CLEANUP TOOL
                    Past Event Removal & Channel Manager
═══════════════════════════════════════════════════════════════════════════════

WHAT THIS SCRIPT DOES:
----------------------
This script helps maintain clean Telegram channels by identifying and removing
past events, keeping your channel focused on upcoming events only.

1. SCANS your entire Telegram channel for event posts
2. IDENTIFIES event dates from the message format
3. CATEGORIZES events as Past or Future based on current date
4. DISPLAYS a summary of all events found
5. DELETES past events (with confirmation) to keep channel fresh

KEY FEATURES:
-------------
• Smart Date Extraction: Automatically parses event dates from messages
  - Handles formats like "6 SEP", "06 SEP", "6 SEPTEMBER"
  - Supports international month names (with accents)
  - Correctly handles year transitions (Dec → Jan)

• Intelligent Year Detection:
  - If event date has passed this year, assumes next year
  - Example: In November, "15 JAN" is recognized as next year

• Weekly Summary Detection: Identifies and deletes weekly summary posts when
  their entire date range has passed (e.g., "04 AUG - 10 AUG")
  - Uses the END date of the range to determine if it's past
  - Only deletes when the ENTIRE week is over
  - Example: If today is Aug 18, deletes "11 AUG - 17 AUG" summary

• Daily Summary Detection: Identifies and deletes daily event summaries
  - Detects by Telegram message URLs (t.me/) in the message
  - Recognizes patterns like "today", "tonight", "happening now"
  - Uses the POST DATE as the event date (deleted next day)
  - Cleans up orphaned summaries even if linked events are gone

• Safe Deletion Process:
  - Shows all past events before deletion
  - Requires explicit confirmation
  - Test channel available for practice runs
  - No accidental deletions

• Comprehensive Event Display:
  - Shows event date, title, and Telegram URL
  - Sorted chronologically (future ascending, past descending)
  - Clean tabular format using pandas

MESSAGE FORMATS SUPPORTED:
--------------------------
1. Single Events:
    6 SEP | Event Title
    Venue Name • Starts at 23:00
    ...rest of message...

2. Weekly Summaries:
    📅 Here's what's going on this week:
    (04 AUG - 10 AUG)
    • FR: Event 1
    • SA: Event 2
    ...list of events...

3. Daily Summaries:
    ✨ Today's event is ready for you:

    • **Club Vaag invites KNTRVRLST**
       Facebook | Tickets | Ticketswap

The script will:
- For single events: Use the event date
- For weekly summaries: Use the END date of the range
- For daily summaries: Use the POST date (deleted next day)
- Only delete when dates are completely in the past

ENVIRONMENT VARIABLES (.env):
------------------------------
Required:
- TELEGRAM_API_ID: Your Telegram API ID
- TELEGRAM_API_HASH: Your Telegram API hash

Optional:
- TELEGRAM_LIVE_CHANNEL: Production channel (default: 'raveinbelgium')
- TELEGRAM_TEST_CHANNEL: Test channel (default: 'testchannel1234123434')

USAGE:
------
Run the script:
    python telegram_cleanup_delete_past_events.py

Options:
    --live    Use live channel directly (skips channel selection)
    --test    Use test channel directly (skips channel selection)
    --dry-run Show what would be deleted without actually deleting

Examples:
    python telegram_cleanup_delete_past_events.py --live
    python telegram_cleanup_delete_past_events.py --test
    python telegram_cleanup_delete_past_events.py --live --dry-run

The script will:
1. Ask which channel to scan (test/live) - OR use --live/--test flag
2. When using --live flag: proceeds without confirmation
3. Scan all messages in the channel
4. Display future events (kept)
5. Display past events (to be deleted)
6. Ask for confirmation before deleting (unless --dry-run)
7. Delete past events if confirmed

═══════════════════════════════════════════════════════════════════════════════
"""

from telethon import TelegramClient
import pandas as pd
from dateutil.parser import parse
import re
from datetime import datetime
from dotenv import load_dotenv
import os
import sys
import argparse

# Load environment variables
load_dotenv()

# ═══════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════

# Telegram credentials
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

# Channel configuration
LIVE_CHANNEL = os.getenv('TELEGRAM_LIVE_CHANNEL', 'raveinbelgium')
TEST_CHANNEL = os.getenv('TELEGRAM_TEST_CHANNEL', 'testchannel1234123434')

# Initialize Telegram client
client = TelegramClient('cleanup_session', api_id, api_hash)


# ═══════════════════════════════════════════════════════════════
# DATE EXTRACTION FUNCTIONS
# ═══════════════════════════════════════════════════════════════

def is_weekly_summary(text: str) -> bool:
    """Check if message is a weekly summary"""
    if "📅" in text:
        weekly_patterns = [
            "Here's what's going on",
            "this week",
            "Good evening",
            "what's up this",
            "weekend",
            "Upcoming events"
        ]
        return any(pattern.lower() in text.lower() for pattern in weekly_patterns)

    # Check for date range pattern even without emoji
    if re.search(r'\(\d{1,2}\s+[A-Z]{3}\s*-\s*\d{1,2}\s+[A-Z]{3}\)', text):
        return "•" in text  # Has bullet points

    return False


def is_daily_summary(text: str, message_date) -> bool:
    """Check if message is a daily summary"""
    # Count Telegram URLs
    telegram_url_count = text.count("t.me/")

    # Check message characteristics
    is_short = len(text.split('\n')) <= 20
    first_line = text.split('\n', 1)[0]
    has_date_in_first_line = bool(re.search(r'(\d{1,2})\s*([A-Za-zÀ-ÿ]{3,})', first_line))

    # Check for "today" references
    daily_indicators = [
        "today", "tonight", "this evening", "this afternoon",
        "happening now", "later today", "daily", "today's"
    ]
    has_today_reference = any(indicator in text.lower() for indicator in daily_indicators)

    # Check for script patterns
    script_patterns = ["• **", "   Facebook", "   Tickets", "   Ticketswap", "★ <a href="]
    has_script_pattern = any(pattern in text for pattern in script_patterns)

    # Check for daily emojis
    daily_emojis = ["✨", "🔥", "🎉", "🎊", "💫", "⭐"]
    has_daily_emoji = any(emoji in text for emoji in daily_emojis)

    # Daily summary detection logic
    if has_script_pattern and has_today_reference and telegram_url_count >= 1:
        return True
    elif has_daily_emoji and has_today_reference and telegram_url_count >= 1 and is_short:
        return True
    elif has_today_reference and telegram_url_count >= 1 and is_short and not has_date_in_first_line:
        return True
    elif telegram_url_count >= 2 and is_short and not has_date_in_first_line:
        return True

    return False


def extract_weekly_summary_date(text: str, post_date) -> datetime:
    """Extract end date from weekly summary"""
    range_match = re.search(r'\((\d{1,2})\s+([A-Z]{3})\s*-\s*(\d{1,2})\s+([A-Z]{3})\)', text)
    if not range_match:
        return None

    try:
        # Remove timezone info if present
        if hasattr(post_date, 'tzinfo') and post_date.tzinfo:
            post_date = post_date.replace(tzinfo=None)

        # Parse end date
        end_day = range_match.group(3)
        end_month = range_match.group(4)
        end_date = parse(f"{end_day} {end_month} {post_date.year}", dayfirst=True)

        # Parse start date for validation
        start_day = range_match.group(1)
        start_month = range_match.group(2)
        start_date = parse(f"{start_day} {start_month} {post_date.year}", dayfirst=True)

        # Check if dates are in the future relative to post
        days_diff = (start_date - post_date).days
        if days_diff < -7:  # More than a week in the past
            start_date = start_date.replace(year=post_date.year + 1)
            end_date = end_date.replace(year=post_date.year + 1)

        # Handle month transition
        if end_date < start_date:
            end_date = end_date.replace(year=start_date.year + 1)

        return end_date
    except:
        return None


def extract_regular_event_date(text: str, post_date) -> datetime:
    """Extract date from regular event post"""
    first_line = text.split('\n', 1)[0]
    match = re.search(r'(\d{1,2})\s*([A-Za-zÀ-ÿ]{3,})', first_line)

    if not match:
        return None

    try:
        # Remove timezone info if present
        if hasattr(post_date, 'tzinfo') and post_date.tzinfo:
            post_date = post_date.replace(tzinfo=None)

        day, month = match.groups()
        event_date = parse(f"{day} {month} {post_date.year}", dayfirst=True)

        # If event date is before post date, it's probably next year
        if (event_date.month, event_date.day) < (post_date.month, post_date.day):
            event_date = event_date.replace(year=post_date.year + 1)

        return event_date
    except:
        return None


def extract_event_date(text: str, post_date) -> datetime:
    """Main function to extract event date based on message type"""
    # Check weekly summary first (highest priority)
    if is_weekly_summary(text):
        date = extract_weekly_summary_date(text, post_date)
        if date:
            return date

    # Check daily summary
    if is_daily_summary(text, post_date):
        # Return post date without timezone
        if hasattr(post_date, 'tzinfo') and post_date.tzinfo:
            return post_date.replace(tzinfo=None)
        return post_date

    # Regular event
    return extract_regular_event_date(text, post_date)


def extract_event_title(text: str, is_weekly: bool, is_daily: bool, event_date, telegram_urls: int) -> str:
    """Extract appropriate title based on message type"""
    if is_weekly:
        range_match = re.search(r'\((\d{1,2}\s+[A-Z]{3}\s*-\s*\d{1,2}\s+[A-Z]{3})\)', text)
        if range_match:
            return f"📅 Weekly Summary: {range_match.group(1)}"
        return "📅 Weekly Summary"

    elif is_daily:
        date_str = event_date.strftime('%d %b').upper() if event_date else "Unknown"
        event_word = "event" if telegram_urls == 1 else "events"
        return f"📆 Daily Summary: {date_str} ({telegram_urls} {event_word})"

    else:
        # Regular event title
        first_line = text.split('\n', 1)[0].replace("*", "").strip()
        if '|' in first_line:
            _, title = [part.strip() for part in first_line.split('|', 1)]
            return title
        return first_line.strip()


# ═══════════════════════════════════════════════════════════════
# MAIN SCANNING FUNCTION
# ═══════════════════════════════════════════════════════════════

async def scan_and_clean_channel(channel_name: str, dry_run: bool = False, auto_confirm: bool = False):
    """Main function to scan channel and manage events"""
    await client.start()

    events = []
    today = datetime.now()

    print(f"\n📅 Current date: {today.strftime('%Y-%m-%d')} ({today.strftime('%A, %d %B %Y')})")
    print(f"📡 Scanning channel: {channel_name}")
    if dry_run:
        print("🧪 DRY RUN MODE - No messages will be deleted")
    print("⏳ This may take a moment for channels with many messages...\n")

    message_count = 0

    # Scan all messages
    async for message in client.iter_messages(channel_name):
        message_count += 1
        if message_count % 100 == 0:
            print(f"   Scanned {message_count} messages...")

        if not message.text:
            continue

        # Determine message type
        is_weekly = is_weekly_summary(message.text)
        is_daily = is_daily_summary(message.text, message.date)
        telegram_urls = message.text.count("t.me/")

        # Extract event date
        event_date = extract_event_date(message.text, message.date)

        if not event_date:
            continue

        # Extract title
        title = extract_event_title(message.text, is_weekly, is_daily, event_date, telegram_urls)

        # Create event record
        events.append({
            'event_date': event_date.strftime('%Y-%m-%d'),
            'title': title,
            'url': f"https://t.me/{channel_name}/{message.id}",
            'status': 'Past' if event_date.date() < today.date() else 'Future',
            'message_id': message.id,
            'is_weekly_summary': is_weekly,
            'is_daily_summary': is_daily,
            'is_summary': is_weekly or is_daily
        })

    if not events:
        print("📭 No events found in this channel.")
        return

    # Create DataFrame and sort
    df = pd.DataFrame(events)
    past_df = df[df['status'] == 'Past'].sort_values(by='event_date', ascending=False)
    future_df = df[df['status'] == 'Future'].sort_values(by='event_date', ascending=True)

    # Display summary
    print("=" * 70)
    print(f"📊 CHANNEL SUMMARY for @{channel_name}")
    print("=" * 70)
    print(f"Total messages scanned: {message_count}")
    print(f"Events found: {len(df)}")

    # Count by type
    regular_count = len(df[~df['is_summary']])
    weekly_count = len(df[df['is_weekly_summary']])
    daily_count = len(df[df['is_daily_summary']])

    print(f"  - Regular events: {regular_count}")
    print(f"  - Weekly summaries: {weekly_count}")
    print(f"  - Daily summaries: {daily_count}")
    print(f"Future events: {len(future_df)} (will be kept)")
    print(f"Past events: {len(past_df)} (can be deleted)")

    # Show breakdown of past events
    if len(past_df) > 0:
        past_weekly = len(past_df[past_df['is_weekly_summary']])
        past_daily = len(past_df[past_df['is_daily_summary']])
        if past_weekly > 0:
            print(f"  - Including {past_weekly} past weekly summaries")
        if past_daily > 0:
            print(f"  - Including {past_daily} past daily summaries")

    # Display future events
    print(f"\n🚀 FUTURE EVENTS (keeping these):")
    print("-" * 70)
    if not future_df.empty:
        print(future_df[['event_date', 'title', 'url']].to_string(index=False))
    else:
        print("No future events found.")

    # Display past events
    print(f"\n🗓️ PAST EVENTS (can be deleted):")
    print("-" * 70)
    if not past_df.empty:
        print(past_df[['event_date', 'title', 'url']].to_string(index=False))

        if dry_run:
            print(f"\n🧪 DRY RUN: Would delete {len(past_df)} past events")
            print("   Run without --dry-run to actually delete these messages")
        else:
            # Deletion confirmation
            print("\n" + "=" * 70)
            print(f"⚠️  Found {len(past_df)} past events that can be deleted")
            
            if auto_confirm:
                confirm = 'yes'
                print("🤖 Auto-confirming deletion (--live flag used)")
            else:
                confirm = input("🚨 Do you want to DELETE these PAST EVENTS from Telegram? (yes/no): ").strip().lower()

            if confirm in ['yes', 'y']:
                print(f"\n🗑️ Deleting {len(past_df)} past events...")
                deleted_count = 0

                for message_id in past_df['message_id']:
                    try:
                        await client.delete_messages(channel_name, message_id)
                        deleted_count += 1
                        if deleted_count % 10 == 0:
                            print(f"   Deleted {deleted_count}/{len(past_df)} messages...")
                    except Exception as e:
                        print(f"   ⚠️ Could not delete message {message_id}: {e}")

                print(f"\n✅ Successfully deleted {deleted_count} past events!")
            else:
                print("\n❌ Deletion cancelled. No messages were deleted.")
    else:
        print("No past events found. Channel is already clean! 🎉")


# ═══════════════════════════════════════════════════════════════
# ARGUMENT PARSING
# ═══════════════════════════════════════════════════════════════

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Telegram Channel Cleanup Tool - Remove past events from your channel",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python telegram_cleanup_delete_past_events.py --live
  python telegram_cleanup_delete_past_events.py --test
  python telegram_cleanup_delete_past_events.py --live --dry-run
  python telegram_cleanup_delete_past_events.py
        """
    )

    channel_group = parser.add_mutually_exclusive_group()
    channel_group.add_argument(
        '--live',
        action='store_true',
        help='Use live channel directly (skips channel selection)'
    )
    channel_group.add_argument(
        '--test',
        action='store_true',
        help='Use test channel directly (skips channel selection)'
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be deleted without actually deleting'
    )

    return parser.parse_args()


# ═══════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════════

def main():
    """Main entry point"""
    args = parse_arguments()

    print("=" * 70)
    print("       🧹 TELEGRAM CHANNEL CLEANUP TOOL")
    print("=" * 70)
    print("\nThis tool will help you remove past events from your Telegram channel.")
    print("Deleted messages cannot be recovered, so please use with caution.\n")

    # Determine channel based on arguments
    if args.live:
        channel = LIVE_CHANNEL
        print(f"📡 Using LIVE channel: {LIVE_CHANNEL}")
        
        if not args.dry_run:
            print(f"\n⚠️  WARNING: Running on LIVE channel ({LIVE_CHANNEL})")
            print("   Deletions cannot be undone!")
        else:
            print("   (Dry run mode - no actual deletions will occur)")

    elif args.test:
        channel = TEST_CHANNEL
        print(f"📝 Using TEST channel: {TEST_CHANNEL}")
        print("   This is safe for testing the cleanup process.")

    else:
        # Interactive mode (original behavior)
        print(f"Available channels:")
        print(f"  📝 test: {TEST_CHANNEL}")
        print(f"  📡 live: {LIVE_CHANNEL}")
        print()

        # Get channel selection
        environment = input("Enter environment (test/live): ").strip().lower()

        if environment == 'live':
            channel = LIVE_CHANNEL
            print(f"\n⚠️  WARNING: You selected the LIVE channel ({LIVE_CHANNEL})")
            print("   Deletions in this channel cannot be undone!")
            confirm = input("   Are you sure you want to continue? (yes/no): ").strip().lower()
            if confirm not in ['yes', 'y']:
                print("\n✅ Good call! Cancelled operation.")
                return
        else:
            channel = TEST_CHANNEL
            print(f"\n✅ Using TEST channel: {TEST_CHANNEL}")
            print("   This is safe for testing the cleanup process.")

    # Run the cleanup
    # Auto-confirm deletions when using --live flag
    auto_confirm = args.live
    with client:
        client.loop.run_until_complete(scan_and_clean_channel(channel, args.dry_run, auto_confirm))


if __name__ == "__main__":
    main()