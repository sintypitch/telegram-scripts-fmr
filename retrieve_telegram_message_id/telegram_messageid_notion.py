"""
Telegram-Notion Event Linker with Smart Caching
Supports both LIVE and TEST channels
"""

import asyncio
import json
import os
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Set
from dataclasses import dataclass, asdict

from telethon import TelegramClient
from notion_client import Client
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
NOTION_TOKEN = os.getenv('NOTION_TOKEN')
if not NOTION_TOKEN:
    raise ValueError("NOTION_TOKEN not found in .env file")

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

MASTER_DB_ID = "1f5b2c11515b801ebd95cd423b72eb55"
LIVE_CHANNEL = 'raveinbelgium'
TEST_CHANNEL = os.getenv('TELEGRAM_TEST_CHANNEL', 'testchannel1234123434')
CACHE_FILE = '../event_link_cache.json'
CACHE_EXPIRY_DAYS = 30  # Keep cache for 30 days after event has passed

# Initialize clients
notion = Client(auth=NOTION_TOKEN)


@dataclass
class CachedLink:
    """Represents a cached link between Telegram and Notion"""
    telegram_id: int
    telegram_test_id: Optional[int]  # Test channel ID
    notion_id: str
    event_date: str
    event_title: str
    linked_at: str
    last_verified: str

    def is_expired(self) -> bool:
        """Check if this cache entry should be removed (30 days after event passed)"""
        try:
            current_date = datetime.now()
            current_year = current_date.year

            # Parse the date with current year first
            date_str = f"{self.event_date} {current_year}"
            event_date = datetime.strptime(date_str, "%d %b %Y")

            # If the event date is more than 2 months in the past, it might be next year
            if event_date < current_date - timedelta(days=60):
                date_str = f"{self.event_date} {current_year + 1}"
                event_date = datetime.strptime(date_str, "%d %b %Y")

            # Keep the cache entry until 30 days after the event
            expiry_date = event_date + timedelta(days=CACHE_EXPIRY_DAYS)
            return current_date > expiry_date

        except Exception:
            # If we can't parse, check how old the link is
            try:
                linked_date = datetime.fromisoformat(self.linked_at)
                return datetime.now() - linked_date > timedelta(days=365)
            except:
                return False


class CacheManager:
    """Manages the cache of linked events"""

    def __init__(self):
        self.cache: Dict[int, CachedLink] = {}  # Keyed by Telegram message ID
        self.cache_test: Dict[int, CachedLink] = {}  # Keyed by Test channel message ID
        self.notion_to_telegram: Dict[str, int] = {}  # Reverse lookup
        self.notion_to_test: Dict[str, int] = {}  # Reverse lookup for test channel
        self.last_full_scan: Optional[str] = None
        self.load_cache()

    def load_cache(self):
        """Load cache from file"""
        if os.path.exists(CACHE_FILE):
            try:
                with open(CACHE_FILE, 'r') as f:
                    data = json.load(f)

                    # Track unique notion IDs to avoid duplicates
                    seen_notion_ids = set()

                    # Load cached links
                    for item in data.get('links', []):
                        # Handle old cache format
                        if 'telegram_test_id' not in item:
                            item['telegram_test_id'] = None

                        link = CachedLink(**item)
                        if not link.is_expired():
                            if link.telegram_id and link.telegram_id > 0:
                                self.cache[link.telegram_id] = link
                                self.notion_to_telegram[link.notion_id] = link.telegram_id
                            if link.telegram_test_id and link.telegram_test_id > 0:
                                self.cache_test[link.telegram_test_id] = link
                                self.notion_to_test[link.notion_id] = link.telegram_test_id

                            seen_notion_ids.add(link.notion_id)

                    self.last_full_scan = data.get('last_full_scan')
                    print(f"📦 Loaded cache: {len(self.cache)} live links, {len(self.cache_test)} test links")

                    # Report expired entries
                    original_count = len(data.get('links', []))
                    if original_count > len(seen_notion_ids):
                        print(f"   Expired {original_count - len(seen_notion_ids)} old entries")
            except Exception as e:
                print(f"⚠️  Cache load error: {e}. Starting fresh.")
                self.cache = {}
                self.cache_test = {}
                self.notion_to_telegram = {}
                self.notion_to_test = {}

    def save_cache(self):
        """Save cache to file"""
        try:
            self.clean_expired()

            # Combine all unique links by notion_id
            all_links = {}

            # Add live channel links
            for telegram_id, link in self.cache.items():
                if link.notion_id not in all_links:
                    all_links[link.notion_id] = link
                else:
                    all_links[link.notion_id].telegram_id = link.telegram_id
                    all_links[link.notion_id].last_verified = link.last_verified

            # Add test channel links
            for telegram_test_id, link in self.cache_test.items():
                if link.notion_id in all_links:
                    all_links[link.notion_id].telegram_test_id = telegram_test_id
                else:
                    all_links[link.notion_id] = link

            data = {
                'links': [asdict(link) for link in all_links.values()],
                'last_full_scan': self.last_full_scan,
                'saved_at': datetime.now().isoformat()
            }

            with open(CACHE_FILE, 'w') as f:
                json.dump(data, f, indent=2)

            print(f"💾 Saved {len(all_links)} links to cache")
        except Exception as e:
            print(f"❌ Cache save error: {e}")

    def clean_expired(self):
        """Remove expired entries"""
        expired_live = [tid for tid, link in self.cache.items() if link.is_expired()]
        for tid in expired_live:
            link = self.cache[tid]
            del self.cache[tid]
            if link.notion_id in self.notion_to_telegram:
                del self.notion_to_telegram[link.notion_id]

        expired_test = [tid for tid, link in self.cache_test.items() if link.is_expired()]
        for tid in expired_test:
            link = self.cache_test[tid]
            del self.cache_test[tid]
            if link.notion_id in self.notion_to_test:
                del self.notion_to_test[link.notion_id]

    def add_link(self, telegram_id: int, notion_id: str, event_date: str, event_title: str,
                 is_test: bool = False):
        """Add a new link to cache"""
        now = datetime.now().isoformat()

        # Look for existing link
        existing_link = None
        for link in self.cache.values():
            if link.notion_id == notion_id:
                existing_link = link
                break

        if not existing_link:
            for link in self.cache_test.values():
                if link.notion_id == notion_id:
                    existing_link = link
                    break

        if existing_link:
            # Update existing link
            if is_test:
                existing_link.telegram_test_id = telegram_id
                self.cache_test[telegram_id] = existing_link
                self.notion_to_test[notion_id] = telegram_id
            else:
                existing_link.telegram_id = telegram_id
                self.cache[telegram_id] = existing_link
                self.notion_to_telegram[notion_id] = telegram_id
            existing_link.last_verified = now
        else:
            # Create new link
            if is_test:
                new_link = CachedLink(
                    telegram_id=0,
                    telegram_test_id=telegram_id,
                    notion_id=notion_id,
                    event_date=event_date,
                    event_title=event_title,
                    linked_at=now,
                    last_verified=now
                )
                self.cache_test[telegram_id] = new_link
                self.notion_to_test[notion_id] = telegram_id
            else:
                new_link = CachedLink(
                    telegram_id=telegram_id,
                    telegram_test_id=None,
                    notion_id=notion_id,
                    event_date=event_date,
                    event_title=event_title,
                    linked_at=now,
                    last_verified=now
                )
                self.cache[telegram_id] = new_link
                self.notion_to_telegram[notion_id] = telegram_id

    def is_linked(self, telegram_id: int = None, notion_id: str = None, is_test: bool = False) -> bool:
        """Check if an event is already linked"""
        if is_test:
            if telegram_id:
                return telegram_id in self.cache_test
            if notion_id:
                return notion_id in self.notion_to_test
        else:
            if telegram_id:
                return telegram_id in self.cache
            if notion_id:
                return notion_id in self.notion_to_telegram
        return False

    def needs_full_scan(self) -> bool:
        """Check if we need a full scan (once per day)"""
        if not self.last_full_scan:
            return True

        try:
            last_scan = datetime.fromisoformat(self.last_full_scan)
            hours_since = (datetime.now() - last_scan).total_seconds() / 3600
            return hours_since >= 24
        except:
            return True

    def mark_full_scan(self):
        """Mark that a full scan was completed"""
        self.last_full_scan = datetime.now().isoformat()
        self.save_cache()


class EventMatcher:
    """Handles matching between Telegram posts and Notion events"""

    SUMMARY_INDICATORS = [
        "📅 Good evening", "FESTIVALS", "TECHNO CLUBS",
        "LOCAL FAVORITES", "Weekly summary", "Daily summary",
        "<!-- WEEKLY_SUMMARY -->", "<!-- DAILY_SUMMARY -->"
    ]

    def __init__(self, cache: CacheManager, test_mode: bool = False):
        self.cache = cache
        self.test_mode = test_mode
        self.notion_events: Dict[str, dict] = {}

    def is_event_message(self, text: str) -> bool:
        """Check if a Telegram message is a single event post"""
        if not text or len(text) < 50:
            return False

        if any(indicator in text for indicator in self.SUMMARY_INDICATORS):
            return False

        if text.count('★') > 3 or text.count('Starts at') > 1 or text.count('Lineup:') > 1:
            return False

        has_date = bool(re.search(r'\d{1,2}\s+(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)', text))
        has_event_format = ("•" in text and "Starts at" in text) or "Lineup:" in text

        return has_date and has_event_format

    def extract_event_data(self, text: str) -> Optional[Tuple[str, str, str]]:
        """Extract date, location, and title from Telegram message"""
        date_match = re.search(r'(\d{1,2}\s+(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC))', text)
        if not date_match:
            return None

        date = date_match.group(1).strip()
        lines = text.split('\n')
        location = None
        title = None

        # Extract title from first line
        if lines and '|' in lines[0]:
            parts = lines[0].split('|')
            if len(parts) > 1:
                title = parts[1].strip().strip('*').strip()

        # Extract location from second line
        if len(lines) > 1 and "•" in lines[1]:
            location = lines[1].split("•")[0].strip()

        if date and location:
            return date, location, title or "Unknown Event"
        return None

    async def smart_link_events(self, client: TelegramClient, channel_name: str = None, force_full_scan: bool = False) -> dict:
        """Smart linking that uses cache to minimize API calls"""
        # Determine which channel to scan
        if channel_name == 'test':
            channel = TEST_CHANNEL
            is_test = True
            print(f"📱 Scanning TEST channel @{TEST_CHANNEL}...")
        else:
            channel = LIVE_CHANNEL
            is_test = False
            print(f"📱 Scanning LIVE channel @{LIVE_CHANNEL}...")

        stats = {
            "newly_linked": 0,
            "already_linked": 0,
            "updated_links": 0,
            "cached_links": len(self.cache.cache) if not is_test else len(self.cache.cache_test),
            "messages_scanned": 0,
            "notion_queries": 0,
            "cache_hits": 0
        }

        channel_entity = await client.get_entity(channel)

        # Determine scan depth - full scan applies to BOTH channels
        if force_full_scan or self.cache.needs_full_scan():
            print("🔄 Performing full scan (all messages)...")
            limit = None  # Scan all messages
        else:
            print("⚡ Quick scan (last 50 messages)...")
            limit = 50

        telegram_events = []  # Collect unlinked events
        existing_message_ids = set()  # Track which message IDs exist

        async for message in client.iter_messages(channel_entity, limit=limit):
            stats["messages_scanned"] += 1
            existing_message_ids.add(message.id)

            if not message.text or not self.is_event_message(message.text):
                continue

            # Check cache first
            if self.cache.is_linked(telegram_id=message.id, is_test=is_test):
                stats["cache_hits"] += 1
                continue

            # Extract event data
            event_data = self.extract_event_data(message.text)
            if event_data:
                date, location, title = event_data
                telegram_events.append({
                    'id': message.id,
                    'date': date,
                    'location': location,
                    'title': title,
                    'key': f"{date}|{location}".lower()
                })

        # Only query Notion if we found unlinked Telegram events OR doing full scan
        if telegram_events or (force_full_scan or self.cache.needs_full_scan()):
            print(f"🔍 Found {len(telegram_events)} unlinked posts. Checking Notion...")
            stats["notion_queries"] = 1

            # Load Notion events
            await self._load_notion_events_for_dates(
                {event['date'] for event in telegram_events} if telegram_events else set()
            )

            # Check for orphaned Notion entries (old message IDs that no longer exist)
            if force_full_scan or self.cache.needs_full_scan():
                await self._check_and_update_orphaned_links(
                    client, channel_entity, existing_message_ids, is_test, stats
                )

            # Match and link new events
            for tg_event in telegram_events:
                if tg_event['key'] in self.notion_events:
                    notion_event = self.notion_events[tg_event['key']]

                    # Check if already linked
                    if is_test:
                        existing_id = notion_event.get('telegram_test_channel_id')
                        if existing_id == tg_event['id']:
                            stats["already_linked"] += 1
                            self.cache.add_link(
                                tg_event['id'],
                                notion_event['id'],
                                tg_event['date'],
                                tg_event['title'],
                                is_test=True
                            )
                            continue
                        elif existing_id and existing_id in existing_message_ids:
                            # Old ID still exists, don't update
                            print(f"   ⚠️  Event already linked to message {existing_id}, skipping")
                            continue
                    else:
                        existing_id = notion_event.get('telegram_message_id')
                        if existing_id == tg_event['id']:
                            stats["already_linked"] += 1
                            self.cache.add_link(
                                tg_event['id'],
                                notion_event['id'],
                                tg_event['date'],
                                tg_event['title'],
                                is_test=False
                            )
                            continue
                        elif existing_id and existing_id in existing_message_ids:
                            # Old ID still exists, don't update
                            print(f"   ⚠️  Event already linked to message {existing_id}, skipping")
                            continue

                    # Link the event (new or replacing orphaned)
                    if not self.test_mode:
                        is_update = bool(existing_id)
                        success = await self._update_notion_event(
                            notion_event['id'],
                            tg_event['id'],
                            channel_entity.username,
                            is_test=is_test,
                            is_update=is_update
                        )
                        if success:
                            if is_update:
                                stats["updated_links"] += 1
                            else:
                                stats["newly_linked"] += 1
                            self.cache.add_link(
                                tg_event['id'],
                                notion_event['id'],
                                tg_event['date'],
                                tg_event['title'],
                                is_test=is_test
                            )
                    else:
                        print(f"   [TEST MODE] Would link: {tg_event['title']} → Message {tg_event['id']}")
                        stats["newly_linked"] += 1
        else:
            print("✅ All events already linked (from cache)")

        # Save cache after each channel
        if not self.test_mode:
            self.cache.save_cache()

        return stats

    async def _check_and_update_orphaned_links(self, client, channel_entity, existing_message_ids, is_test, stats):
        """Check for Notion entries with orphaned message IDs and clear them"""
        print("🔍 Checking for orphaned message IDs...")

        # Load ALL Notion events with IDs
        all_events_response = notion.databases.query(
            database_id=MASTER_DB_ID,
            filter={
                "property": "telegram_message_id" if not is_test else "telegram_test_channel_id",
                "number": {"is_not_empty": True}
            }
        )

        for item in all_events_response["results"]:
            p = item["properties"]

            if is_test:
                existing_id = p.get("telegram_test_channel_id", {}).get("number")
                field_name = "telegram_test_channel_id"
            else:
                existing_id = p.get("telegram_message_id", {}).get("number")
                field_name = "telegram_message_id"

            if existing_id and existing_id not in existing_message_ids:
                # This ID no longer exists in Telegram
                title = p["title"]["title"][0]["plain_text"] if p.get("title", {}).get("title") else "Unknown"
                print(f"   🔄 Found orphaned ID {existing_id} for '{title}' - message no longer exists")

                # We don't clear it here, but mark it for potential replacement
                # The new message with same event will update it

    async def _load_notion_events_for_dates(self, dates: Set[str]):
        """Load Notion events with ALL their IDs"""
        self.notion_events.clear()

        # Date range for filtering
        min_date = datetime.now().date()
        max_date = datetime.now().date() + timedelta(days=365)

        response = notion.databases.query(
            database_id=MASTER_DB_ID,
            filter={
                "and": [
                    {"property": "event_date", "date": {
                        "on_or_after": min_date.isoformat(),
                        "on_or_before": max_date.isoformat()
                    }},
                    {"or": [
                        {"property": "data_status", "multi_select": {"does_not_contain": "skipped"}},
                        {"property": "data_status", "multi_select": {"is_empty": True}}
                    ]}
                ]
            }
        )

        for item in response["results"]:
            event = self._parse_notion_event(item)
            if event:
                # Match dates
                for date in dates:
                    if date.upper() in event['date'].upper() or event['date'].upper() in date.upper():
                        # Generate keys for matching
                        for key in self._generate_keys(event['date'], event['location']):
                            self.notion_events[key] = event

    def _parse_notion_event(self, item: dict) -> Optional[dict]:
        """Parse a Notion database item with ALL ID fields"""
        p = item["properties"]

        status_items = p.get("data_status", {}).get("multi_select", [])
        if any(s.get("name", "").lower() == "skipped" for s in status_items):
            return None

        event_date = p.get("event_date", {}).get("date", {}).get("start", "")
        if not event_date:
            return None

        try:
            date_obj = datetime.strptime(event_date, "%Y-%m-%d")
            formatted_date = f"{date_obj.day} {date_obj.strftime('%b').upper()}"
        except:
            return None

        title = p["title"]["title"][0]["plain_text"] if p.get("title", {}).get("title") else ""
        location = self._safe_get_text(p.get("event_location", {}).get("rich_text", []))

        # Get both IDs
        live_id = p.get("telegram_message_id", {}).get("number")
        test_id = p.get("telegram_test_channel_id", {}).get("number")

        return {
            'id': item["id"],
            'title': title,
            'date': formatted_date,
            'location': location,
            'telegram_message_id': live_id,
            'telegram_test_channel_id': test_id
        }

    def _safe_get_text(self, rich_text_list: list) -> str:
        """Safely extract plain text from Notion rich text field"""
        if rich_text_list and len(rich_text_list) > 0:
            return rich_text_list[0].get("plain_text", "")
        return ""

    def _generate_keys(self, date: str, location: str) -> List[str]:
        """Generate multiple matching keys"""
        keys = []
        base_key = f"{date}|{location}".lower()
        keys.append(base_key)

        # Add padded version for single digit dates
        if len(date.split()[0]) == 1:
            padded = f"0{date}|{location}".lower()
            keys.append(padded)

        return keys

    async def _update_notion_event(self, notion_id: str, message_id: int, username: str, is_test: bool = False, is_update: bool = False) -> bool:
        """Update Notion event with Telegram link"""
        try:
            if is_test:
                # Update test channel ID only
                action = "Updating" if is_update else "Linking"
                notion.pages.update(
                    page_id=notion_id,
                    properties={
                        "telegram_test_channel_id": {"number": message_id}
                    }
                )
                print(f"   ✅ {action} TEST message {message_id}")
            else:
                # Update live channel fields
                action = "Updating" if is_update else "Linking"
                post_url = f"https://t.me/{username}/{message_id}" if username else f"https://t.me/c/{LIVE_CHANNEL}/{message_id}"

                notion.pages.update(
                    page_id=notion_id,
                    properties={
                        "telegram_url": {"url": post_url},
                        "telegram_message_id": {"number": message_id}
                    }
                )
                print(f"   ✅ {action} LIVE message {message_id}")

            return True
        except Exception as e:
            print(f"   ❌ Error: {e}")
            return False


async def run_cached_linker(test_mode: bool = False):
    """Main entry point - always scans both channels"""
    print("🔗 TELEGRAM-NOTION LINKER (Dual Channel Support)")
    print("=" * 50)

    if test_mode:
        print("🧪 TEST MODE - no changes will be made\n")

    cache = CacheManager()
    matcher = EventMatcher(cache, test_mode=test_mode)

    # Check if we need a full scan
    do_full_scan = cache.needs_full_scan()

    async with TelegramClient('cached_linker_session', api_id, api_hash) as client:
        print("\n📱 SCANNING LIVE CHANNEL")
        print("-" * 30)
        stats_live = await matcher.smart_link_events(client, 'live', force_full_scan=do_full_scan)

        print("\n📱 SCANNING TEST CHANNEL")
        print("-" * 30)
        stats_test = await matcher.smart_link_events(client, 'test', force_full_scan=do_full_scan)

        # Mark full scan as completed after both channels
        if do_full_scan and not test_mode:
            cache.mark_full_scan()

        # Summary
        print("\n📊 SUMMARY")
        print("=" * 50)
        print(f"LIVE Channel (@{LIVE_CHANNEL}):")
        print(f"  ✅ Newly linked: {stats_live['newly_linked']}")
        print(f"  🔄 Updated (reposted): {stats_live.get('updated_links', 0)}")
        print(f"  💾 Cached links: {stats_live['cached_links']}")
        print(f"  📱 Messages scanned: {stats_live['messages_scanned']}")
        print(f"\nTEST Channel (@{TEST_CHANNEL}):")
        print(f"  ✅ Newly linked: {stats_test['newly_linked']}")
        print(f"  🔄 Updated (reposted): {stats_test.get('updated_links', 0)}")
        print(f"  💾 Cached links: {stats_test['cached_links']}")
        print(f"  📱 Messages scanned: {stats_test['messages_scanned']}")

        total_linked = stats_live['newly_linked'] + stats_test['newly_linked']
        total_updated = stats_live.get('updated_links', 0) + stats_test.get('updated_links', 0)

        if total_linked > 0 or total_updated > 0:
            print(f"\n✨ Total changes:")
            if total_linked > 0:
                print(f"   Newly linked: {total_linked}")
            if total_updated > 0:
                print(f"   Updated (reposted): {total_updated}")


def main():
    """CLI entry point - simplified"""
    import sys

    if '--test' in sys.argv or '-t' in sys.argv:
        asyncio.run(run_cached_linker(test_mode=True))
    elif '--prod' in sys.argv or '-p' in sys.argv:
        asyncio.run(run_cached_linker(test_mode=False))
    elif '--clean' in sys.argv:
        if os.path.exists(CACHE_FILE):
            os.remove(CACHE_FILE)
            print("🗑️  Cache cleared")
        else:
            print("No cache to clear")
    else:
        print("Telegram-Notion Linker")
        print("-" * 30)
        print("Usage:")
        print("  --test, -t    Test mode (no changes)")
        print("  --prod, -p    Production mode")
        print("  --clean       Clear cache")
        print()

        choice = input("Run in [t]est or [p]roduction mode? ").lower()
        if choice == 't':
            asyncio.run(run_cached_linker(test_mode=True))
        elif choice == 'p':
            asyncio.run(run_cached_linker(test_mode=False))
        else:
            print("Cancelled.")


if __name__ == "__main__":
    main()