"""
Telegram-Notion Event Linker with Smart Caching
Minimizes API calls by tracking processed events
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
from telegram_secrets import api_id, api_hash
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
NOTION_TOKEN = os.getenv('NOTION_TOKEN')
MASTER_DB_ID = "1f5b2c11515b801ebd95cd423b72eb55"
CHANNEL = 'raveinbelgium'
CACHE_FILE = '../event_link_cache.json'
CACHE_EXPIRY_DAYS = 30  # Keep cache for 30 days after event has passed

# Initialize clients
notion = Client(auth=NOTION_TOKEN)


@dataclass
class CachedLink:
    """Represents a cached link between Telegram and Notion"""
    telegram_id: int
    notion_id: str
    event_date: str
    event_title: str
    linked_at: str
    last_verified: str

    def is_expired(self) -> bool:
        """Check if this cache entry should be removed (30 days after event passed)"""
        try:
            # Parse event date (format: "6 SEP" or "06 SEP")
            current_date = datetime.now()
            current_year = current_date.year

            # Parse the date with current year first
            date_str = f"{self.event_date} {current_year}"
            event_date = datetime.strptime(date_str, "%d %b %Y")

            # If the event date is more than 2 months in the past, it might be next year
            if event_date < current_date - timedelta(days=60):
                # Try next year
                date_str = f"{self.event_date} {current_year + 1}"
                event_date = datetime.strptime(date_str, "%d %b %Y")

            # Keep the cache entry until 30 days after the event
            expiry_date = event_date + timedelta(days=CACHE_EXPIRY_DAYS)
            return current_date > expiry_date

        except Exception as e:
            # If we can't parse, check how old the link is
            # If linked more than a year ago, probably safe to expire
            try:
                linked_date = datetime.fromisoformat(self.linked_at)
                return datetime.now() - linked_date > timedelta(days=365)
            except:
                # Keep it if we can't determine
                return False


class CacheManager:
    """Manages the cache of linked events"""

    def __init__(self):
        self.cache: Dict[int, CachedLink] = {}  # Keyed by Telegram message ID
        self.notion_to_telegram: Dict[str, int] = {}  # Reverse lookup
        self.last_full_scan: Optional[str] = None
        self.load_cache()

    def load_cache(self):
        """Load cache from file"""
        if os.path.exists(CACHE_FILE):
            try:
                with open(CACHE_FILE, 'r') as f:
                    data = json.load(f)

                    # Load cached links
                    for item in data.get('links', []):
                        link = CachedLink(**item)
                        if not link.is_expired():
                            self.cache[link.telegram_id] = link
                            self.notion_to_telegram[link.notion_id] = link.telegram_id

                    self.last_full_scan = data.get('last_full_scan')

                    print(f"📦 Loaded cache: {len(self.cache)} active links")

                    # Report if any were expired
                    original_count = len(data.get('links', []))
                    if original_count > len(self.cache):
                        print(f"   Expired {original_count - len(self.cache)} old entries")
            except Exception as e:
                print(f"⚠️  Cache load error: {e}. Starting fresh.")
                self.cache = {}
                self.notion_to_telegram = {}

    def save_cache(self):
        """Save cache to file"""
        try:
            # Clean expired entries before saving
            self.clean_expired()

            data = {
                'links': [asdict(link) for link in self.cache.values()],
                'last_full_scan': self.last_full_scan,
                'saved_at': datetime.now().isoformat()
            }

            with open(CACHE_FILE, 'w') as f:
                json.dump(data, f, indent=2)

            print(f"💾 Saved {len(self.cache)} links to cache")
        except Exception as e:
            print(f"❌ Cache save error: {e}")

    def clean_expired(self):
        """Remove expired entries"""
        expired = [tid for tid, link in self.cache.items() if link.is_expired()]
        for tid in expired:
            link = self.cache[tid]
            del self.cache[tid]
            if link.notion_id in self.notion_to_telegram:
                del self.notion_to_telegram[link.notion_id]

    def add_link(self, telegram_id: int, notion_id: str, event_date: str, event_title: str):
        """Add a new link to cache"""
        now = datetime.now().isoformat()
        link = CachedLink(
            telegram_id=telegram_id,
            notion_id=notion_id,
            event_date=event_date,
            event_title=event_title,
            linked_at=now,
            last_verified=now
        )
        self.cache[telegram_id] = link
        self.notion_to_telegram[notion_id] = telegram_id

    def is_linked(self, telegram_id: int = None, notion_id: str = None) -> bool:
        """Check if an event is already linked"""
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
            # If can't parse, do a full scan
            return True

    def mark_full_scan(self):
        """Mark that a full scan was completed"""
        self.last_full_scan = datetime.now().isoformat()
        self.save_cache()  # Save immediately so next run knows


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

    async def smart_link_events(self, client: TelegramClient) -> dict:
        """Smart linking that uses cache to minimize API calls"""
        stats = {
            "newly_linked": 0,
            "already_linked": 0,
            "cached_links": len(self.cache.cache),
            "messages_scanned": 0,
            "notion_queries": 0,
            "cache_hits": 0
        }

        channel = await client.get_entity(CHANNEL)

        # Determine scan depth based on cache state
        if self.cache.needs_full_scan():
            print("🔄 Performing daily full scan...")
            limit = None  # Scan all
            self.cache.mark_full_scan()
        else:
            print("⚡ Quick scan (last 50 messages)...")
            limit = 50  # Only recent messages

        # Scan Telegram messages
        print(f"📱 Scanning Telegram channel @{CHANNEL}...")

        telegram_events = []  # Collect unlinked events

        async for message in client.iter_messages(channel, limit=limit):
            stats["messages_scanned"] += 1

            if not message.text or not self.is_event_message(message.text):
                continue

            # Check cache first
            if self.cache.is_linked(telegram_id=message.id):
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

        # Only query Notion if we found unlinked Telegram events
        if telegram_events:
            print(f"🔍 Found {len(telegram_events)} unlinked posts. Checking Notion...")
            stats["notion_queries"] = 1

            # Load only necessary Notion events
            await self._load_notion_events_for_dates(
                {event['date'] for event in telegram_events}
            )

            # Match and link
            for tg_event in telegram_events:
                if tg_event['key'] in self.notion_events:
                    notion_event = self.notion_events[tg_event['key']]

                    # Check if Notion already has a different Telegram link
                    if notion_event.get('has_telegram_url'):
                        stats["already_linked"] += 1
                        # Update cache with this info
                        self.cache.add_link(
                            tg_event['id'],
                            notion_event['id'],
                            tg_event['date'],
                            tg_event['title']
                        )
                        continue

                    # Link the event
                    if not self.test_mode:
                        success = await self._update_notion_event(
                            notion_event['id'],
                            tg_event['id'],
                            channel.username
                        )
                        if success:
                            stats["newly_linked"] += 1
                            self.cache.add_link(
                                tg_event['id'],
                                notion_event['id'],
                                tg_event['date'],
                                tg_event['title']
                            )
                    else:
                        print(f"   [TEST] Would link: {tg_event['title']} → Message {tg_event['id']}")
                        stats["newly_linked"] += 1
        else:
            print("✅ All events already linked (from cache)")

        # Save updated cache
        if not self.test_mode:
            self.cache.save_cache()

        return stats

    async def _load_notion_events_for_dates(self, dates: Set[str]):
        """Load only Notion events for specific dates"""
        self.notion_events.clear()

        # Convert date strings to search range
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
                # Check if this event's date matches any we're looking for
                for date in dates:
                    if date.upper() in event['date'].upper() or event['date'].upper() in date.upper():
                        # Generate multiple keys for matching
                        for key in self._generate_keys(event['date'], event['location']):
                            self.notion_events[key] = event

    def _parse_notion_event(self, item: dict) -> Optional[dict]:
        """Parse a Notion database item"""
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
        has_url = bool(p.get("telegram_url", {}).get("url"))

        return {
            'id': item["id"],
            'title': title,
            'date': formatted_date,
            'location': location,
            'has_telegram_url': has_url
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

        # Add padded version
        if len(date.split()[0]) == 1:
            padded = f"0{date}|{location}".lower()
            keys.append(padded)

        return keys

    async def _update_notion_event(self, notion_id: str, message_id: int, username: str) -> bool:
        """Update Notion event with Telegram link"""
        try:
            post_url = f"https://t.me/{username}/{message_id}" if username else f"https://t.me/c/{CHANNEL}/{message_id}"

            notion.pages.update(
                page_id=notion_id,
                properties={
                    "telegram_url": {"url": post_url},
                    "telegram_message_id": {"number": message_id}
                }
            )
            print(f"   ✅ Linked message {message_id}")
            return True
        except Exception as e:
            print(f"   ❌ Error: {e}")
            return False


async def run_cached_linker(test_mode: bool = False):
    """Main entry point with caching"""
    print("🔗 TELEGRAM-NOTION LINKER (Cached)")
    print("=" * 50)

    if test_mode:
        print("🧪 TEST MODE - no changes will be made\n")

    cache = CacheManager()
    matcher = EventMatcher(cache, test_mode=test_mode)

    async with TelegramClient('cached_linker_session', api_id, api_hash) as client:
        stats = await matcher.smart_link_events(client)

    # Print summary
    print("\n📊 SUMMARY")
    print("=" * 50)
    print(f"✅ Newly linked: {stats['newly_linked']}")
    print(f"💾 Cached links: {stats['cached_links']}")
    print(f"⚡ Cache hits: {stats['cache_hits']}")
    print(f"📱 Messages scanned: {stats['messages_scanned']}")
    print(f"🔍 Notion API calls: {stats['notion_queries']}")

    # Efficiency report
    if stats['cache_hits'] > 0:
        efficiency = (stats['cache_hits'] / (stats['cache_hits'] + stats['newly_linked'])) * 100
        print(f"\n💡 Efficiency: {efficiency:.1f}% events handled from cache")


def main():
    """CLI entry point"""
    import sys

    if '--test' in sys.argv or '-t' in sys.argv:
        asyncio.run(run_cached_linker(test_mode=True))
    elif '--prod' in sys.argv or '-p' in sys.argv:
        asyncio.run(run_cached_linker(test_mode=False))
    elif '--clean' in sys.argv:
        # Clean cache option
        if os.path.exists(CACHE_FILE):
            os.remove(CACHE_FILE)
            print("🗑️  Cache cleared")
        else:
            print("No cache to clear")
    else:
        print("Telegram-Notion Linker (Cached)")
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