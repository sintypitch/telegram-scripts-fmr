# Telegram-Notion Event Linker

A Python script that automatically links Telegram channel posts with Notion database events, maintaining bidirectional references between your event promotion posts and event database.

## 🎯 Purpose

This tool bridges your Telegram event promotion channels with a Notion events database by:
- Scanning Telegram channels for event announcements
- Matching them with corresponding events in Notion
- Adding Telegram message IDs and URLs to Notion entries
- Supporting both live and test channels for staging workflows
- Using smart caching to minimize API calls

## 📋 Prerequisites

- Python 3.7+
- A Telegram account with access to target channels
- Notion integration token with database access
- Telegram API credentials (API ID and Hash)

## 🚀 Installation

1. **Clone the repository**
   ```bash
   git clone <your-repo-url>
   cd telegram-notion-linker
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

   Required packages:
   - `telethon` - Telegram client
   - `notion-client` - Notion API client
   - `python-dotenv` - Environment variable management

3. **Set up environment variables**

   Create a `.env` file in the project root:
   ```env
   # Notion Configuration
   NOTION_TOKEN=your_notion_integration_token_here

   # Telegram API Credentials
   TELEGRAM_API_ID=your_telegram_api_id
   TELEGRAM_API_HASH=your_telegram_api_hash

   # Optional: Test channel (defaults to 'testchannel1234123434')
   TELEGRAM_TEST_CHANNEL=your_test_channel_username
   ```

4. **Configure your Notion database**

   Your Notion database should have these properties:
   - `title` (title) - Event name
   - `event_date` (date) - Event date
   - `event_location` (text) - Venue/location
   - `telegram_url` (url) - Link to Telegram post
   - `telegram_message_id` (number) - Live channel message ID
   - `telegram_test_channel_id` (number) - Test channel message ID
   - `data_status` (multi-select) - Should not contain "skipped" for events to be linked

## 💻 Usage

### Command Line Options

```bash
# Run in production mode (makes actual changes)
python telegram_messageid_notion.py --prod
python telegram_messageid_notion.py -p

# Run in test mode (read-only, no changes made)
python telegram_messageid_notion.py --test
python telegram_messageid_notion.py -t

# Clear the cache
python telegram_messageid_notion.py --clean

# Interactive mode (prompts for choice)
python telegram_messageid_notion.py
```

### First Run

On first run, you'll need to authenticate with Telegram:
1. Enter your phone number (with country code, e.g., +32477218796)
2. Enter the verification code sent to your Telegram app
3. The session will be saved for future runs

## 🔄 Automation

### Cron Setup (Linux/Mac)

To run every 30 minutes, add to crontab (`crontab -e`):

```bash
*/30 * * * * cd /path/to/script && /usr/bin/python3 telegram_messageid_notion.py --prod >> /var/log/telegram_linker.log 2>&1
```

### Shell Script

Create `run_linker.sh`:
```bash
#!/bin/bash
cd /path/to/telegram-notion-linker
source venv/bin/activate  # if using virtual environment
python telegram_messageid_notion.py --prod
```

## 📁 File Structure

```
telegram-notion-linker/
├── telegram_messageid_notion.py   # Main script
├── .env                           # Environment variables (create this)
├── cached_linker_session.session  # Telegram session (auto-created)
├── event_link_cache.json          # Cache file (auto-created)
└── requirements.txt               # Python dependencies
```

## 🎭 How It Works

1. **Dual Channel Support**: Scans both LIVE (`raveinbelgium`) and TEST channels
2. **Smart Caching**:
   - Caches linked events for 30 days after event date
   - Quick scan: Last 50 messages (default)
   - Full scan: All messages (once per 24 hours)
3. **Message Detection**: Identifies event posts by format, date patterns, and content structure
4. **Matching Logic**: Links events based on date + location combination
5. **Orphan Detection**: Identifies and updates Notion entries with outdated message IDs

## 📊 Cache Management

The script maintains a cache file (`event_link_cache.json`) that:
- Stores successful links between Telegram and Notion
- Reduces API calls by skipping already-linked events
- Auto-expires entries 30 days after event date
- Updates when messages are reposted with new IDs

Clear cache if needed:
```bash
python telegram_messageid_notion.py --clean
```

## 🧪 Test Mode

Test mode (`--test` or `-t`) allows you to:
- See what would be linked without making changes
- Verify event matching logic
- Check for orphaned links
- Debug connection issues

Output shows:
```
[TEST MODE] Would link: Event Name → Message 123
```

## ⚙️ Configuration Details

### Channels
- **Live Channel**: `raveinbelgium` (hardcoded)
- **Test Channel**: Set via `TELEGRAM_TEST_CHANNEL` env variable

### Notion Database
- **Database ID**: `1f5b2c11515b801ebd95cd423b72eb55` (hardcoded)
- Events with `data_status` = "skipped" are ignored

### Scan Behavior
- **Quick Scan**: Last 50 messages (when last full scan < 24 hours ago)
- **Full Scan**: All messages (automatic every 24 hours or with `--force-full`)

## 🐛 Troubleshooting

### Common Issues

1. **Authentication Error**
   - Delete `cached_linker_session.session` file
   - Run script again to re-authenticate

2. **No Events Found**
   - Check date format in Telegram posts (should be "DD MMM")
   - Verify Notion database has matching events
   - Ensure events aren't marked as "skipped"

3. **Cache Issues**
   - Run `python telegram_messageid_notion.py --clean`
   - Cache rebuilds automatically on next run

4. **API Rate Limits**
   - Script uses caching to minimize API calls
   - If hitting limits, increase time between runs

## 📝 Event Message Format

The script expects Telegram event posts in this format:
```
Event Title | *Venue Name*
Location • City
📅 15 JAN
Starts at 22:00
Lineup: Artist 1, Artist 2
```

## 🔒 Security Notes

- Never commit `.env` file to version control
- Keep Telegram session file secure
- Use read-only Notion token if only reading is needed
- Consider using separate Telegram account for automation

## 📄 License

[Your License Here]

## 🤝 Contributing

[Your Contributing Guidelines Here]

## 📧 Support

[Your Contact Information Here]