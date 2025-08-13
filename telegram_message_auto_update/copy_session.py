#!/usr/bin/env python3
"""
Helper script to copy an existing Telegram session for use with the updater.
This ensures cron jobs can use an already-authenticated session.
"""

import os
import shutil
import sys

def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    target_session = os.path.join(script_dir, "updater_session.session")
    
    print("🔍 Looking for existing Telegram session files...")
    
    # Check if target already exists
    if os.path.exists(target_session):
        print(f"✅ Session already exists: {target_session}")
        print(f"   Size: {os.path.getsize(target_session)} bytes")
        return 0
    
    # Look for alternative session files
    possible_sources = [
        # Other session names in same directory
        "local_updater_session.session",
        "replit_updater_session.session",
        "scheduler_session.session",
        # Parent directory (telegram event scheduler)
        "../telegram_event_scheduler/scheduler_session.session",
        # Look for any .session file
        "*.session"
    ]
    
    found_sessions = []
    for pattern in possible_sources:
        if "*" in pattern:
            # Use glob for wildcards
            import glob
            files = glob.glob(os.path.join(script_dir, pattern))
            found_sessions.extend(files)
        else:
            path = os.path.join(script_dir, pattern)
            if os.path.exists(path):
                found_sessions.append(path)
    
    if not found_sessions:
        print("❌ No existing session files found!")
        print("   Please run the script manually first to authenticate:")
        print("   python notion_to_telegram_message_update_new_info.py")
        return 1
    
    # Use the first found session
    source = found_sessions[0]
    print(f"📋 Found session: {source}")
    
    # Copy it to the expected location
    try:
        shutil.copy2(source, target_session)
        print(f"✅ Copied to: {target_session}")
        print("   The updater script should now work with cron!")
        return 0
    except Exception as e:
        print(f"❌ Failed to copy session: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())