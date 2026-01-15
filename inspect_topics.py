#!/usr/bin/env python3
# coding: utf-8
"""
inspect_topics.py

Inspect forum topics for a source channel/group and print topic id -> top_message mapping.
Compatible with Telethon 1.41 (functions in channels) and 1.42+ (functions in messages).

Usage: python inspect_topics.py
Requires: tgd_config.py in same folder providing SOURCE_GROUPS, session, api_id, api_hash
"""
import asyncio
from datetime import datetime, timezone
from telethon import TelegramClient, functions
import telethon
import tgd_config as cfg

SESSION = getattr(cfg, "session", "user_session")
API_ID = cfg.api_id
API_HASH = cfg.api_hash
#SOURCE = cfg.SOURCE_GROUPS[0] if getattr(cfg, "SOURCE_GROUPS", None) else None
SOURCE = cfg.TARGET_GROUP if getattr(cfg, "TARGET_GROUP", None) else None

async def main():
    print("Telethon version:", getattr(telethon, "__version__", "(unknown)"))
    if SOURCE is None:
        print("No SOURCE_GROUPS defined in tgd_config.py")
        return

    client = TelegramClient(SESSION, API_ID, API_HASH)
    await client.start()
    print("Client started at", datetime.now(timezone.utc).isoformat())

    ent = await client.get_entity(SOURCE)
    inp = await client.get_input_entity(ent)
    print("Resolved source:", SOURCE, "->", getattr(ent, "id", ent))
    print("Resolved Target:", SOURCE, "->", getattr(ent, "id", ent))

    # Decide which RPC to call based on availability
    tried = []
    topics = None

    # Prefer messages.getForumTopics (Telethon 1.42+)
    try:
        print("Trying functions.messages.GetForumTopicsRequest ...")
        resp = await client(functions.messages.GetForumTopicsRequest(
            peer=inp,
            offset_date=0,
            offset_id=0,
            offset_topic=0,
            limit=200
        ))
        topics = getattr(resp, "topics", None)
        tried.append("messages.GetForumTopicsRequest")
    except Exception as e:
        print("messages.GetForumTopicsRequest unavailable / failed:", repr(e))

    # Fallback to channels.GetForumTopicsRequest (Telethon 1.41)
    if topics is None:
        try:
            print("Trying functions.channels.GetForumTopicsRequest ...")
            resp = await client(functions.channels.GetForumTopicsRequest(
                channel=inp,
                offset_date=0,
                offset_id=0,
                offset_topic=0,
                limit=200
            ))
            topics = getattr(resp, "topics", None)
            tried.append("channels.GetForumTopicsRequest")
        except Exception as e:
            print("channels.GetForumTopicsRequest unavailable / failed:", repr(e))

    if not topics:
        print("No topics returned (tried: {})".format(", ".join(tried)))
        await client.disconnect()
        return

    print(f"Found {len(topics)} topics (via {', '.join(tried)})")
    for t in topics:
        # topic id in different TLs could be .id or .topic_id
        t_id = getattr(t, "id", None) or getattr(t, "topic_id", None)
        top_msg = getattr(t, "top_message", None) or getattr(t, "top_message_id", None)
        title = getattr(t, "title", None)
        print(f"topic.id: {str(t_id):10} -> top_message: {str(top_msg):10}  title: {title!s}")

    await client.disconnect()
    print("Done. Disconnected.")

if __name__ == '__main__':
    asyncio.run(main())

