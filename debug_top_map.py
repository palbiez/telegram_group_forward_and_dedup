# debug_top_map.py
import asyncio
from telethon import TelegramClient
from config import api_id, api_hash, session, SOURCE_GROUPS
from telethon.tl.functions.channels import GetForumTopicsRequest

async def main():
    client = TelegramClient(session, api_id, api_hash)
    await client.start()
    ent = await client.get_entity(SOURCE_GROUPS[0])
    resp = await client(GetForumTopicsRequest(channel=ent, q="", offset_date=None, offset_id=0, offset_topic=0, limit=200))
    topics = getattr(resp, "topics", None) or getattr(resp, "forum_topics", None) or []
    top_map = {}
    for t in topics:
        top = getattr(t, "top_message", None)
        tid = getattr(t, "id", getattr(t,"topic_id", None))
        if top and tid:
            top_map[int(top)] = int(tid)
    print("top_map entries:", len(top_map))
    for k,v in list(top_map.items())[:50]:
        print(" top_message", k, " -> topic", v)
    await client.disconnect()

asyncio.run(main())
