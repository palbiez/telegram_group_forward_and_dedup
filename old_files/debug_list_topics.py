# debug_list_topics.py
import asyncio
from telethon import TelegramClient
from tgd_config import api_id, api_hash, session, SOURCE_GROUPS

async def main():
    client = TelegramClient(session, api_id, api_hash)
    await client.start()
    ent = await client.get_entity(SOURCE_GROUPS[0])
    print("Source:", ent)
    from telethon.tl.functions.channels import GetForumTopicsRequest
    resp = await client(GetForumTopicsRequest(channel=ent, q="", offset_date=None, offset_id=0, offset_topic=0, limit=200))
    topics = getattr(resp, "topics", None) or getattr(resp, "forum_topics", None) or []
    for t in topics:
        print("TOPIC id=", getattr(t,"id", getattr(t,"topic_id", None)), " title=", getattr(t,"title",None), " top_message=", getattr(t,"top_message",None))
    await client.disconnect()

asyncio.run(main())
