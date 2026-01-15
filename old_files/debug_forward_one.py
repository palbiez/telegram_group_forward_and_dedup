# debug_forward_one.py
import asyncio
from telethon import TelegramClient
from tgd_config import api_id, api_hash, session, SOURCE_GROUPS, TARGET_GROUP

MID = 7  # replace with a real id from dump
SRC = SOURCE_GROUPS[0]

async def main():
    client = TelegramClient(session, api_id, api_hash)
    await client.start()
    src_ent = await client.get_entity(SRC)
    tgt_ent = await client.get_entity(TARGET_GROUP)
    try:
        await client.forward_messages(tgt_ent, [MID], src_ent)
        print("forward_messages OK for", MID)
    except Exception as e:
        print("forward_messages failed:", repr(e))
    await client.disconnect()

asyncio.run(main())