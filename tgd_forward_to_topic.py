# tgd_forward_to_topic.py
from telethon import TelegramClient, helpers
import asyncio, logging

API_ID = 
API_HASH = ""
SESSION = ""

SRC_PEER = ""
TGT_PEER = ""

CAND_MSG_ID = 240         # ID in SOURCE (deine Angabe)
TARGET_TOPIC_ID = 6404    # topic.id (nicht top_message) — aus deinem Topic-Listing
# optional: PLACEHOLDER_MSG_ID = 6414  # top_message; meist nicht nötig beim high-level call

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("fwd")

async def main():
    client = TelegramClient(SESSION, API_ID, API_HASH)
    await client.start()
    src = await client.get_entity(SRC_PEER)
    tgt = await client.get_entity(TGT_PEER)
    src_msg = await client.get_messages(src, ids=CAND_MSG_ID)
    # high-level forward - reply_to is topic id to send into that thread
    msg = await client.send_message(
        entity=tgt,
        message=src_msg.message,
        reply_to=TARGET_TOPIC_ID
    )
    log.info("Forward result: %s", getattr(msg, "id", repr(msg)[:200]))

    await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
