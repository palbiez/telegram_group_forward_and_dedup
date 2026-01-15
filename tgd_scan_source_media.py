# tgd_scan_source_media.py
import asyncio
import logging
from telethon import TelegramClient
from telethon.tl.functions.messages import GetForumTopicsRequest

API_ID = 
API_HASH = ""
SESSION = ""

SRC_PEER = ""
LIMIT_PER_TOPIC = 50   # wie viele Messages pro Topic geprüft werden sollen

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("tgd_scan")

async def main():
    client = TelegramClient(SESSION, API_ID, API_HASH)
    await client.start()

    try:
        src = await client.get_entity(SRC_PEER)

        # Alle Topics holen
        res = await client(GetForumTopicsRequest(
            peer=src,
            offset_date=None,
            offset_id=0,
            offset_topic=0,
            limit=100
        ))

        log.info("Gefundene Topics: %d", len(res.topics))

        for topic in res.topics:
            topic_id = topic.id
            top_msg_id = topic.top_message
            title = topic.title

            # Messages dieses Topics holen
            async for msg in client.iter_messages(
                src,
                limit=LIMIT_PER_TOPIC,
                reply_to=topic_id
            ):
                if msg.media:
                    log.info(
                        "MEDIA FOUND | msg_id=%s | topic_id=%s | top_message=%s | topic_title=%r",
                        msg.id,
                        topic_id,
                        top_msg_id,
                        title
                    )

    finally:
        await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
