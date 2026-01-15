import asyncio
import time
import logging
from telethon import TelegramClient
from telethon.tl.functions.channels import GetForumTopicsRequest
from telethon.tl.functions.messages import GetHistoryRequest
from tqdm import tqdm

from tgd_config import (
    api_id, api_hash, session,
    TARGET_GROUP,
    DRY_RUN,
    DELETE_CROSS_TOPIC,
    MAX_MESSAGES_PER_TOPIC,
    RATE_LIMIT_DELAY,
    LOG_FILE
)

# Logging
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s | %(message)s"
)


async def get_topics(client, group):
    entity = await client.get_entity(group)
    result = await client(GetForumTopicsRequest(
        channel=entity,
        offset_date=None,
        offset_id=0,
        offset_topic=0,
        limit=100
    ))
    return result.topics


async def deduplicate_topic(client, entity, topic, global_seen):
    seen = global_seen if DELETE_CROSS_TOPIC else set()
    deleted = 0
    processed = 0
    offset = 0
    limit = 100

    while True:
        history = await client(GetHistoryRequest(
            peer=entity,
            offset_id=offset,
            offset_date=None,
            add_offset=0,
            limit=limit,
            max_id=0,
            min_id=0,
            hash=0,
            topic_id=topic.id
        ))

        if not history.messages:
            break

        for msg in history.messages:
            if not msg.file:
                continue

            processed += 1
            uid = msg.file.unique_id

            if uid in seen:
                deleted += 1
                logging.info(
                    f"DELETE | topic='{topic.title}' | msg_id={msg.id} | uid={uid}"
                )
                if not DRY_RUN:
                    await client.delete_messages(entity, msg.id)
                    await asyncio.sleep(RATE_LIMIT_DELAY)
            else:
                seen.add(uid)

            if MAX_MESSAGES_PER_TOPIC and processed >= MAX_MESSAGES_PER_TOPIC:
                return deleted

        offset = history.messages[-1].id

    return deleted


async def main():
    async with TelegramClient(session, api_id, api_hash) as client:
        entity = await client.get_entity(TARGET_GROUP)
        topics = await get_topics(client, TARGET_GROUP)

        print(f"🔎 {len(topics)} Themen gefunden")
        logging.info(f"START | Topics={len(topics)} | DryRun={DRY_RUN}")

        total_deleted = 0
        global_seen = set()

        for topic in tqdm(topics, desc="Duplikate prüfen"):
            deleted = await deduplicate_topic(
                client,
                entity,
                topic,
                global_seen
            )
            total_deleted += deleted

            if deleted:
                print(f"🧹 {deleted} Duplikate in '{topic.title}'")

        print("\n✅ Fertig")
        print(f"🗑️ Gesamte Duplikate: {total_deleted}")

        if DRY_RUN:
            print("⚠️ DRY_RUN aktiv – nichts wurde gelöscht")

        logging.info(f"END | Deleted={total_deleted}")


if __name__ == "__main__":
    asyncio.run(main())
