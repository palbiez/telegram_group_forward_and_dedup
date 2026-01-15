#!/usr/bin/env python3

import asyncio
import time
import logging
import json
from typing import Set, Any, Dict, List, Optional

from telethon import TelegramClient, errors
from telethon.tl.functions.channels import GetForumTopicsRequest
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.types import Message

from tgd_config import (
    api_id, api_hash, session,
    TARGET_GROUP,
    DRY_RUN,
    DELETE_CROSS_TOPIC,
    MAX_MESSAGES_PER_TOPIC,
    RATE_LIMIT_DELAY,
    LOG_FILE
)

# logging
if LOG_FILE:
    logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
else:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("deduplicate_topics")

# --- helper: safe invoke with FloodWait/backoff ---
async def handle_floodwait(e: errors.FloodWaitError):
    wait = int(getattr(e, "seconds", 0))
    buf = globals().get("FLOODWAIT_BUFFER", 5)
    total = wait + buf
    log.warning("FloodWait: sleeping %s (+%s buffer) seconds", wait, buf)
    await asyncio.sleep(total)

async def safe_invoke(func, *args, **kwargs):
    """
    Call a coroutine/function and handle FloodWait / transient errors.
    func should be an awaitable function (coroutine function or lambda returning awaitable).
    """
    backoff = 1
    while True:
        try:
            return await func(*args, **kwargs)
        except errors.FloodWaitError as e:
            await handle_floodwait(e)
            backoff = 1
        except (errors.RPCError, ConnectionError, OSError, asyncio.TimeoutError) as e:
            log.warning("Transient RPC/network error: %s — retrying in %s s", e, backoff)
            await asyncio.sleep(backoff)
            backoff = min(300, backoff * 2)
        except Exception:
            # unexpected -> rethrow so operator sees trace
            raise

# --- utils ---
def get_file_unique_id(msg: Message) -> Optional[str]:
    """
    Robust getter for unique id of a file/media across Telethon versions.
    """
    f = getattr(msg, "file", None) or getattr(msg, "media", None)
    if not f:
        return None
    # common attributes: unique_id, file_unique_id
    for attr in ("unique_id", "file_unique_id", "id"):
        val = getattr(f, attr, None)
        if val:
            return str(val)
    return None

# --- requests (use instances, not types) ---
async def get_topics(client: TelegramClient, group) -> List[Any]:
    """List forum topics for a channel/forum - returns list of topic objects/dicts"""
    try:
        # instantiate request and call client(req)
        req = GetForumTopicsRequest(channel=group, q="", offset_date=None, offset_id=0, offset_topic=0, limit=200)
        async def _call():
            return await client(req)
        resp = await safe_invoke(_call)
        # Telethon may return topics under resp.topics or resp.forum_topics
        topics = getattr(resp, "topics", None) or getattr(resp, "forum_topics", None) or []
        return topics
    except Exception as e:
        log.warning("Could not list topics for %s: %s", group, e)
        return []

async def fetch_history_for_topic(client: TelegramClient, peer, topic_id: int, offset_id: int, limit: int):
    """Return GetHistoryRequest result (instantiate request)"""
    try:
        req = GetHistoryRequest(peer=peer, offset_id=offset_id, offset_date=None, add_offset=0,
                                limit=limit, max_id=0, min_id=0, hash=0, topic_id=topic_id)
        async def _call():
            return await client(req)
        return await safe_invoke(_call)
    except Exception as e:
        log.exception("GetHistoryRequest failed for peer=%s topic=%s: %s", peer, topic_id, e)
        raise

# --- core de-dup logic ---
async def deduplicate_topic(client: TelegramClient, entity, topic, global_seen: Set[str]) -> int:
    """
    Deduplicate messages with identical file unique ids inside a topic.
    Returns number deleted.
    """
    seen = global_seen if DELETE_CROSS_TOPIC else set()
    deleted = 0
    processed = 0
    offset = 0
    limit = 100

    while True:
        history = await fetch_history_for_topic(client, entity, topic.id, offset, limit)
        messages = getattr(history, "messages", []) or []
        if not messages:
            break

        # note: GetHistoryRequest often returns newest-first; if so, we iterate in given order
        for msg in messages:
            # only consider messages with media/file
            file_id = get_file_unique_id(msg)
            if not file_id:
                continue

            processed += 1
            uid = file_id

            if uid in seen:
                deleted += 1
                log.info("DELETE | topic='%s' | msg_id=%s | uid=%s", getattr(topic, "title", "<no-title>"), getattr(msg, "id", None), uid)
                if not DRY_RUN:
                    try:
                        # use safe_invoke for delete_messages (wrap into coroutine)
                        async def _del():
                            # Telethon delete_messages accepts entity and ids (single or list)
                            return await client.delete_messages(entity, msg.id)
                        await safe_invoke(_del)
                    except errors.FloodWaitError as e:
                        await handle_floodwait(e)
                    except Exception as e:
                        log.exception("Failed to delete message %s: %s", getattr(msg, "id", None), e)
                    # small pause to reduce rate
                    await asyncio.sleep(RATE_LIMIT_DELAY)
            else:
                seen.add(uid)

            if MAX_MESSAGES_PER_TOPIC and processed >= MAX_MESSAGES_PER_TOPIC:
                return deleted

        # advance offset to the last message id we processed
        # be careful: if API returns newest-first, offset logic might need to use min/max; keep consistent with your Telethon behavior
        offset = messages[-1].id

    return deleted

# --- main ---
async def main():
    client = TelegramClient(session, api_id, api_hash)
    await client.start()
    if not await client.is_user_authorized():
        log.error("Session not authorized. Authorize locally and copy session file.")
        await client.disconnect()
        return

    entity = await client.get_entity(TARGET_GROUP)
    # list topics (request instance inside function)
    topics = await get_topics(client, entity)

    print(f"🔎 {len(topics)} Themen gefunden")
    log.info(f"START | Topics={len(topics)} | DryRun={DRY_RUN}")

    total_deleted = 0
    global_seen: Set[str] = set()

    # iterate topics sequentially
    for topic in topics:
        title = getattr(topic, "title", "<no-title>")
        print(f"Prüfe Topic: {title}")
        deleted = await deduplicate_topic(client, entity, topic, global_seen)
        total_deleted += deleted
        if deleted:
            print(f"🧹 {deleted} Duplikate in '{title}'")
        # small pause between topics (avoid aggressive patterns)
        await asyncio.sleep(1.0)

    print("\n✅ Fertig")
    print(f"🗑️ Gesamte Duplikate: {total_deleted}")
    if DRY_RUN:
        print("⚠️ DRY_RUN aktiv – nichts wurde gelöscht")

    log.info(f"END | Deleted={total_deleted}")
    await client.disconnect()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Interrupted by user")
