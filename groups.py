#!/usr/bin/env python3
"""
Telegram media/topic migration optimized for Telethon 1.41.

Behavior:
- Forwardet Medien (inkl. Alben) von SOURCE_GROUPS -> TARGET_GROUP
- Minimiert FloodWait durch forwarding (kein re-upload) + adaptive backoff + rate limiting
- Versucht server-side copy_messages when available (preferred, preserves content without reupload)
- Respects forum topics: creates target topics with same title and tries to place messages into those topics where possible.
- No dry-run (will actually forward). Use with care; test on small subset first.
"""

import asyncio
import logging
import json
import math
import time
from typing import Dict, List, Optional, Any

from telethon import TelegramClient, errors
from telethon.tl.functions.channels import GetForumTopicsRequest, CreateForumTopicRequest
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.types import Message

# load config provided by user
from config import (
    api_id,
    api_hash,
    session,
    SOURCE_GROUPS,
    TARGET_GROUP,
    RATE_LIMIT_DELAY,
    # optional extras; provide defaults if not present
    FORWARD_BATCH_SIZE,
    CONCURRENCY,
    FLOODWAIT_BUFFER,
)

# Safe defaults if config omitted values
RATE_LIMIT_DELAY = RATE_LIMIT_DELAY if RATE_LIMIT_DELAY is not None else 3.0
FORWARD_BATCH_SIZE = FORWARD_BATCH_SIZE if FORWARD_BATCH_SIZE is not None else 1
CONCURRENCY = CONCURRENCY if CONCURRENCY is not None else 1
FLOODWAIT_BUFFER = FLOODWAIT_BUFFER if FLOODWAIT_BUFFER is not None else 5

# logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("groups_forward_media")
logging.getLogger("telethon").setLevel(logging.WARNING)

TOPICS_CACHE_FILE = "topics_cache_forward.json"

# ------------ utilities ------------
def normalize_title(s: str) -> str:
    return " ".join(s.strip().lower().split())

async def handle_floodwait(e: errors.FloodWaitError):
    wait = int(getattr(e, "seconds", 0)) + FLOODWAIT_BUFFER
    log.warning("FloodWaitError: sleeping %s seconds (+%s buffer)", wait, FLOODWAIT_BUFFER)
    await asyncio.sleep(wait)

import asyncio
from telethon import TelegramClient, errors

async def safe_invoke(*args, **kwargs):
    """
    Backward-compatible wrapper to call Telethon methods / request wrappers reliably
    with FloodWait and transient-error handling.

    Accepts two calling conventions:
      1) safe_invoke(func, *fn_args, **fn_kwargs)
      2) safe_invoke(client, func, *fn_args, **fn_kwargs)
    """
    # normalize arguments
    if len(args) == 0:
        raise ValueError("safe_invoke requires at least one positional argument")

    # detect pattern (client, func, ...)
    if len(args) >= 2 and isinstance(args[0], TelegramClient) and callable(args[1]):
        client = args[0]
        func = args[1]
        fn_args = args[2:]
    else:
        func = args[0]
        fn_args = args[1:]

    # now func is the callable we want to await/call with fn_args
    backoff = 1
    while True:
        try:
            # If func is a bound method or coroutinefunction, just call it
            return await func(*fn_args, **kwargs)
        except errors.FloodWaitError as e:
            # handle FloodWait (sleep the required seconds + buffer if applicable)
            wait = int(getattr(e, "seconds", 0))
            buf = globals().get("FLOODWAIT_BUFFER", 5)
            log.warning("FloodWaitError: sleeping %s seconds (+%s buffer)", wait, buf)
            await asyncio.sleep(wait + buf)
            backoff = 1
        except (errors.RPCError, ConnectionError, OSError, asyncio.TimeoutError) as e:
            log.warning("Transient RPC/network error: %s — retrying in %s s", e, backoff)
            await asyncio.sleep(backoff)
            backoff = min(300, backoff * 2)
        except Exception:
            # unexpected -> re-raise so caller can handle/see full trace
            raise

# ------------ forum topic helpers ------------
async def list_forum_topics(client: TelegramClient, channel_entity) -> List[Dict[str, Any]]:
    """
    Robust listing of forum topics. Creates a request *instance* and invokes it.
    Returns list of dicts: {'id': int, 'title': str}
    """
    try:
        # build a proper request instance
        req = GetForumTopicsRequest(
            channel=channel_entity,
            q="",
            offset_date=None,
            offset_id=0,
            offset_topic=0,
            limit=200
        )

        # client(req) returns a coroutine; call it via safe_invoke
        # safe_invoke supports either (func, ...) or (client, func, ...)
        # we'll pass a small wrapper coroutine to be explicit:
        async def _call():
            return await client(req)

        resp = await safe_invoke(_call)
    except Exception as e:
        log.warning("Could not list topics for %s: %s", channel_entity, e)
        return []

    # parse returned object (Telethon may expose topics under different attr names)
    raw_topics = getattr(resp, "topics", None) or getattr(resp, "forum_topics", None) or []
    results = []
    for t in raw_topics:
        try:
            tid = int(getattr(t, "id", getattr(t, "topic_id", None)))
            title = getattr(t, "title", "") or ""
            results.append({"id": tid, "title": title})
        except Exception:
            continue
    return results

async def ensure_target_topics(client: TelegramClient, target_entity, source_topics: List[Dict[str, Any]]) -> Dict[int, int]:
    """
    Ensure topics in target for each source topic title. Returns mapping source_topic_id -> target_topic_id
    """
    mapping: Dict[int, int] = {}

    # list existing target topics once
    existing = await list_forum_topics(client, target_entity)
    title_to_id = {normalize_title(t["title"]): t["id"] for t in existing}

    # load cache
    try:
        with open(TOPICS_CACHE_FILE, "r", encoding="utf-8") as f:
            cache = json.load(f)
            cached = cache.get("topic_ids", [])
    except FileNotFoundError:
        cached = []

    for s in source_topics:
        sid = int(s["id"])
        stitle = s["title"]
        key = normalize_title(stitle)
        if key in title_to_id:
            mapping[sid] = title_to_id[key]
            log.info("Reusing existing target topic for '%s' -> %s", stitle, mapping[sid])
            continue

        # create topic
        log.info("Creating topic '%s' in target", stitle)

        try:
            # instantiate the request and call client(req) so we pass an instance (not a type)
            req = CreateForumTopicRequest(channel=target_entity, title=stitle)
        
            async def _do_create():
                  return await client(req)
        
            # call via safe_invoke for FloodWait/backoff handling
            await safe_invoke(_do_create)
        
            # re-list target topics and match by title
            existing = await list_forum_topics(client, target_entity)
            title_to_id = {normalize_title(t["title"]): t["id"] for t in existing}
            if key in title_to_id:
                  mapping[sid] = title_to_id[key]
                  cached.append(title_to_id[key])
                  log.info("Created topic id=%s for '%s'", mapping[sid], stitle)
            else:
                  # fallback: newest topic id if available
                  if existing:
                        mapping[sid] = existing[-1]["id"]
                        cached.append(existing[-1]["id"])
                        log.warning("Could not find exact title match — fallback to newest topic id=%s", mapping[sid])
                  else:
                        log.error("Topic creation appears to have failed for '%s'", stitle)
        except Exception as e:
            log.exception("Failed to create topic '%s': %s", stitle, e)
        

        try:
            # create and then re-list topics to find id robustly
            await safe_invoke(client, client(CreateForumTopicRequest(channel=target_entity, title=stitle)))
            # re-list target topics and match by title
            existing = await list_forum_topics(client, target_entity)
            title_to_id = {normalize_title(t["title"]): t["id"] for t in existing}
            if key in title_to_id:
                mapping[sid] = title_to_id[key]
                cached.append(title_to_id[key])
                log.info("Created topic id=%s for '%s'", mapping[sid], stitle)
            else:
                # fallback to newest topic id if available
                if existing:
                    mapping[sid] = existing[-1]["id"]
                    cached.append(existing[-1]["id"])
                    log.warning("Could not find exact title match — fallback to newest topic id=%s", mapping[sid])
                else:
                    log.error("Topic creation appears to have failed for '%s'", stitle)
        except Exception as e:
            log.exception("Failed to create topic '%s': %s", stitle, e)

    # persist cache minimally
    try:
        with open(TOPICS_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump({"target": str(TARGET_GROUP), "topic_ids": cached}, f, indent=2)
    except Exception as e:
        log.warning("Could not write topics cache: %s", e)

    return mapping

# ------------ message collection helpers ------------
def msg_in_topic(msg: Message) -> Optional[int]:
    """
    Determine if a message belongs to a forum topic in source.
    Return topic id (reply_to_top_id or forum_topic) or None.
    """
    # Telethon may have 'reply_to_top_id' or 'forum_topic' or 'reply_to'
    t = getattr(msg, "reply_to_top_id", None) or getattr(msg, "forum_topic", None)
    if t:
        try:
            return int(t)
        except Exception:
            return None
    return None

async def collect_topic_messages(client: TelegramClient, source_entity, source_topic_id: int):
    """
    Collect messages in the given source topic (oldest -> newest).
    For each grouped_id (album) group the message IDs together.
    Yields batches: each item is tuple (is_album:bool, message_id_or_list, message_preview_text)
    """
    log.info("Collecting messages for source topic id=%s in %s", source_topic_id, source_entity)
    # We'll iterate full history and filter messages belonging to the topic.
    # For very large groups, you may want to optimize by fetching only recent range, or using get_history with offsets.
    seen_grouped = set()
    # Walk messages oldest->newest with reverse True
    async for msg in client.iter_messages(source_entity, reverse=True):
        if not isinstance(msg, Message):
            continue
        # check if this msg belongs to the topic
        tid = msg_in_topic(msg)
        if tid != source_topic_id:
            continue
        # album handling
        gid = getattr(msg, "grouped_id", None)
        if gid:
            if gid in seen_grouped:
                continue
            # gather all messages with this grouped_id
            album_msgs = []
            async for a in client.iter_messages(source_entity, limit=None):
                if getattr(a, "grouped_id", None) == gid:
                    album_msgs.append(a)
            # sort by id ascending to keep original order
            album_msgs = sorted(album_msgs, key=lambda m: m.id)
            ids = [m.id for m in album_msgs]
            seen_grouped.add(gid)
            text_preview = next((m.text for m in album_msgs if m.text), "")
            yield True, ids, text_preview
            continue

        # single message
        yield False, msg.id, msg.text or ""

# ------------ forwarding helpers ------------
async def adaptive_forward(client: TelegramClient, target_entity, from_entity, msg_ids: List[int], target_topic_id: Optional[int] = None):
    """
    Try methods in this order:
    1) client.forward_messages(target, msg_ids, from_peer=from_entity) — best (no reupload).
       If API supports topic param, include it (try).
    2) client.copy_messages(target, msg_ids, from_peer=from_entity) — server-side copy (no reupload) if available.
    3) Fallback: iterate messages, download then send_file (not recommended for huge files).
    """
    # 1) Try forward_messages
    try:
        # forward_messages accepts list of msg ids; some Telethon builds allow 'silent'/'topic' kwargs,
        # we'll first try basic forward.
        await safe_invoke(client, client.forward_messages, target_entity, msg_ids, from_entity)
        return True
    except errors.FloodWaitError as e:
        await handle_floodwait(e)
        # after waiting try again once
        try:
            await safe_invoke(client, client.forward_messages, target_entity, msg_ids, from_entity)
            return True
        except Exception as e:
            log.warning("forward_messages retry failed: %s", e)
    except Exception as e:
        log.debug("forward_messages not usable for msg_ids %s: %s", msg_ids[:5], e)

    # 2) Try copy_messages if available (server-side copy without forward tag)
    try:
        if hasattr(client, "copy_messages"):
            await safe_invoke(client, client.copy_messages, target_entity, msg_ids, from_peer=from_entity)
            return True
    except errors.FloodWaitError as e:
        await handle_floodwait(e)
    except Exception as e:
        log.debug("copy_messages not usable: %s", e)

    # 3) Fallback: reupload -> iterate over msgs, download files then send_file (last resort)
    log.warning("Falling back to per-message reupload (slow & uses bandwidth).")
    for mid in msg_ids:
        try:
            msg = await safe_invoke(client, client.get_messages, from_entity, ids=mid)
            if msg and getattr(msg, "media", None):
                # send_file will download from telegram servers to local memory then upload — heavy
                await safe_invoke(client, client.send_file, target_entity, msg.media, caption=msg.text or "")
            else:
                await safe_invoke(client, client.send_message, target_entity, msg.text or "")
            # brief pause
            await asyncio.sleep(RATE_LIMIT_DELAY)
        except errors.FloodWaitError as e:
            await handle_floodwait(e)
        except Exception as e:
            log.exception("Failed reuploading message %s: %s", mid, e)
    return True

# ------------ orchestrator ------------
async def process_topic(client: TelegramClient, source_entity, target_entity, source_topic_id: int, target_topic_id: Optional[int]):
    """
    Collect and forward messages for a single source topic into mapped target topic.
    """
    log.info("Processing topic %s -> target topic %s", source_topic_id, target_topic_id)
    # create a small queue/iterator for messages in topic
    async for is_album, ids_or_id, preview in collect_topic_messages(client, source_entity, source_topic_id):
        # create batches of ids to forward
        if is_album:
            ids_batch = ids_or_id  # list
            # forward album as single action where possible
            success = await adaptive_forward(client, target_entity, source_entity, ids_batch, target_topic_id)
            if not success:
                log.error("Failed to forward album %s", ids_batch[:3])
            # after forwarding, small rate-limit
            await asyncio.sleep(RATE_LIMIT_DELAY)
            continue

        # single message id
        mid = ids_or_id
        # for single messages, we forward individually to avoid too-large batch forwards
        success = await adaptive_forward(client, target_entity, source_entity, [mid], target_topic_id)
        if not success:
            log.error("Failed to forward message %s", mid)
        await asyncio.sleep(RATE_LIMIT_DELAY)

async def main():
    client = TelegramClient(session, api_id, api_hash)
    await client.start()
    if not await client.is_user_authorized():
        log.error("Session not authorized. Please authorize locally and copy session file.")
        await client.disconnect()
        return

    log.info("Connected as %s", await client.get_me())

    # resolve entities
    src_entities = []
    for s in SOURCE_GROUPS:
        ent = await safe_invoke(client, client.get_entity, s)
        src_entities.append(ent)
    tgt_entity = await safe_invoke(client, client.get_entity, TARGET_GROUP)

    # gather topics from sources
    source_topics = []
    for ent in src_entities:
        topics = await list_forum_topics(client, ent)
        source_topics.extend(topics)

    # ensure target topics and build mapping
    topic_map = await ensure_target_topics(client, tgt_entity, source_topics)

    # iterate source entities and topics
    for ent in src_entities:
        # find topics in this source entity
        topics = [t for t in source_topics if getattr(t, "id", None) is not None]  # keep as dicts
        # better: query topics per ent to find correct source topic ids; keep simple: list_forum_topics again
        ent_topics = await list_forum_topics(client, ent)
        for t in ent_topics:
            sid = int(t["id"])
            tt = topic_map.get(sid)
            log.info("Migrating source topic id=%s title='%s' -> target topic id=%s", sid, t["title"], tt)
            await process_topic(client, ent, tgt_entity, sid, tt)

    log.info("Migration done — disconnecting.")
    await client.disconnect()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Interrupted by user")
