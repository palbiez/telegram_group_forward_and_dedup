#!/usr/bin/env python3
"""
groups.py — Telethon 1.41 migration helper (final).
Kompatibel mit Telethon 1.41 (kein copy_messages/message_thread_id).
Per-run logfile in ./logs/. Batch-forward + dedupe + persist nach Batch.
"""

import asyncio
import logging
import json
import os
from datetime import datetime
from typing import Dict, List, Optional, Any, Set

from telethon import TelegramClient, errors
from telethon.tl.functions.channels import CreateForumTopicRequest, GetForumTopicsRequest
from telethon.tl.types import Message

# Achtung: Telethon-Version 1.41 wird absichtlich verwendet (siehe Projektentscheidung).
from config import (
    api_id, api_hash, session, SOURCE_GROUPS, TARGET_GROUP,
    RATE_LIMIT_DELAY, FORWARD_BATCH_SIZE, CONCURRENCY, FLOODWAIT_BUFFER,
    PERSIST_FORWARDED_UIDS_FILE
)

# ---------------- Logging setup ----------------
def setup_logging(script_name: str, log_dir: str = "logs") -> str:
    os.makedirs(log_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_filename = os.path.join(log_dir, f"{script_name}_{ts}.log")
    handlers = [logging.FileHandler(log_filename, encoding="utf-8")]
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=handlers,
    )
    logging.getLogger("telethon").setLevel(logging.WARNING)
    return log_filename

SCRIPT_NAME = "groups"
LOG_FILE = setup_logging(SCRIPT_NAME)
log = logging.getLogger(SCRIPT_NAME)

# ---------------- Defaults & constants ----------------
TOPICS_CACHE_FILE = "topics_cache_forward.json"

RATE_LIMIT_DELAY = RATE_LIMIT_DELAY if RATE_LIMIT_DELAY is not None else 3.0
FORWARD_BATCH_SIZE = FORWARD_BATCH_SIZE if FORWARD_BATCH_SIZE is not None else 100
CONCURRENCY = CONCURRENCY if CONCURRENCY is not None else 1
FLOODWAIT_BUFFER = FLOODWAIT_BUFFER if FLOODWAIT_BUFFER is not None else 5
PERSIST_FORWARDED_UIDS_FILE = PERSIST_FORWARDED_UIDS_FILE if PERSIST_FORWARDED_UIDS_FILE is not None else "forwarded_uids.json"

log.info("=== Script start ===")
log.info("Logging to %s", LOG_FILE)
log.info("Python PID=%s", os.getpid())
log.info("Working directory=%s", os.getcwd())
log.info("FORWARD_BATCH_SIZE=%s", FORWARD_BATCH_SIZE)
log.info("RATE_LIMIT_DELAY=%s", RATE_LIMIT_DELAY)
log.info("SOURCE_GROUPS=%s", SOURCE_GROUPS)
log.info("TARGET_GROUP=%s", TARGET_GROUP)

def normalize_title(s: str) -> str:
    return " ".join(s.strip().lower().split())

# ---------------- helpers ----------------
async def handle_floodwait(e: errors.FloodWaitError):
    wait = int(getattr(e, "seconds", 0))
    total = wait + FLOODWAIT_BUFFER
    log.warning("FloodWaitError: sleeping %s seconds (+%s buffer)", wait, FLOODWAIT_BUFFER)
    await asyncio.sleep(total)

async def safe_invoke(*args, **kwargs):
    from telethon import TelegramClient as _TC
    if not args:
        raise ValueError("safe_invoke requires at least one argument")
    if len(args) >= 2 and isinstance(args[0], _TC) and callable(args[1]):
        func = args[1]
        fn_args = args[2:]
    else:
        func = args[0]
        fn_args = args[1:]

    backoff = 1
    while True:
        try:
            return await func(*fn_args, **kwargs)
        except errors.FloodWaitError as e:
            await handle_floodwait(e)
            backoff = 1
        except (errors.RPCError, ConnectionError, OSError, asyncio.TimeoutError) as e:
            log.warning("Transient network/RPC error: %s — retrying in %s s", e, backoff)
            await asyncio.sleep(backoff)
            backoff = min(300, backoff * 2)
        except Exception:
            raise

# ---------------- Topic helpers ----------------
async def list_forum_topics(client: TelegramClient, channel_entity) -> List[Dict[str, Any]]:
    try:
        req = GetForumTopicsRequest(channel=channel_entity, q="", offset_date=None, offset_id=0, offset_topic=0, limit=200)
        async def _call(): return await client(req)
        resp = await safe_invoke(_call)
    except Exception as e:
        log.warning("Could not list topics for %s: %s", channel_entity, e)
        return []
    raw = getattr(resp, "topics", None) or getattr(resp, "forum_topics", None) or []
    out = []
    for t in raw:
        try:
            tid = int(getattr(t, "id", getattr(t, "topic_id", None)))
            title = getattr(t, "title", "") or ""
            out.append({"id": tid, "title": title, "top_message": getattr(t, "top_message", None)})
        except Exception:
            continue
    return out

async def ensure_target_topics(client: TelegramClient, target_entity, source_topics: List[Dict[str, Any]]) -> Dict[int, int]:
    mapping: Dict[int, int] = {}
    existing = await list_forum_topics(client, target_entity)
    title_to_id = {normalize_title(t["title"]): t["id"] for t in existing}
    try:
        with open(TOPICS_CACHE_FILE, "r", encoding="utf-8") as f:
            cached = json.load(f).get("topic_ids", [])
    except Exception:
        cached = []

    for s in source_topics:
        sid = int(s["id"])
        stitle = s["title"]
        key = normalize_title(stitle)
        if key in title_to_id:
            mapping[sid] = title_to_id[key]
            log.info("Reusing existing target topic '%s' id=%s for source id=%s", stitle, mapping[sid], sid)
            continue

        log.info("Creating topic '%s' in target", stitle)
        try:
            req = CreateForumTopicRequest(channel=target_entity, title=stitle)
            async def _do_create(): return await client(req)
            await safe_invoke(_do_create)
            existing = await list_forum_topics(client, target_entity)
            title_to_id = {normalize_title(t["title"]): t["id"] for t in existing}
            if key in title_to_id:
                mapping[sid] = title_to_id[key]
                if mapping[sid] not in cached:
                    cached.append(mapping[sid])
                log.info("Created topic id=%s for '%s'", mapping[sid], stitle)
            else:
                if existing:
                    mapping[sid] = existing[-1]["id"]
                    cached.append(existing[-1]["id"])
                    log.warning("Could not find exact title match — fallback to newest topic id=%s", mapping[sid])
                else:
                    log.error("Topic creation appears to have failed for '%s'", stitle)
        except Exception as e:
            log.exception("Failed to create topic '%s': %s", stitle, e)

    try:
        with open(TOPICS_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump({"target": str(TARGET_GROUP), "topic_ids": cached}, f, indent=2)
    except Exception as e:
        log.warning("Could not write topics cache: %s", e)

    return mapping

# ---------------- top_message map ----------------
async def build_top_message_map(client: TelegramClient, source_entity) -> Dict[int, int]:
    mapping: Dict[int, int] = {}
    try:
        req = GetForumTopicsRequest(channel=source_entity, q="", offset_date=None, offset_id=0, offset_topic=0, limit=200)
        async def _call(): return await client(req)
        resp = await safe_invoke(_call)
        topics = getattr(resp, "topics", None) or getattr(resp, "forum_topics", None) or []
        for t in topics:
            top_msg = getattr(t, "top_message", None)
            tid = getattr(t, "id", getattr(t, "topic_id", None))
            if top_msg and tid:
                mapping[int(top_msg)] = int(tid)
    except Exception as e:
        log.warning("Could not build top_message_map for %s: %s", source_entity, e)
    log.info("Built top_message_map with %d entries for %s", len(mapping), source_entity)
    return mapping

# ---------------- helper: resolve reply chain ----------------
async def resolve_topic_from_reply(client: TelegramClient, reply_msg_id: int, top_map: Dict[int,int], max_depth: int = 6) -> Optional[int]:
    depth = 0
    cur_id = reply_msg_id
    try:
        while cur_id is not None and depth < max_depth:
            try:
                m = await safe_invoke(client, client.get_messages, None, ids=cur_id)
            except Exception:
                m = None

            if not m:
                return None

            forum_topic = getattr(m, "forum_topic", None)
            if forum_topic:
                try:
                    return int(forum_topic)
                except Exception:
                    pass

            rt_top = getattr(m, "reply_to_top_id", None)
            if rt_top is not None:
                try:
                    rt_top_int = int(rt_top)
                    if rt_top_int in top_map:
                        return top_map[rt_top_int]
                except Exception:
                    pass

            if getattr(m, "id", None) in top_map:
                return top_map[int(m.id)]

            nxt = getattr(m, "reply_to_msg_id", None)
            if nxt is None:
                return None
            cur_id = int(nxt)
            depth += 1
    except Exception as e:
        log.debug("resolve_topic_from_reply error: %s", e)
    return None

# ---------------- collector: single pass with robust topic detection ----------------
async def collect_topic_messages_single_pass(client: TelegramClient, source_entity, source_topic_id: int, top_map: Dict[int, int]):
    seen_grouped = set()
    yield_count = 0
    checked = 0

    async for msg in client.iter_messages(source_entity, reverse=True):
        checked += 1
        if not isinstance(msg, Message):
            continue

        topic_for_msg = None
        forum_topic = getattr(msg, "forum_topic", None)
        if forum_topic:
            try:
                topic_for_msg = int(forum_topic)
            except Exception:
                topic_for_msg = None

        if topic_for_msg is None:
            rt_top = getattr(msg, "reply_to_top_id", None)
            if rt_top is not None:
                try:
                    rt_top_int = int(rt_top)
                    if rt_top_int in top_map:
                        topic_for_msg = top_map[rt_top_int]
                except Exception:
                    pass

        if topic_for_msg is None:
            rt_msg_id = getattr(msg, "reply_to_msg_id", None)
            if rt_msg_id is not None:
                try:
                    rt_msg_int = int(rt_msg_id)
                    if rt_msg_int in top_map:
                        topic_for_msg = top_map[rt_msg_int]
                    else:
                        resolved = await resolve_topic_from_reply(client, rt_msg_int, top_map, max_depth=6)
                        if resolved is not None:
                            topic_for_msg = int(resolved)
                            log.debug("Resolved topic via reply-chain: msg=%s -> topic=%s (via reply %s)", getattr(msg, "id", None), topic_for_msg, rt_msg_int)
                except Exception:
                    pass

        try:
            if topic_for_msg is None and getattr(msg, "id", None) in top_map:
                topic_for_msg = top_map[msg.id]
        except Exception:
            pass

        if topic_for_msg != source_topic_id:
            if checked % 1000 == 0:
                log.debug("collect: checked %s msgs, found %s yields so far for topic %s", checked, yield_count, source_topic_id)
            continue

        gid = getattr(msg, "grouped_id", None)
        if gid:
            if gid in seen_grouped:
                continue
            seen_grouped.add(gid)
            start = max(1, msg.id - 25)
            end = msg.id + 25
            ids_window = list(range(start, end + 1))
            try:
                album_msgs = await safe_invoke(client, client.get_messages, source_entity, ids=ids_window)
            except Exception:
                yield False, msg.id, getattr(msg, "message", "") or ""
                yield_count += 1
                continue
            album_msgs = [m for m in album_msgs if getattr(m, "grouped_id", None) == gid]
            album_msgs = sorted(album_msgs, key=lambda m: m.id)
            if not album_msgs:
                yield False, msg.id, getattr(msg, "message", "") or ""
                yield_count += 1
                continue
            ids = [m.id for m in album_msgs]
            preview = next((getattr(m, "message", "") for m in album_msgs if getattr(m, "message", None)), "")
            yield True, ids, preview
            yield_count += 1
            continue

        yield False, msg.id, getattr(msg, "message", "") or ""
        yield_count += 1

    log.info("Collector finished for topic %s: checked=%s yields=%s", source_topic_id, checked, yield_count)

# ---------------- helpers: file uid ----------------
def get_file_unique_id_from_msg(msg: Message) -> Optional[str]:
    media = getattr(msg, "media", None) or getattr(msg, "file", None)
    if not media:
        return None
    for attr in ("file_unique_id", "unique_id", "id"):
        v = getattr(media, attr, None)
        if v:
            return str(v)
    doc = getattr(media, "document", None)
    if doc:
        for attr in ("file_unique_id", "unique_id", "id"):
            v = getattr(doc, attr, None)
            if v:
                return str(v)
    return None

# ---------------- forwarding (robust for Telethon 1.41) ----------------
async def adaptive_forward(client: TelegramClient, target_entity, from_entity, msg_ids: List[int], target_topic_id: Optional[int] = None):
    # validate msg ids in source
    valid_ids = []
    for mid in msg_ids:
        try:
            m = await safe_invoke(client, client.get_messages, from_entity, ids=mid)
        except Exception as e:
            log.warning("Could not fetch source msg %s: %s", mid, e)
            m = None
        if not m:
            log.warning("Skipping invalid/non-existing msg id %s", mid)
            continue
        valid_ids.append(mid)

    if not valid_ids:
        log.info("No valid message ids to forward after validation.")
        return True

    # If target is root (no topic), try forward_messages (fast)
    if target_topic_id is None:
        try:
            await safe_invoke(client, client.forward_messages, target_entity, valid_ids, from_entity)
            log.info("adaptive_forward: forward_messages succeeded for %d ids (root)", len(valid_ids))
            return True
        except errors.FloodWaitError as e:
            await handle_floodwait(e)
        except Exception as e:
            log.debug("forward_messages failed (root): %s", e)

    # No reliable copy_messages/message_thread_id in pinned 1.41 -> fallback to per-message reupload when targeting topics
    log.info("adaptive_forward: falling back to per-message reupload for %d ids (topic=%s)", len(valid_ids), target_topic_id)
    for mid in valid_ids:
        try:
            m = await safe_invoke(client, client.get_messages, from_entity, ids=mid)
            if not m:
                log.warning("Cannot reupload missing msg %s", mid)
                continue

            # media send_file with reply_to to put into topic
            if getattr(m, "media", None):
                if target_topic_id is not None:
                    await safe_invoke(client, client.send_file, target_entity, m.media, caption=(m.message or ""), reply_to=target_topic_id)
                    log.info("REUPLOAD MEDIA | topic=%s | msg_id=%s", target_topic_id, mid)
                else:
                    await safe_invoke(client, client.send_file, target_entity, m.media, caption=(m.message or ""))
                    log.info("REUPLOAD MEDIA (root) | msg_id=%s", mid)
            else:
                if target_topic_id is not None:
                    await safe_invoke(client, client.send_message, target_entity, m.message or "", reply_to=target_topic_id)
                    log.info("REUPLOAD TEXT | topic=%s | msg_id=%s", target_topic_id, mid)
                else:
                    await safe_invoke(client, client.send_message, target_entity, m.message or "")
                    log.info("REUPLOAD TEXT (root) | msg_id=%s", mid)

            await asyncio.sleep(RATE_LIMIT_DELAY)
        except errors.FloodWaitError as e:
            await handle_floodwait(e)
        except Exception as e:
            log.exception("Reupload failed for msg %s: %s", mid, e)
    return True

async def adaptive_forward_batch(client, target_entity, from_entity, ids_list: List[int], target_topic_id: Optional[int] = None):
    if not ids_list:
        return True
    max_chunk = min(max(1, FORWARD_BATCH_SIZE), 200)
    for i in range(0, len(ids_list), max_chunk):
        chunk = ids_list[i:i+max_chunk]
        log.info("adaptive_forward_batch: sending chunk of %d (ids %s...)", len(chunk), chunk[:3])
        try:
            await adaptive_forward(client, target_entity, from_entity, chunk, target_topic_id)
        except Exception as e:
            log.exception("Batch forward failed for %s: %s", chunk[:3], e)
        await asyncio.sleep(RATE_LIMIT_DELAY)
    return True

# ---------------- process topic: batch & dedupe (persist after each batch) ----------------
async def process_topic(client: TelegramClient, source_entity, target_entity, source_topic_id: int, target_topic_id: Optional[int]):
    log.info("Processing topic %s -> target topic %s", source_topic_id, target_topic_id)
    top_map = await build_top_message_map(client, source_entity)

    forwarded_file_uids: Set[str] = set()
    if os.path.exists(PERSIST_FORWARDED_UIDS_FILE):
        try:
            with open(PERSIST_FORWARDED_UIDS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list):
                    forwarded_file_uids = set(data)
        except Exception as e:
            log.warning("Could not load persisted forwarded uids: %s", e)

    pending_batch: List[int] = []
    processed = 0

    async for is_album, ids_or_id, preview in collect_topic_messages_single_pass(client, source_entity, source_topic_id, top_map):
        if is_album:
            if pending_batch:
                log.info("Flushing batch before album: topic=%s sending %d messages", source_topic_id, len(pending_batch))
                await adaptive_forward_batch(client, target_entity, source_entity, pending_batch, target_topic_id)
                # update forwarded_file_uids and persist after batch
                try:
                    msgs_after = await safe_invoke(client, client.get_messages, source_entity, ids=pending_batch)
                except Exception:
                    msgs_after = []
                for m in msgs_after:
                    uid = get_file_unique_id_from_msg(m)
                    if uid:
                        forwarded_file_uids.add(uid)
                # persist now
                try:
                    with open(PERSIST_FORWARDED_UIDS_FILE, "w", encoding="utf-8") as f:
                        json.dump(list(forwarded_file_uids), f)
                except Exception as e:
                    log.warning("Could not persist forwarded uids after batch: %s", e)
                pending_batch = []
                await asyncio.sleep(RATE_LIMIT_DELAY)

            album_ids = ids_or_id
            try:
                album_msgs = await safe_invoke(client, client.get_messages, source_entity, ids=album_ids)
            except Exception as e:
                log.warning("Could not fetch album msgs %s: %s", album_ids[:5], e)
                album_msgs = []

            skip_album = False
            for m in album_msgs:
                uid = get_file_unique_id_from_msg(m)
                if uid and uid in forwarded_file_uids:
                    skip_album = True
                    break

            if skip_album:
                log.info("Skipping album (already forwarded in this run): %s", album_ids[:5])
            else:
                log.info("Forwarding album %s (len=%d) -> target_topic=%s", album_ids[:3], len(album_ids), target_topic_id)
                success = await adaptive_forward(client, target_entity, source_entity, album_ids, target_topic_id)
                if success:
                    for m in album_msgs:
                        uid = get_file_unique_id_from_msg(m)
                        if uid:
                            forwarded_file_uids.add(uid)
                    # persist after album
                    try:
                        with open(PERSIST_FORWARDED_UIDS_FILE, "w", encoding="utf-8") as f:
                            json.dump(list(forwarded_file_uids), f)
                    except Exception as e:
                        log.warning("Could not persist forwarded uids after album: %s", e)
                await asyncio.sleep(RATE_LIMIT_DELAY)

            processed += len(album_ids)
            continue

        mid = ids_or_id
        try:
            msg = await safe_invoke(client, client.get_messages, source_entity, ids=mid)
        except Exception as e:
            log.warning("Could not fetch msg %s: %s", mid, e)
            continue

        uid = get_file_unique_id_from_msg(msg)
        if uid and uid in forwarded_file_uids:
            log.debug("Skipping message %s — uid already forwarded", mid)
            continue

        pending_batch.append(mid)
        log.info("Batch add: topic=%s append msg_id=%s (pending=%d/%s)", source_topic_id, mid, len(pending_batch), FORWARD_BATCH_SIZE)

        if len(pending_batch) >= FORWARD_BATCH_SIZE:
            log.info("Flushing batch: topic=%s sending %d messages (ids start=%s...)", source_topic_id, len(pending_batch), pending_batch[:3])
            await adaptive_forward_batch(client, target_entity, source_entity, pending_batch, target_topic_id)
            # update uids and persist after batch
            try:
                msgs_after = await safe_invoke(client, client.get_messages, source_entity, ids=pending_batch)
            except Exception:
                msgs_after = []
            for m in msgs_after:
                uid2 = get_file_unique_id_from_msg(m)
                if uid2:
                    forwarded_file_uids.add(uid2)
            try:
                with open(PERSIST_FORWARDED_UIDS_FILE, "w", encoding="utf-8") as f:
                    json.dump(list(forwarded_file_uids), f)
            except Exception as e:
                log.warning("Could not persist forwarded uids after batch: %s", e)
            pending_batch = []
            await asyncio.sleep(RATE_LIMIT_DELAY)

        processed += 1

    if pending_batch:
        log.info("Flushing final batch: topic=%s sending %d messages", source_topic_id, len(pending_batch))
        await adaptive_forward_batch(client, target_entity, source_entity, pending_batch, target_topic_id)
        try:
            msgs_after = await safe_invoke(client, client.get_messages, source_entity, ids=pending_batch)
        except Exception:
            msgs_after = []
        for m in msgs_after:
            uid2 = get_file_unique_id_from_msg(m)
            if uid2:
                forwarded_file_uids.add(uid2)
        try:
            with open(PERSIST_FORWARDED_UIDS_FILE, "w", encoding="utf-8") as f:
                json.dump(list(forwarded_file_uids), f)
        except Exception as e:
            log.warning("Could not persist forwarded uids after final batch: %s", e)
        pending_batch = []

    log.info("Finished processing topic %s (processed ~%s msgs)", source_topic_id, processed)

# ---------------- main ----------------
async def main():
    client = TelegramClient(session, api_id, api_hash)
    await client.start()
    if not await client.is_user_authorized():
        log.error("Session not authorized. Please authorize locally and copy session file.")
        await client.disconnect()
        return

    log.info("Connected as %s", await client.get_me())

    # resolve source entities
    src_entities = []
    for s in SOURCE_GROUPS:
        try:
            ent = await safe_invoke(client, client.get_entity, s)
            src_entities.append(ent)
        except Exception as e:
            log.exception("Could not resolve source entity '%s': %s", s, e)

    if not src_entities:
        log.error("No source entities resolved. Exiting.")
        await client.disconnect()
        return

    # resolve target entity
    try:
        tgt_entity = await safe_invoke(client, client.get_entity, TARGET_GROUP)
    except Exception as e:
        log.exception("Could not resolve target entity '%s': %s", TARGET_GROUP, e)
        await client.disconnect()
        return

    # gather topics from each source entity
    source_topics = []
    for ent in src_entities:
        try:
            ts = await list_forum_topics(client, ent)
            for t in ts:
                t["_source_entity"] = ent
                source_topics.append(t)
            log.info("Found %d topics for source %s", len(ts), getattr(ent, "id", str(ent)))
        except Exception as e:
            log.exception("Error listing topics for %s: %s", ent, e)

    if not source_topics:
        log.warning("No topics discovered across all source entities. Nothing to migrate.")
        await client.disconnect()
        return

    # ensure / create target topics and get mapping
    try:
        topic_map = await ensure_target_topics(client, tgt_entity, source_topics)
    except Exception as e:
        log.exception("Failed to ensure/create target topics: %s", e)
        topic_map = {}

    # process topics per source entity
    for ent in src_entities:
        ent_topics = [t for t in source_topics if t.get("_source_entity") == ent]
        if not ent_topics:
            log.info("No topics to process for source entity %s", getattr(ent, "id", str(ent)))
            continue

        for t in ent_topics:
            sid = int(t["id"])
            tt = topic_map.get(sid)
            log.info("Migrating source topic id=%s title='%s' -> target topic id=%s", sid, t.get("title", "‹no-title›"), tt)
            try:
                await process_topic(client, ent, tgt_entity, sid, tt)
            except Exception as e:
                # catch per-topic errors and continue with next topic
                log.exception("Unhandled error while processing topic %s (source %s): %s", sid, getattr(ent, "id", str(ent)), e)

    log.info("Migration done — disconnecting.")
    await client.disconnect()
