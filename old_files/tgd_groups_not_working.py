#!/usr/bin/env python3
# tgd_groups.py — Migration helper (placeholder-reply strategy)
# - Builds message_id -> source_topic_id map (single scan)
# - Ensures target topics + placeholder message per topic
# - Forwards messages grouped by source_topic_id, replying to placeholder
# - Persists state to JSON, persists forwarded file uids for dedupe
# - Logging to file (log_dir from config) and stdout, periodic progress prints

from __future__ import annotations
import asyncio
import json
import logging
import os
import sys
import time
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Set, Tuple, Any

from telethon import TelegramClient, errors
from telethon.tl.functions.channels import CreateForumTopicRequest, GetForumTopicsRequest
from telethon.tl.functions.messages import ForwardMessagesRequest
from telethon.tl.types import Message, InputReplyToMessage
from telethon.tl.functions.messages import SendMessageRequest
import random
# Try to import user config, fallback defaults if missing
#try:
from tgd_config import api_id, api_hash, session, SOURCE_GROUPS, TARGET_GROUP, RATE_LIMIT_DELAY, FORWARD_BATCH_SIZE, CONCURRENCY, FLOODWAIT_BUFFER, PERSIST_FORWARDED_UIDS_FILE, LOG_DIR, STATE_FILE

#except Exception:
#    # sensible defaults
#   api_id = None
#    api_hash = None
#    session = "session"
#    SOURCE_GROUPS = []
#    TARGET_GROUP = None
#    RATE_LIMIT_DELAY = 3.0
#    FORWARD_BATCH_SIZE = 50
#    CONCURRENCY = 1
#    FLOODWAIT_BUFFER = 5
#    PERSIST_FORWARDED_UIDS_FILE = "forwarded_uids.json"
#    LOG_DIR = "./logs"
#    STATE_FILE = "state.json"

# ensure numeric/config defaults
RATE_LIMIT_DELAY = RATE_LIMIT_DELAY if RATE_LIMIT_DELAY is not None else 3.0
FORWARD_BATCH_SIZE = FORWARD_BATCH_SIZE if FORWARD_BATCH_SIZE is not None else 50
CONCURRENCY = CONCURRENCY if CONCURRENCY is not None else 1
FLOODWAIT_BUFFER = FLOODWAIT_BUFFER if FLOODWAIT_BUFFER is not None else 5
PERSIST_FORWARDED_UIDS_FILE = PERSIST_FORWARDED_UIDS_FILE if PERSIST_FORWARDED_UIDS_FILE is not None else "forwarded_uids.json"
LOG_DIR = LOG_DIR if LOG_DIR is not None else "./logs"
STATE_FILE = STATE_FILE if STATE_FILE is not None else "state.json"

os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, f"groups_batched_{time.strftime('%Y-%m-%d_%H-%M-%S')}.log")
# --- Logging setup: file + stdout with same format ---
LOG_FMT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FMT, handlers=[])
root_logger = logging.getLogger()
# file handler
fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
fh.setFormatter(logging.Formatter(LOG_FMT))
root_logger.addHandler(fh)
# stdout (stream) handler
sh = logging.StreamHandler(sys.stdout)
sh.setFormatter(logging.Formatter(LOG_FMT))
root_logger.addHandler(sh)

log = logging.getLogger("groups_batched")
logging.getLogger("telethon").setLevel(logging.WARNING)

# --- quick config sanity output (DEBUG) ---
import pprint
log.info("Loaded config values:")
try:
    log.info("TARGET_GROUP=%s", TARGET_GROUP)
    log.info("SOURCE_GROUPS=%s", SOURCE_GROUPS)
    log.info("SESSION=%s", session)
    log.info("FORWARD_BATCH_SIZE=%s CONCURRENCY=%s RATE_LIMIT_DELAY=%s", FORWARD_BATCH_SIZE, CONCURRENCY, RATE_LIMIT_DELAY)
except Exception as e:
    log.exception("Config print failed: %s", e)

# When connecting, validate entity resolution early:
async def validate_entities(client):
    try:
        for g in SOURCE_GROUPS:
            try:
                ent = await client.get_entity(g)
                log.info("Validated source entity: %s -> %s", g, ent)
            except Exception as e:
                log.error("Could NOT resolve source '%s': %s", g, e)
        try:
            te = await client.get_entity(TARGET_GROUP)
            log.info("Validated target entity: %s -> %s", TARGET_GROUP, te)
        except Exception as e:
            log.error("Could NOT resolve target '%s': %s", TARGET_GROUP, e)
    except Exception:
        log.exception("validate_entities failed")

# Rufe validate_entities bei client-Startup auf (z.B. direkt nach async with TelegramClient(...) as client:)
# await validate_entities(client)

# --- Data classes for state ---
@dataclass
class TopicMapping:
    source_topic_id: int
    source_title: str
    target_topic_id: Optional[int]
    placeholder_msg_id: Optional[int]

@dataclass
class State:
    # mapping: source_topic_id (str) -> TopicMapping
    mappings: Dict[str, Dict[str, Any]]
    # optional: message_id -> source_topic_id map (persisted so we can resume)
    message_map: Dict[str, int]
    forwarded_uids: List[str]

    def to_json(self):
        return {
            "mappings": self.mappings,
            "message_map_len": len(self.message_map),
            "forwarded_uids_len": len(self.forwarded_uids),
        }

# load/save state helpers
def load_state(path: str) -> State:
    if not os.path.exists(path):
        return State(mappings={}, message_map={}, forwarded_uids=[])
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
            mappings = raw.get("mappings", {})
            message_map = raw.get("message_map", {})
            forwarded_uids = raw.get("forwarded_uids", [])
            return State(mappings=mappings, message_map=message_map, forwarded_uids=forwarded_uids)
    except Exception as e:
        log.warning("Could not load state file %s: %s — starting fresh", path, e)
        return State(mappings={}, message_map={}, forwarded_uids=[])

def save_state(path: str, state: State):
    tmp = path + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({
                "mappings": state.mappings,
                "message_map": state.message_map,
                "forwarded_uids": state.forwarded_uids
            }, f, indent=2, ensure_ascii=False)
        os.replace(tmp, path)
    except Exception as e:
        log.exception("Failed to save state to %s: %s", path, e)

# forwarded uids persistence (backwards-compatible)
def load_forwarded_uids(path: str) -> Set[str]:
    if not os.path.exists(path):
        return set()
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, list):
                return set(data)
    except Exception as e:
        log.warning("Could not load forwarded uids: %s", e)
    return set()

def save_forwarded_uids(path: str, uids: Set[str]):
    try:
        with open(path + ".tmp", "w", encoding="utf-8") as f:
            json.dump(list(uids), f, indent=2)
        os.replace(path + ".tmp", path)
    except Exception as e:
        log.warning("Could not save forwarded uids: %s", e)

# --- Telethon helpers: safe_invoke with FloodWait handling ---
async def handle_floodwait(e: errors.FloodWaitError):
    wait = int(getattr(e, "seconds", 0))
    total = wait + FLOODWAIT_BUFFER
    log.warning("FloodWaitError: sleeping %s seconds (+%s buffer)", wait, FLOODWAIT_BUFFER)
    await asyncio.sleep(total)

async def safe_invoke(func, *args, **kwargs):
    backoff = 1
    while True:
        try:
            return await func(*args, **kwargs)
        except errors.FloodWaitError as e:
            await handle_floodwait(e)
            backoff = 1
        except (errors.RPCError, ConnectionError, OSError, asyncio.TimeoutError) as e:
            log.warning("Transient error: %s — retrying in %s s", e, backoff)
            await asyncio.sleep(backoff)
            backoff = min(300, backoff * 2)
        except Exception:
            raise

# --- utilities for message/media unique id ---
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

# --- topic & placeholder helpers ---
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

async def ensure_target_topics_and_placeholders(client: TelegramClient, target_entity, source_topics: List[Dict[str, Any]], state: State) -> Dict[int, Tuple[int, int]]:
    """
    Ensure target topics exist and placeholder messages exist.
    Returns mapping: source_topic_id -> (target_topic_id, placeholder_msg_id)
    Updates state.mappings in-place.
    """
    existing = await list_forum_topics(client, target_entity)
    title_to_id = {normalize_title(t["title"]): t["id"] for t in existing}

    results: Dict[int, Tuple[int, int]] = {}
    for s in source_topics:
        sid = int(s["id"])
        stitle = s["title"]
        key = normalize_title(stitle)
        # target_topic_id: reuse by title match or create
        tgt_id = None
        if key in title_to_id:
            tgt_id = title_to_id[key]
            log.info("Found existing target topic '%s' id=%s for source %s", stitle, tgt_id, sid)
        else:
            # create it
            log.info("Creating target topic '%s' ...", stitle)
            try:
                async def _create(): return await client(CreateForumTopicRequest(channel=target_entity, title=stitle))
                await safe_invoke(_create)
                # refresh
                existing = await list_forum_topics(client, target_entity)
                title_to_id = {normalize_title(t["title"]): t["id"] for t in existing}
                if key in title_to_id:
                    tgt_id = title_to_id[key]
                    log.info("Created topic id=%s for '%s'", tgt_id, stitle)
                else:
                    # fallback: newest
                    if existing:
                        tgt_id = existing[-1]["id"]
                        log.warning("Fallback: using newest topic id=%s for '%s'", tgt_id, stitle)
                    else:
                        log.error("Failed to create topic and no existing topics for '%s'", stitle)
            except Exception as e:
                log.exception("Failed to create topic '%s': %s", stitle, e)
        # placeholder
        placeholder_id = None
        map_key = str(sid)
        if map_key in state.mappings:
            try:
                placeholder_id = state.mappings[map_key].get("placeholder_msg_id")
                tgt_existing = state.mappings[map_key].get("target_topic_id")
                if tgt_existing and tgt_existing != tgt_id:
                    log.info("Note: stored mapping target_topic_id %s differs from current %s; using current", tgt_existing, tgt_id)
            except Exception:
                placeholder_id = None

        if placeholder_id is None and tgt_id is not None:
            try:
                # send placeholder in that topic
                text = f"{stitle} {sid} placeholder"
                req = SendMessageRequest(                                                                                                                                                            peer=target_entity,                                                                                                                                                              message=text,
                    random_id=random.getrandbits(64),   # 64-bit random id, entspricht telethon intern
                    topic_id=tgt_id,
                    no_webpage=True,                    # optional, wie du magst
                )
                sent = await safe_invoke(client, req)
                placeholder_id = getattr(sent, "id", None)
                log.info("Created placeholder for source=%s in target_topic=%s placeholder_msg_id=%s", sid, tgt_id, placeholder_id)
            except Exception as e:
                log.exception("Could not create placeholder message for topic %s: %s", tgt_id, e)

        # update state mapping
        state.mappings[map_key] = {
            "source_topic_id": sid,
            "source_title": stitle,
            "target_topic_id": tgt_id,
            "placeholder_msg_id": placeholder_id
        }
        results[sid] = (tgt_id, placeholder_id)
    return results

def normalize_title(s: str) -> str:
    return " ".join(s.strip().lower().split())

# --- build top_message_map (top_message_id -> source_topic_id) ---
async def build_top_message_map(client: TelegramClient, source_entity) -> Dict[int, int]:
    mapping: Dict[int, int] = {}
    try:
        req = GetForumTopicsRequest(channel=source_entity, q="", offset_date=None, offset_id=0, offset_topic=0, limit=200)
        async def _call(): return await client(req)
        resp = await safe_invoke(_call)
    except Exception as e:
        log.warning("Could not build top_message_map for %s: %s", source_entity, e)
        return mapping
    topics = getattr(resp, "topics", None) or getattr(resp, "forum_topics", None) or []
    for t in topics:
        top_msg = getattr(t, "top_message", None)
        tid = getattr(t, "id", getattr(t, "topic_id", None))
        if top_msg and tid:
            mapping[int(top_msg)] = int(tid)
    log.info("Built top_message_map with %d entries for %s", len(mapping), source_entity)
    return mapping

# --- resolve topic by walking reply chain (used for detection) ---
async def resolve_topic_from_reply(client: TelegramClient, reply_msg_id: int, top_map: Dict[int,int], max_depth: int = 6) -> Optional[int]:
    depth = 0
    cur_id = reply_msg_id
    try:
        while cur_id is not None and depth < max_depth:
            try:
                m = await safe_invoke(client.get_messages, None, ids=cur_id)
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

# --- single full pass: build message_id -> source_topic_id mapping ---
async def build_message_map(client: TelegramClient, source_entity, top_map: Dict[int,int], state: State, progress: Dict[str,int]):
    """
    Walks through all messages in the source_entity and records msg.id -> topic_id in state.message_map.
    This is done only if state.message_map is empty (so we can resume).
    """
    if state.message_map:
        log.info("Message map already present (len=%s). Skipping full-scan.", len(state.message_map))
        return

    log.info("Starting full message scan for %s — this may take some time.", source_entity)
    checked = 0
    found = 0
    async for msg in client.iter_messages(source_entity, reverse=True):
        checked += 1
        progress["checked"] = checked
        if not isinstance(msg, Message):
            continue
        topic_for_msg = None
        # 1) direct
        forum_topic = getattr(msg, "forum_topic", None)
        if forum_topic:
            try:
                topic_for_msg = int(forum_topic)
            except Exception:
                topic_for_msg = None
        # 2) reply_to_top_id
        if topic_for_msg is None:
            rt_top = getattr(msg, "reply_to_top_id", None)
            if rt_top is not None:
                try:
                    rt_top_int = int(rt_top)
                    if rt_top_int in top_map:
                        topic_for_msg = top_map[rt_top_int]
                except Exception:
                    pass
        # 3) reply_to_msg_id chain
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
                except Exception:
                    pass
        # 4) msg.id itself is top_message
        try:
            if topic_for_msg is None and getattr(msg, "id", None) in top_map:
                topic_for_msg = top_map[msg.id]
        except Exception:
            pass

        if topic_for_msg is not None:
            state.message_map[str(msg.id)] = int(topic_for_msg)
            found += 1
            progress["found"] = found

        if checked % 1000 == 0:
            log.info("Full-scan progress: checked=%s found=%s", checked, found)
            # persist partial state to avoid losing work
            save_state(STATE_FILE, state)
    log.info("Full message scan finished: checked=%s found=%s", checked, found)
    save_state(STATE_FILE, state)

# --- forwarding helpers ---
async def forward_batch_as_reply(client: TelegramClient, from_entity, to_entity, msg_ids: List[int], placeholder_msg_id: Optional[int]):
    """
    Try ForwardMessagesRequest first with reply_to; fallback to client.forward_messages.
    """
    if not msg_ids:
        return True
    try:
        # use ForwardMessagesRequest for explicit reply_to
        req = ForwardMessagesRequest(from_peer=from_entity, id=msg_ids, to_peer=to_entity,
                                     reply_to=InputReplyToMessage(placeholder_msg_id) if placeholder_msg_id else None)
        res = await safe_invoke(client, req)
        log.info("ForwardMessagesRequest result: %s", getattr(res, "__class__", res))
        return True
    except errors.FloodWaitError as e:
        await handle_floodwait(e)
    except Exception as e:
        log.debug("ForwardMessagesRequest failed: %s — fallback to client.forward_messages", e)
        try:
            await safe_invoke(client.forward_messages, to_entity, msg_ids, from_entity)
            log.info("client.forward_messages used as fallback for %d ids", len(msg_ids))
            return True
        except Exception as e2:
            log.exception("Fallback forward_messages failed: %s", e2)
            return False

# --- main processing for topics (group by topic using message_map) ---
async def process_all_topics(client: TelegramClient, source_entity, target_entity, state: State, progress: Dict[str,int]):
    # group message ids by source_topic_id
    groups: Dict[int, List[int]] = {}
    for mid_str, topic_id in state.message_map.items():
        try:
            mid = int(mid_str)
            groups.setdefault(int(topic_id), []).append(mid)
        except Exception:
            continue

    total_topics = len(groups)
    log.info("Will forward messages for %d source topics (based on message_map)", total_topics)

    topics_done = 0
    forwarded_uids = set(state.forwarded_uids or [])
    for source_topic_str, mapping in state.mappings.items():
        source_topic_id = int(source_topic_str)
        target_topic_id = mapping.get("target_topic_id")
        placeholder_msg_id = mapping.get("placeholder_msg_id")
        if source_topic_id not in groups:
            log.info("No messages found for source topic %s — skipping", source_topic_id)
            topics_done += 1
            progress["topics_done"] = topics_done
            continue
        ids = sorted(groups[source_topic_id])
        log.info("Forwarding %d messages for source topic %s -> target_topic=%s (placeholder=%s)",
                 len(ids), source_topic_id, target_topic_id, placeholder_msg_id)

        # chunk into batches (FORWARD_BATCH_SIZE)
        chunk_size = min(max(1, FORWARD_BATCH_SIZE), 200)
        sent_count = 0
        for i in range(0, len(ids), chunk_size):
            chunk = ids[i:i+chunk_size]
            # dedupe chunk by uid: fetch messages and skip those whose uid already forwarded
            try:
                msgs = await safe_invoke(client.get_messages, source_entity, ids=chunk)
            except Exception:
                msgs = []
            filtered_chunk = []
            for m in msgs:
                uid = get_file_unique_id_from_msg(m)
                if uid and uid in forwarded_uids:
                    log.debug("Skipping msg %s — uid already forwarded", getattr(m, "id", None))
                    continue
                filtered_chunk.append(getattr(m, "id", None))
            if not filtered_chunk:
                continue
            success = await forward_batch_as_reply(client, source_entity, target_entity, filtered_chunk, placeholder_msg_id)
            if success:
                sent_count += len(filtered_chunk)
                # update forwarded_uids set
                try:
                    msgs_after = await safe_invoke(client.get_messages, source_entity, ids=filtered_chunk)
                except Exception:
                    msgs_after = []
                for m in msgs_after:
                    uid = get_file_unique_id_from_msg(m)
                    if uid:
                        forwarded_uids.add(uid)
                # persist forwarded uids periodically
                save_forwarded_uids(PERSIST_FORWARDED_UIDS_FILE, forwarded_uids)
            await asyncio.sleep(RATE_LIMIT_DELAY)
            progress["forwards"] = progress.get("forwards", 0) + len(filtered_chunk)
        log.info("Finished forwarding for source topic %s — sent ~%s messages", source_topic_id, sent_count)
        topics_done += 1
        progress["topics_done"] = topics_done
        # persist state after each topic
        state.forwarded_uids = list(forwarded_uids)
        save_state(STATE_FILE, state)

    log.info("All topics processed. topics_done=%s", topics_done)

# --- periodic status printer ---
async def status_printer(progress: Dict[str,int]):
    # prints to stdout/log at least every 10 seconds
    while True:
        checked = progress.get("checked", 0)
        found = progress.get("found", 0)
        forwards = progress.get("forwards", 0)
        topics_done = progress.get("topics_done", 0)
        msg = f"STATUS: scanned={checked} matched_msgs={found} forwards={forwards} topics_done={topics_done}"
        # print to stdout (handler already prints) — keep also as plain print for immediate visibility
        log.info(msg)
        # ensure raw stdout appearance too (some environments buffer)
        print(msg, file=sys.stdout)
        await asyncio.sleep(10)

# --- entrypoint ---
async def main():
    if not SOURCE_GROUPS or not TARGET_GROUP:
        log.error("SOURCE_GROUPS or TARGET_GROUP not configured in config.py. Aborting.")
        return

    state = load_state(STATE_FILE)
    forwarded_set = load_forwarded_uids(PERSIST_FORWARDED_UIDS_FILE)
    if state.mappings:
        log.info("Loaded %d topic mappings from state", len(state.mappings))
    else:
        log.info("No topic mappings in state (fresh run)")

    progress: Dict[str,int] = {"checked": 0, "found": 0, "forwards": 0, "topics_done": 0}

    client = TelegramClient(session, api_id, api_hash)
    await client.start()
    if not await client.is_user_authorized():
        log.error("Session not authorized. Please authorize locally and copy session file.")
        await client.disconnect()
        return
    log.info("Client started as %s", await client.get_me())

    # resolve entities
    src_entities = []
    for s in SOURCE_GROUPS:
        ent = await safe_invoke(client.get_entity, s)
        log.info("Source entity: %s", ent)
        src_entities.append(ent)
    tgt_entity = await safe_invoke(client.get_entity, TARGET_GROUP)
    log.info("Target entity: %s", tgt_entity)

    # gather topics from each source entity
    source_topics: List[Dict[str, Any]] = []
    for ent in src_entities:
        ts = await list_forum_topics(client, ent)
        for t in ts:
            t["_source_entity"] = ent
            source_topics.append(t)

    # ensure target topics and placeholders
    await ensure_target_topics_and_placeholders(client, tgt_entity, source_topics, state)
    save_state(STATE_FILE, state)

    # Build top maps per source and run a full-scan per source (message_map keyed by message id)
    # For simplicity we scan each source sequentially and aggregate into the same state.message_map
    for ent in src_entities:
        top_map = await build_top_message_map(client, ent)
        # Only run full-scan if no message_map present (so we can resume)
        await build_message_map(client, ent, top_map, state, progress)

    # start status printer task
    status_task = asyncio.create_task(status_printer(progress))

    # final: process all topics forward
    await process_all_topics(client, src_entities[0], tgt_entity, state, progress)

    # cleanup
    status_task.cancel()
    try:
        await status_task
    except Exception:
        pass

    # persist final state
    save_state(STATE_FILE, state)
    save_forwarded_uids(PERSIST_FORWARDED_UIDS_FILE, set(state.forwarded_uids or []))

    log.info("Migration finished — disconnecting.")
    await client.disconnect()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Interrupted by user")
