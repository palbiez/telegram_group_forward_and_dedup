#!/usr/bin/env python3
# coding: utf-8
"""
tgd_placeholder.py
Create a placeholder message in a forum topic and write metadata including the topic's top_message id
(needed for reliably forwarding into the same forum topic). Compatible with Telethon 1.41 and 1.42+.
Writes tgd_placeholder.json with fields:
 - target_channel_id, target_message_id, requested_topic (topic id), topic_top_message (top_message id),
 - posted_with_topic (bool), target_peer, ts
"""
import asyncio
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from telethon import TelegramClient, functions, types

# user config import (tgd_config.py must exist)
try:
    import tgd_config as cfg
except Exception as e:
    raise SystemExit("Missing tgd_config.py: " + repr(e))

SESSION = getattr(cfg, "session", "user_session")
API_ID = cfg.api_id
API_HASH = cfg.api_hash
TARGET = getattr(cfg, "TARGET_GROUP", None) or getattr(cfg, "TARGET_PEER", None)
REQUESTED_TOPIC = getattr(cfg, "TARGET_TOPIC", None) or getattr(cfg, "REQUESTED_TOPIC", None)  # numeric topic id (UI/topic.id)
LOG_LEVEL = getattr(cfg, "LOG_LEVEL", "INFO")

PLACEHOLDER_META = Path("tgd_placeholder.json")

logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s | %(levelname)s | tgd_post_placeholder | %(message)s")
logger = logging.getLogger("tgd_post_placeholder")


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


async def resolve_entity(client, peer):
    ent = await client.get_entity(peer)
    inp = await client.get_input_entity(ent)
    return ent, inp


async def find_topic_top_message(client, target_input, topic_id):
    """
    Try messages.GetForumTopicsByIDRequest (Telethon 1.42+), else channels.GetForumTopicsRequest (1.41)
    Return the top_message id (int) or None.
    """
    try:
        logger.info("Trying functions.messages.GetForumTopicsByIDRequest for topic=%s", topic_id)
        resp = await client(functions.messages.GetForumTopicsByIDRequest(peer=target_input, topics=[topic_id]))
        topics = getattr(resp, "topics", None)
        if topics:
            t = topics[0]
            top_msg = getattr(t, "top_message", None) or getattr(t, "top_message_id", None)
            logger.info("Got top_message=%s from messages.GetForumTopicsByIDRequest", top_msg)
            return top_msg
    except Exception as e:
        logger.debug("messages.GetForumTopicsByIDRequest not available/failed: %s", e)
    try:
        logger.info("Trying functions.channels.GetForumTopicsRequest (fallback) ...")
        resp = await client(functions.channels.GetForumTopicsRequest(channel=target_input, offset_date=0, offset_id=0, offset_topic=0, limit=200))
        topics = getattr(resp, "topics", None)
        if topics:
            for t in topics:
                t_id = getattr(t, "id", None) or getattr(t, "topic_id", None)
                if t_id == topic_id:
                    top_msg = getattr(t, "top_message", None) or getattr(t, "top_message_id", None)
                    logger.info("Got top_message=%s from channels.GetForumTopicsRequest", top_msg)
                    return top_msg
    except Exception as e:
        logger.debug("channels.GetForumTopicsRequest not available/failed: %s", e)
    logger.warning("Could not determine top_message for topic_id=%s", topic_id)
    return None


async def main():
    logger.info("Starting tgd_post_placeholder at %s", _now_iso())
    if TARGET is None:
        logger.error("TARGET_GROUP / TARGET_PEER not set in tgd_config.py")
        return

    client = TelegramClient(SESSION, API_ID, API_HASH)
    await client.start()
    logger.info("Client started")

    try:
        target_ent, target_input = await resolve_entity(client, TARGET)
        logger.info("Resolved target peer: %s", getattr(target_ent, "id", TARGET))
    except Exception as e:
        logger.exception("Failed to resolve target: %s", e)
        await client.disconnect()
        return

    posted_with_topic = False
    sent_msg = None

    # Try high-level send with explicit topic (Telethon versions that support it)
    if REQUESTED_TOPIC is not None:
        try:
            logger.info("Trying high-level client.send_message(..., topic=%s)", REQUESTED_TOPIC)
            # Some Telethon versions accept `topic` or `thread`; try common combos, but wrap in try/except.
            sent_msg = await client.send_message(
                target_ent,
                "PLACEHOLDER: topic={} (will be used as anchor) - created at {}".format(REQUESTED_TOPIC, _now_iso()),
                topic=REQUESTED_TOPIC
            )
            logger.info("High-level send_message returned id=%s", getattr(sent_msg, "id", None))
            posted_with_topic = True
        except TypeError as te:
            logger.debug("send_message signature did not accept topic/thread: %s", te)
        except Exception as e:
            logger.debug("High-level send_message(..., topic=...) failed: %s", e)

    # Fallback: try to send as reply_to the topic (some Telethon builds accept reply_to=topic)
    if sent_msg is None and REQUESTED_TOPIC is not None:
        try:
            logger.info("Fallback: attempting to send via reply_to (topic)")
            sent_msg = await client.send_message(
                target_ent,
                "PLACEHOLDER: topic={} (will be used as anchor) - created at {}".format(REQUESTED_TOPIC, _now_iso()),
                reply_to=REQUESTED_TOPIC
            )
            logger.info("High-level send_message(..., reply_to=topic) succeeded id=%s", getattr(sent_msg, "id", None))
            posted_with_topic = True
        except Exception as e:
            logger.debug("Fallback send_message(..., reply_to=topic) failed: %s", e)

    # Final fallback: simple send_message without topic/reply
    if sent_msg is None:
        sentmsg = await client.send_message(target_ent, "PLACEHOLDER (no topic support) - created at {}".format(_now_iso()))
        logger.info("Fallback: sent placeholder without explicit topic id; id=%s", getattr(sent_msg, "id", None))

    # Determine top_message id for the requested topic (if any)
    # ------------------------------------------------------------------
    # Determine top_message id for the requested topic (if any)
    # - Try immediately, then retry a few times (best-effort) because the
    #   server may need a short moment to register the new placeholder/topic.
    # - Log clearly what we resolved so other tools (tgd_reply_latest) can rely on it.
    # ------------------------------------------------------------------
    topic_top_message = None
    if REQUESTED_TOPIC is not None:
        # 1) First, attempt to resolve before/after posting (fast path)
        try:
            topic_top_message = await find_topic_top_message(client, target_input, REQUESTED_TOPIC)
        except Exception as e:
            logger.debug("Initial find_topic_top_message failed: %s", e)

        # 2) If not found, try a small retry loop (server-side state propagation)
        if topic_top_message is None:
            max_retries = 3
            delay_secs = 1.0
            for attempt in range(1, max_retries + 1):
                logger.info("top_message not found yet for topic=%s; retry attempt %s/%s (sleep %.1fs)",
                            REQUESTED_TOPIC, attempt, max_retries, delay_secs)
                await asyncio.sleep(delay_secs)
                try:
                    topic_top_message = await find_topic_top_message(client, target_input, REQUESTED_TOPIC)
                except Exception as e:
                    logger.debug("find_topic_top_message retry %s failed: %s", attempt, e)
                if topic_top_message is not None:
                    logger.info("Resolved topic_top_message=%s for topic=%s on attempt %s", topic_top_message, REQUESTED_TOPIC, attempt)
                    break
                # Exponential or linear backoff
                delay_secs *= 2

        # 3) Final fallback: try listing topics (if helper didn't)
        if topic_top_message is None:
            try:
                logger.info("Final fallback: list forum topics to find topic=%s", REQUESTED_TOPIC)
                resp = await client(functions.messages.GetForumTopicsRequest(peer=target_input, offset_date=0, offset_id=0, offset_topic=0, limit=200))
                topics = getattr(resp, "topics", None)
                if topics:
                    for t in topics:
                        t_id = getattr(t, "id", None) or getattr(t, "topic_id", None)
                        if t_id == REQUESTED_TOPIC:
                            topic_top_message = getattr(t, "top_message", None) or getattr(t, "top_message_id", None)
                            logger.info("Found topic_top_message=%s via GetForumTopicsRequest listing", topic_top_message)
                            break
            except Exception as e:
                logger.debug("Final listing fallback failed: %s", e)

    # Write metadata (include whatever top_message we resolved or None)
    meta = {
        "target_channel_id": getattr(target_ent, "id", None),
        "target_message_id": getattr(sent_msg, "id", None),
        "requested_topic": REQUESTED_TOPIC,
        "topic_top_message": topic_top_message,
        "posted_with_topic": posted_with_topic,
        "target_peer": TARGET,
        "ts": _now_iso()
    }
    PLACEHOLDER_META.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    logger.info("Wrote placeholder metadata to %s (topic_top_message=%s)", PLACEHOLDER_META, topic_top_message)

    await client.disconnect()
    logger.info("Client disconnected ▒ done.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.warning("Interrupted by user")

