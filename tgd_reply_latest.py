#!/usr/bin/env python3
# coding: utf-8
"""
tgd_reply_latest.py — Telethon 1.42-ready (full file)

Reads tgd_placeholder.json and forwards the latest message from a source topic
into the target, attempting to place it into the same forum topic using the
topic anchor (top_message) when available.

Requirements:
 - tgd_config.py present with: session, api_id, api_hash, SOURCE_GROUPS (list)
 - tgd_placeholder.json created by tgd_placeholder.py
"""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from telethon import TelegramClient, functions, types, helpers

# --- Paths & config ---
PLACEHOLDER_META = Path("tgd_placeholder.json")
LOG_DIR = Path("logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "tgd_reply_latest.log"

try:
    import tgd_config as cfg
except Exception as e:
    raise SystemExit("Missing or invalid tgd_config.py: " + repr(e))

SESSION = getattr(cfg, "session", "user_session")
API_ID = getattr(cfg, "api_id", None)
API_HASH = getattr(cfg, "api_hash", None)
SOURCE_GROUPS = getattr(cfg, "SOURCE_GROUPS", [])
SEARCH_ITER_LIMIT = int(getattr(cfg, "SEARCH_ITER_LIMIT", 2000))
LOG_LEVEL = getattr(cfg, "LOG_LEVEL", "INFO")

# Logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s | %(levelname)s | tgd_reply_latest | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("tgd_reply_latest")


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


async def resolve_entity(client: TelegramClient, peer_identifier):
    ent = await client.get_entity(peer_identifier)
    inp = await client.get_input_entity(ent)
    return ent, inp


async def find_candidate_message(
    client: TelegramClient,
    source_entity,
    source_input,
    source_topic_id: int,
    search_limit: int = SEARCH_ITER_LIMIT
) -> Optional[types.Message]:
    """
    Find a candidate (top_message / representative message) for a given source topic id.
    Order:
      1) messages.GetForumTopicsByIDRequest(peer, topics=[topic_id])
      2) messages.GetForumTopicsRequest(...) list & match
      3) channels.GetForumTopicsRequest(...) fallback (Telethon 1.41)
      4) iter_messages scanning with many heuristics
    Returns a telethon.types.Message or None.
    """
    # 1) Try direct by-ID
    try:
        logger.info("Attempting messages.GetForumTopicsByIDRequest for topic=%s", source_topic_id)
        resp = await client(functions.messages.GetForumTopicsByIDRequest(peer=source_input, topics=[source_topic_id]))
        topics = getattr(resp, "topics", None)
        if topics:
            t = topics[0]
            top_msg = getattr(t, "top_message", None) or getattr(t, "top_message_id", None)
            logger.debug("messages.GetForumTopicsByIDRequest -> top_message=%s", top_msg)
            if top_msg:
                msgs = await client.get_messages(source_entity, ids=[top_msg])
                if msgs:
                    return msgs[0]
    except Exception as e:
        logger.debug("messages.GetForumTopicsByIDRequest unavailable/failed: %s", e)

    # # 2) Try listing topics via messages.GetForumTopicsRequest (Telethon 1.42+)
    # try:
    #     logger.info("Attempting functions.messages.GetForumTopicsRequest (list topics)")
    #     resp = await client(functions.messages.GetForumTopicsRequest(
    #         peer=source_input, offset_date=0, offset_id=0, offset_topic=0, limit=200
    #     ))
    #     topics = getattr(resp, "topics", None)
    #     if topics:
    #         for t in topics:
    #             t_id = getattr(t, "id", None) or getattr(t, "topic_id", None)
    #             if t_id == source_topic_id:
    #                 top_msg = getattr(t, "top_message", None) or getattr(t, "top_message_id", None)
    #                 logger.debug("messages.GetForumTopicsRequest matched topic %s -> top_message=%s", t_id, top_msg)
    #                 if top_msg:
    #                     msgs = await client.get_messages(source_entity, ids=[top_msg])
    #                     if msgs:
    #                         return msgs[0]
    # except Exception as e:
    #     logger.debug("messages.GetForumTopicsRequest unavailable/failed: %s", e)

    # # 3) Fallback to channels.* (Telethon 1.41)
    # try:
    #     logger.info("Attempting channels.GetForumTopicsRequest (fallback)")
    #     resp = await client(functions.channels.GetForumTopicsRequest(
    #         channel=source_input, offset_date=0, offset_id=0, offset_topic=0, limit=1000
    #     ))
    #     topics = getattr(resp, "topics", None)
    #     if topics:
    #         for t in topics:
    #             t_id = getattr(t, "id", None) or getattr(t, "topic_id", None)
    #             if t_id == source_topic_id:
    #                 top_msg = getattr(t, "top_message", None) or getattr(t, "top_message_id", None)
    #                 logger.debug("channels.GetForumTopicsRequest matched topic %s -> top_message=%s", t_id, top_msg)
    #                 if top_msg:
    #                     msgs = await client.get_messages(source_entity, ids=[top_msg])
    #                     if msgs:
    #                         return msgs[0]
    # except Exception as e:
    #     logger.debug("channels.GetForumTopicsRequest failed/absent: %s", e)

    # 4) Last resort: iter_messages scan with extended heuristics
    logger.warning("Falling back to iter_messages scanning for messages belonging to topic %s (limit=%s)", source_topic_id, search_limit)
    checked = 0
    async for msg in client.iter_messages(source_entity, limit=search_limit):
        checked += 1
        if checked % 250 == 0:
            logger.info("iter_messages: checked %s messages so far...", checked)

        # Gather candidate attribute values in many shapes
        candidates = []

        for attr_name in ("reply_to_top_id", "reply_to_msg_id", "topic_id", "thread_id", "forum_topic_id"):
            candidates.append(getattr(msg, attr_name, None))

        forum_topic = getattr(msg, "forum_topic", None)
        if forum_topic is not None:
            if hasattr(forum_topic, "id"):
                candidates.append(getattr(forum_topic, "id", None))
            if hasattr(forum_topic, "top_message"):
                candidates.append(getattr(forum_topic, "top_message", None))
            try:
                if isinstance(forum_topic, dict):
                    candidates.append(forum_topic.get("id"))
                    candidates.append(forum_topic.get("top_message"))
            except Exception:
                pass

        # Try msg.to_dict() keys (safe guarded)
        try:
            md = msg.to_dict()
            for k in ("reply_to_top_id", "topic_id", "thread_id", "forum_topic", "forum_topic_id"):
                if k in md:
                    candidates.append(md.get(k))
            ft = md.get("forum_topic")
            if isinstance(ft, dict):
                candidates.append(ft.get("id"))
                candidates.append(ft.get("top_message"))
        except Exception:
            pass

        # Extract numbers from message text as a last-ditch heuristic
        try:
            st = getattr(msg, "message", "")
            if st:
                import re
                found = re.findall(r"\b(\d{2,6})\b", st)
                for f in found:
                    try:
                        candidates.append(int(f))
                    except Exception:
                        pass
        except Exception:
            pass

        # Normalize and test
        for att in candidates:
            if att is None:
                continue
            # Unwrap iterables
            try:
                if isinstance(att, (list, tuple, set)):
                    iter_values = list(att)
                else:
                    iter_values = [att]
            except Exception:
                iter_values = [att]

            for val in iter_values:
                try:
                    if isinstance(val, int) and val == source_topic_id:
                        logger.info("iter_messages: matched int attribute on message id=%s (checked=%s): %s", getattr(msg, "id", None), checked, val)
                        return msg
                    if isinstance(val, str) and val.isdigit() and int(val) == source_topic_id:
                        logger.info("iter_messages: matched numeric-string attribute on message id=%s (checked=%s): %s", getattr(msg, "id", None), checked, val)
                        return msg
                except Exception:
                    pass
                try:
                    if hasattr(val, "id") and getattr(val, "id") == source_topic_id:
                        logger.info("iter_messages: matched nested.id on message id=%s (checked=%s)", getattr(msg, "id", None), checked)
                        return msg
                    if hasattr(val, "top_message") and getattr(val, "top_message") == source_topic_id:
                        logger.info("iter_messages: matched nested.top_message on message id=%s (checked=%s)", getattr(msg, "id", None), checked)
                        return msg
                except Exception:
                    pass

    logger.error("iter_messages scan completed (%s messages checked) without finding a matching message for topic %s", checked, source_topic_id)
    return None


async def forward_candidate(
    client: TelegramClient,
    source_entity,
    source_input,
    target_entity,
    target_input,
    candidate_msg: types.Message,
    placeholder_msg_id: int,
    placeholder_top_msg_id: Optional[int] = None
) -> bool:
    """
    Forward candidate_msg so it appears inside the target topic thread.
    Strategy (preferred -> fallback):
      A) low-level ForwardMessagesRequest with reply_to=InputReplyToMessage(reply_to_msg_id=0, top_msg_id=...)
         (this places the forwarded message into the topic anchor)
      B) low-level ForwardMessagesRequest with reply_to=InputReplyToMessage(reply_to_msg_id=placeholder_msg_id)
      C) CopyMessagesRequest + small reply to placeholder (best-effort)
      D) Quoted-text fallback
    Note: we still call high-level client.forward_messages once earlier (best-effort), but prefer low-level with top_msg_id.
    """
    logger.info("Forwarding candidate id=%s (source) to target; reply_to placeholder id=%s top_msg=%s",
                candidate_msg.id, placeholder_msg_id, placeholder_top_msg_id)

    # Try high-level forward first (no explicit reply) - often works but doesn't guarantee topic placement
    try:
        await client.forward_messages(entity=target_entity, messages=candidate_msg.id, from_peer=source_entity)
        logger.info("High-level client.forward_messages succeeded (no explicit reply).")
    except Exception as e_high:
        logger.debug("High-level forward_messages failed/unsupported: %s", e_high)

    # Preferred: low-level ForwardMessagesRequest with top_msg_id (anchor). Use reply_to_msg_id=0 + top_msg_id.
    if placeholder_top_msg_id is not None:
        try:
            logger.info("Attempting low-level ForwardMessagesRequest with top_msg_id=%s", placeholder_top_msg_id)
            resp = await client(functions.messages.ForwardMessagesRequest(
                from_peer=source_input,
                id=[candidate_msg.id],
                to_peer=target_input,
                random_id=[helpers.generate_random_long()],
                reply_to=types.InputReplyToMessage(reply_to_msg_id=0, top_msg_id=placeholder_top_msg_id)
            ))
            async def verify_and_fallback_after_forward(client, target_ent, target_input, placeholder_msg_id, placeholder_top_msg_id, forward_resp, candidate_msg):
                """
                Verifies forwarded message placement and attempts fallbacks.
                - target_ent: resolved entity (high-level) for get_messages
                - target_input: InputPeer version for low-level calls
                - placeholder_msg_id: int (message id of placeholder in target)
                - placeholder_top_msg_id: int or None (top_message anchor)
                - forward_resp: response object returned from ForwardMessagesRequest (may be Updates)
                - candidate_msg: original source message (telethon.types.Message)
                """
                logger = logging.getLogger("tgd_reply_latest")
                new_msg_id = None

                # 1) Try to extract created message id(s) from the forward response
                try:
                    # Updates may contain UpdateMessageID which maps old->new id, or UpdateNewChannelMessage containing msg
                    if forward_resp is None:
                        logger.debug("No low-level response object available to inspect.")
                    else:
                        # attempt common patterns
                        if hasattr(forward_resp, "updates"):
                            for u in forward_resp.updates:
                                # UpdateMessageID maps msg_id -> new_id
                                if isinstance(u, types.UpdateMessageID):
                                    # UpdateMessageID has 'id' and 'random_id' or 'message' depending; try available attrs
                                    # Note: Telethon's UpdateMessageID fields vary; prefer UpdateNewChannelMessage below
                                    try:
                                        new_msg_id = getattr(u, "id", None)
                                    except:
                                        pass
                                if isinstance(u, types.UpdateNewChannelMessage) or isinstance(u, types.UpdateNewChannelMessage):
                                    # update contains 'message' attribute
                                    msg = getattr(u, "message", None)
                                    if msg:
                                        new_msg_id = getattr(msg, "id", None)
                        # Some ForwardMessagesRequest return telethon.tl.types.Updates with UpdateNewMessage objects
                        # If still None, leave it and attempt to fetch recent messages instead.
                except Exception as e:
                    logger.debug("Failed to parse forward_resp for new id: %s", e)

                # 2) If we didn't obtain a new_msg_id, attempt to get the most recent messages and identify candidate by text / timestamp
                if new_msg_id is None:
                    try:
                        # fetch last N messages to find a likely match
                        recent = await client.get_messages(target_ent, limit=10)
                        # Heuristic: find a message with same text or from same original author preview and nearly same timestamp
                        for m in recent:
                            # compare textual content to candidate or check reply_to etc
                            if candidate_msg.message and m.message and candidate_msg.message.strip() == m.message.strip():
                                new_msg_id = m.id
                                logger.info("Heuristic matched forwarded message as id=%s (by identical text).", new_msg_id)
                                break
                    except Exception as e:
                        logger.debug("Failed to heuristic-match recent messages: %s", e)

                # 3) If we have a new_msg_id, query message and inspect fields
                inspected = None
                if new_msg_id is not None:
                    try:
                        inspected = await client.get_messages(target_ent, ids=new_msg_id)
                        logger.info("Inspected forwarded message id=%s: to_dict keys: %s", new_msg_id, list(inspected.to_dict().keys()))
                        logger.info("Message raw dict: %s", inspected.to_dict())
                        # Fields of interest to log explicitly (names vary between versions)
                        # topic_id/thread_id, reply_to_msg_id, reply_to_top_id, peer_id
                        logger.info("Fields -> id=%s topic_id=%s reply_to_msg_id=%s reply_to_top_id=%s",
                                    inspected.id,
                                    getattr(inspected, "topic_id", None),
                                    getattr(inspected, "reply_to_msg_id", None),
                                    getattr(inspected, "reply_to_top_id", None))
                        # If topic_id or reply_to_top_id present and equals placeholder_top_msg_id -> success
                        topic_ok = (getattr(inspected, "topic_id", None) is not None) or (getattr(inspected, "reply_to_top_id", None) == placeholder_top_msg_id) or (getattr(inspected, "reply_to_msg_id", None) == placeholder_msg_id)
                        if topic_ok:
                            logger.info("Forward landed in topic/thread as expected.")
                            return True
                        else:
                            logger.warning("Forward did NOT land in topic/thread (fields show no topic).")
                    except Exception as e:
                        logger.debug("Failed to inspect new message: %s", e)

                # 4) Fallback A: try CopyMessagesRequest with reply_to top_msg_id (preserve media)
                try:
                    if placeholder_top_msg_id is not None:
                        logger.info("Attempting CopyMessagesRequest fallback with top_msg_id=%s", placeholder_top_msg_id)
                        copy_resp = await client(functions.messages.CopyMessagesRequest(
                            from_peer=source_input,    # NOTE: must be available in scope; else pass as param
                            id=[candidate_msg.id],
                            to_peer=target_input,
                            random_id=[helpers.generate_random_long()]
                        ))
                        # After copying, send a tiny reply anchored to top to force thread placement
                        await client.send_message(target_ent, "(copied)", reply_to=placeholder_msg_id)
                        logger.info("CopyMessagesRequest fallback executed.")
                        return True
                except Exception as e:
                    logger.debug("CopyMessagesRequest fallback failed: %s", e)

                # 5) Fallback B: quoted-text reply (worst-case)
                try:
                    logger.info("Attempting quoted-text fallback reply to anchor inside topic.")
                    text = candidate_msg.message or "(forward fallback)"
                    # limit length
                    text = text[:800]
                    await client.send_message(target_ent, "Forward fallback:\n\n" + text, reply_to=placeholder_msg_id)
                    logger.info("Quoted fallback posted as reply to placeholder_msg_id=%s", placeholder_msg_id)
                    return True
                except Exception as e:
                    logger.debug("Quoted fallback failed: %s", e)

                logger.error("All verification/fallback attempts failed for forwarded candidate.")
                return False
            logger.info("Low-level ForwardMessagesRequest succeeded with top_msg_id (type=%s)", type(resp).__name__)
            return True
        except Exception as e_top:
            logger.debug("Forward with top_msg_id failed: %s", e_top)

    # Next: low-level ForwardMessagesRequest with reply_to_msg_id (placeholder)
    if placeholder_msg_id:
        try:
            logger.info("Attempting low-level ForwardMessagesRequest with reply_to_msg_id=%s", placeholder_msg_id)
            resp = await client(functions.messages.ForwardMessagesRequest(
                from_peer=source_input,
                id=[candidate_msg.id],
                to_peer=target_input,
                random_id=[helpers.generate_random_long()],
                reply_to=types.InputReplyToMessage(reply_to_msg_id=placeholder_msg_id)
            ))
            logger.info("Low-level ForwardMessagesRequest succeeded with reply_to_msg_id (type=%s)", type(resp).__name__)
            return True
        except Exception as e_low:
            logger.debug("Forward with reply_to_msg_id failed: %s", e_low)

    # CopyMessagesRequest fallback (preserves media). Then post a small reply to anchor it.
    try:
        logger.info("Attempting CopyMessagesRequest + small reply fallback")
        copy_resp = await client(functions.messages.CopyMessagesRequest(
            from_peer=source_input,
            id=[candidate_msg.id],
            to_peer=target_input,
            random_id=[helpers.generate_random_long()]
        ))
        if placeholder_msg_id:
            await client.send_message(target_entity, "(copied)", reply_to=placeholder_msg_id)
        logger.info("CopyMessagesRequest fallback succeeded.")
        return True
    except Exception as e_copy:
        logger.debug("CopyMessagesRequest failed or unavailable: %s", e_copy)

    # Final quoted-text fallback
    try:
        if candidate_msg.message:
            quote = f"Forward (fallback):\n\n{candidate_msg.message[:800]}"
            await client.send_message(target_entity, quote, reply_to=placeholder_msg_id)
            logger.info("Fallback quoted reply posted.")
            return True
    except Exception as e_q:
        logger.debug("Fallback quoted reply failed: %s", e_q)

    logger.error("All forward/reply attempts failed for candidate id=%s", candidate_msg.id)
    return False


async def main():
    logger.info("Starting tgd_reply_latest at %s", _now_iso())

    if not PLACEHOLDER_META.exists():
        logger.error("Placeholder metadata file '%s' not found. Run tgd_placeholder.py first.", PLACEHOLDER_META)
        return

    meta = json.loads(PLACEHOLDER_META.read_text(encoding="utf-8"))
    placeholder_msg_id = meta.get("target_message_id")
    target_peer = meta.get("target_peer") or meta.get("target_channel_id")
    placeholder_top_msg = meta.get("topic_top_message")  # anchor top_message id for placing into topic
    requested_topic = meta.get("requested_topic", None)

    if not (placeholder_msg_id and target_peer):
        logger.error("Placeholder metadata incomplete: %s", meta)
        return

    if not SOURCE_GROUPS:
        logger.error("tgd_config.SOURCE_GROUPS is empty or unset.")
        return

    source_link = SOURCE_GROUPS[0]

    # determine source topic id: config overrides placeholder.requested_topic
    source_topic_id = getattr(cfg, "SOURCE_TOPIC_ID", None) or requested_topic
    if source_topic_id is None:
        logger.error("No source topic id available. Set cfg.SOURCE_TOPIC_ID or ensure tgd_placeholder.json contains requested_topic.")
        return

    client = TelegramClient(SESSION, API_ID, API_HASH)
    await client.start()
    logger.info("Client started")

    try:
        source_entity, source_input = await resolve_entity(client, source_link)
        target_entity, target_input = await resolve_entity(client, target_peer)
        logger.info("Resolved source=%s target=%s", getattr(source_entity, "id", source_entity), getattr(target_entity, "id", target_entity))
    except Exception as e:
        logger.exception("Failed to resolve entities: %s", e)
        await client.disconnect()
        return

    candidate = await find_candidate_message(client, source_entity, source_input, source_topic_id)
    if candidate is None:
        logger.error("Could not find any candidate message in source topic %s", source_topic_id)
        await client.disconnect()
        return

    logger.info("Candidate message selected id=%s date=%s", getattr(candidate, "id", None), getattr(candidate, "date", None))
# ensure placeholder_top_msg (top_message anchor) is known
    if not placeholder_top_msg and requested_topic:
        try:
            logger.info("placeholder_top_msg missing — resolving top_message for requested_topic=%s via GetForumTopicsByIDRequest", requested_topic)
            resp = await client(functions.messages.GetForumTopicsByIDRequest(peer=target_input, topics=[requested_topic]))
            topics = getattr(resp, "topics", None)
            if topics and len(topics) > 0:
                t = topics[0]
                placeholder_top_msg = getattr(t, "top_message", None) or getattr(t, "top_message_id", None)
                logger.info("resolved top_message=%s for requested_topic=%s", placeholder_top_msg, requested_topic)
            else:
                # try listing topics as fallback
                resp2 = await client(functions.messages.GetForumTopicsRequest(peer=target_input, offset_date=0, offset_id=0, offset_topic=0, limit=200))
                for t in getattr(resp2, "topics", []) or []:
                    tid = getattr(t, "id", None) or getattr(t, "topic_id", None)
                    if tid == requested_topic:
                        placeholder_top_msg = getattr(t, "top_message", None) or getattr(t, "top_message_id", None)
                        logger.info("resolved top_message via GetForumTopicsRequest -> %s", placeholder_top_msg)
                        break
        except Exception as e:
            logger.warning("Failed to resolve top_message at runtime: %s", e)

    # pass requested_topic (UI/topic id) / placeholder_top_msg to forward_candidate so it tries top_msg_id first
    ok = await forward_candidate(
        client,
        source_entity,
        source_input,
        target_entity,
        target_input,
        candidate,
        placeholder_msg_id,
        placeholder_top_msg  # top_message anchor from tgd_placeholder.json
    )
    if not ok:
        logger.error("Forward/reply process failed for candidate id=%s", candidate.id)
    else:
        logger.info("Forward/reply completed for candidate id=%s", candidate.id)

    await client.disconnect()
    logger.info("Client disconnected — done.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.warning("Interrupted by user")
