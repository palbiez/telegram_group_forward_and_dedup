# tgd_reply_latest.py
"""
Reply the newest message from source topic 3350 as a forward/reply to the placeholder message
created earlier in tgd_post_placeholder.py.

Behavior:
- Loads placeholder info from tgd_placeholder.json to know which message to reply to.
- Resolves source group (link in tgd_config.SOURCE_GROUPS -- first entry or specific one).
- Tries channels.GetForumTopicsByIDRequest to fetch topic metadata (may include 'top_message').
- If that returns a concrete top_message id, fetch that message and forward it as a reply.
- Otherwise fallback: iter_messages over the source channel (newest first) and filter by
  attributes commonly used to signal topic membership (forum_topic, reply_to_top_id, thread_id).
- Forward the found message to the target channel, replying to the placeholder message id.
- Logging to LOG_DIR / tgd_reply_latest.log
"""
import asyncio
import json
import logging
from pathlib import Path
from datetime import datetime
from telethon import TelegramClient, functions, utils
from telethon.tl import functions as tl_functions

# import user config
try:
    import tgd_config as cfg
except Exception as e:
    raise SystemExit("Could not import tgd_config.py: " + repr(e))

LOG_DIR = Path(cfg.LOG_DIR if hasattr(cfg, "LOG_DIR") else "logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)
log_file = LOG_DIR / "tgd_reply_latest.log"

logging.basicConfig(
    level=getattr(logging, getattr(cfg, "LOG_LEVEL", "INFO")),
    format="%(asctime)s | %(levelname)s | tgd_reply_latest | %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding="utf-8"),
        logging.StreamHandler()
    ],
)

logger = logging.getLogger("tgd_reply_latest")

PLACEHOLDER_JSON = Path("tgd_placeholder.json")
SOURCE_LINK = None
SOURCE_TOPIC_ID = 3350
SEARCH_ITER_LIMIT = 5000  # safety limit for message scanning


async def main():
    logger.info("Starting tgd_reply_latest")

    # Check placeholder meta
    if not PLACEHOLDER_JSON.exists():
        logger.error("Placeholder metadata %s not found. Run tgd_post_placeholder first.", PLACEHOLDER_JSON)
        return
    meta = json.loads(PLACEHOLDER_JSON.read_text())

    target_peer = meta.get("target_peer") or meta.get("target_channel_id")
    placeholder_msg_id = meta.get("target_message_id")
    if not (target_peer and placeholder_msg_id):
        logger.error("Placeholder metadata incomplete: %s", meta)
        return

    # Determine source link: use first SOURCE_GROUP from config if available OR explicit string
    if getattr(cfg, "SOURCE_GROUPS", None):
        if isinstance(cfg.SOURCE_GROUPS, (list, tuple)) and len(cfg.SOURCE_GROUPS) > 0:
            SOURCE_LINK = cfg.SOURCE_GROUPS[0]
    if not SOURCE_LINK:
        logger.error("No SOURCE_GROUP configured in tgd_config.SOURCE_GROUPS. Aborting.")
        return

    client = TelegramClient(cfg.session, cfg.api_id, cfg.api_hash)
    await client.start()
    logger.info("Client started")

    # resolve entities
    try:
        source_entity = await client.get_entity(SOURCE_LINK)
        target_entity = await client.get_entity(target_peer)
        source_input = await client.get_input_entity(source_entity)
        target_input = await client.get_input_entity(target_entity)
        logger.info("Resolved source entity: %s", repr(source_entity))
        logger.info("Resolved target entity: %s", repr(target_entity))
    except Exception as e:
        logger.exception("Could not resolve entities: %s", e)
        await client.disconnect()
        return

    # 1) Try to fetch topic metadata via channels.GetForumTopicsByIDRequest
    candidate_msg = None
    try:
        logger.info("Trying channels.GetForumTopicsByIDRequest for topic_id=%s", SOURCE_TOPIC_ID)
        resp = await client(functions.channels.GetForumTopicsByIDRequest(
            channel=source_input,
            topics_ids=[SOURCE_TOPIC_ID]
        ))
        # resp structure: may include .topics list with objects containing top_message or top_message_id
        topics = getattr(resp, "topics", None)
        if topics:
            logger.debug("GetForumTopicsByIDResponse topics: %s", topics)
            # try to extract top_message or top_message_id
            t = topics[0]
            top_msg_id = getattr(t, "top_message", None) or getattr(t, "top_message_id", None) or getattr(t, "top_message_id", None)
            if top_msg_id:
                logger.info("Topic metadata provided top_message id: %s", top_msg_id)
                # fetch that message (latest/anchor)
                msgs = await client.get_messages(source_entity, ids=[top_msg_id])
                if msgs and len(msgs) > 0:
                    candidate_msg = msgs[0]
    except TypeError as e:
        logger.warning("GetForumTopicsByIDRequest signature unsupported in this Telethon version: %s", e)
    except Exception as e:
        logger.warning("GetForumTopicsByIDRequest failed or returned no usable top_message: %s", e)

    # 2) Fallback: iterate messages from newest to oldest and find one that 'belongs' to the topic
    if candidate_msg is None:
        logger.warning("Falling back to iter_messages and attribute-based detection for topic=%s", SOURCE_TOPIC_ID)
        found = None
        checked = 0
        async for msg in client.iter_messages(source_entity, limit=SEARCH_ITER_LIMIT):
            checked += 1
            # Several Telethon builds represent topic-membership in different attributes.
            # Check a variety of attribute names to be robust.
            try:
                # Direct attributes known/seen in various runs
                att_candidates = [
                    getattr(msg, "forum_topic", None),
                    getattr(msg, "reply_to_top_id", None),
                    getattr(msg, "topic_id", None),
                    getattr(msg, "thread_id", None),
                    getattr(msg, "forum_topic_id", None),
                ]
            except Exception:
                att_candidates = [None]

            matched = False
            for att in att_candidates:
                if att is None:
                    continue
                # att could be an object or int
                try:
                    att_val = int(att)
                except Exception:
                    # Maybe attribute contains object with .id or .top_id
                    att_val = None
                    if hasattr(att, "id"):
                        try:
                            att_val = int(getattr(att, "id"))
                        except Exception:
                            att_val = None
                    elif hasattr(att, "top_message"):
                        try:
                            att_val = int(getattr(att, "top_message"))
                        except Exception:
                            att_val = None
                if att_val == SOURCE_TOPIC_ID:
                    matched = True
                    break

            if matched:
                found = msg
                logger.info("Found a message matching topic=%s id=%s (checked=%s)", SOURCE_TOPIC_ID, getattr(msg, "id", None), checked)
                break

        if found:
            candidate_msg = found
        else:
            logger.error("No messages found in source topic %s (source=%s) after checking %s messages",
                         SOURCE_TOPIC_ID, SOURCE_LINK, checked)
            await client.disconnect()
            return

    # candidate_msg should now be a telethon Message to forward
    if candidate_msg is None:
        logger.error("No candidate message to forward/reply to placeholder.")
        await client.disconnect()
        return

    # Forward as reply to the placeholder message
    try:
        logger.info("Forwarding message id=%s from source to target as reply to placeholder id=%s",
                    candidate_msg.id, placeholder_msg_id)
        # high-level forward_messages supports reply_to parameter
        await client.forward_messages(
            entity=target_entity,
            messages=candidate_msg.id,
            from_peer=source_entity,
            reply_to=placeholder_msg_id
        )
        logger.info("Forward succeeded (high-level API).")
    except TypeError as e:
        # signature mismatch for reply_to param - fallback to low-level call
        logger.warning("High-level forward_messages signature not usable: %s", e)
        try:
            resp = await client(functions.messages.ForwardMessagesRequest(
                from_peer=source_input,
                id=[candidate_msg.id],
                to_peer=target_input,
                # reply_to_msg_id expects int
                reply_to_msg_id=placeholder_msg_id
            ))
            logger.info("Low-level ForwardMessagesRequest returned: %s", resp)
        except Exception as e2:
            logger.exception("Low-level ForwardMessagesRequest failed: %s", e2)
    except Exception as e:
        logger.exception("Forward failed: %s", e)

    await client.disconnect()
    logger.info("Client disconnected — done.")


if __name__ == "__main__":
    asyncio.run(main())

