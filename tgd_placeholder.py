"""
Post a placeholder message into TARGET_GROUP topic id 43 (if supported),
otherwise post a normal channel message and record metadata.

Behavior:
- Load credentials & TARGET_GROUP from tgd_config.py
- Try high-level client.send_message(..., topic=43)
- If that raises (not supported), fallback to low-level messages.SendMessageRequest
  (which will post without explicit topic) and record that fact.
- Write metadata to tgd_placeholder.json:
  { "target_channel_id": ..., "target_message_id": ..., "requested_topic": 43, "posted_with_topic": bool }
- Logging -> file in LOG_DIR (from tgd_config.py).
"""
import asyncio
import json
import logging
import os
from pathlib import Path
from datetime import datetime
from telethon import TelegramClient, functions, utils
from telethon.tl import functions as tl_functions
from telethon.tl import types

# Load your tgd_config module (ensure tgd_config.py in same dir)
try:
    import tgd_config as cfg
except Exception as e:
    raise SystemExit("Could not import tgd_config.py: " + repr(e))

LOG_DIR = Path(cfg.LOG_DIR if hasattr(cfg, "LOG_DIR") else "logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)
log_file = LOG_DIR / "tgd_post_placeholder.log"

logging.basicConfig(
    level=getattr(logging, getattr(cfg, "LOG_LEVEL", "INFO")),
    format="%(asctime)s | %(levelname)s | tgd_post_placeholder | %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding="utf-8"),
        logging.StreamHandler()  # also print to stdout
    ],
)

logger = logging.getLogger("tgd_post_placeholder")


PLACEHOLDER_TEXT = "PLACEHOLDER: topic=43 (will be used as anchor) - created at {}".format(
    datetime.utcnow().isoformat()
)
PLACEHOLDER_TOPIC_ID = 43
PLACEHOLDER_JSON = Path("tgd_placeholder.json")


async def main():
    logger.info("Starting tgd_post_placeholder")
    # Validate config
    if not getattr(cfg, "TARGET_GROUP", None):
        logger.error("TARGET_GROUP not set in tgd_config.py. Aborting.")
        return

    client = TelegramClient(cfg.session, cfg.api_id, cfg.api_hash)
    await client.start()
    logger.info("Client started")

    # resolve entities
    try:
        target_entity = await client.get_entity(cfg.TARGET_GROUP)
        target_input = await client.get_input_entity(target_entity)
        logger.info("Resolved target entity: %s", repr(target_entity))
    except Exception as e:
        logger.exception("Could not resolve TARGET_GROUP %r: %s", cfg.TARGET_GROUP, e)
        await client.disconnect()
        return

    posted_with_topic = False
    posted_msg_id = None

    # 1) Try high-level send_message(topic=...)
    try:
        logger.info("Trying high-level client.send_message(..., topic=%s)", PLACEHOLDER_TOPIC_ID)
        sent = await client.send_message(target_entity, PLACEHOLDER_TEXT, topic=PLACEHOLDER_TOPIC_ID)
        posted_msg_id = sent.id
        posted_with_topic = True
        logger.info("High-level send_message succeeded: id=%s (topic=%s)", posted_msg_id, PLACEHOLDER_TOPIC_ID)
    except TypeError as e:
        # likely send_message got unexpected keyword 'topic'
        logger.warning("send_message(..., topic=...) not supported by this Telethon version. Falling back. %s", e)
    except Exception as e:
        logger.warning("High-level send_message failed, will attempt low-level fallback: %s", e)

    # 2) Fallback: low-level SendMessageRequest (this will post but not necessarily in the topic)
    if posted_msg_id is None:
        try:
            logger.info("Fallback: attempting low-level messages.SendMessageRequest (no explicit topic)")
            rand_id = utils.get_random_id()
            # Use messages.SendMessageRequest from messages module
            res = await client(functions.messages.SendMessageRequest(
                peer=target_input,
                message=PLACEHOLDER_TEXT,
                random_id=rand_id
            ))
            # The result usually contains UpdateNewChannelMessage with message id in .updates -> find it
            # telethon returns different shapes; try to extract the created id robustly:
            msg_id = None
            if hasattr(res, "updates"):
                # search updates
                for u in res.updates:
                    if hasattr(u, "message") and getattr(u.message, "id", None):
                        msg_id = u.message.id
                        break
            # Fallback: try to inspect res.message
            if msg_id is None and hasattr(res, "message") and getattr(res.message, "id", None):
                msg_id = res.message.id
            if msg_id is None:
                # as last resort, fetch last message from channel (may be the placeholder)
                logger.debug("Could not extract id from low-level response, fetching last message to detect id")
                last = await client.get_messages(target_entity, limit=1)
                if last:
                    msg_id = last[0].id
            if msg_id is None:
                logger.error("Could not determine posted message id after low-level send.")
            else:
                posted_msg_id = msg_id
                posted_with_topic = False
                logger.info("Fallback: sent placeholder message without explicit topic id; id=%s", posted_msg_id)
        except Exception as e:
            logger.exception("Low-level SendMessageRequest failed: %s", e)

    # Write metadata JSON so reply script can find the anchor message
    if posted_msg_id:
        meta = {
            "target_channel_id": getattr(target_entity, "id", None),
            "target_message_id": posted_msg_id,
            "requested_topic": PLACEHOLDER_TOPIC_ID,
            "posted_with_topic": posted_with_topic,
            "target_peer": cfg.TARGET_GROUP,
            "ts": datetime.utcnow().isoformat()
        }
        try:
            PLACEHOLDER_JSON.write_text(json.dumps(meta, indent=2))
            logger.info("Wrote placeholder metadata to %s", PLACEHOLDER_JSON)
        except Exception as e:
            logger.exception("Could not write placeholder JSON: %s", e)
    else:
        logger.error("No message was posted; no metadata written.")

    await client.disconnect()
    logger.info("Client disconnected — done.")


if __name__ == "__main__":
    asyncio.run(main())
# tgd_post_placeholder.py
"""
Post a placeholder message into TARGET_GROUP topic id 43 (if supported),
otherwise post a normal channel message and record metadata.

Behavior:
- Load credentials & TARGET_GROUP from tgd_config.py
- Try high-level client.send_message(..., topic=43)
- If that raises (not supported), fallback to low-level messages.SendMessageRequest
  (which will post without explicit topic) and record that fact.
- Write metadata to tgd_placeholder.json:
  { "target_channel_id": ..., "target_message_id": ..., "requested_topic": 43, "posted_with_topic": bool }
- Logging -> file in LOG_DIR (from tgd_config.py).
"""
import asyncio
import json
import logging
import os
from pathlib import Path
from datetime import datetime
from telethon import TelegramClient, functions, utils
from telethon.tl import functions as tl_functions
from telethon.tl import types

# Load your tgd_config module (ensure tgd_config.py in same dir)
try:
    import tgd_config as cfg
except Exception as e:
    raise SystemExit("Could not import tgd_config.py: " + repr(e))

LOG_DIR = Path(cfg.LOG_DIR if hasattr(cfg, "LOG_DIR") else "logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)
log_file = LOG_DIR / "tgd_post_placeholder.log"

logging.basicConfig(
    level=getattr(logging, getattr(cfg, "LOG_LEVEL", "INFO")),
    format="%(asctime)s | %(levelname)s | tgd_post_placeholder | %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding="utf-8"),
        logging.StreamHandler()  # also print to stdout
    ],
)

logger = logging.getLogger("tgd_post_placeholder")


PLACEHOLDER_TEXT = "PLACEHOLDER: topic=43 (will be used as anchor) - created at {}".format(
    datetime.utcnow().isoformat()
)
PLACEHOLDER_TOPIC_ID = 43
PLACEHOLDER_JSON = Path("tgd_placeholder.json")


async def main():
    logger.info("Starting tgd_post_placeholder")
    # Validate config
    if not getattr(cfg, "TARGET_GROUP", None):
        logger.error("TARGET_GROUP not set in tgd_config.py. Aborting.")
        return

    client = TelegramClient(cfg.session, cfg.api_id, cfg.api_hash)
    await client.start()
    logger.info("Client started")

    # resolve entities
    try:
        target_entity = await client.get_entity(cfg.TARGET_GROUP)
        target_input = await client.get_input_entity(target_entity)
        logger.info("Resolved target entity: %s", repr(target_entity))
    except Exception as e:
        logger.exception("Could not resolve TARGET_GROUP %r: %s", cfg.TARGET_GROUP, e)
        await client.disconnect()
        return

    posted_with_topic = False
    posted_msg_id = None

    # 1) Try high-level send_message(topic=...)
    try:
        logger.info("Trying high-level client.send_message(..., topic=%s)", PLACEHOLDER_TOPIC_ID)
        sent = await client.send_message(target_entity, PLACEHOLDER_TEXT, topic=PLACEHOLDER_TOPIC_ID)
        posted_msg_id = sent.id
        posted_with_topic = True
        logger.info("High-level send_message succeeded: id=%s (topic=%s)", posted_msg_id, PLACEHOLDER_TOPIC_ID)
    except TypeError as e:
        # likely send_message got unexpected keyword 'topic'
        logger.warning("send_message(..., topic=...) not supported by this Telethon version. Falling back. %s", e)
    except Exception as e:
        logger.warning("High-level send_message failed, will attempt low-level fallback: %s", e)

    # 2) Fallback: low-level SendMessageRequest (this will post but not necessarily in the topic)
    if posted_msg_id is None:
        try:
            logger.info("Fallback: attempting low-level messages.SendMessageRequest (no explicit topic)")
            rand_id = utils.get_random_id()
            # Use messages.SendMessageRequest from messages module
            res = await client(functions.messages.SendMessageRequest(
                peer=target_input,
                message=PLACEHOLDER_TEXT,
                random_id=rand_id
            ))
            # The result usually contains UpdateNewChannelMessage with message id in .updates -> find it
            # telethon returns different shapes; try to extract the created id robustly:
            msg_id = None
            if hasattr(res, "updates"):
                # search updates
                for u in res.updates:
                    if hasattr(u, "message") and getattr(u.message, "id", None):
                        msg_id = u.message.id
                        break
            # Fallback: try to inspect res.message
            if msg_id is None and hasattr(res, "message") and getattr(res.message, "id", None):
                msg_id = res.message.id
            if msg_id is None:
                # as last resort, fetch last message from channel (may be the placeholder)
                logger.debug("Could not extract id from low-level response, fetching last message to detect id")
                last = await client.get_messages(target_entity, limit=1)
                if last:
                    msg_id = last[0].id
            if msg_id is None:
                logger.error("Could not determine posted message id after low-level send.")
            else:
                posted_msg_id = msg_id
                posted_with_topic = False
                logger.info("Fallback: sent placeholder message without explicit topic id; id=%s", posted_msg_id)
        except Exception as e:
            logger.exception("Low-level SendMessageRequest failed: %s", e)

    # Write metadata JSON so reply script can find the anchor message
    if posted_msg_id:
        meta = {
            "target_channel_id": getattr(target_entity, "id", None),
            "target_message_id": posted_msg_id,
            "requested_topic": PLACEHOLDER_TOPIC_ID,
            "posted_with_topic": posted_with_topic,
            "target_peer": cfg.TARGET_GROUP,
            "ts": datetime.utcnow().isoformat()
        }
        try:
            PLACEHOLDER_JSON.write_text(json.dumps(meta, indent=2))
            logger.info("Wrote placeholder metadata to %s", PLACEHOLDER_JSON)
        except Exception as e:
            logger.exception("Could not write placeholder JSON: %s", e)
    else:
        logger.error("No message was posted; no metadata written.")

    await client.disconnect()
    logger.info("Client disconnected — done.")


if __name__ == "__main__":
    asyncio.run(main())
