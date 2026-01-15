# reproduce_forward_topmsg.py
import asyncio, logging
from telethon import TelegramClient, functions, types, helpers
from datetime import datetime

API_ID =  
API_HASH = ""
SESSION = ""
SRC_PEER = ""
TGT_PEER = ""
CAND_MSG_ID =     # die message id in source that you want to forward
TOP_MSG_ID =      # topic top_message id (server anchor)
PLACEHOLDER_MSG_ID =   # id of placeholder in target (if one exists)

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger("repro")

async def main():
    client = TelegramClient(SESSION, API_ID, API_HASH)
    await client.start()
    src_ent = await client.get_entity(SRC_PEER)
    tgt_ent = await client.get_entity(TGT_PEER)
    src_input = await client.get_input_entity(src_ent)
    tgt_input = await client.get_input_entity(tgt_ent)

    try:
        # Low-level forward using top_msg_id
        log.info("Forward ")
        resp =  await client(functions.messages.ForwardMessagesRequest(
            from_peer=src_input,
            id=[CAND_MSG_ID],
            to_peer=tgt_input,
            random_id=[helpers.generate_random_long()],
            reply_to=types.InputReplyToMessage(
                reply_to_msg_id=0, 
                top_msg_id=TOP_MSG_ID
            )
        ))
        log.info("Forward resp type: %s repr: %s", type(resp).__name__, repr(resp)[:400])

        # Try to discover new message id(s)
        new_id = None
        if hasattr(resp, "updates"):
            for u in resp.updates:
                if isinstance(u, types.UpdateNewChannelMessage) and getattr(u, "message", None):
                    new_id = getattr(u.message, "id", None)
                # UpdateMessageID etc. might contain mapping; dump them all in logs
                log.debug("update item: %s", u)
        log.info("Inferred new_id: %s", new_id)

        # If no id extracted, pull last messages from target to heuristic-match
        if new_id is None:
            recent = await client.get_messages(tgt_ent, limit=6)
            for m in recent:
                log.debug("recent msg candidate: id=%s text=%s", m.id, m.message)
            # choose one by eye / heuristics and set new_id
        if new_id:
            inspected = await client.get_messages(tgt_ent, ids=new_id)
            log.info("Inspected forwarded message: %s", inspected.to_dict())
        else:
            log.warning("No new_id found in resp; review recent messages in target manually.")

    finally:
        await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
