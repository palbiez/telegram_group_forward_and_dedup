# tgd_min_repro_forward.py
import asyncio, logging, os, sys
from telethon import TelegramClient, functions, types, helpers

API_ID = 
API_HASH = ""
SESSION = ""

SRC_PEER = "https://t.me/"
TGT_PEER = "https://t.me/"

CAND_MSG_ID =            # test message in source (media)
TARGET_TOPIC_ID =       # topic.id (used for send_file)
TOP_MSG_ID =            # top_message (used for InputReplyToMessage)
PLACEHOLDER_MSG_ID =    # an existing message id in the target topic

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("min_repro")

async def main():
    client = TelegramClient(SESSION, API_ID, API_HASH)
    await client.start()
    try:
        print("=== ENV ===")
        print("python:", sys.version.splitlines()[0])
        try:
            import telethon
            print("telethon:", telethon.__version__, telethon.__file__)
        except Exception:
            print("telethon: (import failed)")

        src_ent = await client.get_entity(SRC_PEER)
        tgt_ent = await client.get_entity(TGT_PEER)
        src_input = await client.get_input_entity(src_ent)
        tgt_input = await client.get_input_entity(tgt_ent)

        # fetch source message
        src_msg = await client.get_messages(src_ent, ids=CAND_MSG_ID)
        if not src_msg:
            print("SOURCE MSG NOT FOUND id=", CAND_MSG_ID)
            return
        print("SOURCE MSG:", {"id": src_msg.id, "has_media": bool(src_msg.media), "text_len": len((src_msg.text or "")[:200])})

        # 1) send_file test (no download if possible)
        if src_msg.media:
            print("\n=== send_file test (no-forward) ===")
            try:
                sent = await client.send_file(tgt_ent, file=src_msg.media, caption=(src_msg.message or src_msg.text or ""), reply_to=TARGET_TOPIC_ID)
                print("send_file returned id:", getattr(sent, "id", None))
                inspect_sent = await client.get_messages(tgt_ent, ids=getattr(sent, "id", None))
                print("send_file inspect keys:", list(inspect_sent.to_dict().keys()) if inspect_sent else None)
                print("inspect thread_id, forward:", getattr(inspect_sent, "thread_id", None), getattr(inspect_sent, "forward", None) is not None)
            except Exception as e:
                print("send_file exception:", repr(e))

        # 2) ForwardMessagesRequest test (low-level)
        print("\n=== ForwardMessagesRequest test ===")
        try:
            resp = await client(functions.messages.ForwardMessagesRequest(
                from_peer=src_input,
                id=[CAND_MSG_ID],
                to_peer=tgt_input,
                random_id=[helpers.generate_random_long()],
                reply_to=types.InputReplyToMessage(
                    reply_to_msg_id=PLACEHOLDER_MSG_ID or 0,
                    top_msg_id=TOP_MSG_ID
                )
            ))
        except Exception as e:
            print("ForwardMessagesRequest exception:", repr(e))
            return

        print("Forward resp repr:\n", repr(resp))
        if hasattr(resp, "updates"):
            print("\nForward resp.updates:")
            for u in resp.updates:
                print(" -", type(u).__name__, repr(u)[:500])

        # try to extract ids
        new_ids = []
        if hasattr(resp, "updates"):
            for u in resp.updates:
                if isinstance(u, types.UpdateNewChannelMessage) and getattr(u, "message", None):
                    new_ids.append(u.message.id)
                if isinstance(u, types.UpdateMessageID):
                    if hasattr(u, "id"):
                        new_ids.append(u.id)
        print("Extracted new ids:", new_ids)

        # inspect recent messages in target for heuristics
        recent = await client.get_messages(tgt_ent, limit=12)
        print("Recent target messages (id, thread_id, forward, text-preview):")
        for m in recent:
            print(" -", m.id, getattr(m, "thread_id", None), getattr(m, "forward", None) is not None, (m.text or "")[:80])

    finally:
        await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
