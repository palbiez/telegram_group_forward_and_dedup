# tgd_send_media_no_download.py
import asyncio, logging, os
from telethon import TelegramClient, functions, types, helpers, errors

# --- Konfiguration ---
API_ID = 
API_HASH = ""
SESSION = ""

SRC_PEER = ""
TGT_PEER = ""

CAND_MSG_ID = 237           # message id in source
TARGET_TOPIC_ID = 6404      # topic.id (reply_to)
TOP_MSG_ID = 6414           # top_message of target topic (for forward fallback)
PLACEHOLDER_MSG_ID = 6414   # existing message in target topic (for forward fallback)

TMP_DIR = "/tmp/tgd_send_media_no_download"
os.makedirs(TMP_DIR, exist_ok=True)

# Verhalten:
PREFER_FORWARD = False   # True: versuche zuerst Forward (behält Forward-Header), False: versuche send_file(media) zuerst
# ----------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("tgd_sendmedia")

async def try_send_media_without_download(client, tgt_ent, src_msg):
    """
    Versucht, die Media-Objekt-Instanz direkt zu senden (ohne lokalen Download).
    Gibt das gesendete Message-Objekt zurück oder wirft Exception.
    """
    log.info("Versuch: send_file(..., file=src_msg.media) ohne Download")
    return await client.send_file(
        tgt_ent,
        file=src_msg.media,
        caption=(src_msg.message or src_msg.text or ""),
        reply_to=TARGET_TOPIC_ID
    )

async def try_forward_lowlevel(client, src_input, tgt_input):
    log.info("Versuch: low-level ForwardMessagesRequest (behält Forward-Metadaten)")
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
    return resp

async def try_send_with_download(client, tgt_ent, src_msg):
    log.info("Fallback: Download + send_file")
    tmp = await src_msg.download_media(file=TMP_DIR)
    if not tmp:
        raise RuntimeError("Download lieferte keinen Pfad")
    sent = await client.send_file(tgt_ent, file=tmp, caption=(src_msg.message or src_msg.text or ""), reply_to=TARGET_TOPIC_ID)
    try:
        os.remove(tmp)
    except Exception:
        log.debug("tmp cleanup failed for %s", tmp)
    return sent

async def main():
    client = TelegramClient(SESSION, API_ID, API_HASH)
    await client.start()
    try:
        src_ent = await client.get_entity(SRC_PEER)
        tgt_ent = await client.get_entity(TGT_PEER)
        src_input = await client.get_input_entity(src_ent)
        tgt_input = await client.get_input_entity(tgt_ent)

        src_msg = await client.get_messages(src_ent, ids=CAND_MSG_ID)
        if not src_msg:
            log.error("Quell-Nachricht nicht gefunden: id=%s", CAND_MSG_ID)
            return

        if not getattr(src_msg, "media", None):
            log.error("Quell-Nachricht enthält kein Media; nichts zu tun.")
            return

        # Ausprobieren in logischer Reihenfolge
        # Variante A: Forward zuerst (wenn gewünscht)
        if PREFER_FORWARD:
            try:
                resp = await try_forward_lowlevel(client, src_input, tgt_input)
                log.info("ForwardMessagesRequest successful: repr(resp)=%s", repr(resp)[:300])
                # Extrahiere neue IDs falls vorhanden (siehe earlier scripts)
                return
            except Exception as e:
                log.warning("Forward failed: %s - fahre mit send_file-Versuchen fort", e)

        # Variante B: Versuch, media-Objekt direkt zu senden (kein Download)
        try:
            sent = await try_send_media_without_download(client, tgt_ent, src_msg)
            log.info("send_file ohne Download erfolgreich: id=%s", getattr(sent, "id", None))
            inspect = await client.get_messages(tgt_ent, ids=getattr(sent, "id", None))
            log.info("inspect: thread_id=%s forward=%s keys=%s",
                     getattr(inspect, "thread_id", None),
                     getattr(inspect, "forward", None) is not None,
                     list(inspect.to_dict().keys()) if inspect else None)
            return
        except Exception as e:
            log.warning("send_file mit src_msg.media schlug fehl: %s", e)

        # Letzter Fallback: lade herunter und sende
        try:
            sent2 = await try_send_with_download(client, tgt_ent, src_msg)
            log.info("send_file nach Download erfolgreich: id=%s", getattr(sent2, "id", None))
            inspect2 = await client.get_messages(tgt_ent, ids=getattr(sent2, "id", None))
            log.info("inspect2: thread_id=%s forward=%s keys=%s",
                     getattr(inspect2, "thread_id", None),
                     getattr(inspect2, "forward", None) is not None,
                     list(inspect2.to_dict().keys()) if inspect2 else None)
            return
        except Exception as e:
            log.exception("Download+send fallback schlug fehl: %s", e)

    finally:
        await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
