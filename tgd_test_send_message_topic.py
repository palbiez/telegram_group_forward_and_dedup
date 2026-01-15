# tgd_test_send_message_topic.py
"""
Testscript: sende Quell-Nachricht (Text oder Media) per send_message/send_file
in ein bestimmtes Topic (reply_to == topic_id). Ziel: prüfen, ob Topic-Routing
funktioniert unabhängig vom ForwardMessagesRequest.
Dateiname-Konvention: tgd_*
Voraussetzung: Telethon 1.42, gültige SESSION, API_ID/API_HASH.
"""

import asyncio
import logging
import tempfile
import os
import time
from telethon import TelegramClient, errors

# ---------------- Konfiguration (anpassen) ----------------
API_ID = 
API_HASH = ""
SESSION = ""

# Quelle (Chat mit der zu testenden Message)
SRC_PEER = "
CAND_MSG_ID =    # die message id in source, die du testen willst

# Ziel (Chat / Kanal mit Topics)
TGT_PEER = "
TARGET_TOPIC_ID =   # topic.id (nicht top_message) — reply_to für Topic-Routing

# Optional: temporäres Verzeichnis für Media-Downloads
TMP_DIR = "/tmp/tgd_test_send"
# --------------------------------------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("tgd_test_send")

async def safe_invoke(callable_coro, *args, max_retries=3, **kwargs):
    """
    Simple safe wrapper: fängt FloodWait ab und retryt nach Wartezeit.
    Gibt Ergebnis oder wirft Exception nach Exhaustion.
    """
    attempt = 0
    while True:
        try:
            return await callable_coro(*args, **kwargs)
        except errors.FloodWaitError as e:
            attempt += 1
            wait = int(e.seconds) + 1
            log.warning("FloodWait %ds (attempt %d/%d) - sleeping...", wait, attempt, max_retries)
            time.sleep(wait)
            if attempt >= max_retries:
                raise
        except Exception:
            # propagate other exceptions upward
            raise

async def main():
    os.makedirs(TMP_DIR, exist_ok=True)
    client = TelegramClient(SESSION, API_ID, API_HASH)
    await client.start()
    try:
        src_ent = await client.get_entity(SRC_PEER)
        tgt_ent = await client.get_entity(TGT_PEER)

        log.info("Quell-Entity: %s  Ziel-Entity: %s", getattr(src_ent, "id", None), getattr(tgt_ent, "id", None))

        # 1) Hole Quelle komplett
        src_msg = await client.get_messages(src_ent, ids=CAND_MSG_ID)
        if not src_msg:
            log.error("Quell-Nachricht id=%s nicht gefunden. Beende.", CAND_MSG_ID)
            return

        log.info("Quelle id=%s, has_media=%s, text-len=%d",
                 src_msg.id,
                 bool(getattr(src_msg, "media", None)),
                 len((src_msg.text or "")[:200]))

        # 2) Wenn Media, lade temporär herunter und sende mit send_file(reply_to=topic_id)
        if getattr(src_msg, "media", None):
            log.info("Media erkannt — lade herunter und sende per send_file in Topic %s", TARGET_TOPIC_ID)

            # download to temp file (Telethon Message.download_media)
            tmp_path = None
            try:
                tmp_path = await src_msg.download_media(file=TMP_DIR)
                if not tmp_path:
                    log.error("Media-Download hat keinen Pfad zurückgegeben. Abbruch.")
                    return
                log.info("Media heruntergeladen: %s", tmp_path)

                # send file into topic (reply_to should be topic.id for thread)
                sent = await safe_invoke(
                    client.send_file,
                    tgt_ent,
                    file=tmp_path,
                    caption=(src_msg.message or src_msg.text or ""),
                    reply_to=TARGET_TOPIC_ID
                )
                log.info("send_file result id=%s", getattr(sent, "id", None))

            finally:
                # optional cleanup
                if tmp_path and os.path.exists(tmp_path):
                    try:
                        os.remove(tmp_path)
                        log.debug("tmp file removed")
                    except Exception:
                        log.exception("Failed to remove tmp file")
        else:
            # 3) Kein Media: sende den Text direkt in Topic
            body = src_msg.message or src_msg.text or ""
            if not body:
                log.warning("Quell-Nachricht hat keinen Text und kein Media -> nichts zu senden.")
                return
            log.info("Sende Text in Topic %s: %.120s", TARGET_TOPIC_ID, body[:120])
            sent = await safe_invoke(
                client.send_message,
                tgt_ent,
                body,
                reply_to=TARGET_TOPIC_ID
            )
            log.info("send_message result id=%s", getattr(sent, "id", None))

        # 4) Inspektion: hole die gerade gesendete Nachricht aus Ziel
        # (wenn `sent` gesetzt)
        if 'sent' in locals() and sent:
            new_id = getattr(sent, "id", None)
            if new_id:
                inspect = await client.get_messages(tgt_ent, ids=new_id)
                log.info("Inspected sent message id=%s -> keys: %s", new_id, list(inspect.to_dict().keys()) if inspect else None)
                log.info("Inspect preview: forward=%s, thread_id=%s, text-preview=%.80s",
                         getattr(inspect, "forward", None) is not None,
                         getattr(inspect, "thread_id", None),
                         (inspect.text or "")[:80] if inspect else "")
            else:
                log.warning("Keine id in 'sent' erhalten - eventuell non-message object returned.")
        else:
            log.warning("Kein 'sent' Objekt gefunden; wahrscheinlich send schlug fehl.")

    finally:
        await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
