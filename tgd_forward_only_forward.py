# tgd_forward_only_forward.py
"""
Echtes Forward-Testscript (low-level ForwardMessagesRequest).
Versucht, die Quell-Media-Message CAND_MSG_ID per ForwardMessagesRequest
in das Ziel-Topic zu forwarden. Extrahiert und inspiziert die resultierenden Updates.
"""

import asyncio
import logging
import os
import time
from telethon import TelegramClient, functions, types, helpers, errors

# ----------------- Konfiguration (anpassen falls nötig) -----------------
API_ID = 
API_HASH = ""
SESSION = ""

SRC_PEER = ""
TGT_PEER = ""

CAND_MSG_ID =            # <- die Message in der Quelle, die forwarded werden soll
TOP_MSG_ID =            # top_message der Ziel-Topic (z. B. 6414)
PLACEHOLDER_MSG_ID =    # existierende Message-ID im Ziel-Topic (als reply_to_msg_id)
# -----------------------------------------------------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("tgd_forward_only")

async def safe_forward(client, src_input, tgt_input, cand_msg_id, placeholder_msg_id, top_msg_id):
    """
    Führt ein ForwardMessagesRequest aus und gibt das resp-Objekt zurück.
    Behandelt FloodWait-Exceptions mit Retry-Backoff.
    """
    max_retries = 3
    attempt = 0
    while True:
        try:
            resp = await client(functions.messages.ForwardMessagesRequest(
                from_peer=src_input,
                id=[cand_msg_id],
                to_peer=tgt_input,
                random_id=[helpers.generate_random_long()],
                reply_to=types.InputReplyToMessage(
                    reply_to_msg_id=placeholder_msg_id or 0,
                    top_msg_id=top_msg_id
                )
            ))
            return resp
        except errors.FloodWaitError as e:
            attempt += 1
            wait = int(e.seconds) + 1
            log.warning("FloodWait %ds (attempt %d/%d) - sleeping...", wait, attempt, max_retries)
            time.sleep(wait)
            if attempt >= max_retries:
                raise
        except errors.MessageIdInvalidError:
            # propagate for caller to handle diagnostics
            raise
        except Exception:
            # propagate unexpected exceptions
            raise

async def extract_new_ids_from_resp(resp):
    """
    Versucht, alle relevanten neuen Message-IDs aus resp.updates zu extrahieren.
    Liefert Liste (kann leer sein).
    """
    new_ids = []
    if not hasattr(resp, "updates"):
        return new_ids

    for u in resp.updates:
        # UpdateNewChannelMessage -> enthält message Objekt
        if isinstance(u, types.UpdateNewChannelMessage) and getattr(u, "message", None):
            try:
                new_ids.append(u.message.id)
            except Exception:
                pass
        # UpdateMessageID -> mapping; u.id oder u.new_id kann enthalten sein
        if isinstance(u, types.UpdateMessageID):
            try:
                # Try common attributes
                if hasattr(u, "id") and u.id:
                    new_ids.append(u.id)
            except Exception:
                pass
        # Weitere Update-Types prüfen (falls relevant)
        # Beispiel: UpdateShort/UpdateShortMessage sind seltener hier.
    # Deduplicate and return
    return sorted(set(new_ids))

async def inspect_ids(client, tgt_ent, ids):
    """
    Holt und loggt inspect-Infos für gegebene IDs im Ziel.
    """
    for nid in ids:
        try:
            msg = await client.get_messages(tgt_ent, ids=nid)
            if not msg:
                log.warning("Inspektionsversuch: id=%s nicht gefunden im Ziel.", nid)
                continue
            log.info("Inspected forwarded msg id=%s -> thread_id=%s forward=%s keys=%s",
                     nid,
                     getattr(msg, "thread_id", None),
                     getattr(msg, "forward", None) is not None,
                     list(msg.to_dict().keys()))
            # Optional: kürzere Preview-Ausgabe
            preview = (msg.text or msg.message or "")
            log.info("Preview (%.200s): %s", min(200, len(preview)), preview[:200])
        except Exception:
            log.exception("Fehler bei Inspektion von id=%s", nid)

async def main():
    client = TelegramClient(SESSION, API_ID, API_HASH)
    await client.start()
    try:
        # Entities / Inputs
        src_ent = await client.get_entity(SRC_PEER)
        tgt_ent = await client.get_entity(TGT_PEER)
        src_input = await client.get_input_entity(src_ent)
        tgt_input = await client.get_input_entity(tgt_ent)

        log.info("Quell-Entity id=%s Ziel-Entity id=%s", getattr(src_ent, "id", None), getattr(tgt_ent, "id", None))

        # Vorab-Check: existiert die Quelle-Nachricht?
        src_msg = await client.get_messages(src_ent, ids=CAND_MSG_ID)
        if not src_msg:
            log.error("Quell-Nachricht id=%s nicht gefunden oder nicht zugreifbar. Abbruch.", CAND_MSG_ID)
            return
        log.info("Quelle: id=%s has_media=%s text_len=%s", src_msg.id, bool(getattr(src_msg, "media", None)), len((src_msg.text or "")[:200]))

        # Versuch: Forward (low-level)
        log.info("Starte ForwardMessagesRequest: source_msg=%s -> target (reply_to_msg_id=%s top_msg_id=%s)",
                 CAND_MSG_ID, PLACEHOLDER_MSG_ID, TOP_MSG_ID)
        try:
            resp = await safe_forward(client, src_input, tgt_input, CAND_MSG_ID, PLACEHOLDER_MSG_ID, TOP_MSG_ID)
        except errors.MessageIdInvalidError as e:
            log.error("MessageIdInvalidError beim Forward: %s", e)
            # Diagnose: liste letzte Nachrichten in Quelle und Ziel zur Analyse
            try:
                recent_src = await client.get_messages(src_ent, limit=8)
                log.info("Letzte Nachrichten in Quelle (id, has_media, text-preview): %s",
                         [(m.id, bool(m.media), (m.text or m.message or "")[:60]) for m in recent_src])
            except Exception:
                log.exception("Konnte Quelle nicht listen")
            try:
                recent_tgt = await client.get_messages(tgt_ent, limit=8)
                log.info("Letzte Nachrichten im Ziel (id, has_media, text-preview): %s",
                         [(m.id, bool(m.media), (m.text or m.message or "")[:60]) for m in recent_tgt])
            except Exception:
                log.exception("Konnte Ziel nicht listen")
            return
        except Exception:
            log.exception("Unerwarteter Fehler beim Forward")
            return

        log.info("Forward resp repr (truncated): %s", repr(resp)[:400])

        # Dump updates (für Debug/Issue-Analyse)
        if hasattr(resp, "updates"):
            for u in resp.updates:
                log.info("update: type=%s repr=%s", type(u).__name__, repr(u)[:400])

        # Extrahiere neue IDs und inspecte sie
        new_ids = await extract_new_ids_from_resp(resp)
        log.info("Extracted new ids from resp: %s", new_ids)

        if not new_ids:
            # Heuristik: liste zuletzt gesendete Nachrichten im Ziel zur manuellen Suche
            log.info("Keine IDs aus resp extrahiert - hole recent messages im Ziel als Heuristik.")
            recent = await client.get_messages(tgt_ent, limit=12)
            log.info("Recent target messages: %s", [(m.id, bool(m.media), getattr(m, "thread_id", None), (m.text or "")[:40]) for m in recent])
            return

        # Inspektion der neu erzeugten Message(s)
        await inspect_ids(client, tgt_ent, new_ids)

    finally:
        await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
