#!/usr/bin/env python3
"""
extract_existing_uids.py

Scannt die TARGET_GROUP aus config.py und sammelt alle vorhandenen
file_unique_ids von Medien-Nachrichten.
Schreibt sie nach PERSIST_FORWARDED_UIDS_FILE (JSON-Liste).

Read-only: keine Nachrichten werden verändert.
Telethon 1.41 kompatibel.
"""

import asyncio
import json
import os
import logging
from typing import Optional, Set

from telethon import TelegramClient
from telethon.tl.types import Message

from config import (
    api_id,
    api_hash,
    session,
    TARGET_GROUP,
    PERSIST_FORWARDED_UIDS_FILE,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("extract_existing_uids")


def get_file_unique_id(msg: Message) -> Optional[str]:
    """
    Robust extraktion der Datei-UID aus einer Message.
    """
    media = getattr(msg, "media", None) or getattr(msg, "file", None)
    if not media:
        return None

    for attr in ("file_unique_id", "unique_id", "id"):
        val = getattr(media, attr, None)
        if val:
            return str(val)

    doc = getattr(media, "document", None)
    if doc:
        for attr in ("file_unique_id", "unique_id", "id"):
            val = getattr(doc, attr, None)
            if val:
                return str(val)

    return None


async def main():
    client = TelegramClient(session, api_id, api_hash)
    await client.start()

    if not await client.is_user_authorized():
        log.error("Session nicht autorisiert – bitte zuerst Login durchführen.")
        await client.disconnect()
        return

    target = await client.get_entity(TARGET_GROUP)
    log.info("Scanne TARGET_GROUP: %s", TARGET_GROUP)

    found_uids: Set[str] = set()
    scanned = 0
    with_media = 0

    async for msg in client.iter_messages(target, reverse=True):
        if not isinstance(msg, Message):
            continue

        scanned += 1
        uid = get_file_unique_id(msg)
        if uid:
            with_media += 1
            found_uids.add(uid)

        if scanned % 1000 == 0:
            log.info(
                "Fortschritt: %d Messages gescannt, %d Medien, %d eindeutige UIDs",
                scanned, with_media, len(found_uids)
            )

    log.info(
        "Scan abgeschlossen: %d Messages, %d Medien, %d eindeutige UIDs",
        scanned, with_media, len(found_uids)
    )

    # bestehende Datei ggf. einlesen und vereinigen
    existing: Set[str] = set()
    if os.path.exists(PERSIST_FORWARDED_UIDS_FILE):
        try:
            with open(PERSIST_FORWARDED_UIDS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list):
                    existing = set(map(str, data))
        except Exception as e:
            log.warning("Konnte bestehende UID-Datei nicht lesen: %s", e)

    merged = sorted(existing.union(found_uids))

    with open(PERSIST_FORWARDED_UIDS_FILE, "w", encoding="utf-8") as f:
        json.dump(merged, f, indent=2)

    log.info(
        "UID-Datei geschrieben: %s (%d Einträge)",
        PERSIST_FORWARDED_UIDS_FILE, len(merged)
    )

    await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
