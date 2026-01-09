import asyncio
from telethon import TelegramClient
import qrcode
from config import (
    api_id,
    api_hash,
    session
    )

async def main():
    client = TelegramClient(session, api_id, api_hash)
    await client.connect()

    if not await client.is_user_authorized():
        qr = await client.qr_login()
        print("\n📱 QR-Code scannen:")
        print("Telegram → Einstellungen → Geräte → Gerät hinzufügen\n")

        # ASCII QR ohne console-Module
        qr_img = qrcode.QRCode(border=1)
        qr_img.add_data(qr.url)
        qr_img.make(fit=True)

        # ASCII-Ausgabe
        qr_matrix = qr_img.get_matrix()
        for row in qr_matrix:
            print("".join("██" if cell else "  " for cell in row))

        await qr.wait()

    me = await client.get_me()
    print(f"\n✅ Eingeloggt als {me.first_name} ({me.id})")
    await client.disconnect()

asyncio.run(main())
