"""
Telegram Video Streaming System Backend
-----------------------------------------
Telegram'dagi video postlarni Firebase Realtime DB'ga saqlaydi va
Range so'rovlari (HTTP Range) orqali to'g'ridan-to'g'ri Telegramdan
video oqimini (streaming) beradi.

DIQQAT: Bu versiyada server diskiga (Render kabi ephemeral disk muhitlarda)
hech qanday video fayl saqlanmaydi. Har bir so'rov to'g'ridan-to'g'ri
Telegram'dan jonli oqim qilinadi. Qaytadan tomosha qilishda tezlik uchun
keshlash endi backendda emas, foydalanuvchi brauzerida (Service Worker
orqali) amalga oshiriladi.

Bu fayl StreamX Admin Panel (admin.html) bilan birga ishlaydi:
  - POST   /api/save-video        -> Telegram havolasidan video qo'shish (Admin -> "Video yuklash")
  - DELETE /api/videos/{key}      -> Videoni katalogdan o'chirish (Admin -> "Videolar" jadvali)
  - GET    /api/stream/{c}/{m}    -> Videoni Range so'rovlari bilan oqim sifatida uzatish (Player)
  - GET    /                      -> Backend ishlab turganini tekshirish (health-check)

Ishga tushirishdan oldin quyidagi environment o'zgaruvchilarini sozlang (.env fayliga qarang):
  TG_API_ID, TG_API_HASH, FIREBASE_DB_URL, BACKEND_BASE_URL, ALLOWED_ORIGINS
"""

import os
import re
import time
import logging
from typing import AsyncGenerator, Union, Optional, List

from dotenv import load_dotenv
import httpx
from fastapi import FastAPI, HTTPException, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse, Response
from pydantic import BaseModel

from telethon import TelegramClient, errors
from telethon.tl.types import MessageMediaDocument, DocumentAttributeVideo

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("tg-stream-backend")

load_dotenv()  # .env faylidagi o'zgaruvchilarni avtomatik yuklaydi

# --- KONFIGURATSIYA (.env yoki environment orqali) ---
API_ID = int(os.getenv("TG_API_ID", "0"))
API_HASH = os.getenv("TG_API_HASH", "")
FIREBASE_DB_URL = os.getenv(
    "FIREBASE_DB_URL",
    "https://anime-fee0d-default-rtdb.asia-southeast1.firebasedatabase.app",
)
BACKEND_BASE_URL = os.getenv("BACKEND_BASE_URL", "http://localhost:8000")

# Frontend manzillarini aniq ko'rsating (allow_credentials=True bo'lsa "*" ishlamaydi)
ALLOWED_ORIGINS = [
    origin.strip()
    for origin in os.getenv("ALLOWED_ORIGINS", "http://localhost:3000").split(",")
    if origin.strip()
]

# Telegram CDN chunk hajmi (4096ga karrali, 1MB gacha bo'lishi kerak)
CHUNK_REQUEST_SIZE = 1024 * 1024  # 1 MB

logger.info("Ruxsat etilgan CORS originlar: %s", ALLOWED_ORIGINS)

if not API_ID or not API_HASH:
    logger.warning(
        "TG_API_ID / TG_API_HASH environment o'zgaruvchilari topilmadi. "
        "Ularni sozlamasangiz Telegram clienti ishlamaydi."
    )

app = FastAPI(title="Telegram Video Streaming System Backend")

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

client = TelegramClient("telegram_session", API_ID, API_HASH)

# Firebase bilan ishlash uchun bitta umumiy async HTTP klient
http_client: httpx.AsyncClient | None = None


@app.on_event("startup")
async def startup_event():
    global http_client
    http_client = httpx.AsyncClient(timeout=10.0)

    await client.connect()
    if not await client.is_user_authorized():
        logger.warning(
            "-----------------------------------------------------------------\n"
            "Telegram hisobi hali avtorizatsiyadan o'tmagan.\n"
            "Hozir terminalda telefon raqami va tasdiqlash kodini kiriting.\n"
            "-----------------------------------------------------------------"
        )
        # client.start() — telefon raqami/kodni so'rab, bir martalik loginni
        # yakunlaydi va sessiyani "telegram_session.session" fayliga yozadi.
        await client.start()

    logger.info("Backend tayyor: server diskiga video saqlanmaydi (faqat jonli oqim).")


@app.on_event("shutdown")
async def shutdown_event():
    if http_client:
        await http_client.aclose()
    if client.is_connected():
        await client.disconnect()


@app.get("/")
async def health_check():
    """Admin panel va frontend backend ishlab turganini tekshirishi uchun."""
    return {
        "status": "ok",
        "service": "Telegram Video Streaming Backend",
        "telegram_authorized": await client.is_user_authorized() if client.is_connected() else False,
    }


class LinkPayload(BaseModel):
    telegramLink: str
    customTitle: Optional[str] = None
    category: Optional[str] = None
    tags: Optional[List[str]] = None


def parse_telegram_link(url: str):
    """
    Telegram post linklarini ajratib oladi.
    Qo'llab-quvvatlanadigan formatlar:
      - https://t.me/c/1234567890/5432            (yopiq kanal/guruh)
      - https://t.me/public_channel_username/5432 (ochiq kanal)
    Qaytaradi: (channel_identifier, message_id)
      channel_identifier -> int (yopiq kanal uchun, -100 prefiksi bilan)
                          -> str (ochiq kanal uchun, username)
    """
    private_pattern = r"t\.me/c/(\d+)/(\d+)"
    public_pattern = r"t\.me/([a-zA-Z0-9_]+)/(\d+)"

    private_match = re.search(private_pattern, url)
    if private_match:
        channel_id = int(f"-100{private_match.group(1)}")
        message_id = int(private_match.group(2))
        return channel_id, message_id

    public_match = re.search(public_pattern, url)
    if public_match:
        username = public_match.group(1)
        message_id = int(public_match.group(2))
        return username, message_id

    raise ValueError("Telegram post linki formati noto'g'ri. Kanal va xabar ID topilmadi.")


def channel_identifier_to_str(channel_id: Union[int, str]) -> str:
    """Saqlash uchun: kanal identifikatorini qatorga aylantiradi."""
    return str(channel_id)


def channel_identifier_from_str(raw: str) -> Union[int, str]:
    """Stream endpoint uchun: saqlangan qatorni qayta int/str ko'rinishiga keltiradi."""
    if re.fullmatch(r"-?\d+", raw):
        return int(raw)
    return raw


async def get_tg_message(channel_identifier: Union[int, str], message_id: int):
    try:
        entity = await client.get_input_entity(channel_identifier)
    except ValueError as e:
        raise ValueError(f"Kanal topilmadi yoki kirish huquqi yo'q: {e}")
    except errors.ChannelPrivateError:
        raise ValueError("Bu kanal yopiq va bot/akkaunt unga a'zo emas.")

    try:
        messages = await client.get_messages(entity, ids=[message_id])
    except errors.FloodWaitError as e:
        raise ValueError(f"Telegram so'rovlar chegarasi: {e.seconds} soniya kuting.")

    if not messages or messages[0] is None:
        raise ValueError("Xabar topilmadi yoki o'chirilgan.")
    return messages[0]


@app.post("/api/save-video")
async def save_video(payload: LinkPayload):
    if not await client.is_user_authorized():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Telegram klient sessiyasi avtorizatsiyadan o'tmagan.",
        )

    try:
        channel_identifier, message_id = parse_telegram_link(payload.telegramLink)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    try:
        msg = await get_tg_message(channel_identifier, message_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

    if not msg.media or not isinstance(msg.media, MessageMediaDocument):
        raise HTTPException(
            status_code=400, detail="Berilgan Telegram postida fayl biriktirilmagan."
        )

    document = msg.media.document
    is_video = any(isinstance(attr, DocumentAttributeVideo) for attr in document.attributes)
    mime_type = document.mime_type or ""

    if not is_video and not mime_type.startswith("video/"):
        raise HTTPException(
            status_code=400, detail="Bu Telegram postidagi fayl video emas."
        )

    # Sarlavha: admin panelda kiritilgan bo'lsa shu ishlatiladi,
    # aks holda fayl nomi yoki xabar matnidan olinadi.
    if payload.customTitle and payload.customTitle.strip():
        video_title = payload.customTitle.strip()
    else:
        video_title = "Nomsiz video"
        for attr in document.attributes:
            if getattr(attr, "file_name", None):
                video_title = attr.file_name
                break
        if video_title == "Nomsiz video" and msg.message:
            video_title = msg.message.split("\n")[0][:60].strip() or video_title

    channel_str = channel_identifier_to_str(channel_identifier)
    generated_id = f"vid_{int(time.time() * 1000)}"
    stream_url = f"{BACKEND_BASE_URL}/api/stream/{channel_str}/{message_id}"

    firebase_payload = {
        "id": generated_id,
        "title": video_title,
        "telegramLink": payload.telegramLink,
        "streamUrl": stream_url,
        "messageId": message_id,
        "channelId": channel_str,
        "fileSize": document.size,
        "mimeType": mime_type or "video/mp4",
        "category": (payload.category or "Boshqa").strip(),
        "tags": payload.tags or [],
        "createdAt": int(time.time() * 1000),
    }

    try:
        fb_res = await http_client.put(
            f"{FIREBASE_DB_URL}/videos/{generated_id}.json", json=firebase_payload
        )
        if fb_res.status_code != 200:
            raise HTTPException(
                status_code=502, detail=f"Firebase yozish xatosi: {fb_res.text}"
            )
    except httpx.RequestError as e:
        raise HTTPException(
            status_code=502, detail=f"Firebase bilan bog'lanib bo'lmadi: {e}"
        )

    return JSONResponse(status_code=200, content={"success": True, "data": firebase_payload})


@app.delete("/api/videos/{firebase_key}")
async def delete_video(firebase_key: str):
    """
    Videoni Firebase katalogidan o'chiradi. Admin panelning "Videolar"
    jadvalidagi 🗑️ tugmasi shu endpointga murojaat qiladi. Server diskida
    video fayllar saqlanmagani uchun bu yerda kesh tozalash kerak emas.
    """
    try:
        get_res = await http_client.get(f"{FIREBASE_DB_URL}/videos/{firebase_key}.json")
    except httpx.RequestError as e:
        raise HTTPException(status_code=502, detail=f"Firebase bilan bog'lanib bo'lmadi: {e}")

    record = get_res.json()
    if not record:
        raise HTTPException(status_code=404, detail="Video topilmadi (allaqachon o'chirilgan bo'lishi mumkin).")

    try:
        del_res = await http_client.delete(f"{FIREBASE_DB_URL}/videos/{firebase_key}.json")
        if del_res.status_code != 200:
            raise HTTPException(status_code=502, detail=f"Firebase'dan o'chirishda xatolik: {del_res.text}")
    except httpx.RequestError as e:
        raise HTTPException(status_code=502, detail=f"Firebase bilan bog'lanib bo'lmadi: {e}")

    return {"success": True, "deleted": firebase_key}


@app.get("/api/stream/{channel_id}/{message_id}")
async def stream_video(channel_id: str, message_id: int, request: Request):
    """
    Telegramdagi faylni HAR DOIM to'g'ridan-to'g'ri Telegramdan, diskka
    saqlamasdan, HTTP Range so'rovlariga mos ravishda jonli oqim sifatida
    uzatadi. Qaytadan tomosha qilishda tezlik uchun keshlash frontend
    tomonida (Service Worker, brauzer keshi) amalga oshiriladi — server
    diskiga hech narsa yozilmaydi.
    """
    if not await client.is_user_authorized():
        raise HTTPException(status_code=503, detail="Telegram klient avtorizatsiyadan o'tmagan.")

    channel_identifier = channel_identifier_from_str(channel_id)

    try:
        msg = await get_tg_message(channel_identifier, message_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

    if not msg.media or not isinstance(msg.media, MessageMediaDocument):
        raise HTTPException(status_code=400, detail="Bu xabarda yaroqli media topilmadi.")

    document = msg.media.document
    file_size = document.size
    mime_type = document.mime_type or "video/mp4"

    # --- Range header'ni tahlil qilish (bytes=start-end yoki bytes=-suffix) ---
    range_header = request.headers.get("range")
    start_byte = 0
    end_byte = file_size - 1
    is_partial = False

    if range_header:
        match = re.match(r"bytes=(\d*)-(\d*)", range_header)
        if not match:
            raise HTTPException(status_code=416, detail="Range header formati noto'g'ri.")

        start_str, end_str = match.groups()
        if start_str == "" and end_str == "":
            raise HTTPException(status_code=416, detail="Range header bo'sh.")

        if start_str == "":
            # suffix-range: oxirgi N baytni so'rash (bytes=-500)
            suffix_len = int(end_str)
            start_byte = max(file_size - suffix_len, 0)
            end_byte = file_size - 1
        else:
            start_byte = int(start_str)
            end_byte = int(end_str) if end_str else file_size - 1

        is_partial = True

    if start_byte >= file_size or end_byte >= file_size or start_byte > end_byte:
        return Response(
            status_code=416,
            headers={"Content-Range": f"bytes */{file_size}"},
        )

    total_needed = end_byte - start_byte + 1

    # --- TELEGRAMDAN JONLI OQIM (har doim, diskka yozmasdan) ---
    async def telegram_buffer_generator() -> AsyncGenerator[bytes, None]:
        aligned_offset = (start_byte // CHUNK_REQUEST_SIZE) * CHUNK_REQUEST_SIZE
        bytes_to_skip = start_byte - aligned_offset
        remaining = total_needed
        skip_left = bytes_to_skip

        try:
            async for chunk in client.iter_download(
                document,
                offset=aligned_offset,
                request_size=CHUNK_REQUEST_SIZE,
            ):
                if remaining <= 0:
                    break

                if skip_left > 0:
                    if skip_left >= len(chunk):
                        skip_left -= len(chunk)
                        continue
                    chunk = chunk[skip_left:]
                    skip_left = 0

                if len(chunk) > remaining:
                    chunk = chunk[:remaining]

                remaining -= len(chunk)
                yield chunk
        except errors.FloodWaitError as e:
            logger.error("Telegram FloodWait: %s soniya", e.seconds)
            return
        except Exception:
            logger.exception("Stream paytida xatolik yuz berdi")
            return

    headers = {
        "Accept-Ranges": "bytes",
        "Content-Length": str(total_needed),
        "Content-Type": mime_type,
        "Cache-Control": "public, max-age=86400",
    }
    if is_partial:
        headers["Content-Range"] = f"bytes {start_byte}-{end_byte}/{file_size}"

    return StreamingResponse(
        telegram_buffer_generator(),
        status_code=206 if is_partial else 200,
        headers=headers,
    )


if __name__ == "__main__":
    import uvicorn

    # Eslatma: birinchi marta ishga tushirishda Telegram login (telefon raqami + kod)
    # interaktiv so'raladi. Bu so'rov startup_event() ichida (client.connect() orqali)
    # avtomatik amalga oshadi. Sessiya "telegram_session.session" faylida saqlanadi.
    #
    # MUHIM: bu yerda "app:app" (qator) emas, to'g'ridan-to'g'ri `app` obyekti
    # beriladi. Aks holda uvicorn modulni qaytadan import qiladi, bu esa
    # TelegramClient'ni ikkinchi marta yaratadi va "database is locked" xatosiga
    # olib keladi (chunki bir xil .session faylga ikki marta ulanishga urinadi).
    uvicorn.run(app, host="0.0.0.0", port=8000)