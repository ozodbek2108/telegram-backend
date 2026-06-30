"""
Telegram Video Streaming System Backend
-----------------------------------------
Telegram'dagi video postlarni Firebase Realtime DB'ga saqlaydi va
Range so'rovlari (HTTP Range) orqali to'g'ridan-to'g'ri Telegramdan
video oqimini (streaming) beradi.

YANGI (StringSession versiyasi): Telegram sessiyasi endi diskka
(.session fayl) yozilmaydi. Buning o'rniga Telethon'ning
StringSession mexanizmi ishlatiladi — sessiya bitta matn satri
(string) ko'rinishida bo'ladi va u to'g'ridan-to'g'ri Firebase
Realtime Database'dagi:

    {FIREBASE_DB_URL}/telegram_config.json

tuguniga yoziladi (api_id, api_hash, phone, session_string birga).
Server qayta ishga tushganda shu yozuvni Firebase'dan o'qib, hech
qanday qo'shimcha fayl yaratmasdan avtomatik qayta ulanadi. Bu degani:
- Lokal `.session` fayl yo'q
- Lokal `telegram_config.json` fayl yo'q
- Render/Railway kabi ephemeral disk muhitlarida ham sessiya
  yo'qolib qolmaydi, chunki u Firebase'da saqlanadi.

DIQQAT: session string akkauntga to'liq kirish huquqini beradi —
Firebase Realtime Database qoidalaringizni (Rules) shunga yarasha
himoyalang (masalan, faqat backend service-account orqali yozish/
o'qish ruxsat etilsin).

Endpointlar:
  -- Telegram ulanishi (admin panel "Telegram ulanishi" bo'limi) --
  POST   /api/telegram/configure      -> api_id/api_hash/phone qabul qiladi, kod yuboradi
  POST   /api/telegram/verify-code    -> SMS/Telegram orqali kelgan kodni tasdiqlaydi
  POST   /api/telegram/verify-password-> 2FA yoqilgan bo'lsa, parolni tasdiqlaydi
  POST   /api/telegram/disconnect     -> joriy sessiyani uzadi (Firebase'dan ham o'chiradi)
  GET    /api/telegram/status         -> joriy ulanish holatini qaytaradi

  -- Video / health --
  POST   /api/save-video        -> Telegram havolasidan video qo'shish (Admin -> "Video yuklash")
  DELETE /api/videos/{key}      -> Videoni katalogdan o'chirish (Admin -> "Videolar" jadvali)
  GET    /api/stream/{c}/{m}    -> Videoni Range so'rovlari bilan oqim sifatida uzatish (Player)
  GET    /                      -> Backend ishlab turganini tekshirish (health-check)

Ishga tushirishdan oldin faqat quyidagilarni sozlang (.env fayliga qarang):
  FIREBASE_DB_URL, BACKEND_BASE_URL, ALLOWED_ORIGINS
(TG_API_ID / TG_API_HASH / .session endi kerak emas — ular admin
paneldan kiritiladi va Firebase'da saqlanadi)
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
from telethon.sessions import StringSession
from telethon.tl.types import MessageMediaDocument, DocumentAttributeVideo

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("tg-stream-backend")

load_dotenv()  # .env faylidagi o'zgaruvchilarni avtomatik yuklaydi

# --- KONFIGURATSIYA (.env yoki environment orqali) ---
FIREBASE_DB_URL = os.getenv(
    "FIREBASE_DB_URL",
    "https://anime-fee0d-default-rtdb.asia-southeast1.firebasedatabase.app",
).rstrip("/")
BACKEND_BASE_URL = os.getenv("BACKEND_BASE_URL", "http://localhost:8000")

# Frontend manzillarini aniq ko'rsating (allow_credentials=True bo'lsa "*" ishlamaydi)
ALLOWED_ORIGINS = [
    origin.strip()
    for origin in os.getenv("ALLOWED_ORIGINS", "http://localhost:3000").split(",")
    if origin.strip()
]

# Telegram CDN chunk hajmi (4096ga karrali, 1MB gacha bo'lishi kerak)
CHUNK_REQUEST_SIZE = 1024 * 1024  # 1 MB

# Firebase'dagi Telegram konfiguratsiyasi manzili (api_id/api_hash/phone/session_string)
TG_CONFIG_FB_PATH = f"{FIREBASE_DB_URL}/telegram_config.json"

logger.info("Ruxsat etilgan CORS originlar: %s", ALLOWED_ORIGINS)
logger.info("Telegram konfiguratsiyasi Firebase'da saqlanadi: %s", TG_CONFIG_FB_PATH)

app = FastAPI(title="Telegram Video Streaming System Backend")

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

# Firebase bilan ishlash uchun bitta umumiy async HTTP klient
http_client: httpx.AsyncClient | None = None

# ============================================================
# TELEGRAM HOLATI (runtime, admin panel orqali to'ldiriladi)
# ============================================================
# status mumkin bo'lgan qiymatlar:
#   not_configured     -> hali api_id/api_hash/phone kiritilmagan
#   code_sent          -> kod yuborildi, tasdiqlash kutilmoqda
#   awaiting_password  -> 2FA yoqilgan, parol kutilmoqda
#   connected          -> to'liq ulangan, video olish/oqim berish mumkin
tg_state = {
    "client": None,          # TelegramClient instance (StringSession bilan)
    "api_id": None,
    "api_hash": None,
    "phone": None,
    "phone_code_hash": None,
    "status": "not_configured",
    "error": None,
}


# ============================================================
# FIREBASE ORQALI TELEGRAM KONFIGURATSIYASINI SAQLASH / O'QISH
# (lokal fayl YO'Q — hammasi shu yerdan o'tadi)
# ============================================================

async def save_tg_config_to_firebase():
    """
    api_id, api_hash, phone va joriy session string'ni
    {FIREBASE_DB_URL}/telegram_config.json tuguniga yozadi.
    Bu chaqiriq muvaffaqiyatli login'dan (kod yoki parol) keyin ishlatiladi.
    """
    client: TelegramClient = tg_state["client"]
    if client is None:
        return
    try:
        session_string = client.session.save()  # StringSession -> matn ko'rinishi
        payload = {
            "api_id": tg_state["api_id"],
            "api_hash": tg_state["api_hash"],
            "phone": tg_state["phone"],
            "session_string": session_string,
            "updatedAt": int(time.time() * 1000),
        }
        res = await http_client.put(TG_CONFIG_FB_PATH, json=payload)
        if res.status_code != 200:
            logger.error("Telegram konfiguratsiyasini Firebase'ga yozib bo'lmadi: %s", res.text)
        else:
            logger.info("Telegram konfiguratsiyasi Firebase'ga saqlandi (%s).", tg_state["phone"])
    except Exception:
        logger.exception("Telegram konfiguratsiyasini Firebase'ga yozishda xatolik")


async def load_tg_config_from_firebase():
    """
    Firebase'dagi /telegram_config tugunini o'qiydi.
    Qaytaradi: dict yoki None (agar mavjud bo'lmasa).
    """
    try:
        res = await http_client.get(TG_CONFIG_FB_PATH)
        if res.status_code != 200:
            return None
        data = res.json()
        return data or None
    except Exception:
        logger.exception("Telegram konfiguratsiyasini Firebase'dan o'qishda xatolik")
        return None


async def delete_tg_config_from_firebase():
    """Uzilganda Firebase'dagi konfiguratsiya yozuvini butunlay o'chiradi."""
    try:
        res = await http_client.delete(TG_CONFIG_FB_PATH)
        if res.status_code != 200:
            logger.error("Telegram konfiguratsiyasini Firebase'dan o'chirib bo'lmadi: %s", res.text)
    except Exception:
        logger.exception("Telegram konfiguratsiyasini Firebase'dan o'chirishda xatolik")


@app.on_event("startup")
async def startup_event():
    global http_client
    http_client = httpx.AsyncClient(timeout=15.0)

    # Avval Firebase'da saqlangan konfiguratsiya bo'lsa, sessiyani
    # avtomatik tiklashga harakat qilamiz (StringSession orqali —
    # hech qanday lokal fayl o'qilmaydi/yozilmaydi).
    cfg = await load_tg_config_from_firebase()
    if cfg and cfg.get("api_id") and cfg.get("api_hash") and cfg.get("session_string"):
        try:
            client = TelegramClient(
                StringSession(cfg["session_string"]),
                int(cfg["api_id"]),
                cfg["api_hash"],
            )
            await client.connect()
            if await client.is_user_authorized():
                tg_state.update({
                    "client": client,
                    "api_id": int(cfg["api_id"]),
                    "api_hash": cfg["api_hash"],
                    "phone": cfg.get("phone"),
                    "status": "connected",
                    "error": None,
                })
                logger.info("Telegram avtomatik ulandi (Firebase session): %s", cfg.get("phone"))
            else:
                await client.disconnect()
                logger.info("Firebase'dagi sessiya yaroqsiz, admin panel orqali qayta ulanish kerak.")
        except Exception:
            logger.exception("Firebase'dagi Telegram sessiyasini tiklashda xatolik")

    logger.info("Backend tayyor: server diskiga video va sessiya saqlanmaydi (hammasi Firebase'da).")


@app.on_event("shutdown")
async def shutdown_event():
    if http_client:
        await http_client.aclose()
    if tg_state["client"] and tg_state["client"].is_connected():
        await tg_state["client"].disconnect()


@app.get("/")
async def health_check():
    """Admin panel va frontend backend ishlab turganini tekshirishi uchun."""
    return {
        "status": "ok",
        "service": "Telegram Video Streaming Backend",
        "telegram_authorized": tg_state["status"] == "connected",
        "telegram_status": tg_state["status"],
    }


# ============================================================
# TELEGRAM ULANISH ENDPOINTLARI (Admin panel "Telegram ulanishi" bo'limi)
# ============================================================

class TelegramConfigPayload(BaseModel):
    api_id: int
    api_hash: str
    phone: str  # xalqaro formatda, masalan +998901234567


@app.post("/api/telegram/configure")
async def telegram_configure(payload: TelegramConfigPayload):
    """
    1-qadam: API ID, API Hash va telefon raqam qabul qilinadi, Telegram'ga
    ulanib, shu raqamga tasdiqlash kodi yuboriladi. Sessiya StringSession
    sifatida xotirada yaratiladi (hali hech qanday faylga yozilmaydi).
    """
    # Eski ulanish bo'lsa, avval uni to'g'ri tugatamiz.
    if tg_state["client"] and tg_state["client"].is_connected():
        await tg_state["client"].disconnect()

    phone = payload.phone.strip()
    client = TelegramClient(StringSession(), payload.api_id, payload.api_hash)

    try:
        await client.connect()
        sent = await client.send_code_request(phone)
    except errors.ApiIdInvalidError:
        raise HTTPException(400, "API ID yoki API Hash noto'g'ri.")
    except errors.PhoneNumberInvalidError:
        raise HTTPException(400, "Telefon raqam formati noto'g'ri (masalan: +998901234567).")
    except errors.FloodWaitError as e:
        raise HTTPException(429, f"Telegram so'rovlar chegarasi: {e.seconds} soniya kuting.")
    except Exception as e:
        raise HTTPException(400, f"Telegram bilan bog'lanib bo'lmadi: {e}")

    tg_state.update({
        "client": client,
        "api_id": payload.api_id,
        "api_hash": payload.api_hash,
        "phone": phone,
        "phone_code_hash": sent.phone_code_hash,
        "status": "code_sent",
        "error": None,
    })
    return {"status": "code_sent", "message": "Tasdiqlash kodi yuborildi."}


class CodePayload(BaseModel):
    code: str


@app.post("/api/telegram/verify-code")
async def telegram_verify_code(payload: CodePayload):
    """2-qadam: Telegram/SMS orqali kelgan kodni tasdiqlaydi."""
    if tg_state["status"] not in ("code_sent",):
        raise HTTPException(400, "Avval telefon raqamga kod yuborilishi kerak.")

    client = tg_state["client"]
    try:
        await client.sign_in(
            tg_state["phone"], payload.code.strip(),
            phone_code_hash=tg_state["phone_code_hash"],
        )
    except errors.SessionPasswordNeededError:
        # 2 bosqichli himoya (2FA) yoqilgan -> parol kerak
        tg_state["status"] = "awaiting_password"
        return {"status": "awaiting_password", "message": "Bu akkauntda 2 bosqichli himoya yoqilgan. Parolni kiriting."}
    except errors.PhoneCodeInvalidError:
        raise HTTPException(400, "Kiritilgan kod noto'g'ri.")
    except errors.PhoneCodeExpiredError:
        raise HTTPException(400, "Kod muddati tugagan. Qaytadan kod so'rang.")
    except Exception as e:
        raise HTTPException(400, f"Kodni tasdiqlashda xatolik: {e}")

    tg_state["status"] = "connected"
    tg_state["error"] = None
    await save_tg_config_to_firebase()  # session string Firebase'ga yoziladi
    return {"status": "connected", "message": "Telegram muvaffaqiyatli ulandi!"}


class PasswordPayload(BaseModel):
    password: str


@app.post("/api/telegram/verify-password")
async def telegram_verify_password(payload: PasswordPayload):
    """3-qadam (faqat 2FA yoqilgan akkauntlar uchun): bulutli parolni tasdiqlaydi."""
    if tg_state["status"] != "awaiting_password":
        raise HTTPException(400, "Hozir parol kutilmayapti.")

    client = tg_state["client"]
    try:
        await client.sign_in(password=payload.password)
    except errors.PasswordHashInvalidError:
        raise HTTPException(400, "Parol noto'g'ri.")
    except Exception as e:
        raise HTTPException(400, f"Parolni tasdiqlashda xatolik: {e}")

    tg_state["status"] = "connected"
    tg_state["error"] = None
    await save_tg_config_to_firebase()  # session string Firebase'ga yoziladi
    return {"status": "connected", "message": "Telegram muvaffaqiyatli ulandi!"}


@app.post("/api/telegram/disconnect")
async def telegram_disconnect():
    """Joriy Telegram sessiyasini uzadi va Firebase'dagi yozuvni o'chiradi."""
    if tg_state["client"]:
        try:
            await tg_state["client"].disconnect()
        except Exception:
            pass

    await delete_tg_config_from_firebase()

    tg_state.update({
        "client": None, "api_id": None, "api_hash": None, "phone": None,
        "phone_code_hash": None, "status": "not_configured", "error": None,
    })
    return {"status": "not_configured", "message": "Telegram uzildi."}


@app.get("/api/telegram/status")
async def telegram_status():
    """Joriy Telegram ulanish holatini qaytaradi (admin panel shu orqali ko'rsatadi)."""
    return {
        "status": tg_state["status"],
        "phone": tg_state.get("phone"),
        "error": tg_state.get("error"),
    }


# ============================================================
# VIDEO BILAN ISHLASH (mavjud funksionallik, faqat tg_state orqali)
# ============================================================

class LinkPayload(BaseModel):
    telegramLink: str
    customTitle: Optional[str] = None
    category: Optional[str] = None
    tags: Optional[List[str]] = None
    thumbnail: Optional[str] = None


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


def require_connected_client() -> TelegramClient:
    if tg_state["status"] != "connected" or tg_state["client"] is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Telegram ulanmagan. Admin panel -> Sozlamalar -> Telegram ulanishi bo'limidan ulang.",
        )
    return tg_state["client"]


async def get_tg_message(client: TelegramClient, channel_identifier: Union[int, str], message_id: int):
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
    client = require_connected_client()

    try:
        channel_identifier, message_id = parse_telegram_link(payload.telegramLink)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    try:
        msg = await get_tg_message(client, channel_identifier, message_id)
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
        "thumbnail": payload.thumbnail,
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
    client = require_connected_client()

    channel_identifier = channel_identifier_from_str(channel_id)

    try:
        msg = await get_tg_message(client, channel_identifier, message_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

    if not msg.media or not isinstance(msg.media, MessageMediaDocument):
        raise HTTPException(status_code=400, detail="Bu xabarda yaroqli media topilmadi.")

    document = msg.media.document
    file_size = document.size
    mime_type = document.mime_type or "video/mp4"

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

    # MUHIM: bu yerda "app:app" (qator) emas, to'g'ridan-to'g'ri `app` obyekti
    # beriladi. Aks holda uvicorn modulni qaytadan import qiladi, bu esa
    # bir xil StringSession ulanishiga ikki marta urinib xatolikka olib
    # kelishi mumkin.
    uvicorn.run(app, host="0.0.0.0", port=8000)