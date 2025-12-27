import os
import json
import logging
import asyncio
import re
import time
import secrets
import sqlite3
import unicodedata
from datetime import datetime

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, LabeledPrice
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
    CallbackQueryHandler,
    PreCheckoutQueryHandler,
)

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError

# ============== AYARLAR ==============
BOT_TOKEN = "8428131175:AAFsV7LYT7O_KRAcJ2jmKcjQTh1euSYCqR0"
API_ID = "24302768"
API_HASH = "7082b3b3331e7d12971ea9ef19e2d58b"

ADMIN_ID = 6840212721
LOG_GROUP_ID = -1002095036242  # Yönetim grubu

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
USER_DATA_FILE = os.path.join(BASE_DIR, "pdf_users.json")
USERBOT_DATA_FILE = os.path.join(BASE_DIR, "userbots.json")
CHANNEL_DATA_FILE = os.path.join(BASE_DIR, "channels.json")

DB_FILE = os.path.join(BASE_DIR, "pdf_index.db")  # ✅ indeks db

MAX_RESULTS = 8
FREE_REQUESTS = 3

# Üyelik planları
MEMBERSHIP_PLANS = {
    'Basic': {'price': 150, 'requests': 150, 'stars': 150},
    'Premium': {'price': 300, 'requests': 300, 'stars': 300},
    'Unlimited': {'price': 749, 'requests': 'Sınırsız', 'stars': 749}
}

# Global değişkenler
userbot_clients = {}  # {userbot_id: TelegramClient}
pending_auth = {}     # {user_id: {'phone': ..., 'phone_code_hash': ..., 'client': ...}}
download_lock = set()  # aynı kullanıcı aynı anda 2 kez indirmesin

# ============== LOG ==============
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ============== JSON YARDIMCI ==============
def load_json(filepath: str) -> dict:
    if not os.path.exists(filepath):
        return {}
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, list):
                return {}
            return data
    except Exception as e:
        logger.error(f"JSON okunamadı ({filepath}): {e}")
        return {}

def save_json(filepath: str, data: dict):
    try:
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"JSON yazılamadı ({filepath}): {e}")

# ============== KULLANICI YÖNETİMİ ==============
def load_user_data() -> dict:
    return load_json(USER_DATA_FILE)

def save_user_data(data: dict):
    save_json(USER_DATA_FILE, data)

def get_user_requests(user_id: int) -> int:
    if user_id == ADMIN_ID:
        return float('inf')

    user_data = load_user_data()
    uid = str(user_id)

    if uid not in user_data:
        user_data[uid] = {"requests_left": FREE_REQUESTS, "membership": None, "total_requests": 0}
        save_user_data(user_data)
        return FREE_REQUESTS

    membership = user_data[uid].get("membership")
    if membership == "Unlimited":
        return float('inf')

    return user_data[uid].get("requests_left", 0)

def decrease_user_requests(user_id: int) -> bool:
    if user_id == ADMIN_ID:
        user_data = load_user_data()
        uid = str(user_id)
        if uid not in user_data:
            user_data[uid] = {"total_requests": 0, "membership": "Admin"}
        user_data[uid]["total_requests"] = user_data[uid].get("total_requests", 0) + 1
        save_user_data(user_data)
        return True

    user_data = load_user_data()
    uid = str(user_id)
    if uid not in user_data:
        user_data[uid] = {"requests_left": FREE_REQUESTS, "membership": None, "total_requests": 0}

    membership = user_data[uid].get("membership")
    if membership == "Unlimited":
        user_data[uid]["total_requests"] = user_data[uid].get("total_requests", 0) + 1
        save_user_data(user_data)
        return True

    left = user_data[uid].get("requests_left", 0)
    if left > 0:
        user_data[uid]["requests_left"] = left - 1
        user_data[uid]["total_requests"] = user_data[uid].get("total_requests", 0) + 1
        save_user_data(user_data)
        return True

    return False

def add_user_requests(user_id: int, membership_type: str):
    user_data = load_user_data()
    uid = str(user_id)
    plan = MEMBERSHIP_PLANS[membership_type]

    if uid not in user_data:
        user_data[uid] = {
            "requests_left": plan['requests'] if plan['requests'] != 'Sınırsız' else 0,
            "membership": membership_type,
            "total_requests": 0,
            "started": True
        }
    else:
        user_data[uid]["membership"] = membership_type
        if plan['requests'] != 'Sınırsız':
            user_data[uid]["requests_left"] = plan['requests']

    save_user_data(user_data)

# ============== USERBOT / KANAL YÖNETİMİ ==============
def load_userbots() -> dict:
    return load_json(USERBOT_DATA_FILE)

def save_userbots(data: dict):
    save_json(USERBOT_DATA_FILE, data)

def load_channels() -> dict:
    return load_json(CHANNEL_DATA_FILE)

def save_channels(data: dict):
    save_json(CHANNEL_DATA_FILE, data)

async def init_userbots():
    userbots = load_userbots()
    for ub_id, ub_data in userbots.items():
        try:
            session = ub_data.get('session')
            if not session:
                continue

            client = TelegramClient(StringSession(session), API_ID, API_HASH)
            await client.connect()
            if await client.is_user_authorized():
                userbot_clients[ub_id] = client

                try:
                    await client.get_entity(LOG_GROUP_ID)
                    logger.info(f"Userbot {ub_id} log grubuna erişebiliyor")
                except Exception as e:
                    logger.warning(f"Userbot {ub_id} log grubuna erişemiyor: {e}")

                logger.info(f"Userbot {ub_id} başlatıldı")
        except Exception as e:
            logger.error(f"Userbot {ub_id} başlatılamadı: {e}")

# ============== SQLITE INDEX ==============
def db_connect():
    conn = sqlite3.connect(DB_FILE)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn

def db_init():
    conn = db_connect()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pdfs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            norm_title TEXT NOT NULL,
            title TEXT NOT NULL,
            channel_link TEXT NOT NULL,
            message_id INTEGER NOT NULL,
            file_name TEXT,
            caption TEXT,
            added_at INTEGER NOT NULL,
            UNIQUE(channel_link, message_id)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_norm_title ON pdfs(norm_title);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_channel_msg ON pdfs(channel_link, message_id);")
    conn.commit()
    conn.close()

def normalize_text(s: str) -> str:
    if not s:
        return ""
    s = s.strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.replace("ı", "i").replace("İ", "i").replace("ş", "s").replace("ğ", "g").replace("ü", "u").replace("ö", "o").replace("ç", "c")
    s = re.sub(r"[\W_]+", " ", s, flags=re.UNICODE)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def build_title(file_name: str, caption: str) -> str:
    # başlık: dosya adı + caption
    fn = (file_name or "").strip()
    cap = (caption or "").strip()
    if fn and cap:
        return f"{fn} | {cap}"
    return fn or cap or "pdf"

def db_upsert_pdf(norm_title: str, title: str, channel_link: str, message_id: int, file_name: str, caption: str):
    conn = db_connect()
    conn.execute(
        """
        INSERT OR IGNORE INTO pdfs(norm_title, title, channel_link, message_id, file_name, caption, added_at)
        VALUES(?,?,?,?,?,?,?)
        """,
        (norm_title, title, channel_link, int(message_id), file_name, caption, int(time.time()))
    )
    conn.commit()
    conn.close()

def db_count():
    conn = db_connect()
    cur = conn.execute("SELECT COUNT(*) FROM pdfs")
    n = cur.fetchone()[0]
    conn.close()
    return n

def db_search(query: str, limit: int = MAX_RESULTS):
    """
    Basit ama etkili skor: query kelimeleri norm_title içinde ne kadar geçiyor
    """
    qn = normalize_text(query)
    if not qn:
        return []

    tokens = qn.split()
    if not tokens:
        return []

    conn = db_connect()

    # Çok geniş LIKE yerine token bazlı filtre
    # Önce ilk token ile daralt, sonra python ile skorla
    first = tokens[0]
    cur = conn.execute(
        "SELECT id, title, channel_link, message_id, file_name, caption, norm_title FROM pdfs WHERE norm_title LIKE ? LIMIT 300",
        (f"%{first}%",)
    )
    rows = cur.fetchall()
    conn.close()

    scored = []
    for (pid, title, channel_link, message_id, file_name, caption, norm_title) in rows:
        score = 0
        # tam ifade bonus
        if qn in norm_title:
            score += 100
        # tüm tokenlar
        if all(t in norm_title for t in tokens):
            score += 50
        # token sayısı
        score += sum(10 for t in tokens if t in norm_title)
        # dosya adı bonus
        fn = normalize_text(file_name or "")
        if fn and qn in fn:
            score += 30
        elif fn:
            score += sum(6 for t in tokens if t in fn)

        if score >= 20:
            scored.append((score, pid, title, channel_link, message_id, file_name, caption))

    scored.sort(key=lambda x: x[0], reverse=True)
    out = []
    for item in scored[:limit]:
        _, pid, title, channel_link, message_id, file_name, caption = item
        out.append({
            "id": pid,
            "title": title,
            "channel_link": channel_link,
            "message_id": message_id,
            "file_name": file_name,
            "caption": caption,
        })
    return out

def db_get_pdf(pdf_id: int):
    conn = db_connect()
    cur = conn.execute(
        "SELECT id, title, channel_link, message_id, file_name, caption FROM pdfs WHERE id=?",
        (int(pdf_id),)
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    pid, title, channel_link, message_id, file_name, caption = row
    return {
        "id": pid,
        "title": title,
        "channel_link": channel_link,
        "message_id": message_id,
        "file_name": file_name,
        "caption": caption,
    }

async def get_channel_id_bounds(client, entity):
    last_msg = await client.get_messages(entity, limit=1)  # en yeni
    first_msg = await client.get_messages(entity, limit=1, reverse=True)  # en eski
    max_id = last_msg[0].id if last_msg else 0
    min_id = first_msg[0].id if first_msg else 0
    return min_id, max_id

def split_ranges(min_id: int, max_id: int, parts: int):
    step = max(1, (max_id - min_id + 1) // parts)
    ranges = []
    start = min_id
    for i in range(parts):
        end = max_id if i == parts - 1 else (start + step - 1)
        ranges.append((start, end))
        start = end + 1
    return ranges



async def index_one_channel(channel_link: str, max_messages: int = 20000, progress_cb=None):
    """
    İndeksleme: kanaldaki mesajları gezip PDF'leri DB'ye yazar.
    progress_cb: async callable(text) -> None (mesaj güncellemek için)
    """
    if not userbot_clients:
        return 0, 0

    client = next(iter(userbot_clients.values()))
    added = 0
    seen = 0
    started = time.time()
    last_update = 0.0

    try:
        ch = await client.get_entity(channel_link)
    except Exception as e:
        logger.error(f"Index channel entity alınamadı {channel_link}: {e}")
        return 0, 0

    try:
        async for msg in client.iter_messages(ch, limit=max_messages):
            seen += 1

            if msg and getattr(msg, "document", None):
                file_name = ""
                try:
                    for attr in msg.document.attributes:
                        if hasattr(attr, "file_name"):
                            file_name = (attr.file_name or "")
                            break
                except:
                    file_name = ""

                # pdf filtresi
                if file_name and not file_name.lower().endswith(".pdf"):
                    pass
                else:
                    caption = msg.text or ""
                    title = build_title(file_name, caption)
                    norm_title = normalize_text(title)

                    if norm_title:
                        await asyncio.to_thread(
                            db_upsert_pdf, norm_title, title, channel_link, msg.id, file_name, caption
                        )
                        added += 1

            # ---- Progress güncelleme (her 3 saniyede bir) ----
            now = time.time()
            if progress_cb and (now - last_update) >= 3:
                elapsed = max(1, int(now - started))
                speed = seen / elapsed  # msg/sn
                remaining = max_messages - seen
                eta = int(remaining / speed) if speed > 0 else 0
                await progress_cb(
                    f"👀 Taranan: {seen}/{max_messages}\n"
                    f"➕ Eklenen PDF: {added}\n"
                    f"⚡ Hız: {speed:.1f} msg/sn\n"
                    f"⏳ Tahmini kalan: {eta}s"
                )
                last_update = now

            # küçük nefes
            if seen % 500 == 0:
                await asyncio.sleep(0.05)

    except FloodWaitError as fw:
        logger.warning(f"FloodWait {fw.seconds}s - {channel_link}")
        if progress_cb:
            await progress_cb(f"⛔ FloodWait: {fw.seconds}s bekleniyor...")
        await asyncio.sleep(fw.seconds + 1)
    except Exception as e:
        logger.error(f"Indexleme hatası {channel_link}: {e}")
        if progress_cb:
            await progress_cb(f"❌ Hata: {e}")

    return added, seen


async def index_channel_range(client, channel_link: str, start_id: int, end_id: int, progress_cb=None):
    added = 0
    seen = 0
    started = time.time()
    last_update = 0.0

    try:
        ch = await client.get_entity(channel_link)
    except Exception as e:
        logger.error(f"Entity alınamadı {channel_link}: {e}")
        return 0, 0

    try:
        async for msg in client.iter_messages(ch, min_id=start_id-1, max_id=end_id+1):
            seen += 1

            if msg and getattr(msg, "document", None):
                file_name = ""
                try:
                    for attr in msg.document.attributes:
                        if hasattr(attr, "file_name"):
                            file_name = (attr.file_name or "")
                            break
                except:
                    file_name = ""

                mime = getattr(msg.document, "mime_type", "") or ""
                is_pdf = (file_name.lower().endswith(".pdf") if file_name else False) or (mime == "application/pdf")
                if not is_pdf:
                    continue

                caption = msg.text or ""
                title = build_title(file_name, caption)
                norm_title = normalize_text(title)

                if norm_title:
                    await asyncio.to_thread(
                        db_upsert_pdf, norm_title, title, channel_link, msg.id, file_name, caption
                    )
                    added += 1

            now = time.time()
            if progress_cb and (now - last_update) >= 3:
                elapsed = max(1, int(now - started))
                speed = seen / elapsed
                approx_total = max(1, (end_id - start_id + 1))
                remaining = max(0, approx_total - seen)
                eta = int(remaining / speed) if speed > 0 else 0
                await progress_cb(
                    f"🧩 Range: {start_id}-{end_id}\n"
                    f"👀 Taranan: {seen}\n"
                    f"➕ Eklenen PDF: {added}\n"
                    f"⚡ Hız: {speed:.1f} msg/sn\n"
                    f"⏳ ETA: {eta}s"
                )
                last_update = now

            if seen % 500 == 0:
                await asyncio.sleep(0.05)

    except FloodWaitError as fw:
        logger.warning(f"FloodWait {fw.seconds}s - {channel_link}")
        if progress_cb:
            await progress_cb(f"⛔ FloodWait: {fw.seconds}s bekleniyor...")
        await asyncio.sleep(fw.seconds + 1)
    except Exception as e:
        logger.error(f"Range index hatası {channel_link}: {e}")
        if progress_cb:
            await progress_cb(f"❌ Hata: {e}")

    return added, seen


# ============== /start ==============
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    left = get_user_requests(user_id)
    left_text = "Sınırsız" if left == float('inf') else str(left)

    metin = (
        "📚 **Kitap Arama Botu**\n\n"
        "🔍 Kitap adını yaz, sana sonuçları butonlu listeyle getireyim.\n\n"
        f"📊 **Kalan İstek Hakkın:** {left_text}\n\n"
        "➡️ Premium için /premium yazın."
    )
    await update.message.reply_text(metin, parse_mode="Markdown")

    try:
        await context.bot.send_message(
            chat_id=LOG_GROUP_ID,
            text=f"✅ Yeni kullanıcı: {update.effective_user.first_name} ({user_id})"
        )
    except:
        pass

# ============== /premium ==============
async def premium_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    left = get_user_requests(user_id)
    left_text = "Sınırsız" if left == float('inf') else str(left)

    premium_info = f"""💎 **Premium Üyelik Planları**

📊 Mevcut İstek Hakkınız: {left_text}

👇 Size uygun paketi seçin:

⭐ **Basic**: 150 istek (150 Star)
💫 **Premium**: 300 istek (300 Star)
🌟 **Unlimited**: Sınırsız istek (749 Star)
"""
    keyboard = []
    for plan_name, details in MEMBERSHIP_PLANS.items():
        keyboard.append([InlineKeyboardButton(f"{plan_name} ({details['stars']} Star)", callback_data=f"buy_{plan_name.lower()}")])

    await update.message.reply_text(premium_info, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(keyboard))

# ============== KULLANICI ARAMA ==============
async def handle_pdf_request(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return
    if update.message.chat.type != "private":
        return

    text = update.message.text.strip()
    if text.startswith("/"):
        return

    user_id = update.effective_user.id

    if get_user_requests(user_id) == 0:
        keyboard = [[InlineKeyboardButton("Premium Al 🌟", callback_data='show_plans')]]
        await update.message.reply_text(
            "❌ **İstek hakkın bitti!**\n\nDevam etmek için premium paketlere göz at.",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown"
        )
        return

    wait_msg = await update.message.reply_text("🔎 İndekste aranıyor...")

    # ✅ DB’den bul
    results = await asyncio.to_thread(db_search, text, MAX_RESULTS)

    if not results:
        await wait_msg.edit_text("❌ Bulunamadı. (İndeks güncel değilse admin /indexle çalıştırmalı)")
        return

    keyboard = []
    for i, r in enumerate(results, start=1):
        title = r["title"] or r["file_name"] or "PDF"
        btn_text = f"{i}) {title[:45]}"
        keyboard.append([InlineKeyboardButton(btn_text, callback_data=f"dlid:{r['id']}")])

    await wait_msg.edit_text(
        "✅ Sonuçlar bulundu. İndirmek istediğini seç:",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

# ============== ADMIN: USERBOT EKLEME ==============
async def admin_add_userbot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id != LOG_GROUP_ID:
        return
    if update.effective_user.id != ADMIN_ID:
        return

    await update.message.reply_text(
        "📱 **Userbot Ekleme**\n\n"
        "Telefon numaranızı başında + ile yazın:\n"
        "Örnek: +905551234567"
    )
    context.user_data['awaiting_phone'] = True

async def handle_admin_messages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id != LOG_GROUP_ID:
        return
    if update.effective_user.id != ADMIN_ID:
        return

    text = update.message.text.strip()

    if context.user_data.get('awaiting_phone'):
        try:
            client = TelegramClient(StringSession(), API_ID, API_HASH)
            await client.connect()

            result = await client.send_code_request(text)
            pending_auth[ADMIN_ID] = {'phone': text, 'phone_code_hash': result.phone_code_hash, 'client': client}

            context.user_data['awaiting_phone'] = False
            context.user_data['awaiting_code'] = True

            await update.message.reply_text("✅ Kod gönderildi!\n\n📱 Telefonunuza gelen kodu yazın:")
        except Exception as e:
            await update.message.reply_text(f"❌ Hata: {e}")

    elif context.user_data.get('awaiting_code'):
        try:
            auth = pending_auth[ADMIN_ID]
            client = auth['client']
            try:
                await client.sign_in(auth['phone'], text, phone_code_hash=auth['phone_code_hash'])
            except Exception:
                context.user_data['awaiting_code'] = False
                context.user_data['awaiting_2fa'] = True
                await update.message.reply_text("🔐 2FA şifrenizi yazın:")
                return

            session_string = client.session.save()
            userbots = load_userbots()
            ub_id = f"ub_{len(userbots) + 1}"
            userbots[ub_id] = {'phone': auth['phone'], 'session': session_string}
            save_userbots(userbots)

            userbot_clients[ub_id] = client
            context.user_data['awaiting_code'] = False
            del pending_auth[ADMIN_ID]

            await update.message.reply_text(f"✅ Userbot eklendi! ID: {ub_id}")
        except Exception as e:
            await update.message.reply_text(f"❌ Hata: {e}")

    elif context.user_data.get('awaiting_2fa'):
        try:
            auth = pending_auth[ADMIN_ID]
            client = auth['client']

            await client.sign_in(password=text)

            session_string = client.session.save()
            userbots = load_userbots()
            ub_id = f"ub_{len(userbots) + 1}"
            userbots[ub_id] = {'phone': auth['phone'], 'session': session_string}
            save_userbots(userbots)

            userbot_clients[ub_id] = client
            context.user_data['awaiting_2fa'] = False
            del pending_auth[ADMIN_ID]

            await update.message.reply_text(f"✅ Userbot eklendi! ID: {ub_id}")
        except Exception as e:
            await update.message.reply_text(f"❌ Hata: {e}")

# ============== ADMIN: KANAL TANIMLAMA ==============
async def admin_define_channel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id != LOG_GROUP_ID:
        return
    if update.effective_user.id != ADMIN_ID:
        return

    if not context.args:
        await update.message.reply_text("❌ Kullanım: /tanimla @kanal veya https://t.me/kanal")
        return

    channel_link = context.args[0]
    channels = load_channels()
    ch_id = f"ch_{len(channels) + 1}"
    channels[ch_id] = {'link': channel_link, 'added_by': ADMIN_ID}
    save_channels(channels)

    await update.message.reply_text(f"✅ Kanal eklendi! ID: {ch_id}")

# ============== ADMIN: INDEXLE ==============
async def admin_indexle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Sadece log grubunda + sadece admin
    if update.effective_chat.id != LOG_GROUP_ID:
        return
    if update.effective_user.id != ADMIN_ID:
        return

    if not userbot_clients:
        await update.message.reply_text("❌ Önce en az 1 userbot ekle: /ekle")
        return

    # Kanal başı tarama sayısı parametresi (bu sürümde aralık bölme kullandığımız için sadece bilgi amaçlı)
    # Yine de istersen burada sabit tutabilirsin.
    max_parallel = 3  # aynı anda kaç userbot çalışsın (3 stabil)

    channels = load_channels()
    if not channels:
        await update.message.reply_text("❌ Önce kanal ekle: /tanimla @kanal")
        return

    msg = await update.message.reply_text(
        "🧾 Paralel indeksleme başladı...\n"
        f"📚 Kanal: {len(channels)}\n"
        f"🤖 Userbot: {len(userbot_clients)} (hedef: 10 parça)\n"
        f"⚙️ Paralellik: {max_parallel}\n"
        "⏳ Bu işlem uzun sürebilir."
    )

    total_added = 0
    total_seen = 0

    # userbot listesi (sabit sıra)
    clients = list(userbot_clients.items())

    for idx, (cid, cdata) in enumerate(channels.items(), start=1):
        link = cdata["link"]

        try:
            await msg.edit_text(
                f"🧾 Kanal hazırlanıyor ({idx}/{len(channels)})\n"
                f"📌 {link}\n"
                "⏳ Mesaj aralığı bulunuyor..."
            )
        except:
            pass

        # Parça sayısı: tek kanal için 10 userbot hedefi
        parts = min(10, len(clients))
        if parts < 1:
            await msg.edit_text("❌ Hiç userbot yok.")
            return

        # min/max message_id (1 client yeter)
        try:
            base_client = clients[0][1]
            ch_entity = await base_client.get_entity(link)
            min_id, max_id = await get_channel_id_bounds(base_client, ch_entity)
            if min_id == 0 or max_id == 0:
                await msg.edit_text(f"❌ Mesaj aralığı alınamadı: {link}")
                continue
        except Exception as e:
            await msg.edit_text(f"❌ Kanal aralığı hatası: {link}\nHata: {e}")
            continue

        ranges = split_ranges(min_id, max_id, parts)

        try:
            await msg.edit_text(
                f"🧾 İndeksleniyor ({idx}/{len(channels)})\n"
                f"📌 {link}\n"
                f"🧩 Range: {min_id} - {max_id}\n"
                f"🤖 Parça: {parts} (10 hedef)\n"
                f"⚙️ Paralellik: {max_parallel}\n"
                "🚀 Başlatılıyor..."
            )
        except:
            pass

        sem = asyncio.Semaphore(max_parallel)

        async def worker(i: int, ub_id: str, client: TelegramClient, r: tuple):
            start_id, end_id = r

            async with sem:
                # Her worker kendi progress'ini aynı mesajda gösterir (son yazan görünür)
                async def _p(txt: str):
                    try:
                        await msg.edit_text(
                            f"🧾 İndeksleniyor ({idx}/{len(channels)})\n"
                            f"📌 {link}\n"
                            f"🤖 {ub_id} ({i+1}/{parts})\n"
                            f"🧩 Range: {start_id}-{end_id}\n\n"
                            f"{txt}"
                        )
                    except:
                        pass

                return await index_channel_range(
                    client=client,
                    channel_link=link,
                    start_id=start_id,
                    end_id=end_id,
                    progress_cb=_p
                )

        tasks = []
        for i in range(parts):
            ub_id, client = clients[i]
            tasks.append(asyncio.create_task(worker(i, ub_id, client, ranges[i])))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        added = 0
        seen = 0
        err_count = 0

        for r in results:
            if isinstance(r, Exception):
                err_count += 1
                continue
            a, s = r
            added += a
            seen += s

        total_added += added
        total_seen += seen

        try:
            await msg.edit_text(
                f"✅ Kanal bitti ({idx}/{len(channels)})\n"
                f"📌 {link}\n\n"
                f"➕ Eklenen PDF: {added}\n"
                f"👀 Taranan mesaj (yaklaşık): {seen}\n"
                f"❗ Hata alan parça: {err_count}/{parts}\n"
                "➡️ Sonraki kanala geçiliyor..."
            )
        except:
            pass

        await asyncio.sleep(0.7)

    # DB toplam kayıt
    try:
        count_now = await asyncio.to_thread(db_count)
    except:
        count_now = 0

    await msg.edit_text(
        "✅ Paralel indeksleme bitti!\n\n"
        f"➕ Toplam eklenen: {total_added}\n"
        f"👀 Toplam taranan (yaklaşık): {total_seen}\n"
        f"📦 DB toplam kayıt: {count_now}"
    )
# ============== ADMIN: İSTATİSTİKLER ==============
async def admin_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id != LOG_GROUP_ID:
        return

    user_data = load_user_data()
    userbots = load_userbots()
    channels = load_channels()
    active_userbots = len(userbot_clients)
    total_users = len(user_data)
    total_channels = len(channels)
    total_pdfs = await asyncio.to_thread(db_count)

    stats = (
        f"📊 **BOT İSTATİSTİKLERİ**\n\n"
        f"👥 Toplam Kullanıcı: {total_users}\n"
        f"🤖 Aktif Userbot: {active_userbots}/{len(userbots)}\n"
        f"📚 Tanımlı Kanal: {total_channels}\n"
        f"📦 İndeks PDF: {total_pdfs}\n"
    )
    await update.message.reply_text(stats, parse_mode="Markdown")

# ============== CALLBACK HANDLER ==============
async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    # =========================
    # 1) İNDİRME (dlid:pdf_id)
    # =========================
    if query.data.startswith("dlid:"):
        pdf_id = int(query.data.split("dlid:", 1)[1])
        user_id = query.from_user.id

        # ✅ Spam / çift tıklama kilidi
        lock_key = (user_id, pdf_id)
        if lock_key in download_lock:
            await query.answer("⏳ İşleniyor...", show_alert=False)
            return
        download_lock.add(lock_key)

        try:
            # ✅ Hak kontrol (şimdilik sadece kontrol)
            if get_user_requests(user_id) == 0:
                await query.edit_message_text("❌ İstek hakkın bitti. /premium")
                return

            rec = await asyncio.to_thread(db_get_pdf, pdf_id)
            if not rec:
                await query.edit_message_text("❌ Kayıt bulunamadı. (İndeks güncel değil)")
                return

            channel_link = rec["channel_link"]
            msg_id = rec["message_id"]
            file_name = rec.get("file_name") or "document.pdf"
            caption = rec.get("caption") or ""

            if not userbot_clients:
                await query.edit_message_text("❌ Userbot yok. Admin /ekle ile eklemeli.")
                return

            await query.edit_message_text("📥 Dosya indiriliyor...")

            # ✅ Bir userbot çalışmazsa diğerini dene
            clients = list(userbot_clients.items())
            last_err = None

            for ub_id, client in clients:
                try:
                    ch = await client.get_entity(channel_link)
                    msg = await client.get_messages(ch, ids=msg_id)
                    if not msg or not msg.document:
                        last_err = "Mesaj yok / doküman yok"
                        continue

                    # ✅ Telethon stabil indirme
                    file_bytes = await client.download_media(msg, file=bytes)
                    if not file_bytes:
                        last_err = "Dosya indirilemedi (boş döndü)"
                        continue

                    # ✅ Kullanıcıya gönder
                    await context.bot.send_document(
                        chat_id=user_id,
                        document=file_bytes,
                        filename=file_name,
                        caption=caption if caption else None
                    )

                    # ✅ SADECE başarılı gönderimde hak düş
                    if not decrease_user_requests(user_id):
                        await query.edit_message_text("✅ Gönderildi, fakat hak düşürme hatası oldu. Admine yaz.")
                        return

                    left = get_user_requests(user_id)
                    left_text = "Sınırsız" if left == float('inf') else str(left)

                    await query.edit_message_text(f"✅ Gönderildi!\n📊 Kalan hakkın: {left_text}")

                    # Log
                    try:
                        await context.bot.send_message(
                            chat_id=LOG_GROUP_ID,
                            text=(
                                "📤 PDF indirildi\n"
                                f"👤 {query.from_user.first_name} ({user_id})\n"
                                f"🤖 Userbot: {ub_id}\n"
                                f"📚 Kanal: {channel_link}\n"
                                f"🆔 Msg: {msg_id}\n"
                                f"📄 Dosya: {file_name}"
                            )
                        )
                    except:
                        pass

                    return

                except FloodWaitError as fw:
                    last_err = f"FloodWait {fw.seconds}s"
                    await asyncio.sleep(min(fw.seconds + 1, 10))
                except Exception as e:
                    last_err = str(e)
                    continue

            logger.error(f"İndirme başarısız (tüm userbotlar). Son hata: {last_err}")
            await query.edit_message_text("❌ Gönderilemedi. (Userbot erişim / mesaj / flood) Tekrar dene.")
            return

        finally:
            # ✅ kilidi kaldır
            if lock_key in download_lock:
                download_lock.remove(lock_key)

    # =========================
    # 2) PAKETLERİ GÖSTER
    # =========================
    if query.data == 'show_plans':
        user_id = query.from_user.id
        left = get_user_requests(user_id)
        left_text = "Sınırsız" if left == float('inf') else str(left)

        premium_info = f"""💎 **Premium Üyelik Planları**

📊 Mevcut İstek Hakkınız: {left_text}

⭐ **Basic**: 150 istek (150 Star)
💫 **Premium**: 300 istek (300 Star)
🌟 **Unlimited**: Sınırsız (749 Star)
"""
        keyboard = []
        for plan_name, details in MEMBERSHIP_PLANS.items():
            keyboard.append([
                InlineKeyboardButton(
                    f"{plan_name} ({details['stars']} Star)",
                    callback_data=f"buy_{plan_name.lower()}"
                )
            ])

        await query.edit_message_text(
            premium_info,
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return

    # =========================
    # 3) SATIN AL (Stars invoice)
    # =========================
    if query.data.startswith('buy_'):
        membership_type = query.data.split('_', 1)[1].capitalize()
        if membership_type not in MEMBERSHIP_PLANS:
            await query.edit_message_text("❌ Paket bulunamadı.")
            return

        plan = MEMBERSHIP_PLANS[membership_type]
        title = f"{membership_type} Üyelik"
        description = f"{'Sınırsız' if plan['requests'] == 'Sınırsız' else plan['requests']} PDF isteme hakkı"
        payload = f"pdf_membership_{membership_type}"
        currency = "XTR"
        prices = [LabeledPrice(title, plan['stars'])]

        await context.bot.send_invoice(
            chat_id=query.from_user.id,
            title=title,
            description=description,
            payload=payload,
            provider_token="",
            currency=currency,
            prices=prices
        )
        return

# ============== ÖDEME İŞLEMLERİ ==============
async def precheckout_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.pre_checkout_query
    if query.invoice_payload.startswith('pdf_membership_'):
        await query.answer(ok=True)
    else:
        await query.answer(ok=False, error_message="Hata!")

async def successful_payment_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    payment = update.message.successful_payment
    user_id = update.effective_user.id
    membership_type = payment.invoice_payload.split('_')[2]

    add_user_requests(user_id, membership_type)

    plan = MEMBERSHIP_PLANS[membership_type]
    requests_txt = "Sınırsız" if plan['requests'] == 'Sınırsız' else f"{plan['requests']} istek"

    await update.message.reply_text(
        f"🎉 Tebrikler!\n\n"
        f"✅ {membership_type} üyeliğiniz aktif!\n"
        f"📊 Yeni hakkınız: {requests_txt}"
    )

    await context.bot.send_message(
        chat_id=LOG_GROUP_ID,
        text=f"💰 Ödeme alındı\n👤 ID: {user_id}\n💎 Plan: {membership_type}\n⭐ Star: {plan['stars']}"
    )

# ============== HATA ==============
async def hata(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.error("Hata:", exc_info=context.error)

# ============== POST INIT ==============
async def post_init(app: Application):
    db_init()
    await init_userbots()
    logger.info(f"✅ {len(userbot_clients)} userbot başlatıldı")
    logger.info("✅ SQLite indeks hazır")

# ============== MAIN ==============
def main():
    app = Application.builder().token(BOT_TOKEN).post_init(post_init).build()

    # Kullanıcı komutları
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("premium", premium_command))

    # Admin komutları (Sadece log grubunda)
    app.add_handler(CommandHandler("ekle", admin_add_userbot, filters=filters.Chat(chat_id=LOG_GROUP_ID)))
    app.add_handler(CommandHandler("tanimla", admin_define_channel, filters=filters.Chat(chat_id=LOG_GROUP_ID)))
    app.add_handler(CommandHandler("indexle", admin_indexle, filters=filters.Chat(chat_id=LOG_GROUP_ID)))
    app.add_handler(CommandHandler("stats", admin_stats, filters=filters.Chat(chat_id=LOG_GROUP_ID)))

    # Admin mesaj handler
    app.add_handler(MessageHandler(
        filters.Chat(chat_id=LOG_GROUP_ID) & filters.TEXT & ~filters.COMMAND,
        handle_admin_messages
    ))

    # Callback ve ödeme
    app.add_handler(CallbackQueryHandler(button_callback))
    app.add_handler(PreCheckoutQueryHandler(precheckout_callback))
    app.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, successful_payment_callback))

    # Kullanıcı arama
    app.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND & filters.ChatType.PRIVATE,
        handle_pdf_request
    ))

    app.add_error_handler(hata)

    logger.info("🤖 Bot başlatılıyor...")
    app.run_polling()

if __name__ == "__main__":
    main()

