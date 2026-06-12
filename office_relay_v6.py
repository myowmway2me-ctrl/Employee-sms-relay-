# ╔══════════════════════════════════════════════════════╗
#   OFFICE SMS RELAY V6 — MAIN BOT
#   All existing features + new monitoring/controls
#   Telegram-only, Groq AI, Full Firebase Management
# ╚══════════════════════════════════════════════════════╝

import re, asyncio, logging, sqlite3, sys, subprocess, secrets, string, json, hashlib
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple

def install(p):
    subprocess.check_call([sys.executable, "-m", "pip", "install", p, "-q"])

try:
    import aiohttp
except ImportError:
    install("aiohttp")
    import aiohttp

try:
    from aiogram import Bot, Dispatcher, F, Router
except ImportError:
    install("aiogram==3.7.0")
    from aiogram import Bot, Dispatcher, F, Router

from aiogram.client.default import DefaultBotProperties
from aiogram.types import (Message, CallbackQuery, InlineKeyboardMarkup, 
                          InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton)
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage

import config
import schemas
from groq_ai import ai_learn_firebase_structure, analyze_ghost_device, test_groq_connection

logging.basicConfig(level=logging.INFO, format=config.LOG_FORMAT)
log = logging.getLogger(__name__)

bot = Bot(token=config.BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher(storage=MemoryStorage())
router = Router()

# ══════════════ RUNTIME STATE ════════════════════════
active_sessions: Dict[int, bool] = {}
last_sms_seen: Dict[str, str] = {}
sse_tasks: Dict[int, asyncio.Task] = {}
firebase_health_cache: Dict[str, dict] = {}
hot_numbers_cache: List[dict] = []
last_cache_update: Dict[str, float] = {}

# ══════════════ DATABASE ═════════════════════════════

class DB:
    path = config.DB_PATH

    def cx(self):
        c = sqlite3.connect(self.path)
        c.row_factory = sqlite3.Row
        return c

    def init(self):
        """Initialize all tables from schemas"""
        with self.cx() as c:
            # Create all tables
            for table_name, sql in schemas.SCHEMA_SCRIPTS.items():
                try:
                    c.execute(sql)
                except sqlite3.OperationalError as e:
                    if "already exists" not in str(e):
                        log.warning(f"Table {table_name}: {e}")

            # Run migrations (these may fail if columns exist, that's OK)
            for migration in schemas.SCHEMA_MIGRATIONS:
                try:
                    c.execute(migration)
                except sqlite3.OperationalError:
                    pass

            # Create indexes
            for idx_sql in schemas.INDEXES:
                try:
                    c.execute(idx_sql)
                except sqlite3.OperationalError:
                    pass

            # Initialize default settings
            defaults = [
                ("refer_limit", "1"),
                ("key_mode", "1"),
                ("firebase_quarantine_enabled", "1"),
                ("auto_probe_enabled", "1"),
                ("pattern_stats_enabled", "1"),
            ]
            for k, v in defaults:
                c.execute(
                    "INSERT OR IGNORE INTO settings VALUES(?,?)",
                    (k, v)
                )
            log.info("✅ Database initialized")

    def reg_user(self, uid, uname, ref_id=None):
        """Register user with referral support"""
        with self.cx() as c:
            u = c.execute("SELECT * FROM users WHERE user_id=?", (uid,)).fetchone()
            if not u:
                c.execute(
                    "INSERT INTO users(user_id,username) VALUES(?,?)",
                    (uid, uname)
                )
                if ref_id and ref_id != uid:
                    if not c.execute("SELECT 1 FROM refer_log WHERE referred_id=?", (uid,)).fetchone():
                        c.execute("INSERT INTO refer_log VALUES(NULL,?,?)", (ref_id, uid))
                        c.execute("UPDATE users SET refer_count=refer_count+1 WHERE user_id=?", (ref_id,))
                        
                        ref = c.execute("SELECT * FROM users WHERE user_id=?", (ref_id,)).fetchone()
                        limit = int(self.get("refer_limit") or 1)
                        if ref and ref["refer_count"] >= limit and not ref["access_key"]:
                            nk = self.gen_key()
                            c.execute("UPDATE users SET access_key=? WHERE user_id=?", (nk, ref_id))
                            c.execute(
                                "INSERT INTO access_keys VALUES(?,?,?,NULL,'temporary',NULL)",
                                (nk, ref_id, datetime.now().strftime("%d-%m-%Y %H:%M"))
                            )
                            return {"is_banned": 0, "notify_ref": ref_id, "new_key": nk}
                return {"is_banned": 0}
            return dict(u)

    def get(self, key):
        r = self.cx().execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        return r["value"] if r else None

    def set(self, key, val):
        with self.cx() as c:
            c.execute("INSERT OR REPLACE INTO settings VALUES(?,?)", (key, str(val)))

    def unlocked(self, uid):
        if uid in config.ADMIN_IDS:
            return True
        u = self.cx().execute("SELECT is_unlocked,is_suspended FROM users WHERE user_id=?", (uid,)).fetchone()
        if not u:
            return False
        if u["is_suspended"]:
            return False
        if not int(self.get("key_mode") or 1):
            return True
        return bool(u["is_unlocked"])

    def gen_key(self):
        """Generate new access key"""
        ch = string.ascii_uppercase + string.digits
        return config.KEY_PREFIX + ''.join(secrets.choice(ch) for _ in range(config.KEY_LENGTH - len(config.KEY_PREFIX)))

    def log_admin_action(self, admin_id, action_type, target_id=None, details=None):
        """Log admin action for audit trail"""
        with self.cx() as c:
            c.execute(
                "INSERT INTO admin_action_logs(admin_id,action_type,target_id,details) VALUES(?,?,?,?)",
                (admin_id, action_type, target_id, details)
            )
        log.info(f"[AdminLog] {admin_id} → {action_type} ({target_id})")

    def add_to_watchlist(self, number, device_id, admin_id, reason=""):
        """Add number to watchlist"""
        with self.cx() as c:
            try:
                c.execute(
                    "INSERT INTO watchlist(number,device_id,added_by,reason) VALUES(?,?,?,?)",
                    (number, device_id, admin_id, reason)
                )
                self.log_admin_action(admin_id, "watchlist_add", number, reason)
                return True
            except sqlite3.IntegrityError:
                return False

    def remove_from_watchlist(self, number):
        """Remove from watchlist"""
        with self.cx() as c:
            c.execute("DELETE FROM watchlist WHERE number=?", (number,))

    def is_watchlisted(self, number):
        """Check if number is watchlisted"""
        r = self.cx().execute("SELECT * FROM watchlist WHERE number=?", (number,)).fetchone()
        return bool(r)

    def get_firebase_health(self, fb_source):
        """Get Firebase health status"""
        r = self.cx().execute(
            "SELECT * FROM firebase_health WHERE fb_source=?",
            (fb_source,)
        ).fetchone()
        return dict(r) if r else None

    def update_firebase_health(self, fb_source, status=None, success_inc=0, failure_inc=0):
        """Update Firebase health metrics"""
        with self.cx() as c:
            c.execute(
                """INSERT INTO firebase_health(fb_source,status,success_count,failure_count,last_check)
                   VALUES(?,?,?,?,CURRENT_TIMESTAMP)
                   ON CONFLICT(fb_source) DO UPDATE SET
                       success_count = success_count + ?,
                       failure_count = failure_count + ?,
                       last_check = CURRENT_TIMESTAMP,
                       status = COALESCE(?,status)""",
                (fb_source, status or "Unknown", success_inc, failure_inc, success_inc, failure_inc, status)
            )

db = DB()
db.init()

# ══════════════ KEY GENERATION ════════════════════════

def gen_key():
    return db.gen_key()

# ══════════════ SMS HELPERS ════════════════════════════

def _norm_phone(raw):
    d = re.sub(r'[^\d]', '', str(raw))
    if len(d) < 7:
        return None
    if len(d) == 10 and d[0] in "6789":
        return "+91" + d
    if len(d) == 12 and d.startswith("91"):
        return "+" + d
    return "+" + d if not str(raw).startswith("+") else str(raw).strip()

def _disp(number):
    return f"Device {number[4:]}" if str(number).startswith("DEV-") else number

def _sms_fingerprint(number, sender, body, timestamp):
    """Generate dedup fingerprint"""
    key = f"{number}|{sender}|{hashlib.md5(body.encode()).hexdigest()}|{timestamp}"
    return hashlib.sha256(key.encode()).hexdigest()

# ══════════════ FIREBASE HEALTH ════════════════════════

async def check_firebase_health(fb_source, api_key=None, timeout=8):
    """Check Firebase health (lightweight ping)"""
    base = fb_source.replace(".json", "").rstrip("/")
    url = f"{base}/.json?shallow=true"
    if api_key:
        url += f"&auth={api_key}"

    try:
        async with aiohttp.ClientSession() as sess:
            async with sess.get(
                url,
                timeout=aiohttp.ClientTimeout(total=timeout)
            ) as r:
                await r.content.read(2048)
                return r.status
    except asyncio.TimeoutError:
        return 0
    except Exception:
        return 0

# ══════════════ STATES ═════════════════════════════════

class S(StatesGroup):
    add_fb = State()
    fb_apikey = State()
    gen_keys = State()
    set_limit = State()
    add_num = State()
    add_ch_id = State()
    add_ch_link = State()
    broadcast_msg = State()
    lookup_num = State()
    watchlist_reason = State()

# ══════════════ MENUS ══════════════════════════════════

def menu_user():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🟢 Active Numbers", callback_data="num_Active"),
         InlineKeyboardButton(text="🔴 Inactive Numbers", callback_data="num_Inactive")],
        [InlineKeyboardButton(text="🔥 Hot Numbers", callback_data="hot_numbers"),
         InlineKeyboardButton(text="📡 Global Radar", callback_data="global_radar")],
        [InlineKeyboardButton(text="👻 Ghost Devices", callback_data="ghost_list_0"),
         InlineKeyboardButton(text="🔍 Lookup", callback_data="num_lookup")],
        [InlineKeyboardButton(text="👥 My Referral", callback_data="my_link"),
         InlineKeyboardButton(text="🔑 Enter Key", callback_data="enter_key")],
    ])

def menu_admin():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔗 Add Firebase", callback_data="adm_fb"),
         InlineKeyboardButton(text="🔄 Sync All", callback_data="adm_sync")],
        [InlineKeyboardButton(text="📋 Firebase List", callback_data="adm_fb_list"),
         InlineKeyboardButton(text="📊 Firebase Health", callback_data="adm_health")],
        [InlineKeyboardButton(text="👻 Ghosts", callback_data="ghost_list_0"),
         InlineKeyboardButton(text="🔥 Hot Numbers", callback_data="hot_numbers")],
        [InlineKeyboardButton(text="📡 Global Radar", callback_data="global_radar"),
         InlineKeyboardButton(text="⭐ Watchlist", callback_data="adm_watchlist")],
        [InlineKeyboardButton(text="🔑 Gen Keys", callback_data="adm_genkeys"),
         InlineKeyboardButton(text="👤 Users", callback_data="adm_users")],
        [InlineKeyboardButton(text="📢 Broadcast", callback_data="adm_broadcast"),
         InlineKeyboardButton(text="📜 Logs", callback_data="adm_logs")],
        [InlineKeyboardButton(text="⚙️ Settings", callback_data="adm_settings"),
         InlineKeyboardButton(text="🤖 AI Test", callback_data="adm_ai_test")],
    ])

# ══════════════ AUTH ═══════════════════════════════════

async def check_auth(uid, obj):
    """Check channel membership + access"""
    channels = db.cx().execute("SELECT * FROM channels").fetchall()
    for ch in channels:
        in_ch = False
        try:
            m = await bot.get_chat_member(ch["channel_id"], uid)
            in_ch = m.status not in ("left", "kicked")
        except Exception as e:
            log.warning(f"Channel check failed {ch['channel_id']}: {e}")

        if not in_ch:
            kb = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="📢 Join Channel", url=ch['channel_link']),
                InlineKeyboardButton(text="✅ I Joined", callback_data="verify_join")
            ]])
            await obj.answer(
                f"╔══════════════════════╗\n"
                f"  📢 <b>JOIN REQUIRED</b>\n"
                f"╚══════════════════════╝\n\n"
                f"Join our channel first:\n"
                f"<a href='{ch['channel_link']}'>👉 Click here</a>",
                reply_markup=kb
            )
            return False

    if not db.unlocked(uid):
        u = db.cx().execute(
            "SELECT refer_count FROM users WHERE user_id=?", (uid,)
        ).fetchone()
        limit = int(db.get("refer_limit") or 1)
        count = u["refer_count"] if u else 0
        me = await bot.get_me()
        link = f"https://t.me/{me.username}?start=ref_{uid}"

        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔑 Enter Key", callback_data="enter_key")],
            [InlineKeyboardButton(text="👥 Refer Friends", url=link)],
        ])
        await obj.answer(
            f"╔══════════════════════╗\n"
            f"  🔒 <b>UNLOCK ACCESS</b>\n"
            f"╚══════════════════════╝\n\n"
            f"Progress: <b>{count}/{limit}</b> referrals\n"
            f"Or enter an admin key.",
            reply_markup=kb
        )
        return False

    return True

# ══════════════ COMMANDS ═══════════════════════════════

@router.message(CommandStart())
async def cmd_start(msg: Message):
    args = msg.text.split()
    ref_id = int(args[1].replace("ref_", "")) if len(args) > 1 and args[1].startswith("ref_") else None
    res = db.reg_user(msg.from_user.id, msg.from_user.username or "User", ref_id)

    if res.get("is_banned"):
        return await msg.answer("⛔ You are banned.")

    if res.get("notify_ref"):
        try:
            await bot.send_message(
                res["notify_ref"],
                f"🎉 <b>REFERRAL SUCCESS!</b>\n\n"
                f"Your new Access Key:\n<code>{res['new_key']}</code>"
            )
        except:
            pass

    a = db.cx().execute("SELECT COUNT(*) as c FROM numbers WHERE status='Active'").fetchone()["c"]
    t = db.cx().execute("SELECT COUNT(*) as c FROM numbers").fetchone()["c"]
    await msg.answer(
        f"╔══════════════════════════╗\n"
        f"  🏢 <b>OFFICE SMS RELAY</b>  v6\n"
        f"╚══════════════════════════╝\n\n"
        f"Welcome! 👋\n\n"
        f"🟢 Active: <b>{a}</b>\n"
        f"📱 Total: <b>{t}</b>",
        reply_markup=menu_user()
    )

@router.message(Command("admin"))
async def cmd_admin(msg: Message):
    if msg.from_user.id not in config.ADMIN_IDS:
        return

    total = db.cx().execute("SELECT COUNT(*) as c FROM numbers").fetchone()["c"]
    active = db.cx().execute("SELECT COUNT(*) as c FROM numbers WHERE status='Active'").fetchone()["c"]
    srcs = db.cx().execute("SELECT COUNT(*) as c FROM firebase_sources").fetchone()["c"]

    await msg.answer(
        f"╔══════════════════════════╗\n"
        f"  🔧 <b>ADMIN CONTROL</b>\n"
        f"╚══════════════════════════╝\n\n"
        f"📱 Numbers: {active}/{total}\n"
        f"🔗 Firebase: {srcs} sources\n"
        f"📊 SSE Streams: {len(sse_tasks)} active",
        reply_markup=menu_admin()
    )

# ══════════════ VERIFICATION ══════════════════════════

@router.callback_query(F.data == "verify_join")
async def cb_verify_join(call: CallbackQuery):
    if await check_auth(call.from_user.id, call.message):
        a = db.cx().execute("SELECT COUNT(*) as c FROM numbers WHERE status='Active'").fetchone()["c"]
        t = db.cx().execute("SELECT COUNT(*) as c FROM numbers").fetchone()["c"]
        await call.message.edit_text(
            f"✅ <b>Welcome!</b>\n\n"
            f"🟢 Active: {a}\n"
            f"📱 Total: {t}",
            reply_markup=menu_user()
        )
    await call.answer()

# ══════════════ WATCHLIST ══════════════════════════════

@router.callback_query(F.data == "adm_watchlist")
async def cb_watchlist(call: CallbackQuery):
    if call.from_user.id not in config.ADMIN_IDS:
        return await call.answer("⛔ Admin only", show_alert=True)

    items = db.cx().execute("SELECT * FROM watchlist ORDER BY added_at DESC").fetchall()
    if not items:
        return await call.answer("No watchlist items.", show_alert=True)

    text = "╔══════════════════════╗\n  ⭐ <b>WATCHLIST</b>\n╚══════════════════════╝\n\n"
    btns = []
    for item in items[:20]:
        text += f"📌 <code>{_disp(item['number'])}</code>\n  Reason: {item['reason'] or 'N/A'}\n\n"
        btns.append([InlineKeyboardButton(
            text=f"🗑 Remove",
            callback_data=f"watchlist_rm_{item['number']}"
        )])

    await call.message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=btns))
    await call.answer()

# ══════════════ HOT NUMBERS ════════════════════════════

async def update_hot_numbers_cache():
    """Update hot numbers ranking (SMS/OTP activity)"""
    global hot_numbers_cache, last_cache_update

    now = asyncio.get_event_loop().time()
    last_update = last_cache_update.get("hot_numbers", 0)

    if now - last_update < config.HOT_NUMBERS_CACHE_TTL:
        return hot_numbers_cache

    # Query: recent SMS count + OTP count by number
    rows = db.cx().execute("""
        SELECT number, COUNT(*) as sms_count,
               SUM(CASE WHEN otp IS NOT NULL THEN 1 ELSE 0 END) as otp_count,
               MAX(received_at) as last_activity
        FROM sms_log
        WHERE received_at > datetime('now', '-1 hour')
        GROUP BY number
        ORDER BY sms_count DESC, otp_count DESC
        LIMIT 30
    """).fetchall()

    hot_numbers_cache = [dict(r) for r in rows]
    last_cache_update["hot_numbers"] = now
    return hot_numbers_cache

@router.callback_query(F.data == "hot_numbers")
async def cb_hot_numbers(call: CallbackQuery):
    if not await check_auth(call.from_user.id, call.message):
        return await call.answer()

    hot = await update_hot_numbers_cache()
    if not hot:
        return await call.answer("No activity in last hour.", show_alert=True)

    text = "╔══════════════════════╗\n  🔥 <b>HOT NUMBERS</b>\n╚══════════════════════╝\n\n"
    for i, row in enumerate(hot[:10], 1):
        text += f"{i}. <code>{_disp(row['number'])}</code>\n"
        text += f"   📨 SMS: {row['sms_count']}  🎯 OTP: {row['otp_count'] or 0}\n"
        text += f"   🕐 {row['last_activity']}\n\n"

    await call.message.answer(text)
    await call.answer()

# ══════════════ MAIN LOOP ══════════════════════════════

dp.include_router(router)

async def main():
    log.info("🚀 Office Relay V6 starting...")
    log.info(f"👥 Admin IDs: {config.ADMIN_IDS}")
    log.info(f"🔗 Groq API: {'✅ Configured' if config.GROQ_API_KEY else '❌ Not set'}")

    # Test Groq connection
    if config.GROQ_API_KEY:
        ok, msg = await test_groq_connection()
        log.info(f"🤖 Groq: {msg}")

    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
