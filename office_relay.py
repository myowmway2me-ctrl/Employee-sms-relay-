# ╔══════════════════════════════════════════════════╗
#   OFFICE SMS RELAY MONITOR — v5 CYBER EDITION
#   Features: SSE Streaming · Smart Firebase Setup
#             Dynamic Schema · Pagination · Health
# ╚══════════════════════════════════════════════════╝

import os
BOT_TOKEN        = os.environ.get("TG_BOT_TOKEN", "6379711237:AAEQamc5bWsR-wbF_2s6CdpL6ZKpMIUjG5k")
ADMIN_IDS        = [int(x) for x in os.environ.get("TG_ADMIN_IDS", "6013007573").split(",") if x.strip().isdigit()]
KEY_PREFIX       = "RELAY-"
KEY_LENGTH       = 30
RESYNC_INTERVAL  = 600   # seconds
PAGE_SIZE        = 15    # numbers per page

import re, asyncio, logging, sqlite3, sys, subprocess, secrets, string, json
from datetime import datetime, timedelta

def install(p): subprocess.check_call([sys.executable, "-m", "pip", "install", p, "-q"])
try: import aiohttp
except ImportError: install("aiohttp"); import aiohttp
try: from aiogram import Bot, Dispatcher, F, Router
except ImportError: install("aiogram==3.7.0"); from aiogram import Bot, Dispatcher, F, Router

from aiogram.client.default import DefaultBotProperties
from aiogram.types import (Message, CallbackQuery,
                           InlineKeyboardMarkup, InlineKeyboardButton)
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

bot    = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp     = Dispatcher(storage=MemoryStorage())
router = Router()

# ══════════════ KEY GEN ══════════════════════════
def gen_key():
    ch = string.ascii_uppercase + string.digits
    return KEY_PREFIX + ''.join(secrets.choice(ch) for _ in range(KEY_LENGTH - len(KEY_PREFIX)))

# ══════════════ DATABASE ══════════════════════════
class DB:
    path = "office_relay.db"
    def cx(self):
        c = sqlite3.connect(self.path); c.row_factory = sqlite3.Row; return c

    def init(self):
        with self.cx() as c:
            c.executescript("""
                CREATE TABLE IF NOT EXISTS users(
                    user_id INTEGER PRIMARY KEY, username TEXT,
                    refer_count INTEGER DEFAULT 0, is_banned INTEGER DEFAULT 0,
                    access_key TEXT DEFAULT NULL, is_unlocked INTEGER DEFAULT 0);
                CREATE TABLE IF NOT EXISTS refer_log(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    referrer_id INTEGER, referred_id INTEGER);
                CREATE TABLE IF NOT EXISTS access_keys(
                    key TEXT PRIMARY KEY, owner_id INTEGER,
                    created_at TEXT, used_by INTEGER DEFAULT NULL);
                CREATE TABLE IF NOT EXISTS numbers(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, number TEXT UNIQUE,
                    device_id TEXT, device_name TEXT, sim_slot TEXT DEFAULT 'sim1',
                    carrier TEXT, status TEXT DEFAULT 'Active',
                    assigned_to INTEGER DEFAULT NULL, fb_source TEXT DEFAULT NULL,
                    sms_path TEXT DEFAULT NULL, status_path TEXT DEFAULT NULL,
                    struct_type TEXT DEFAULT NULL,
                    is_ghost INTEGER DEFAULT 0,
                    last_seen_ts INTEGER DEFAULT NULL);
                CREATE TABLE IF NOT EXISTS firebase_sources(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, url TEXT UNIQUE,
                    label TEXT, added_at TEXT, last_synced TEXT,
                    num_count INTEGER DEFAULT 0, struct_type TEXT DEFAULT NULL,
                    api_key TEXT DEFAULT NULL);
                CREATE TABLE IF NOT EXISTS channels(
                    channel_id TEXT PRIMARY KEY, channel_link TEXT);
                CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, value TEXT);
                CREATE TABLE IF NOT EXISTS sms_log(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    number TEXT, sender TEXT, otp TEXT, full_msg TEXT, received_at TEXT);
            """)
            for col in [
                "ALTER TABLE numbers ADD COLUMN sms_path TEXT DEFAULT NULL",
                "ALTER TABLE numbers ADD COLUMN status_path TEXT DEFAULT NULL",
                "ALTER TABLE numbers ADD COLUMN struct_type TEXT DEFAULT NULL",
                "ALTER TABLE numbers ADD COLUMN is_ghost INTEGER DEFAULT 0",
                "ALTER TABLE numbers ADD COLUMN last_seen_ts INTEGER DEFAULT NULL",
                "ALTER TABLE firebase_sources ADD COLUMN struct_type TEXT DEFAULT NULL",
                "ALTER TABLE firebase_sources ADD COLUMN api_key TEXT DEFAULT NULL",
            ]:
                try: c.execute(col)
                except: pass
            for k, v in [("refer_limit", "1"), ("key_mode", "1")]:
                if not c.execute("SELECT 1 FROM settings WHERE key=?", (k,)).fetchone():
                    c.execute("INSERT INTO settings VALUES(?,?)", (k, v))

    def reg_user(self, uid, uname, ref_id=None):
        with self.cx() as c:
            u = c.execute("SELECT * FROM users WHERE user_id=?", (uid,)).fetchone()
            if not u:
                c.execute("INSERT INTO users(user_id,username) VALUES(?,?)", (uid, uname))
                if ref_id and ref_id != uid:
                    if not c.execute("SELECT 1 FROM refer_log WHERE referred_id=?", (uid,)).fetchone():
                        c.execute("INSERT INTO refer_log VALUES(NULL,?,?)", (ref_id, uid))
                        c.execute("UPDATE users SET refer_count=refer_count+1 WHERE user_id=?", (ref_id,))
                        ref = c.execute("SELECT * FROM users WHERE user_id=?", (ref_id,)).fetchone()
                        limit = int(self.get("refer_limit") or 1)
                        if ref and ref["refer_count"] + 1 >= limit and not ref["access_key"]:
                            nk = gen_key()
                            c.execute("UPDATE users SET access_key=? WHERE user_id=?", (nk, ref_id))
                            c.execute("INSERT INTO access_keys VALUES(?,?,?,NULL)",
                                      (nk, ref_id, datetime.now().strftime("%d-%m-%Y %H:%M")))
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
        if uid in ADMIN_IDS: return True
        u = self.cx().execute("SELECT is_unlocked FROM users WHERE user_id=?", (uid,)).fetchone()
        if not u: return False
        if not int(self.get("key_mode") or 1): return True
        return bool(u["is_unlocked"])


db = DB(); db.init()

# ══════════════ RUNTIME STATE ════════════════════
active_sessions: dict[int, bool] = {}
last_sms_seen:   dict[str, str]  = {}
sse_tasks:       dict[int, asyncio.Task] = {}

# ══════════════ FIREBASE HTTP HELPERS ════════════

async def fb_ping(sess, base_url, api_key=None, timeout=8):
    """
    Lightweight connection test — NEVER downloads full DB.
    Uses shallow=true and reads at most 2 KB.
    Returns (http_status_code, error_string_or_None).
    """
    base = base_url.replace(".json", "").rstrip("/")
    url  = f"{base}/.json?shallow=true"
    if api_key: url += f"&auth={api_key}"
    try:
        async with sess.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as r:
            await r.content.read(2048)   # read only 2 KB — enough to confirm 200
            return r.status, None
    except Exception as e:
        return 0, str(e)


async def fb_get(sess, url, timeout=15, api_key=None):
    """GET a Firebase .json endpoint. Returns parsed JSON or None."""
    u = url.split("?")[0]
    q = ("?" + url.split("?")[1]) if "?" in url else ""
    # Ensure .json suffix — only add it if missing, never strip-and-re-add
    # (strip+re-add destroys root URLs like .../firebaseio.com/.json → .../firebaseio.com.json)
    if not u.endswith(".json"):
        u = u.rstrip("/") + ".json"
    if api_key:
        sep = "&" if q else "?"
        q = (q or "?") + sep.replace("?", "") + f"auth={api_key}"
        if not q.startswith("?"): q = "?" + q
    try:
        async with sess.get(u + q, timeout=aiohttp.ClientTimeout(total=timeout)) as r:
            if r.status == 200: return await r.json(content_type=None)
            log.warning("Firebase HTTP %d → %s", r.status, u)
            return None
    except Exception as e:
        log.warning("Firebase error %s: %s", u, e)
        return None


async def fb_delete(sess, url, api_key=None, timeout=10):
    """DELETE a Firebase node."""
    u = url.rstrip("/")
    if not u.endswith(".json"): u += ".json"
    if api_key: u += f"?auth={api_key}"
    try:
        async with sess.delete(u, timeout=aiohttp.ClientTimeout(total=timeout)) as r:
            return r.status in (200, 204)
    except Exception as e:
        log.warning("Firebase delete error %s: %s", u, e)
        return False


async def fb_status(sess, url, timeout=8):
    """Return HTTP status code using lightweight ping."""
    base = url.replace(".json", "").rstrip("/")
    try:
        async with sess.get(f"{base}/.json?shallow=true",
                            timeout=aiohttp.ClientTimeout(total=timeout)) as r:
            await r.content.read(512)
            return r.status
    except: return 0


def _get_fb_apikey(fb_source):
    if not fb_source: return None
    base = fb_source.replace(".json", "").rstrip("/")
    row = db.cx().execute("SELECT api_key FROM firebase_sources WHERE url=?", (base,)).fetchone()
    return row["api_key"] if row and row["api_key"] else None


# ══════════════ SMS HELPERS ═══════════════════════

# Firebase system nodes that are never SMS data — skip always
_FB_SYSTEM_KEYS = frozenset({
    "fcmDelivery", "fcm", "fcm_tokens", "fcmTokens", "firebase-messaging",
    "notifications", "analytics", "remoteConfig", "remote_config",
    "crashlytics", "performance", "__fbfiles__", "__storage__",
    "appCheck", "hosting", "rules", "indexes", "functions",
    "firestore", "auth", "identitytoolkit", "securetoken",
})


def _norm_phone(raw):
    d = re.sub(r'[^\d]', '', str(raw))
    if len(d) < 7: return None
    if len(d) == 10 and d[0] in "6789": return "+91" + d
    if len(d) == 12 and d.startswith("91"): return "+" + d
    return "+" + d if not str(raw).startswith("+") else str(raw).strip()


def _status_str(v):
    return "Inactive" if str(v).strip().lower() in ("offline", "0", "false", "inactive") else "Active"

def _extract_last_seen_ts(node):
    """Extract Unix timestamp (seconds) of last device activity from any known field."""
    if not isinstance(node, dict): return None
    for f in ("lastSeen","last_seen","lastActive","last_active",
              "timestamp","backupTime","lastSync","last_sync",
              "lastOnline","updatedAt","updated_at","lastBackup","time"):
        v = node.get(f)
        if v is None: continue
        s = str(v).strip()
        if s.isdigit():
            ts = int(s)
            if ts > 1_000_000_000_000: ts //= 1000
            if 1_000_000_000 < ts < 9_000_000_000: return ts
    return None

def _status_from_node(node):
    """Determine Active/Inactive from node fields + recency; much smarter than _status_str."""
    if not isinstance(node, dict): return "Active"
    # Check explicit boolean/string status fields
    for sf in ("status","Status","online","isOnline","Online","active","Active","isActive"):
        v = node.get(sf)
        if v is None: continue
        s = str(v).strip().lower()
        if s in ("offline","0","false","inactive","no","disconnected"): return "Inactive"
        if s in ("online","1","true","active","yes","connected"):       return "Active"
    # Timestamp-based: if last activity > 48 h ago → Inactive; ≤ 2 h → Active; else uncertain → Active
    ts = _extract_last_seen_ts(node)
    if ts is not None:
        age_h = (datetime.now().timestamp() - ts) / 3600
        if age_h > 48: return "Inactive"
        if age_h <= 2:  return "Active"
    return "Active"


def _carrier(raw):
    s = str(raw)
    return s.split(" - ")[-1].strip() if " - " in s else ""


def _parse_time(entry):
    for f in ("timestamp", "backupTime", "date", "datetime", "dateTime"):
        ts = entry.get(f)
        if ts is None: continue
        s = str(ts).strip()
        if s.isdigit() and len(s) > 10:
            try: return datetime.fromtimestamp(int(s) / 1000).strftime("%d-%m-%Y %H:%M:%S")
            except: pass
        if s.isdigit() and len(s) <= 10:
            try: return datetime.fromtimestamp(int(s)).strftime("%d-%m-%Y %H:%M:%S")
            except: pass
        if not s.isdigit() and len(s) > 5: return s.replace(" | ", " ")
    return datetime.now().strftime("%d-%m-%Y %H:%M:%S")


def _norm_sms(entry):
    if not isinstance(entry, dict): return None, None, None
    msg = (entry.get("body") or entry.get("message") or
           entry.get("msg")  or entry.get("text") or "")
    sender = (entry.get("sender") or entry.get("from") or
              entry.get("ph")     or entry.get("address") or "Unknown")
    return msg, str(sender), _parse_time(entry)


def _latest(node):
    if not isinstance(node, dict) or not node: return None, None
    def key_score(k):
        s = str(k)
        if s.lstrip("-").isdigit() and len(s.lstrip("-")) >= 5:
            return (1, int(s.lstrip("-")))
        parts = s.split("_")
        if len(parts) >= 3 and parts[-1].isdigit() and len(parts[-1]) >= 10:
            return (1, int(parts[-1]))
        return (0, s)
    try: best = max(node.keys(), key=key_score)
    except: best = list(node.keys())[-1]
    return node[best], str(best)


def _top_n(node, n=5):
    """Return list of (entry, key) for the N newest SMS entries."""
    if not isinstance(node, dict) or not node: return []
    def key_score(k):
        s = str(k)
        if s.lstrip("-").isdigit() and len(s.lstrip("-")) >= 5:
            return (1, int(s.lstrip("-")))
        parts = s.split("_")
        if len(parts) >= 3 and parts[-1].isdigit() and len(parts[-1]) >= 10:
            return (1, int(parts[-1]))
        return (0, s)
    try:
        sorted_keys = sorted(node.keys(), key=key_score, reverse=True)[:n]
    except: sorted_keys = list(node.keys())[-n:]
    return [(node[k], str(k)) for k in sorted_keys if isinstance(node.get(k), dict)]


# ══════════════ DYNAMIC PHONE & CARRIER EXTRACTORS ══

_PH_FIELDS = (
    "phoneNumber", "phone_number", "phone", "number", "mobile",
    "mobile_number", "sim1", "sim2", "sim1Number", "sim2Number",
    "SIM1", "SIM2", "simNumber", "simPhone", "subscriberNumber",
    "mobileNumber", "contactNumber", "telNumber",
)
_CARRIER_FIELDS = (
    "operator", "network", "carrier", "simOperator", "networkOperator",
    "telephonyNetworkOperator", "carrierName", "serviceProvider",
    "networkName", "operatorName",
)
_PH_RE = re.compile(r'^(\+?91)?[6-9]\d{9}$|^\+\d{7,15}$')


def _extract_phone_deep(node):
    """
    Scan 20+ known fields + nested simInfo nodes for any real phone number.
    Returns first valid phone found, or None.
    """
    if not isinstance(node, dict): return None
    for key in _PH_FIELDS:
        v = node.get(key)
        if v and isinstance(v, (str, int)):
            ph = _norm_phone(str(v).split(" - ")[0])
            if ph: return ph
    for nest_key in ("simInfo", "sim_info", "SimInfo", "simINFO", "SIMInfo",
                     "deviceInfo", "device_info", "info", "Info"):
        sub = node.get(nest_key)
        if isinstance(sub, dict):
            for sk in _PH_FIELDS:
                v = sub.get(sk)
                if v:
                    ph = _norm_phone(str(v).split(" - ")[0])
                    if ph: return ph
    return None


async def _auto_probe_number(device_id: str, fb_source: str, api_key=None):
    """
    When a number is stored as DEV-{device_id}, try fetching the raw Firebase
    device node from 12 common path patterns and scan every field for a real
    phone number + carrier. Returns (phone, carrier) or (None, None).
    """
    if not fb_source: return None, None
    base = fb_source.replace(".json", "").rstrip("/")
    did  = device_id

    probe_paths = [
        # Pattern A3 (v16-style): phone in simDetails
        f"{base}/All_Users/simDetails/{did}",
        # Pattern A2 (v108-style): phone in DeviceInfo
        f"{base}/All_Users/DeviceInfo/{did}",
        # Common fallback paths
        f"{base}/{did}",
        f"{base}/devices/{did}",
        f"{base}/Devices/{did}",
        f"{base}/All_Users/{did}",
        f"{base}/all_users/{did}",
        f"{base}/users/{did}",
        f"{base}/user/{did}",
        f"{base}/info/{did}",
        f"{base}/deviceInfo/{did}",
        f"{base}/device_info/{did}",
        f"{base}/simInfo/{did}",
        f"{base}/Numbers/{did}",
    ]

    def _check_node(data):
        """Return (phone, carrier) from a node dict, or (None, None)."""
        if not isinstance(data, dict):
            return None, None
        # Pattern A3: sim1Number / sim2Number fields
        for ph_field in ("sim1Number", "sim2Number", "simNumber", "phoneNumber"):
            v = data.get(ph_field)
            if v:
                ph = _norm_phone(str(v))
                if ph:
                    ca = (data.get("sim1Provider") or data.get("sim2Provider")
                          or _extract_carrier_deep(data) or "")
                    return ph, ca
        ph = _extract_phone_deep(data)
        ca = _extract_carrier_deep(data)
        if ph:
            return ph, ca or ""
        # One level of children
        for child in data.values():
            if isinstance(child, dict):
                ph2 = _extract_phone_deep(child)
                if ph2:
                    return ph2, _extract_carrier_deep(child) or ca or ""
        return None, None

    # Probe all paths concurrently in batches of 4 for speed (3 s timeout each)
    async with aiohttp.ClientSession() as sess:
        BATCH = 4
        for i in range(0, len(probe_paths), BATCH):
            batch = probe_paths[i:i + BATCH]
            results = await asyncio.gather(
                *[fb_get(sess, p, timeout=3, api_key=api_key) for p in batch],
                return_exceptions=True)
            for data in results:
                if isinstance(data, Exception) or not isinstance(data, dict):
                    continue
                ph, ca = _check_node(data)
                if ph:
                    return ph, ca
    return None, None


def _extract_carrier_deep(node):
    """Scan known carrier fields and return operator string or ''."""
    if not isinstance(node, dict): return ""
    for key in _CARRIER_FIELDS:
        v = node.get(key)
        if v and isinstance(v, str) and len(v) > 1:
            return str(v)[:40]
    for slot in ("sim1", "sim2", "SIM1", "SIM2"):
        v = node.get(slot)
        if isinstance(v, str) and " - " in v:
            return v.split(" - ")[-1].strip()[:40]
    for nest in ("simInfo", "sim_info", "SimInfo"):
        sub = node.get(nest)
        if isinstance(sub, dict):
            for key in _CARRIER_FIELDS:
                v = sub.get(key)
                if v and isinstance(v, str): return str(v)[:40]
    return ""


# ══════════════ FIREBASE STRUCTURE PARSERS ════════

async def _pat_A(sess, base, api_key=None):
    shallow = await fb_get(sess, f"{base}/.json?shallow=true", api_key=api_key)
    if not isinstance(shallow, dict): return None
    all_rows = []
    for rk in shallow:
        if rk in _FB_SYSTEM_KEYS: continue
        sim = await fb_get(sess, f"{base}/{rk}/All_User/SimINFO.json", api_key=api_key)
        if not isinstance(sim, dict): continue
        info = await fb_get(sess, f"{base}/{rk}/All_User/Info.json", api_key=api_key) or {}
        for dev_id, sd in sim.items():
            if not isinstance(sd, dict): continue
            dv   = (info.get(dev_id) or {}) if isinstance(info, dict) else {}
            name = dv.get("Name", dev_id) if dv else dev_id
            st   = _status_str(dv.get("status", "Online")) if dv else "Active"
            carrier_found = _extract_carrier_deep(sd) or _extract_carrier_deep(dv)
            found_any = False
            for slot in ("sim1", "sim2"):
                raw = sd.get(slot)
                if not raw: continue
                num = _norm_phone(str(raw).split(" - ")[0])
                if not num:
                    num = _extract_phone_deep(sd)
                if not num: continue
                found_any = True
                carrier = _carrier(raw) or carrier_found
                all_rows.append(dict(number=num, device_id=dev_id,
                    device_name=str(name), sim_slot=slot, carrier=carrier,
                    status=st, struct_type="A",
                    sms_path=f"{base}/{rk}/All_User/Sms/{dev_id}",
                    status_path=f"{base}/{rk}/All_User/Info/{dev_id}"))
            if not found_any:
                ph = _extract_phone_deep(sd) or _extract_phone_deep(dv)
                if ph:
                    all_rows.append(dict(number=ph, device_id=dev_id,
                        device_name=str(name), sim_slot="sim1", carrier=carrier_found,
                        status=st, struct_type="A",
                        sms_path=f"{base}/{rk}/All_User/Sms/{dev_id}",
                        status_path=f"{base}/{rk}/All_User/Info/{dev_id}"))
    return (all_rows, "A") if all_rows else None


async def _pat_A2(sess, base, root_keys, api_key=None):
    if "All_Users" not in root_keys: return None
    devs   = await fb_get(sess, f"{base}/All_Users/DeviceInfo.json?shallow=true", api_key=api_key)
    sms_sh = await fb_get(sess, f"{base}/All_Users/sms.json?shallow=true", api_key=api_key)
    if not isinstance(devs, dict): return None
    sms_ids = set(sms_sh.keys()) if isinstance(sms_sh, dict) else set()
    rows = []
    for dev_id in devs:
        if dev_id not in sms_ids: continue
        di = await fb_get(sess, f"{base}/All_Users/DeviceInfo/{dev_id}.json", api_key=api_key)
        name = "Unknown"; st = "Active"; carrier = ""
        if isinstance(di, dict):
            name    = str(di.get("Model") or di.get("Brand") or dev_id[:12])
            st      = _status_str(di.get("Status") or di.get("status", "Online"))
            carrier = _extract_carrier_deep(di)
            ph      = _extract_phone_deep(di)
        else: ph = None
        rows.append(dict(number=ph or f"DEV-{dev_id}", device_id=dev_id,
            device_name=name, sim_slot="sim1", carrier=carrier, status=st,
            struct_type="A2",
            sms_path=f"{base}/All_Users/sms/{dev_id}",
            status_path=f"{base}/All_Users/DeviceInfo/{dev_id}"))
    return (rows, "A2") if rows else None


async def _pat_F(sess, base, root_keys, api_key=None):
    if "sms" not in root_keys or "admin" not in root_keys: return None
    cfs = await fb_get(sess, f"{base}/admin/callForwardingStatus.json?shallow=true", api_key=api_key)
    if not isinstance(cfs, dict): return None
    rows = []; seen = set()
    for dev_id in cfs:
        ents = await fb_get(sess, f"{base}/admin/callForwardingStatus/{dev_id}.json", api_key=api_key)
        if not isinstance(ents, dict): continue
        phone = None
        for v in ents.values():
            if isinstance(v, dict):
                fn = v.get("forwardNumber")
                if fn: phone = _norm_phone(str(fn)); break
        if not phone:
            phone = _extract_phone_deep(ents)
        if phone and dev_id not in seen:
            seen.add(dev_id)
            rows.append(dict(number=phone, device_id=dev_id,
                device_name=dev_id[:12], sim_slot="sim1", carrier="", status="Active",
                struct_type="F",
                sms_path=f"{base}/sms/{dev_id}",
                status_path=f"{base}/admin/callForwardingStatus/{dev_id}"))
    return (rows, "F") if rows else None


async def _pat_G(sess, base, root_keys, api_key=None):
    sms_key = next((k for k in ("user_sms", "sms") if k in root_keys), None)
    dev_key = next((k for k in ("user_data", "user_list") if k in root_keys), None)
    if not sms_key: return None
    sms_sh = await fb_get(sess, f"{base}/{sms_key}.json?shallow=true", api_key=api_key)
    if not isinstance(sms_sh, dict) or not sms_sh: return None
    dev_names = {}; dev_phones = {}; dev_carriers = {}
    if dev_key:
        dev_sh = await fb_get(sess, f"{base}/{dev_key}.json?shallow=true", api_key=api_key)
        if isinstance(dev_sh, dict):
            for did in dev_sh.keys():          # no [:50] limit — fetch all devices
                if did in _FB_SYSTEM_KEYS: continue
                dd = await fb_get(sess, f"{base}/{dev_key}/{did}.json", api_key=api_key)
                if isinstance(dd, dict):
                    dev_names[did]   = str(dd.get("d_name") or dd.get("name") or did[:12])
                    dev_phones[did]  = _extract_phone_deep(dd)
                    dev_carriers[did]= _extract_carrier_deep(dd)
    rows = []
    for dev_id in sms_sh:
        ph = dev_phones.get(dev_id) or f"DEV-{dev_id}"
        rows.append(dict(number=ph, device_id=dev_id,
            device_name=dev_names.get(dev_id, dev_id[:12]),
            sim_slot="sim1", carrier=dev_carriers.get(dev_id, ""), status="Active",
            struct_type="G",
            sms_path=f"{base}/{sms_key}/{dev_id}",
            status_path=f"{base}/{dev_key}/{dev_id}" if dev_key else base))
    return (rows, "G") if rows else None


async def _pat_H(sess, base, root_keys, api_key=None):
    if "messages" not in root_keys: return None
    msg_sh = await fb_get(sess, f"{base}/messages.json?shallow=true", api_key=api_key)
    if not isinstance(msg_sh, dict): return None
    dev_key = next((k for k in ("userdata", "clients", "users") if k in root_keys), None)
    rows = []
    for dev_id in msg_sh:
        name = dev_id[:12]; carrier = ""; ph = None
        if dev_key:
            dd = await fb_get(sess, f"{base}/{dev_key}/{dev_id}.json", api_key=api_key)
            if isinstance(dd, dict):
                name    = str(dd.get("name") or dd.get("d_name") or dd.get("deviceName") or name)
                ph      = _extract_phone_deep(dd)
                carrier = _extract_carrier_deep(dd)
        rows.append(dict(number=ph or f"DEV-{dev_id}", device_id=dev_id,
            device_name=name, sim_slot="sim1", carrier=carrier, status="Active",
            struct_type="H",
            sms_path=f"{base}/messages/{dev_id}",
            status_path=f"{base}/{dev_key}/{dev_id}" if dev_key else base))
    return (rows, "H") if rows else None


async def _pat_Y(sess, base, root_keys, api_key=None):
    """
    Pattern Y — flat device dict at root level.
    Handles schemas like yono-sb41 where devices are root keys
    with nested info containing phone/carrier but no sim1/sim2 keys.
    """
    sms_keys = root_keys - _FB_SYSTEM_KEYS
    if len(sms_keys) < 1 or len(sms_keys) > 200: return None
    rows = []; count = 0
    for rk in list(sms_keys)[:100]:
        node = await fb_get(sess, f"{base}/{rk}.json", api_key=api_key)
        if not isinstance(node, dict): continue
        ph      = _extract_phone_deep(node)
        carrier = _extract_carrier_deep(node)
        name    = str(node.get("Name") or node.get("name") or
                      node.get("deviceName") or node.get("device_name") or rk[:14])
        st      = _status_from_node(node)
        sms_sub = next((node.get(k) for k in ("sms","Sms","SMS","messages") if node.get(k)), None)
        if ph or sms_sub:
            rows.append(dict(number=ph or f"DEV-{rk}", device_id=rk,
                device_name=name, sim_slot="sim1", carrier=carrier, status=st,
                struct_type="Y",
                sms_path=f"{base}/{rk}/sms",
                status_path=f"{base}/{rk}"))
            count += 1
    return (rows, "Y") if rows else None


async def _pat_Z(sess, base, root_keys, api_key=None):
    """
    Pattern Z — universal fallback / aggressive deep scanner.
    Handles unknown schemas like raaz-5287d that only expose device IDs at root.
    Fetches each root key's node and recursively hunts for phone numbers
    in any field, and looks for SMS sub-paths.
    """
    sms_keys = root_keys - _FB_SYSTEM_KEYS
    if not sms_keys: return None
    rows = []; seen = set()
    for rk in list(sms_keys)[:200]:
        try:
            node = await fb_get(sess, f"{base}/{rk}.json", api_key=api_key, timeout=10)
            if not isinstance(node, dict):
                # Scalar root key — skip
                continue
            ph      = _extract_phone_deep(node)
            carrier = _extract_carrier_deep(node)
            # Didn't find phone at root level? Try one level deeper
            if not ph:
                child_keys = [k for k in node if k not in _FB_SYSTEM_KEYS][:15]
                for ck in child_keys:
                    child = node.get(ck)
                    if isinstance(child, dict):
                        ph = ph or _extract_phone_deep(child)
                        carrier = carrier or _extract_carrier_deep(child)
                        if ph: break
                    elif isinstance(child, (str, int)):
                        # Value could be phone directly
                        cand = str(child).strip()
                        if _PH_RE.match(cand):
                            ph = _norm_phone(cand); break
            # Device name from common fields
            name = str(node.get("Name") or node.get("name") or
                       node.get("deviceName") or node.get("device_name") or
                       node.get("model") or node.get("Model") or rk[:16])
            st = _status_from_node(node)
            # Find SMS sub-path
            sms_sub = None
            for sk in ("sms","Sms","SMS","messages","Messages","inbox","received"):
                if sk in node:
                    sms_sub = f"{base}/{rk}/{sk}"; break
            number_key = ph or f"DEV-{rk}"
            if number_key not in seen:
                seen.add(number_key)
                rows.append(dict(number=number_key, device_id=rk,
                    device_name=name, sim_slot="sim1", carrier=carrier or "",
                    status=st, struct_type="Z",
                    sms_path=sms_sub or f"{base}/{rk}/sms",
                    status_path=f"{base}/{rk}"))
        except Exception as e:
            log.debug("_pat_Z rk=%s err=%s", rk, e)
            continue
    return (rows, "Z") if rows else None


def _sim_entries_from(data, base, struct):
    rows = []
    if not isinstance(data, dict): return rows
    for dev_id, dd in data.items():
        if not isinstance(dd, dict): continue
        name    = str(dd.get("Name") or dd.get("name") or dev_id)
        st      = _status_str(dd.get("status", "Online"))
        carrier = _extract_carrier_deep(dd)
        found   = False
        for slot in ("sim1", "sim2", "SIM1", "SIM2"):
            raw = dd.get(slot)
            if not raw: continue
            num = _norm_phone(str(raw).split(" - ")[0])
            if not num: continue
            found = True
            rows.append(dict(number=num, device_id=dev_id, device_name=name,
                sim_slot=slot.lower(), carrier=_carrier(raw) or carrier, status=st,
                struct_type=struct,
                sms_path=f"{base}/{dev_id}/sms",
                status_path=f"{base}/{dev_id}"))
        if not found:
            ph = _extract_phone_deep(dd)
            if ph:
                rows.append(dict(number=ph, device_id=dev_id, device_name=name,
                    sim_slot="sim1", carrier=carrier, status=st, struct_type=struct,
                    sms_path=f"{base}/{dev_id}/sms",
                    status_path=f"{base}/{dev_id}"))
    return rows


def _pat_E(full_data, base):
    rows = []; seen = set()
    def scan(node, path, d=0):
        if d > 8 or not isinstance(node, dict): return
        for k, v in node.items():
            cur = f"{path}/{k}" if path else k
            if isinstance(v, (str, int)):
                s = str(v).strip()
                if _PH_RE.match(s):
                    ph = _norm_phone(s)
                    if ph and ph not in seen:
                        seen.add(ph)
                        parent = "/".join(cur.split("/")[:-1]) if "/" in cur else ""
                        dev_id = path.split("/")[-1] if "/" in path else cur
                        rows.append(dict(number=ph, device_id=dev_id,
                            device_name=dev_id[:12], sim_slot="sim1",
                            carrier="", status="Active", struct_type="E",
                            sms_path=f"{base}/{parent}/sms" if parent else f"{base}/sms",
                            status_path=f"{base}/{parent}" if parent else base))
            elif isinstance(v, dict): scan(v, cur, d + 1)
    scan(full_data, "")
    return (rows, "E") if rows else (None, None)


# ══════════════ MASTER DETECTOR ═══════════════════

async def detect_firebase(sess, url, api_key=None):
    """Try patterns A→Y→E. Returns (entries_list, struct_type, error_or_None)."""
    base = url.replace(".json", "").rstrip("/")

    code, _ = await fb_ping(sess, base, api_key=api_key)
    if code in (401, 403):
        return [], "?", "Access Denied (private database, needs auth token)"
    if code in (423,):
        return [], "?", "Database is locked/disabled"
    if code == 0:
        return [], "?", "Could not connect (timeout or network error)"
    if code != 200:
        return [], "?", f"HTTP {code} error"

    shallow = await fb_get(sess, f"{base}/.json?shallow=true", api_key=api_key)
    if not isinstance(shallow, dict):
        return [], "?", "Could not read Firebase root"
    root_keys = set(shallow.keys()) - _FB_SYSTEM_KEYS

    r = await _pat_A(sess, base, api_key)
    if r: return r[0], r[1], None

    r = await _pat_A2(sess, base, root_keys, api_key)
    if r: return r[0], r[1], None

    r = await _pat_F(sess, base, root_keys, api_key)
    if r: return r[0], r[1], None

    r = await _pat_H(sess, base, root_keys, api_key)
    if r: return r[0], r[1], None

    r = await _pat_G(sess, base, root_keys, api_key)
    if r: return r[0], r[1], None

    r = await _pat_Y(sess, base, root_keys, api_key)
    if r: return r[0], r[1], None

    r = await _pat_Z(sess, base, root_keys, api_key)
    if r: return r[0], r[1], None

    # Safety guard: only attempt full download if root has ≤200 keys
    # (large databases like customer03support have thousands — skip to avoid crash)
    if len(root_keys) > 200:
        return [], "?", f"Database too large for full scan ({len(root_keys)} root keys). Try a more specific path."

    full = await fb_get(sess, f"{base}/.json", api_key=api_key)
    if isinstance(full, dict):
        for rk, rv in full.items():
            if not isinstance(rv, dict): continue
            for ck in ("devices", "Devices", "device", "Device"):
                devs = rv.get(ck)
                rows = _sim_entries_from(devs, f"{base}/{rk}/{ck}", "B")
                if rows: return rows, "B", None
            rows = _sim_entries_from(rv, f"{base}/{rk}", "D")
            if rows: return rows, "D", None
        rows = _sim_entries_from(full, base, "C")
        if rows: return rows, "C", None
        rows, stype = _pat_E(full, base)
        if rows: return rows, stype, None

    return [], "?", "No phone numbers found in any known structure"


# ══════════════ SYNC ══════════════════════════════

async def sync_one(url, label=None, api_key=None):
    base = url.replace(".json", "").rstrip("/")
    if not api_key: api_key = _get_fb_apikey(base)
    async with aiohttp.ClientSession() as sess:
        entries, stype, err = await detect_firebase(sess, url, api_key=api_key)
        if err: return 0, err, "?"
        n = 0; ghost_n = 0
        with db.cx() as c:
            for e in entries:
                is_ghost = 1 if str(e["number"]).startswith("DEV-") else 0
                if is_ghost: ghost_n += 1
                c.execute("""INSERT INTO numbers
                    (number,device_id,device_name,sim_slot,carrier,status,
                     fb_source,sms_path,status_path,struct_type,is_ghost)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(number) DO UPDATE SET
                        device_id=excluded.device_id,device_name=excluded.device_name,
                        sim_slot=excluded.sim_slot,carrier=excluded.carrier,
                        status=excluded.status,fb_source=excluded.fb_source,
                        sms_path=excluded.sms_path,status_path=excluded.status_path,
                        struct_type=excluded.struct_type,
                        is_ghost=CASE WHEN excluded.is_ghost=0 THEN 0 ELSE numbers.is_ghost END""",
                    (e["number"], e["device_id"], e["device_name"], e["sim_slot"],
                     e["carrier"], e["status"], base,
                     e["sms_path"], e["status_path"], e["struct_type"], is_ghost))
                n += 1
        now = datetime.now().strftime("%d-%m-%Y %H:%M")
        lbl = label or base.split("//")[-1].split(".")[0]
        with db.cx() as c:
            c.execute("""INSERT INTO firebase_sources(url,label,added_at,last_synced,num_count,struct_type,api_key)
                VALUES(?,?,?,?,?,?,?)
                ON CONFLICT(url) DO UPDATE SET
                    label=COALESCE(excluded.label,label),
                    last_synced=excluded.last_synced,
                    num_count=excluded.num_count,struct_type=excluded.struct_type,
                    api_key=COALESCE(excluded.api_key,api_key)""",
                (base, lbl, now, now, n, stype, api_key))
        log.info("Synced %s → %d numbers (real=%d ghost=%d struct=%s)",
                 base, n, n - ghost_n, ghost_n, stype)
        return n, None, stype


# ══════════════ AUTO RE-SYNC ══════════════════════

async def auto_resync():
    await asyncio.sleep(60)
    ghost_probe_counter = 0
    while True:
        srcs = db.cx().execute("SELECT url FROM firebase_sources").fetchall()
        if srcs:
            log.info("[AutoSync] %d source(s)...", len(srcs))
            tot = 0
            for s in srcs:
                n, err, _ = await sync_one(s["url"])
                tot += n
                if err: log.warning("[AutoSync] %s: %s", s["url"], err)
            log.info("[AutoSync] Done. Total=%d", tot)

        # ── Ghost Device Auto-Probe (every 10 min = every RESYNC_INTERVAL cycle) ──
        ghost_probe_counter += 1
        ghosts = db.cx().execute(
            "SELECT * FROM numbers WHERE is_ghost=1 OR number LIKE 'DEV-%'").fetchall()
        if ghosts:
            log.info("[GhostProbe] Scanning %d ghost device(s)...", len(ghosts))
            resolved = []
            async with aiohttp.ClientSession() as sess:
                for g in ghosts:
                    api_key = _get_fb_apikey(g["fb_source"])
                    ph, ca  = await _auto_probe_number(g["device_id"], g["fb_source"], api_key)
                    if ph and ph != g["number"]:
                        with db.cx() as cx:
                            cx.execute(
                                "UPDATE numbers SET number=?, carrier=?, is_ghost=0 WHERE id=?",
                                (ph, ca or g["carrier"], g["id"]))
                        resolved.append((g["device_id"], ph, ca))
                        log.info("[GhostProbe] Resolved %s → %s", g["device_id"], ph)
            if resolved:
                lines = "\n".join(
                    f"  ✅ <code>{did[:12]}</code> → <code>{ph}</code>"
                    + (f" [{ca}]" if ca else "")
                    for did, ph, ca in resolved)
                msg_text = (
                    f"╔══════════════════════╗\n"
                    f"  👻 <b>GHOST RESOLVED!</b>\n"
                    f"╚══════════════════════╝\n\n"
                    f"Found real numbers for <b>{len(resolved)}</b> ghost device(s):\n\n"
                    f"{lines}\n\n"
                    f"<i>Numbers moved to Active section.</i>")
                for aid in ADMIN_IDS:
                    try: await bot.send_message(aid, msg_text)
                    except Exception as e: log.warning("[GhostProbe] notify %d: %s", aid, e)

        await asyncio.sleep(RESYNC_INTERVAL)


# ══════════════ FETCH SMS (with multi-source failover) ══

async def _try_fetch_from_path(sess, path, device_id, fb_source, api_key=None):
    node = await fb_get(sess, f"{path}.json", api_key=api_key)
    if not isinstance(node, dict) or not node: return None
    entry, eid = _latest(node)
    if entry is None: return None
    ck = f"{device_id}:{fb_source}"
    if last_sms_seen.get(ck) == eid: return None
    last_sms_seen[ck] = eid
    msg, sender, ts = _norm_sms(entry)
    if not msg: return None
    otp = re.search(r'\b(\d{4,8})\b', msg)
    return {"otp": otp.group(1) if otp else None,
            "sender": sender, "message": msg, "time": ts}


async def fetch_sms(number, device_id, fb_source, sms_path=None):
    try:
        async with aiohttp.ClientSession() as sess:
            api_key = _get_fb_apikey(fb_source)
            paths = []
            if sms_path: paths.append((sms_path, fb_source, api_key))

            # Build candidate paths from primary source
            if fb_source:
                b = fb_source.replace(".json", "").rstrip("/")
                sh = await fb_get(sess, f"{b}/.json?shallow=true", api_key=api_key)
                # Pick first non-system root key as the app namespace
                rk = next((k for k in (sh or {}) if k not in _FB_SYSTEM_KEYS), None)
                extras = [
                    f"{b}/user_sms/{device_id}",
                    f"{b}/sms/{device_id}",
                    f"{b}/messages/{device_id}",
                    f"{b}/All_Users/sms/{device_id}",
                    f"{b}/{device_id}/sms",
                ]
                if rk: extras = [f"{b}/{rk}/All_User/Sms/{device_id}"] + extras
                for p in extras:
                    if (p, fb_source, api_key) not in paths:
                        paths.append((p, fb_source, api_key))

            # Failover: try all other Firebase sources for this device_id
            all_srcs = db.cx().execute(
                "SELECT url, api_key FROM firebase_sources WHERE url != ?",
                (fb_source or "",)).fetchall()
            for src in all_srcs:
                src_base = src["url"]; src_key = src["api_key"]
                for p in [f"{src_base}/sms/{device_id}",
                           f"{src_base}/user_sms/{device_id}",
                           f"{src_base}/messages/{device_id}"]:
                    paths.append((p, src_base, src_key))

            for path, src, key in paths:
                result = await _try_fetch_from_path(sess, path, device_id, src, key)
                if result:
                    result["number"] = number
                    return result

    except Exception as e:
        log.warning("fetch_sms error: %s", e)
    return None


async def fetch_last_n_sms(number, device_id, fb_source, sms_path=None, n=5):
    """Fetch N most recent SMS entries for a device."""
    results = []
    try:
        async with aiohttp.ClientSession() as sess:
            api_key = _get_fb_apikey(fb_source)
            paths = []
            if sms_path: paths.append(sms_path)
            if fb_source:
                b = fb_source.replace(".json", "").rstrip("/")
                sh = await fb_get(sess, f"{b}/.json?shallow=true", api_key=api_key)
                rk = list(sh.keys())[0] if isinstance(sh, dict) and sh else None
                extras = [
                    f"{b}/sms/{device_id}", f"{b}/user_sms/{device_id}",
                    f"{b}/messages/{device_id}", f"{b}/{device_id}/sms",
                ]
                if rk: extras = [f"{b}/{rk}/All_User/Sms/{device_id}"] + extras
                for p in extras:
                    if p not in paths: paths.append(p)
            for path in paths:
                node = await fb_get(sess, f"{path}.json", api_key=api_key)
                if not isinstance(node, dict) or not node: continue
                entries = _top_n(node, n)
                for entry, _ in entries:
                    msg, sender, ts = _norm_sms(entry)
                    if msg:
                        otp = re.search(r'\b(\d{4,8})\b', msg)
                        results.append({"sender": sender, "message": msg,
                                        "time": ts, "otp": otp.group(1) if otp else None})
                if results: break
    except Exception as e:
        log.warning("fetch_last_n_sms error: %s", e)
    return results


# ══════════════ SSE STREAMING ════════════════════

async def _sse_listener(sms_path: str, device_id: str, fb_source: str,
                        api_key, queue: asyncio.Queue, uid: int):
    """
    Listen to Firebase path via Server-Sent Events (SSE).
    Yields new SMS entries into queue for instant delivery.
    Falls back gracefully — polling in cb_monitor covers gaps.
    """
    if not sms_path: return
    base_path = sms_path.replace(".json", "").rstrip("/")
    url = f"{base_path}.json?orderBy=%22%24key%22&limitToLast=10"
    if api_key: url += f"&auth={api_key}"
    headers = {"Accept": "text/event-stream", "Cache-Control": "no-cache"}
    seen_keys: set = set()
    initial_loaded = False

    while active_sessions.get(uid):
        try:
            async with aiohttp.ClientSession() as sess:
                async with sess.get(url, headers=headers,
                                    timeout=aiohttp.ClientTimeout(connect=10, total=None)) as resp:
                    if resp.status != 200:
                        await asyncio.sleep(5)
                        continue
                    event_type = None
                    async for raw_line in resp.content:
                        if not active_sessions.get(uid): return
                        line = raw_line.decode("utf-8", errors="ignore").rstrip("\r\n")
                        if line.startswith("event:"):
                            event_type = line[6:].strip()
                        elif line.startswith("data:") and event_type in ("put", "patch"):
                            try:
                                payload = json.loads(line[5:].strip())
                                path    = payload.get("path", "")
                                data    = payload.get("data")
                                if data is None:
                                    initial_loaded = True
                                    event_type = None
                                    continue
                                # Initial snapshot at root — record existing keys
                                if path == "/" and not initial_loaded:
                                    if isinstance(data, dict):
                                        seen_keys.update(data.keys())
                                    initial_loaded = True
                                    event_type = None
                                    continue
                                # Push at root with dict — new or updated entries
                                if path == "/" and isinstance(data, dict):
                                    for k, v in data.items():
                                        if k not in seen_keys and isinstance(v, dict):
                                            seen_keys.add(k)
                                            await queue.put((v, k))
                                # Push at specific key path e.g. "/-NeABC" or "/key"
                                elif path and path != "/" and isinstance(data, dict):
                                    key = path.lstrip("/").split("/")[0]
                                    if key and key not in seen_keys:
                                        seen_keys.add(key)
                                        # data is the entry itself
                                        await queue.put((data, key))
                                    elif key and isinstance(data, dict):
                                        # Update to existing key — may have new SMS sub-entry
                                        for sk, sv in data.items():
                                            sub_key = f"{key}/{sk}"
                                            if sub_key not in seen_keys and isinstance(sv, dict):
                                                seen_keys.add(sub_key)
                                                await queue.put((sv, sub_key))
                            except Exception as e:
                                log.warning("SSE parse: %s", e)
                            event_type = None
                        elif line == "":
                            event_type = None
        except asyncio.CancelledError:
            return
        except Exception as e:
            log.warning("SSE dropped (%s). Reconnect in 5s...", e)
            await asyncio.sleep(5)


# ══════════════ DEVICE STATUS & HEALTH ═══════════

async def dev_online(device_id, fb_source, status_path=None):
    try:
        async with aiohttp.ClientSession() as sess:
            api_key = _get_fb_apikey(fb_source)
            paths = []
            if status_path: paths.append(status_path)
            if fb_source:
                b = fb_source.replace(".json", "").rstrip("/")
                sh = await fb_get(sess, f"{b}/.json?shallow=true", api_key=api_key)
                rk = list(sh.keys())[0] if isinstance(sh, dict) and sh else None
                extras = [f"{b}/user_data/{device_id}", f"{b}/{device_id}"]
                if rk: extras = [f"{b}/{rk}/All_User/Info/{device_id}"] + extras
                paths += [p for p in extras if p not in paths]
            for path in paths:
                info = await fb_get(sess, f"{path}.json", api_key=api_key)
                if not isinstance(info, dict): continue
                sv = (info.get("status") or info.get("Status") or
                      info.get("online") or info.get("isOnline") or "online")
                return str(sv).strip().lower() not in ("offline", "0", "false", "inactive")
    except Exception as e:
        log.warning("dev_online error: %s", e)
    return True


async def dev_health(device_id, fb_source, status_path=None):
    """
    Returns (online: bool, battery: int|None, warning: str|None).
    Fast path: uses stored status_path directly (4 s timeout max).
    """
    try:
        async with aiohttp.ClientSession() as sess:
            api_key = _get_fb_apikey(fb_source)
            # Fast path: we already know exactly which path to query
            paths_to_try = []
            if status_path:
                paths_to_try.append(status_path)
            if fb_source and device_id:
                b = fb_source.replace(".json", "").rstrip("/")
                paths_to_try.append(f"{b}/{device_id}")
            for path in paths_to_try[:3]:  # max 3 paths, 4s each
                try:
                    info = await asyncio.wait_for(
                        fb_get(sess, path if path.endswith(".json") else f"{path}.json",
                               api_key=api_key),
                        timeout=4.0)
                except asyncio.TimeoutError:
                    continue
                if not isinstance(info, dict): continue
                sv     = _status_from_node(info)
                online = sv == "Active"
                battery = None
                for bf in ("battery","Battery","batteryLevel","battery_level","batt"):
                    bv = info.get(bf)
                    if bv is not None:
                        try: battery = int(str(bv).replace("%",""))
                        except: pass
                        break
                warns = []
                if not online: warns.append("device offline")
                if battery is not None and battery < 15:
                    warns.append(f"battery {battery}%")
                warning = ("⚠️ <b>Warning:</b> " + " | ".join(warns).capitalize()
                           + " — SMS may be delayed.") if warns else None
                return online, battery, warning
    except Exception as e:
        log.warning("dev_health error: %s", e)
    return True, None, None  # Default: assume online if unreachable


# ══════════════ FORMATS ══════════════════════════

def _disp(number):
    return f"Device {number[4:]}" if str(number).startswith("DEV-") else number


def fmt_otp(d):
    otp_val  = d.get('otp')
    number   = _disp(d.get('number', ''))
    sender   = d.get('sender', '?')
    time_str = d.get('time', '')
    message  = d.get('message', '')

    if otp_val:
        otp_block = (f"╔══════════════════════╗\n"
                     f"  🪸 <b>OTP RECEIVED</b>  ✅\n"
                     f"╠══════════════════════╣\n"
                     f"  🎯 <code>{otp_val}</code>  <i>(tap-hold to copy)</i>\n"
                     f"╚══════════════════════╝")
    else:
        otp_block = (f"╔══════════════════════╗\n"
                     f"  📨 <b>SMS RECEIVED</b>\n"
                     f"╚══════════════════════╝")

    return (f"{otp_block}\n\n"
            f"📞 <b>Number:</b> <code>{number}</code>\n"
            f"📨 <b>From:</b>   <b>{sender}</b>\n"
            f"🕐 <b>Time:</b>   {time_str}\n\n"
            f"💬 <b>Message:</b>\n"
            f"<i>{message}</i>")


def fmt_off(number, name):
    return (f"⚠️ <b>Device Offline</b>\n"
            f"📞 <code>{_disp(number)}</code>  📱 {name}\n"
            f"<i>SMS paused — auto-resumes when device comes back online.</i>")


# ══════════════ STATES ════════════════════════════

class S(StatesGroup):
    b_msg      = State()
    add_ch_id  = State()
    add_ch_link= State()
    set_limit  = State()
    bulk_fb    = State()
    fb_apikey  = State()   # waiting for API key after 401/403
    add_num    = State()
    enter_key  = State()
    gen_keys   = State()
    lookup_num = State()   # number lookup


# ══════════════ MENUS ════════════════════════════

def menu_user():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🟢 Active Numbers",    callback_data="num_Active"),
         InlineKeyboardButton(text="🔴 Inactive Numbers",  callback_data="num_Inactive")],
        [InlineKeyboardButton(text="🔍 Number Lookup",     callback_data="num_lookup"),
         InlineKeyboardButton(text="📋 SMS History",       callback_data="my_history")],
        [InlineKeyboardButton(text="👻 Ghost Devices",     callback_data="ghost_list_0"),
         InlineKeyboardButton(text="🔄 Refresh",           callback_data="refresh_home")],
        [InlineKeyboardButton(text="👥 My Referral",       callback_data="my_link"),
         InlineKeyboardButton(text="🔑 Enter Access Key",  callback_data="enter_key")],
    ])


def menu_admin():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔗 Add Firebase URLs",   callback_data="adm_fb"),
         InlineKeyboardButton(text="🔄 Sync All Firebase",   callback_data="adm_sync")],
        [InlineKeyboardButton(text="📋 Firebase Sources",    callback_data="adm_fb_list"),
         InlineKeyboardButton(text="📱 Add Number Manually", callback_data="adm_addnum")],
        [InlineKeyboardButton(text="🔑 Generate Keys",       callback_data="adm_genkeys"),
         InlineKeyboardButton(text="🔐 Toggle Key Mode",     callback_data="adm_keymode")],
        [InlineKeyboardButton(text="📈 Set Refer Limit",     callback_data="adm_limit"),
         InlineKeyboardButton(text="📢 Add Channel",         callback_data="adm_addch")],
        [InlineKeyboardButton(text="📊 Live Status",         callback_data="adm_report"),
         InlineKeyboardButton(text="⚡ Broadcast",           callback_data="adm_broadcast")],
        [InlineKeyboardButton(text="🔍 Rescan Ghost→Real",   callback_data="adm_rescan"),
         InlineKeyboardButton(text="👻 Ghost Devices",       callback_data="ghost_list_0")],
        [InlineKeyboardButton(text="🧹 Purge Old SMS (48h)", callback_data="adm_purge")],
    ])


# ══════════════ AUTH ═════════════════════════════

async def check_auth(uid, obj):
    channels = db.cx().execute("SELECT * FROM channels").fetchall()
    for ch in channels:
        in_ch = False
        try:
            m = await bot.get_chat_member(ch["channel_id"], uid)
            in_ch = m.status not in ("left", "kicked")
        except Exception as e:
            log.warning("Channel check failed %s: %s", ch["channel_id"], e)
            in_ch = False
        if not in_ch:
            kb = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="📢 Join Channel", url=ch['channel_link']),
                InlineKeyboardButton(text="✅ I Joined — Verify", callback_data="verify_join")
            ]])
            await obj.answer(
                f"╔══════════════════════╗\n"
                f"  📢 <b>JOIN REQUIRED</b>\n"
                f"╚══════════════════════╝\n\n"
                f"You must join our channel before using this bot.\n\n"
                f"<a href='{ch['channel_link']}'>👉 Tap here to join</a>\n\n"
                f"<i>After joining, tap ✅ I Joined — Verify</i>",
                reply_markup=kb)
            return False
    if not db.unlocked(uid):
        u     = db.cx().execute(
            "SELECT refer_count,access_key FROM users WHERE user_id=?", (uid,)).fetchone()
        limit = int(db.get("refer_limit") or 1)
        count = u["refer_count"] if u else 0
        me    = await bot.get_me()
        link  = f"https://t.me/{me.username}?start=ref_{uid}"
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔑 Enter Access Key",     callback_data="enter_key")],
            [InlineKeyboardButton(text="👥 Refer Friends (Earn Key)", url=link)],
        ])
        await obj.answer(
            f"╔══════════════════════╗\n"
            f"  🔒 <b>ACCESS REQUIRED</b>\n"
            f"╚══════════════════════╝\n\n"
            f"Choose how to unlock:\n\n"
            f"🔑 Enter an Access Key given by Admin\n"
            f"  — OR —\n"
            f"👥 Refer <b>{max(0,limit-count)}</b> more friend(s) to earn your key\n"
            f"📊 Progress: <b>{count}/{limit}</b>",
            reply_markup=kb)
        return False
    return True


@router.callback_query(F.data == "verify_join")
async def cb_verify_join(call: CallbackQuery):
    if await check_auth(call.from_user.id, call.message):
        a = db.cx().execute("SELECT COUNT(*) as c FROM numbers WHERE status='Active'").fetchone()["c"]
        t = db.cx().execute("SELECT COUNT(*) as c FROM numbers").fetchone()["c"]
        await call.message.edit_text(
            _home_text(call.from_user.first_name, a, t),
            reply_markup=menu_user())
    await call.answer()


# ══════════════ HOME TEXT ════════════════════════

def _home_text(first_name, active, total):
    hour = datetime.now().hour
    greet = "🌅 Good Morning" if 5 <= hour < 12 else \
            "☀️ Good Afternoon" if 12 <= hour < 17 else \
            "🌆 Good Evening" if 17 <= hour < 21 else "🌙 Good Night"
    ghosts   = db.cx().execute(
        "SELECT COUNT(*) as c FROM numbers WHERE is_ghost=1 OR number LIKE 'DEV-%'").fetchone()["c"]
    real_tot = total - ghosts
    bar_filled  = min(20, int((active / max(real_tot, 1)) * 20))
    bar_empty   = 20 - bar_filled
    status_bar  = "█" * bar_filled + "░" * bar_empty
    inactive    = real_tot - active
    return (
        f"╔══════════════════════════╗\n"
        f"  🏢 <b>OFFICE SMS RELAY</b>  v5\n"
        f"╚══════════════════════════╝\n\n"
        f"{greet}, <b>{first_name}</b>! 👋\n\n"
        f"╔═ 📊 DATABASE STATUS ══════╗\n"
        f"  🟢 Active   : <b>{active}</b>\n"
        f"  🔴 Inactive : <b>{inactive}</b>\n"
        f"  📱 Real     : <b>{real_tot}</b>\n"
        f"  👻 Ghost    : <b>{ghosts}</b>\n"
        f"  [{status_bar}]\n"
        f"╚═══════════════════════════╝\n\n"
        f"<i>Select an option below to begin:</i>"
    )


# ══════════════ /start ════════════════════════════

@router.message(CommandStart())
async def cmd_start(msg: Message):
    args   = msg.text.split()
    ref_id = int(args[1].replace("ref_", "")) if len(args) > 1 and args[1].startswith("ref_") else None
    res    = db.reg_user(msg.from_user.id, msg.from_user.username or "User", ref_id)
    if res.get("is_banned"): return await msg.answer("⛔️ You are banned.")
    if res.get("notify_ref"):
        try:
            await bot.send_message(res["notify_ref"],
                f"╔══════════════════════╗\n"
                f"  🎉 <b>REFERRAL SUCCESS!</b>\n"
                f"╚══════════════════════╝\n\n"
                f"Your new Access Key:\n\n<code>{res['new_key']}</code>\n\n"
                f"<i>Use this key to unlock full access.</i>")
        except: pass
    channels = db.cx().execute("SELECT * FROM channels").fetchall()
    for ch in channels:
        in_ch = False
        try:
            m = await bot.get_chat_member(ch["channel_id"], msg.from_user.id)
            in_ch = m.status not in ("left", "kicked")
        except: pass
        if not in_ch:
            await msg.answer(
                f"╔══════════════════════╗\n"
                f"  📢 <b>JOIN REQUIRED</b>\n"
                f"╚══════════════════════╝\n\n"
                f"Welcome! First, join our channel:\n\n"
                f"<a href='{ch['channel_link']}'>👉 Click here to join</a>",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="📢 Join Now",           url=ch['channel_link']),
                    InlineKeyboardButton(text="✅ Done — Let Me In",   callback_data="verify_join")
                ]]))
            return

    if not db.unlocked(msg.from_user.id):
        u     = db.cx().execute(
            "SELECT refer_count,access_key FROM users WHERE user_id=?", (msg.from_user.id,)).fetchone()
        limit = int(db.get("refer_limit") or 1)
        count = u["refer_count"] if u else 0
        me    = await bot.get_me()
        link  = f"https://t.me/{me.username}?start=ref_{msg.from_user.id}"
        await msg.answer(
            f"╔══════════════════════╗\n"
            f"  🔒 <b>UNLOCK ACCESS</b>\n"
            f"╚══════════════════════╝\n\n"
            f"Hello <b>{msg.from_user.first_name}</b>! 👋\n\n"
            f"Choose how to get access:\n\n"
            f"1️⃣  <b>Enter Key</b> — if you have an admin key\n"
            f"2️⃣  <b>Refer Friends</b> — invite {max(0,limit-count)} friend(s)\n"
            f"     Progress: <b>{count}/{limit}</b> 👥\n\n"
            f"🔗 Your referral link:\n<code>{link}</code>",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔑 Enter Access Key",      callback_data="enter_key")],
                [InlineKeyboardButton(text="👥 Share & Earn Key",      url=link)],
            ]))
        return

    active = db.cx().execute("SELECT COUNT(*) as c FROM numbers WHERE status='Active'").fetchone()["c"]
    total  = db.cx().execute("SELECT COUNT(*) as c FROM numbers").fetchone()["c"]
    await msg.answer(_home_text(msg.from_user.first_name, active, total), reply_markup=menu_user())


# ══════════════ ACCESS KEY ════════════════════════

@router.callback_query(F.data == "enter_key")
async def cb_enter_key(call: CallbackQuery, state: FSMContext):
    await call.message.answer(
        "🔑 <b>Enter your Access Key:</b>\n"
        "<i>Format: RELAY-XXXXXXXXXXXXXXXXXXXXXXXXXXXX</i>")
    await state.set_state(S.enter_key); await call.answer()


@router.message(S.enter_key)
async def proc_key(msg: Message, state: FSMContext):
    key = msg.text.strip().upper(); uid = msg.from_user.id
    await state.clear()
    row = db.cx().execute("SELECT * FROM access_keys WHERE key=?", (key,)).fetchone()
    if not row:
        return await msg.answer("❌ <b>Invalid Key.</b>",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="🔑 Try Again", callback_data="enter_key")]]))
    if row["used_by"] and row["used_by"] != uid:
        return await msg.answer("❌ <b>Key already used by someone else.</b>")
    with db.cx() as c:
        c.execute("UPDATE users SET is_unlocked=1 WHERE user_id=?", (uid,))
        c.execute("UPDATE access_keys SET used_by=? WHERE key=?", (uid, key))
    active = db.cx().execute("SELECT COUNT(*) as c FROM numbers WHERE status='Active'").fetchone()["c"]
    total  = db.cx().execute("SELECT COUNT(*) as c FROM numbers").fetchone()["c"]
    await msg.answer(
        f"╔══════════════════════╗\n"
        f"  ✅ <b>ACCESS GRANTED!</b>\n"
        f"╚══════════════════════╝\n\n"
        f"Welcome aboard! 🎉\n"
        f"🟢 <b>{active}</b> Active / <b>{total}</b> Total Numbers",
        reply_markup=menu_user())


# ══════════════ USER CALLBACKS ════════════════════

@router.callback_query(F.data == "refresh_home")
async def cb_refresh(call: CallbackQuery):
    a = db.cx().execute("SELECT COUNT(*) as c FROM numbers WHERE status='Active'").fetchone()["c"]
    t = db.cx().execute("SELECT COUNT(*) as c FROM numbers").fetchone()["c"]
    await call.message.edit_text(
        _home_text(call.from_user.first_name, a, t),
        reply_markup=menu_user())
    await call.answer("✅ Refreshed!")


@router.callback_query(F.data == "my_link")
async def cb_mylink(call: CallbackQuery):
    me    = await bot.get_me()
    link  = f"https://t.me/{me.username}?start=ref_{call.from_user.id}"
    u     = db.cx().execute("SELECT refer_count FROM users WHERE user_id=?", (call.from_user.id,)).fetchone()
    limit = int(db.get("refer_limit") or 1)
    count = u["refer_count"] if u else 0
    need  = max(0, limit - count)
    await call.message.answer(
        f"╔══════════════════════╗\n"
        f"  👥 <b>YOUR REFERRAL</b>\n"
        f"╚══════════════════════╝\n\n"
        f"🔗 Your link:\n<code>{link}</code>\n\n"
        f"📊 Progress: <b>{count}/{limit}</b> invites\n"
        f"{'✅ Goal reached! Enter your key.' if need==0 else f'Need <b>{need}</b> more invite(s).'}")
    await call.answer()


@router.callback_query(F.data == "my_history")
async def cb_history(call: CallbackQuery):
    rows = db.cx().execute(
        "SELECT number,sender,otp,received_at FROM sms_log ORDER BY id DESC LIMIT 10").fetchall()
    if not rows: return await call.answer("No SMS received yet.", show_alert=True)
    t = "╔══════════════════════╗\n  📋 <b>RECENT SMS</b>\n╚══════════════════════╝\n\n"
    for r in rows:
        t += (f"📞 <code>{_disp(r['number'])}</code>\n"
              f"   🎯 OTP: <code>{r['otp'] or 'N/A'}</code>  |  📨 {r['sender']}\n"
              f"   🕐 {r['received_at']}\n\n")
    await call.message.answer(t); await call.answer()


# ══════════════ NUMBER LOOKUP ════════════════════

@router.callback_query(F.data == "num_lookup")
async def cb_lookup_start(call: CallbackQuery, state: FSMContext):
    if not await check_auth(call.from_user.id, call.message): return await call.answer()
    await call.message.answer(
        "🔍 <b>Number Lookup</b>\n\n"
        "Send a phone number or device ID to check its status:\n"
        "<i>Example: +919XXXXXXXXX or device-id</i>")
    await state.set_state(S.lookup_num); await call.answer()


@router.message(S.lookup_num)
async def proc_lookup(msg: Message, state: FSMContext):
    await state.clear()
    query = msg.text.strip()
    results = db.cx().execute(
        "SELECT * FROM numbers WHERE number LIKE ? OR device_id LIKE ? OR device_name LIKE ?",
        (f"%{query}%", f"%{query}%", f"%{query}%")).fetchall()
    if not results:
        return await msg.answer(
            f"🔍 <b>No results for:</b> <code>{query}</code>\n"
            f"<i>Try the full number or device ID.</i>")

    text = f"╔══════════════════════╗\n  🔍 <b>LOOKUP RESULTS</b>\n╚══════════════════════╝\n\n"
    btns = []
    for r in results[:10]:
        status_icon = "🟢" if r["status"] == "Active" else "🔴"
        lock_icon   = "🔒" if r["assigned_to"] else "🔓"
        carrier_str = f"  📶 {r['carrier']}" if r["carrier"] else ""
        text += (f"{status_icon} <code>{_disp(r['number'])}</code> {lock_icon}{carrier_str}\n"
                 f"   📱 {r['device_name'] or 'Unknown'}  |  💾 {r['struct_type'] or '?'}\n\n")
        btns.append([InlineKeyboardButton(
            text=f"{status_icon}{lock_icon} {_disp(r['number'])}",
            callback_data=f"lookup_status_{r['id']}")])

    btns.append([InlineKeyboardButton(text="🔙 Back", callback_data="back_home")])
    await msg.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=btns))


@router.callback_query(F.data.startswith("lookup_status_"))
async def cb_lookup_status(call: CallbackQuery):
    nid = int(call.data.split("_")[2])
    num = db.cx().execute("SELECT * FROM numbers WHERE id=?", (nid,)).fetchone()
    if not num: return await call.answer("Not found.", show_alert=True)
    sm = await call.message.answer("⏳ <i>Checking device status...</i>")
    online, battery, health_warn = await dev_health(
        num["device_id"], num["fb_source"], num["status_path"])
    status_icon = "🟢 Online" if online else "🔴 Offline"
    batt_str    = f"🔋 Battery: <b>{battery}%</b>\n" if battery is not None else ""
    lock_str    = f"🔒 Locked by user <code>{num['assigned_to']}</code>" if num["assigned_to"] \
                  else "🔓 Available"
    uid = call.from_user.id
    is_ghost = num.get("is_ghost", 0) or str(num["number"]).startswith("DEV-")
    assign_btn = []
    if not num["assigned_to"]:
        assign_btn = [InlineKeyboardButton(
            text="🔒 Assign to Me", callback_data=f"assign_num_{num['id']}")]
    elif num["assigned_to"] == uid:
        assign_btn = [InlineKeyboardButton(
            text="🔓 Release Number", callback_data=f"release_num_{num['id']}")]

    ghost_str = "👻 <i>Ghost device — no number resolved yet</i>\n" if is_ghost else ""
    reply_kb_rows = []
    if assign_btn: reply_kb_rows.append(assign_btn)
    if not is_ghost and not num["assigned_to"]:
        reply_kb_rows.append([InlineKeyboardButton(
            text="📡 Start Monitoring", callback_data=f"mon_{num['id']}")])
    reply_kb_rows.append([InlineKeyboardButton(text="🔙 Back", callback_data="back_home")])

    await sm.edit_text(
        f"╔══════════════════════╗\n"
        f"  📊 <b>DEVICE STATUS</b>\n"
        f"╚══════════════════════╝\n\n"
        f"📞 <code>{_disp(num['number'])}</code>\n"
        f"📱 {num['device_name'] or 'Unknown'}  |  💾 Struct {num['struct_type'] or '?'}\n"
        f"📶 {num['carrier'] or 'N/A'}  |  {status_icon}\n"
        f"{batt_str}"
        f"{ghost_str}"
        f"{lock_str}\n"
        f"{'⚠️ ' + health_warn if health_warn else ''}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=reply_kb_rows))
    await call.answer()


# ══════════════ ASSIGN / RELEASE NUMBER ═══════════

@router.callback_query(F.data.startswith("assign_num_"))
async def cb_assign_num(call: CallbackQuery):
    uid  = call.from_user.id
    if not await check_auth(uid, call.message): return await call.answer()
    nid  = int(call.data.split("_")[2])
    num  = db.cx().execute("SELECT * FROM numbers WHERE id=?", (nid,)).fetchone()
    if not num: return await call.answer("Number not found.", show_alert=True)
    if num["assigned_to"] and num["assigned_to"] != uid:
        return await call.answer("⛔ Already assigned to another user.", show_alert=True)
    with db.cx() as c:
        c.execute("UPDATE numbers SET assigned_to=? WHERE id=?", (uid, nid))
    await call.answer("✅ Number assigned!", show_alert=False)
    await call.message.answer(
        f"╔══════════════════════╗\n"
        f"  🔒 <b>NUMBER ASSIGNED</b>\n"
        f"╚══════════════════════╝\n\n"
        f"📞 <code>{_disp(num['number'])}</code>\n"
        f"📱 {num['device_name'] or 'Unknown'}\n\n"
        f"✅ Locked to your account.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📡 Start Monitoring", callback_data=f"mon_{nid}")],
            [InlineKeyboardButton(text="🔙 Home",             callback_data="back_home")]]))


@router.callback_query(F.data.startswith("release_num_"))
async def cb_release_num(call: CallbackQuery):
    uid = call.from_user.id
    nid = int(call.data.split("_")[2])
    num = db.cx().execute("SELECT * FROM numbers WHERE id=?", (nid,)).fetchone()
    if not num: return await call.answer("Number not found.", show_alert=True)
    if num["assigned_to"] != uid and uid not in ADMIN_IDS:
        return await call.answer("⛔ Not your number.", show_alert=True)
    with db.cx() as c:
        c.execute("UPDATE numbers SET assigned_to=NULL WHERE id=?", (nid,))
    await call.answer("✅ Number released.", show_alert=False)
    await call.message.answer(
        f"🔓 Released <code>{_disp(num['number'])}</code>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Home", callback_data="back_home")]]))


# ══════════════ GHOST DEVICES SECTION ════════════════

@router.callback_query(F.data.startswith("ghost_list_"))
async def cb_ghost_list(call: CallbackQuery):
    if not await check_auth(call.from_user.id, call.message): return await call.answer()
    await call.answer()
    page = int(call.data.split("_")[2]) if len(call.data.split("_")) > 2 else 0

    ghosts = db.cx().execute(
        "SELECT * FROM numbers WHERE is_ghost=1 OR number LIKE 'DEV-%' "
        "ORDER BY fb_source, device_name").fetchall()

    if not ghosts:
        return await call.message.edit_text(
            "╔══════════════════════╗\n"
            "  👻 <b>GHOST DEVICES</b>\n"
            "╚══════════════════════╝\n\n"
            "✅ No ghost devices! All numbers are resolved.\n\n"
            "<i>Ghost devices appear when Firebase has device entries "
            "but no phone number was found yet.\n"
            "Bot auto-probes every 10 min.</i>",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Back", callback_data="back_home")]]))

    total_pages = max(1, (len(ghosts) + PAGE_SIZE - 1) // PAGE_SIZE)
    page = max(0, min(page, total_pages - 1))
    page_ghosts = ghosts[page * PAGE_SIZE: (page + 1) * PAGE_SIZE]

    text = (f"╔══════════════════════╗\n"
            f"  👻 <b>GHOST DEVICES</b>  ({len(ghosts)} total)\n"
            f"╚══════════════════════╝\n\n"
            f"Devices awaiting number discovery.\n"
            f"Tap any ghost to check status or probe.\n"
            f"Page {page+1}/{total_pages} · Auto-probe every 10 min\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n")

    btns = []
    for g in page_ghosts:
        src_lbl   = (g["fb_source"] or "").split("//")[-1].split(".")[0][:10]
        name_str  = g["device_name"] or g["device_id"] or "Unknown"
        btns.append([InlineKeyboardButton(
            text=f"👻 {name_str[:18]}  [{src_lbl}]",
            callback_data=f"lookup_status_{g['id']}")])

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="⬅️ Prev", callback_data=f"ghost_list_{page-1}"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton(text="Next ➡️", callback_data=f"ghost_list_{page+1}"))
    if nav: btns.append(nav)

    action_row = []
    if call.from_user.id in ADMIN_IDS:
        action_row.append(InlineKeyboardButton(
            text="🔍 Rescan All", callback_data="adm_rescan"))
    action_row.append(InlineKeyboardButton(text="🔙 Back", callback_data="back_home"))
    btns.append(action_row)

    await call.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=btns))


# ══════════════ DATABASE SELECTOR ════════════════

@router.callback_query(F.data.startswith("num_"))
async def cb_pick_db(call: CallbackQuery):
    if call.data == "num_lookup": return
    if not await check_auth(call.from_user.id, call.message): return await call.answer()
    status = call.data.split("_")[1]
    srcs   = db.cx().execute("SELECT * FROM firebase_sources ORDER BY label").fetchall()
    btns = []; grand_total = 0
    for src in srcs:
        cnt = db.cx().execute(
            "SELECT COUNT(*) as c FROM numbers WHERE status=? AND fb_source=? AND is_ghost=0",
            (status, src["url"])).fetchone()["c"]
        if cnt == 0: continue
        grand_total += cnt
        stype = src["struct_type"] or "?"
        btns.append([InlineKeyboardButton(
            text=f"📡 {src['label']}  ({cnt})  [{stype}]",
            callback_data=f"srcn_{src['id']}_{status}_0")])
    orphan = db.cx().execute(
        "SELECT COUNT(*) as c FROM numbers WHERE status=? AND fb_source IS NULL", (status,)).fetchone()["c"]
    if orphan:
        grand_total += orphan
        btns.append([InlineKeyboardButton(
            text=f"📱 Manual ({orphan})", callback_data=f"srcn_0_{status}_0")])
    if not btns:
        return await call.answer(f"No {status} numbers found.", show_alert=True)
    btns.insert(0, [InlineKeyboardButton(
        text=f"🌐 All Databases  ({grand_total})", callback_data=f"srcn_all_{status}_0")])
    btns.append([InlineKeyboardButton(text="🔙 Back", callback_data="back_home")])
    await call.message.edit_text(
        f"╔══════════════════════╗\n"
        f"  📂 <b>SELECT DATABASE</b>\n"
        f"╚══════════════════════╝\n\n"
        f"Status: <b>{status}</b> — Choose a source:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=btns))


# ══════════════ PAGINATED NUMBER LIST ════════════

@router.callback_query(F.data.startswith("srcn_"))
async def cb_show_nums(call: CallbackQuery):
    if not await check_auth(call.from_user.id, call.message): return await call.answer()
    parts  = call.data.split("_")  # srcn_{src_id}_{status}_{page}
    src_id = parts[1]
    status = parts[2]
    page   = int(parts[3]) if len(parts) > 3 else 0

    if src_id == "all":
        nums  = db.cx().execute(
            "SELECT * FROM numbers WHERE status=? AND is_ghost=0", (status,)).fetchall()
        title = "All Databases"
    elif src_id == "0":
        nums  = db.cx().execute(
            "SELECT * FROM numbers WHERE status=? AND fb_source IS NULL AND is_ghost=0",
            (status,)).fetchall()
        title = "Manual Numbers"
    else:
        src   = db.cx().execute("SELECT * FROM firebase_sources WHERE id=?", (src_id,)).fetchone()
        nums  = db.cx().execute(
            "SELECT * FROM numbers WHERE status=? AND fb_source=? AND is_ghost=0",
            (status, src["url"])).fetchall()
        title = src["label"] if src else src_id

    if not nums:
        return await call.answer("No numbers here.", show_alert=True)

    uid        = call.from_user.id
    total_nums = len(nums)
    total_pages= max(1, (total_nums + PAGE_SIZE - 1) // PAGE_SIZE)
    page       = max(0, min(page, total_pages - 1))
    page_nums  = nums[page * PAGE_SIZE: (page + 1) * PAGE_SIZE]

    btns = []
    for n in page_nums:
        mine    = n["assigned_to"] == uid
        other   = n["assigned_to"] and not mine
        is_dev  = str(n["number"]).startswith("DEV-")
        icon    = "🟢" if mine else ("🔒" if other else ("🔍" if is_dev else "🔓"))
        if is_dev:
            short_id = n["device_id"][:8] if n["device_id"] else "??????"
            disp     = f"Unknown [{short_id}...]"
        else:
            disp = _disp(n["number"])
        label = f"{icon} {disp}"
        if n["device_name"]:
            label += f"  [{str(n['device_name'])[:12]}]"
        if mine: label += " ← YOU"
        btns.append([InlineKeyboardButton(text=label, callback_data=f"mon_{n['id']}")])

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="⬅️ Prev", callback_data=f"srcn_{src_id}_{status}_{page-1}"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton(text="Next ➡️", callback_data=f"srcn_{src_id}_{status}_{page+1}"))
    if nav: btns.append(nav)
    btns.append([InlineKeyboardButton(text="🔙 Back", callback_data=f"num_{status}")])

    await call.message.edit_text(
        f"╔══════════════════════╗\n"
        f"  📱 <b>{title}</b>\n"
        f"╚══════════════════════╝\n\n"
        f"<b>{total_nums}</b> {status} numbers  |  Page <b>{page+1}/{total_pages}</b>\n"
        f"Tap a number to lock & monitor:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=btns))


@router.callback_query(F.data == "back_home")
async def cb_back(call: CallbackQuery):
    a = db.cx().execute("SELECT COUNT(*) as c FROM numbers WHERE status='Active'").fetchone()["c"]
    t = db.cx().execute("SELECT COUNT(*) as c FROM numbers").fetchone()["c"]
    await call.message.edit_text(
        _home_text(call.from_user.first_name, a, t), reply_markup=menu_user())


# ══════════════ MONITORING ════════════════════════

@router.callback_query(F.data.startswith("mon_"))
async def cb_monitor(call: CallbackQuery):
    nid = int(call.data.split("_")[1]); uid = call.from_user.id
    num = db.cx().execute("SELECT * FROM numbers WHERE id=?", (nid,)).fetchone()
    if not num: return await call.answer("Number not found.", show_alert=True)
    if num["assigned_to"] and num["assigned_to"] != uid:
        return await call.answer("❌ Locked by another user.", show_alert=True)

    with db.cx() as c: c.execute("UPDATE numbers SET assigned_to=? WHERE id=?", (uid, nid))
    active_sessions[uid] = True

    # Health check before monitoring starts
    _, _, health_warn = await dev_health(num["device_id"], num["fb_source"], num["status_path"])

    nd      = _disp(num["number"])
    carrier = num["carrier"] or "N/A"
    api_key = _get_fb_apikey(num["fb_source"])

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Change Number", callback_data=f"chgnum_{nid}"),
         InlineKeyboardButton(text="🔓 Release",       callback_data=f"relnum_{nid}")],
        [InlineKeyboardButton(text="📥 Last 5 SMS",    callback_data=f"last5_{nid}")],
    ])

    # ── Auto-probe: if number is still DEV-xxx, try to find the real number ──
    is_dev = str(num["number"]).startswith("DEV-")
    if is_dev:
        probe_note = "\n\n🔍 <i>Looking up real number from Firebase...</i>"
    else:
        probe_note = ""

    def _make_base_text(display_num, display_carrier, extra=""):
        return (
            f"╔══════════════════════╗\n"
            f"  🟢 <b>MONITORING ACTIVE</b>\n"
            f"╚══════════════════════╝\n\n"
            f"📞 <code>{display_num}</code>\n"
            f"📱 <b>{num['device_name'] or 'Unknown'}</b>  |  📶 {display_carrier}\n\n"
            f"⚡ <i>SSE streaming active — instant SMS delivery</i>"
            + (f"\n\n{health_warn}" if health_warn else "")
            + extra
        )

    mmsg = await call.message.edit_text(
        _make_base_text(nd, carrier, probe_note), reply_markup=kb)

    # If DEV-*, fire auto-probe now and update message + DB when found
    if is_dev:
        real_ph, real_ca = await _auto_probe_number(
            num["device_id"], num["fb_source"], api_key)
        if real_ph:
            nd      = real_ph
            carrier = real_ca or carrier
            with db.cx() as cx:
                cx.execute(
                    "UPDATE numbers SET number=?, carrier=? WHERE id=?",
                    (real_ph, real_ca or num["carrier"], nid))
            try:
                await mmsg.edit_text(
                    _make_base_text(nd, carrier,
                        f"\n\n✅ <b>Fetched success!</b> <code>{real_ph}</code> saved."),
                    reply_markup=kb)
            except: pass
        else:
            try:
                await mmsg.edit_text(
                    _make_base_text(nd, carrier,
                        "\n\n❌ <b>No number</b> despite searching whole database."),
                    reply_markup=kb)
            except: pass

    # Start SSE listener
    sms_queue = asyncio.Queue()
    sse_task  = asyncio.create_task(
        _sse_listener(num["sms_path"], num["device_id"],
                      num["fb_source"] or "", api_key, sms_queue, uid))
    sse_tasks[uid] = sse_task

    was_offline  = False
    poll_counter = 0
    POLL_INTERVAL = 4.0   # polling fallback cadence (seconds)
    SSE_DRAIN_MS  = 1.5   # max wait for SSE before falling back to poll

    def _process_sms_entry(entry, eid):
        ck = f"{num['device_id']}:{num['fb_source']}"
        if last_sms_seen.get(ck) == eid: return None
        last_sms_seen[ck] = eid
        msg_text, sender, ts = _norm_sms(entry)
        if not msg_text: return None
        otp = re.search(r'\b(\d{4,8})\b', msg_text)
        return {"otp": otp.group(1) if otp else None,
                "sender": sender, "message": msg_text,
                "time": ts, "number": num["number"]}

    last_poll_time = 0.0

    while active_sessions.get(uid):
        try:
            # ── INSTANT SSE path: wait up to 1.5 s for a queued event ──
            # This replaces the old sleep(5) — SSE events now arrive in < 2 s.
            sse_delivered = False
            try:
                entry, eid = await asyncio.wait_for(sms_queue.get(), timeout=SSE_DRAIN_MS)
                sms = _process_sms_entry(entry, eid)
                if sms:
                    with db.cx() as cx:
                        cx.execute("INSERT INTO sms_log VALUES(NULL,?,?,?,?,?)",
                            (sms["number"], sms["sender"], sms.get("otp","N/A"),
                             sms["message"], sms["time"]))
                    await call.message.answer(fmt_otp(sms))
                    sse_delivered = True
                # Drain any additional queued items without waiting
                while True:
                    try:
                        entry2, eid2 = sms_queue.get_nowait()
                        sms2 = _process_sms_entry(entry2, eid2)
                        if sms2:
                            with db.cx() as cx:
                                cx.execute("INSERT INTO sms_log VALUES(NULL,?,?,?,?,?)",
                                    (sms2["number"], sms2["sender"], sms2.get("otp","N/A"),
                                     sms2["message"], sms2["time"]))
                            await call.message.answer(fmt_otp(sms2))
                            sse_delivered = True
                    except asyncio.QueueEmpty:
                        break
            except asyncio.TimeoutError:
                pass  # No SSE event — fall through to polling

            if uid not in active_sessions: break
            poll_counter += 1

            # ── Device online check every ~45 s (every 30 ticks × 1.5 s) ──
            if poll_counter % 30 == 0:
                online = await dev_online(num["device_id"], num["fb_source"], num["status_path"])
                if not online and not was_offline:
                    was_offline = True
                    try:
                        await call.message.answer(fmt_off(num["number"], num["device_name"] or "Unknown"))
                        await mmsg.edit_text(
                            f"⚠️ <b>Device Offline</b>\n"
                            f"📞 <code>{nd}</code>\n"
                            f"<i>Waiting to come back online...</i>", reply_markup=kb)
                    except: pass
                elif online and was_offline:
                    was_offline = False
                    try:
                        await call.message.answer(f"✅ <b>Device Back Online!</b> Resuming.\n📞 <code>{nd}</code>")
                        await mmsg.edit_text(_make_base_text(nd, carrier), reply_markup=kb)
                    except: pass

            if was_offline:
                await asyncio.sleep(3)
                continue

            # ── Polling fallback every POLL_INTERVAL seconds ─────────────
            # Only poll if SSE didn't deliver AND enough time has passed.
            now = asyncio.get_event_loop().time()
            if not sse_delivered and (now - last_poll_time) >= POLL_INTERVAL:
                last_poll_time = now
                sms = await fetch_sms(num["number"], num["device_id"],
                                      num["fb_source"], num["sms_path"])
                if sms:
                    with db.cx() as cx:
                        cx.execute("INSERT INTO sms_log VALUES(NULL,?,?,?,?,?)",
                            (sms["number"], sms["sender"], sms.get("otp","N/A"),
                             sms["message"], sms["time"]))
                    await call.message.answer(fmt_otp(sms))

        except asyncio.CancelledError:
            break
        except Exception as e:
            log.warning("Monitor loop error uid=%d: %s", uid, e)
            await asyncio.sleep(3)

    sse_task.cancel()
    if uid in sse_tasks: del sse_tasks[uid]


@router.callback_query(F.data.startswith("last5_"))
async def cb_last5(call: CallbackQuery):
    nid = int(call.data.split("_")[1])
    num = db.cx().execute("SELECT * FROM numbers WHERE id=?", (nid,)).fetchone()
    if not num: return await call.answer("Not found.", show_alert=True)
    sm = await call.message.answer("📥 <i>Fetching last 5 SMS...</i>")
    entries = await fetch_last_n_sms(num["number"], num["device_id"],
                                     num["fb_source"], num["sms_path"], n=5)
    if not entries:
        await sm.edit_text("📭 <b>No recent SMS found</b> for this number.")
        return await call.answer()
    text = "╔══════════════════════╗\n  📥 <b>LAST 5 SMS</b>\n╚══════════════════════╝\n\n"
    for i, e in enumerate(entries, 1):
        otp_str = f"  🎯 OTP: <code>{e['otp']}</code>" if e.get("otp") else ""
        text += (f"<b>#{i}</b> 📨 {e['sender']}{otp_str}\n"
                 f"     🕐 {e['time']}\n"
                 f"     💬 <i>{e['message'][:120]}</i>\n\n")
    await sm.edit_text(text); await call.answer()


@router.callback_query(F.data.startswith("chgnum_"))
async def cb_chgnum(call: CallbackQuery):
    old = int(call.data.split("_")[1]); uid = call.from_user.id
    if uid in active_sessions: del active_sessions[uid]
    if uid in sse_tasks: sse_tasks[uid].cancel(); del sse_tasks[uid]
    with db.cx() as c: c.execute("UPDATE numbers SET assigned_to=NULL WHERE id=?", (old,))
    nums = db.cx().execute(
        "SELECT * FROM numbers WHERE status='Active' AND (assigned_to IS NULL OR assigned_to=?)",
        (uid,)).fetchall()
    if not nums: return await call.answer("No other Active numbers right now.", show_alert=True)
    btns = [[InlineKeyboardButton(
        text=f"🔓 {_disp(n['number'])}  [{str(n['device_name'] or '')[:12]}]",
        callback_data=f"mon_{n['id']}")] for n in nums if n["id"] != old]
    btns.append([InlineKeyboardButton(text="🔙 Back", callback_data="back_home")])
    await call.message.edit_text("🔄 <b>Pick a new number:</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=btns))


@router.callback_query(F.data.startswith("relnum_"))
async def cb_relnum(call: CallbackQuery):
    nid = int(call.data.split("_")[1]); uid = call.from_user.id
    if uid in active_sessions: del active_sessions[uid]
    if uid in sse_tasks: sse_tasks[uid].cancel(); del sse_tasks[uid]
    with db.cx() as c: c.execute("UPDATE numbers SET assigned_to=NULL WHERE id=?", (nid,))
    await call.message.edit_text(
        "╔══════════════════════╗\n"
        "  🔓 <b>NUMBER RELEASED</b>\n"
        "╚══════════════════════╝\n\n"
        "<i>The number is now free for others.</i>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="🏠 Main Menu", callback_data="back_home")]]))


# ══════════════ ADMIN ════════════════════════════

@router.message(Command("admin"))
async def cmd_admin(msg: Message):
    if msg.from_user.id not in ADMIN_IDS: return
    total  = db.cx().execute("SELECT COUNT(*) as c FROM numbers").fetchone()["c"]
    active = db.cx().execute("SELECT COUNT(*) as c FROM numbers WHERE status='Active'").fetchone()["c"]
    srcs   = db.cx().execute("SELECT COUNT(*) as c FROM firebase_sources").fetchone()["c"]
    km     = "ON 🔐" if db.get("key_mode") == "1" else "OFF 🔓"
    await msg.answer(
        f"╔══════════════════════════╗\n"
        f"  🔧 <b>ADMIN CONTROL CENTER</b>\n"
        f"╚══════════════════════════╝\n\n"
        f"🔗 Firebase Sources: <b>{srcs}</b>\n"
        f"📱 Numbers: <b>{active}</b> Active / <b>{total}</b> Total\n"
        f"🔑 Key Mode: <b>{km}</b>  |  🔄 Auto-sync: 10 min\n"
        f"📡 SSE Streams: <b>{len(sse_tasks)}</b> active",
        reply_markup=menu_admin())


# ══════════════ SMART FIREBASE SETUP ═════════════

@router.callback_query(F.data == "adm_fb")
async def cb_adm_fb(call: CallbackQuery, state: FSMContext):
    if call.from_user.id not in ADMIN_IDS: return
    await call.message.answer(
        "🔗 <b>Add Firebase URLs</b>\n\n"
        "Paste one or more URLs (one per line).\n"
        "Optional label after pipe:\n"
        "<code>https://project.firebaseio.com/.json | Office DB</code>\n\n"
        "🤖 <i>Auto-detection:\n"
        "• Public DB → saved immediately\n"
        "• Private DB → I'll ask for the API key</i>")
    await state.set_state(S.bulk_fb); await call.answer()


@router.message(S.bulk_fb)
async def cb_bulk_fb(msg: Message, state: FSMContext):
    if msg.from_user.id not in ADMIN_IDS: return
    raw = [l.strip() for l in msg.text.strip().splitlines() if l.strip().startswith("http")]
    if not raw:
        await state.clear()
        return await msg.answer("❌ No valid URLs (must start with https://)")
    def _auto_label(url):
        """Generate a friendly random label like relay-k7x2p."""
        slug = url.replace(".json","").rstrip("/").split("//")[-1].split(".")[0]
        rnd  = ''.join(secrets.choice(string.ascii_lowercase+string.digits) for _ in range(5))
        return f"{slug[:12]}-{rnd}"

    pairs = [(l.split("|", 1)[0].strip(),
              l.split("|", 1)[1].strip() if "|" in l else _auto_label(l.split("|",1)[0].strip()))
             for l in raw]

    if len(pairs) == 1:
        url, name = pairs[0]
        sm = await msg.answer(f"🔍 <i>Testing connection...</i>")
        async with aiohttp.ClientSession() as sess:
            code, err = await fb_ping(sess, url)
        if code in (401, 403):
            clean = url.replace(".json", "").rstrip("/")
            await state.update_data(pending_fb_url=clean, pending_fb_name=name)
            await state.set_state(S.fb_apikey)
            return await sm.edit_text(
                f"🔒 <b>Access Denied</b>\n\n"
                f"Database: <code>{clean}</code>\n\n"
                f"📝 Send the <b>API Key / Auth Token</b> for this database:\n"
                f"<i>(Or send /cancel to abort)</i>")
        elif code == 0:
            await state.clear()
            return await sm.edit_text(f"❌ <b>Cannot connect</b>\n<i>{err or 'Timeout'}</i>")
        elif code != 200:
            await state.clear()
            return await sm.edit_text(f"❌ HTTP {code} error. Database not saved.")

        await state.clear()
        await sm.edit_text(f"✅ <i>Public DB detected! Syncing numbers...</i>")
        n, sync_err, stype = await sync_one(url, name)
        if sync_err:
            return await sm.edit_text(f"⚠️ Connected but sync error:\n{sync_err}")
        return await sm.edit_text(
            f"╔══════════════════════╗\n"
            f"  ✅ <b>FIREBASE ADDED!</b>\n"
            f"╚══════════════════════╝\n\n"
            f"🔢 Numbers synced: <b>{n}</b>\n"
            f"🏗 Structure: <b>{stype}</b>\n"
            f"<i>Auto-sync every 10 min.</i>")

    # Bulk mode
    await state.clear()
    sm = await msg.answer(f"🔄 <i>Processing {len(pairs)} URL(s)...</i>")
    results = []; tot = 0
    for url, name in pairs:
        async with aiohttp.ClientSession() as sess:
            code, _ = await fb_ping(sess, url)
        base  = url.replace(".json", "").rstrip("/")
        lbl   = name or base.split("//")[-1].split(".")[0]
        if code in (401, 403):
            results.append(f"🔒 <b>{lbl}</b> — Access Denied (submit one at a time to add API key)")
            continue
        if code not in (200,):
            results.append(f"❌ <b>{lbl}</b> — HTTP {code}")
            continue
        n, err, stype = await sync_one(url, name)
        if err: results.append(f"⚠️ <b>{lbl}</b> — {err}")
        else:   results.append(f"✅ <b>{lbl}</b> — {n} numbers [{stype}]"); tot += n
    a = db.cx().execute("SELECT COUNT(*) as c FROM numbers WHERE status='Active'").fetchone()["c"]
    t = db.cx().execute("SELECT COUNT(*) as c FROM numbers").fetchone()["c"]
    await sm.edit_text(
        f"🔗 <b>Bulk Firebase Sync Done!</b>\n━━━━━━━━━━━━━━━━━━\n"
        + "\n".join(results) +
        f"\n\n🔄 Total Synced: <b>{tot}</b>\n🟢 Active: <b>{a}</b>  |  📱 DB Total: <b>{t}</b>")


@router.message(S.fb_apikey)
async def proc_fb_apikey(msg: Message, state: FSMContext):
    if msg.from_user.id not in ADMIN_IDS: return
    if msg.text.strip() == "/cancel":
        await state.clear()
        return await msg.answer("❌ Cancelled.")
    data    = await state.get_data()
    url     = data.get("pending_fb_url")
    name    = data.get("pending_fb_name")
    api_key = msg.text.strip()
    await state.clear()

    sm = await msg.answer("🔍 <i>Testing with API key...</i>")
    async with aiohttp.ClientSession() as sess:
        code, _ = await fb_ping(sess, url, api_key=api_key)
    if code in (401, 403):
        return await sm.edit_text(
            "🔒 <b>Database requires Admin permission / Invalid Key</b>\n"
            "<i>Could not access database even with the provided key. Not saved.</i>")
    if code != 200:
        return await sm.edit_text(f"❌ Connection failed (HTTP {code}). Database not saved.")

    await sm.edit_text("✅ <i>Key accepted! Syncing numbers...</i>")
    n, err, stype = await sync_one(url, name, api_key=api_key)
    if err:
        return await sm.edit_text(f"⚠️ Connected but sync error:\n{err}")
    await sm.edit_text(
        f"╔══════════════════════╗\n"
        f"  ✅ <b>FIREBASE SAVED!</b>\n"
        f"╚══════════════════════╝\n\n"
        f"🔢 Numbers synced: <b>{n}</b>\n"
        f"🏗 Structure: <b>{stype}</b>\n"
        f"🔑 Secured with API key ✓")


# ══════════════ FIREBASE SOURCES LIST + DELETE ════

@router.callback_query(F.data == "adm_fb_list")
async def cb_fb_list(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return
    rows = db.cx().execute("SELECT * FROM firebase_sources ORDER BY id DESC").fetchall()
    if not rows: return await call.answer("No sources added yet.", show_alert=True)
    text = "╔══════════════════════╗\n  🔗 <b>FIREBASE SOURCES</b>\n╚══════════════════════╝\n\n"
    btns = []
    for r in rows:
        key_icon = "🔑" if r["api_key"] else "🔓"
        text += (f"📡 <b>{r['label']}</b> {key_icon} [{r['struct_type'] or '?'}]\n"
                 f"   📱 {r['num_count']} numbers  |  🕐 {r['last_synced']}\n\n")
        btns.append([
            InlineKeyboardButton(text=f"🔄 {r['label']}", callback_data=f"resync_src_{r['id']}"),
            InlineKeyboardButton(text="🗑 Delete",          callback_data=f"del_src_{r['id']}")
        ])
    await call.message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=btns))
    await call.answer()


@router.callback_query(F.data.startswith("del_src_"))
async def cb_del_src(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return
    sid = int(call.data.split("_")[2])
    src = db.cx().execute("SELECT * FROM firebase_sources WHERE id=?", (sid,)).fetchone()
    if not src: return await call.answer("Source not found.", show_alert=True)
    with db.cx() as c:
        c.execute("DELETE FROM numbers WHERE fb_source=?", (src["url"],))
        c.execute("DELETE FROM firebase_sources WHERE id=?", (sid,))
    await call.answer(f"🗑 Deleted '{src['label']}' and its numbers.", show_alert=True)
    await call.message.delete()


@router.callback_query(F.data.startswith("resync_src_"))
async def cb_resync_src(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return
    sid = int(call.data.split("_")[2])
    src = db.cx().execute("SELECT * FROM firebase_sources WHERE id=?", (sid,)).fetchone()
    if not src: return await call.answer("Not found.", show_alert=True)
    sm = await call.message.answer(f"🔄 <i>Re-syncing {src['label']}...</i>")
    n, err, stype = await sync_one(src["url"])
    if err: await sm.edit_text(f"❌ {src['label']}: {err}")
    else: await sm.edit_text(f"✅ <b>{src['label']}</b> — {n} numbers [{stype}]")
    await call.answer()


# ══════════════ SYNC ALL ═════════════════════════

@router.callback_query(F.data == "adm_sync")
async def cb_sync(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return
    srcs = db.cx().execute("SELECT url FROM firebase_sources").fetchall()
    if not srcs: return await call.answer("No sources saved.", show_alert=True)
    sm = await call.message.answer(f"🔄 <i>Syncing {len(srcs)} source(s)...</i>")
    tot = 0
    for s in srcs:
        n, err, _ = await sync_one(s["url"])
        tot += n
    a = db.cx().execute("SELECT COUNT(*) as c FROM numbers WHERE status='Active'").fetchone()["c"]
    t = db.cx().execute("SELECT COUNT(*) as c FROM numbers").fetchone()["c"]
    await sm.edit_text(f"✅ <b>Synced!</b>\n🔄 {tot} numbers  |  🟢 Active: {a}  |  📱 Total: {t}")
    await call.answer()


# ══════════════ PURGE OLD SMS ════════════════════

@router.callback_query(F.data == "adm_purge")
async def cb_adm_purge(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return
    await call.message.answer(
        "🧹 <b>Purge Old SMS</b>\n\n"
        "This will delete Firebase SMS records older than 48 hours from ALL sources.\n"
        "This keeps your databases small and fast.\n\n"
        "⚠️ <i>This action cannot be undone.</i>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Yes, Purge",   callback_data="adm_purge_confirm"),
             InlineKeyboardButton(text="❌ Cancel",        callback_data="adm_purge_cancel")]
        ]))
    await call.answer()


@router.callback_query(F.data == "adm_purge_cancel")
async def cb_purge_cancel(call: CallbackQuery):
    await call.message.edit_text("❌ Purge cancelled."); await call.answer()


@router.callback_query(F.data == "adm_purge_confirm")
async def cb_purge_confirm(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return
    sm = await call.message.edit_text("🧹 <i>Purging old SMS from Firebase...</i>")
    srcs = db.cx().execute("SELECT url, api_key FROM firebase_sources").fetchall()
    cutoff_ms = int((datetime.now() - timedelta(hours=48)).timestamp() * 1000)
    total_deleted = 0

    async with aiohttp.ClientSession() as sess:
        for src in srcs:
            base    = src["url"]; api_key = src["api_key"]
            sh_data = await fb_get(sess, f"{base}/.json?shallow=true", api_key=api_key)
            if not isinstance(sh_data, dict): continue
            rk = list(sh_data.keys())[0] if sh_data else None

            sms_paths = [f"{base}/sms", f"{base}/user_sms", f"{base}/messages"]
            if rk: sms_paths.insert(0, f"{base}/{rk}/All_User/Sms")

            for sms_root in sms_paths:
                devs = await fb_get(sess, f"{sms_root}.json?shallow=true", api_key=api_key)
                if not isinstance(devs, dict): continue
                for dev_id in list(devs.keys())[:50]:
                    node = await fb_get(sess, f"{sms_root}/{dev_id}.json", api_key=api_key)
                    if not isinstance(node, dict): continue
                    for k, v in node.items():
                        if not isinstance(v, dict): continue
                        ts = None
                        for tf in ("timestamp", "backupTime", "date"):
                            raw = v.get(tf)
                            if raw and str(raw).isdigit():
                                ts = int(str(raw))
                                if len(str(raw)) > 10: ts = ts // 1000
                                break
                        if ts and ts < (cutoff_ms // 1000):
                            deleted = await fb_delete(
                                sess, f"{sms_root}/{dev_id}/{k}.json", api_key=api_key)
                            if deleted: total_deleted += 1

    # Also purge local sms_log
    with db.cx() as c:
        c.execute("DELETE FROM sms_log WHERE received_at < ?",
                  ((datetime.now() - timedelta(hours=48)).strftime("%d-%m-%Y %H:%M:%S"),))

    await sm.edit_text(
        f"╔══════════════════════╗\n"
        f"  🧹 <b>PURGE COMPLETE</b>\n"
        f"╚══════════════════════╝\n\n"
        f"🗑 Firebase records deleted: <b>{total_deleted}</b>\n"
        f"<i>All SMS older than 48 hours removed.</i>")
    await call.answer()


# ══════════════ RESCAN UNKNOWN NUMBERS ════════════

async def _do_rescan(answer_fn):
    """Core rescan logic — shared by callback and /rescan command."""
    devs = db.cx().execute(
        "SELECT * FROM numbers WHERE is_ghost=1 OR number LIKE 'DEV-%'").fetchall()
    if not devs:
        return await answer_fn(
            "╔══════════════════════╗\n"
            "  ✅ <b>ALL NUMBERS RESOLVED</b>\n"
            "╚══════════════════════╝\n\n"
            "No ghost devices found. All numbers are real!")

    sm = await answer_fn(
        f"╔══════════════════════╗\n"
        f"  🔍 <b>RESCAN STARTED</b>\n"
        f"╚══════════════════════╝\n\n"
        f"👻 Found <b>{len(devs)}</b> ghost device(s)\n"
        f"⚡ Probing Firebase for real numbers...\n"
        f"<i>Progress updates every 5 devices.</i>")

    found_list = []; fail_list = []
    async with aiohttp.ClientSession() as probe_sess:
        for idx, n in enumerate(devs, 1):
            # Live progress every 5 devices
            if idx % 5 == 0:
                try:
                    await sm.edit_text(
                        f"╔══════════════════════╗\n"
                        f"  🔍 <b>RESCANNING...</b>  {idx}/{len(devs)}\n"
                        f"╚══════════════════════╝\n\n"
                        f"✅ Found: <b>{len(found_list)}</b>  "
                        f"❌ Missing: <b>{len(fail_list)}</b>\n"
                        f"<i>Working on device {idx}...</i>")
                except: pass
            api_key = _get_fb_apikey(n["fb_source"])
            ph, ca  = await _auto_probe_number(n["device_id"], n["fb_source"], api_key)
            if ph and ph != n["number"]:
                with db.cx() as cx:
                    cx.execute("UPDATE numbers SET number=?, carrier=?, is_ghost=0 WHERE id=?",
                               (ph, ca or n["carrier"], n["id"]))
                short = n["device_id"][:10] if n["device_id"] else "?"
                found_list.append(f"✅ <code>{short}...</code> → <code>{ph}</code>"
                                  + (f" [{ca}]" if ca else ""))
            else:
                short = n["device_id"][:10] if n["device_id"] else "?"
                fail_list.append(f"❌ <code>{short}...</code> — number still unknown")

    lines = found_list + fail_list
    if len(lines) > 30:
        lines = lines[:30] + [f"<i>... and {len(lines)-30} more</i>"]

    summary = (
        f"╔══════════════════════╗\n"
        f"  🔍 <b>RESCAN COMPLETE</b>\n"
        f"╚══════════════════════╝\n\n"
        f"✅ Resolved: <b>{len(found_list)}</b> / {len(devs)}\n"
        f"❌ Still ghost: <b>{len(fail_list)}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        + "\n".join(lines))
    await sm.edit_text(summary)


@router.callback_query(F.data == "adm_rescan")
async def cb_adm_rescan(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return await call.answer()
    await call.answer()
    await _do_rescan(call.message.answer)


@router.message(Command("rescan"))
async def cmd_rescan(msg: Message):
    if msg.from_user.id not in ADMIN_IDS:
        return await msg.answer("⛔ Admin only.")
    await _do_rescan(msg.answer)


# ══════════════ /debug COMMAND ════════════════════

@router.message(Command("debug"))
async def cmd_debug(msg: Message):
    if msg.from_user.id not in ADMIN_IDS:
        return await msg.answer("⛔ Admin only.")
    parts   = msg.text.strip().split(None, 1)
    if len(parts) < 2:
        return await msg.answer(
            "🛠 <b>Usage:</b> <code>/debug &lt;device_id&gt;</code>\n\n"
            "<i>Fetches raw SMS JSON from all known paths for that device.</i>")
    dev_id  = parts[1].strip()
    sm      = await msg.answer(f"🔍 <i>Fetching raw SMS for device: <code>{dev_id}</code>...</i>")
    srcs    = db.cx().execute("SELECT url, api_key FROM firebase_sources").fetchall()
    num_row = db.cx().execute("SELECT * FROM numbers WHERE device_id=?", (dev_id,)).fetchone()

    all_results = []
    async with aiohttp.ClientSession() as sess:
        for src in srcs:
            base    = src["url"]; api_key = src["api_key"]
            sh      = await fb_get(sess, f"{base}/.json?shallow=true", api_key=api_key)
            rk      = list(sh.keys())[0] if isinstance(sh, dict) and sh else None
            paths   = [
                f"{base}/sms/{dev_id}",
                f"{base}/user_sms/{dev_id}",
                f"{base}/messages/{dev_id}",
                f"{base}/{dev_id}/sms",
                f"{base}/All_Users/sms/{dev_id}",
            ]
            if rk: paths.insert(0, f"{base}/{rk}/All_User/Sms/{dev_id}")
            if num_row and num_row["sms_path"]: paths.insert(0, num_row["sms_path"])
            for path in paths:
                node = await fb_get(sess, f"{path}.json", api_key=api_key)
                if not isinstance(node, dict) or not node: continue
                entries = _top_n(node, 10)
                for entry, key in entries:
                    msg_text, sender, ts = _norm_sms(entry)
                    if msg_text:
                        all_results.append((path, key, sender, msg_text, ts))
                if all_results: break
            if all_results: break

    if not all_results:
        return await sm.edit_text(
            f"📭 <b>No SMS found</b> for device:\n<code>{dev_id}</code>\n\n"
            f"<i>Tried {len(srcs)} source(s). Check the device_id or sms_path.</i>")

    text = (f"╔══════════════════════╗\n"
            f"  🛠 <b>DEBUG: {dev_id[:16]}</b>\n"
            f"╚══════════════════════╝\n\n"
            f"📂 Path: <code>{all_results[0][0]}</code>\n"
            f"📨 Last {len(all_results)} entries:\n\n")
    for i, (path, key, sender, body, ts) in enumerate(all_results, 1):
        otp = re.search(r'\b(\d{4,8})\b', body)
        otp_str = f"  🎯 <code>{otp.group(1)}</code>" if otp else ""
        text += (f"<b>#{i}</b> 🕐 {ts}{otp_str}\n"
                 f"  📨 {sender}\n"
                 f"  💬 <i>{body[:100]}</i>\n\n")
    await sm.edit_text(text[:4000])


# ══════════════ ADMIN: other handlers ════════════

@router.callback_query(F.data == "adm_keymode")
async def cb_keymode(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return
    nv = "0" if db.get("key_mode") == "1" else "1"
    db.set("key_mode", nv)
    await call.answer(f"Key Mode: {'ON 🔐' if nv=='1' else 'OFF (open) 🔓'}", show_alert=True)


@router.callback_query(F.data == "adm_genkeys")
async def cb_genkeys(call: CallbackQuery, state: FSMContext):
    if call.from_user.id not in ADMIN_IDS: return
    await call.message.answer("🔑 How many keys? (1–50)")
    await state.set_state(S.gen_keys); await call.answer()


@router.message(S.gen_keys)
async def proc_genkeys(msg: Message, state: FSMContext):
    if msg.from_user.id not in ADMIN_IDS: return
    await state.clear()
    if not msg.text.isdigit() or not (1 <= int(msg.text) <= 50):
        return await msg.answer("❌ Enter 1-50.")
    count = int(msg.text); now = datetime.now().strftime("%d-%m-%Y %H:%M"); keys = []
    with db.cx() as c:
        for _ in range(count):
            k = gen_key()
            c.execute("INSERT OR IGNORE INTO access_keys VALUES(?,?,?,NULL)", (k, msg.from_user.id, now))
            keys.append(k)
    await msg.answer(
        f"🔑 <b>{count} Key(s) Generated</b>\n━━━━━━━━━━━━━━━━━━\n"
        + "\n".join(f"<code>{k}</code>" for k in keys))


@router.callback_query(F.data == "adm_report")
async def cb_report(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return
    locked = db.cx().execute("SELECT * FROM numbers WHERE assigned_to IS NOT NULL").fetchall()
    total  = db.cx().execute("SELECT COUNT(*) as c FROM numbers").fetchone()["c"]
    active = db.cx().execute("SELECT COUNT(*) as c FROM numbers WHERE status='Active'").fetchone()["c"]
    ki     = db.cx().execute("SELECT COUNT(*) as c FROM access_keys").fetchone()["c"]
    ku     = db.cx().execute("SELECT COUNT(*) as c FROM access_keys WHERE used_by IS NOT NULL").fetchone()["c"]
    t = (f"╔══════════════════════╗\n"
         f"  📊 <b>LIVE STATUS</b>\n"
         f"╚══════════════════════╝\n\n"
         f"📱 Total: <b>{total}</b>  |  🟢 Active: <b>{active}</b>  |  🔒 In Use: <b>{len(locked)}</b>\n"
         f"🔑 Keys: <b>{ki}</b> issued / <b>{ku}</b> used\n"
         f"📡 SSE Streams: <b>{len(sse_tasks)}</b> active\n\n")
    t += ("\n".join(
        f"🔒 <code>{_disp(r['number'])}</code> → UID <code>{r['assigned_to']}</code>" for r in locked)
          if locked else "<i>All numbers free.</i>")
    await call.message.answer(t); await call.answer()


@router.callback_query(F.data == "adm_limit")
async def cb_limit(call: CallbackQuery, state: FSMContext):
    if call.from_user.id not in ADMIN_IDS: return
    await call.message.answer("📈 Referrals needed to earn a key (0 = disabled):")
    await state.set_state(S.set_limit)


@router.message(S.set_limit)
async def proc_limit(msg: Message, state: FSMContext):
    if msg.from_user.id not in ADMIN_IDS or not msg.text.isdigit(): return
    db.set("refer_limit", msg.text.strip())
    await msg.answer(f"✅ Refer limit set to <b>{msg.text.strip()}</b>.")
    await state.clear()


@router.callback_query(F.data == "adm_addnum")
async def cb_addnum(call: CallbackQuery, state: FSMContext):
    if call.from_user.id not in ADMIN_IDS: return
    await call.message.answer("📱 Send number (e.g. +919XXXXXXXXX):")
    await state.set_state(S.add_num)


@router.message(S.add_num)
async def proc_addnum(msg: Message, state: FSMContext):
    if msg.from_user.id not in ADMIN_IDS: return
    with db.cx() as c: c.execute("INSERT OR IGNORE INTO numbers(number) VALUES(?)", (msg.text.strip(),))
    t = db.cx().execute("SELECT COUNT(*) as c FROM numbers").fetchone()["c"]
    await msg.answer(f"✅ <code>{msg.text.strip()}</code> added. Total: <b>{t}</b>")
    await state.clear()


@router.callback_query(F.data == "adm_addch")
async def cb_addch(call: CallbackQuery, state: FSMContext):
    if call.from_user.id not in ADMIN_IDS: return
    await call.message.answer(
        "📢 Send Channel ID (e.g. -1001234567890):\n\n"
        "<b>Important:</b> Make this bot an admin in the channel first!")
    await state.set_state(S.add_ch_id)


@router.message(S.add_ch_id)
async def proc_ch_id(msg: Message, state: FSMContext):
    if msg.from_user.id not in ADMIN_IDS: return
    await state.update_data(ch_id=msg.text.strip())
    await msg.answer("🔗 Now send the Channel invite link:")
    await state.set_state(S.add_ch_link)


@router.message(S.add_ch_link)
async def proc_ch_link(msg: Message, state: FSMContext):
    if msg.from_user.id not in ADMIN_IDS: return
    data = await state.get_data()
    with db.cx() as c:
        c.execute("INSERT OR REPLACE INTO channels VALUES(?,?)", (data["ch_id"], msg.text.strip()))
    await msg.answer("✅ Channel added!\n<i>Bot must be admin in the channel to verify membership.</i>")
    await state.clear()


@router.callback_query(F.data == "adm_broadcast")
async def cb_bcast_start(call: CallbackQuery, state: FSMContext):
    if call.from_user.id not in ADMIN_IDS: return
    await call.message.answer("📢 Enter broadcast message:")
    await state.set_state(S.b_msg)


@router.message(S.b_msg)
async def proc_bcast(msg: Message, state: FSMContext):
    if msg.from_user.id not in ADMIN_IDS: return
    text = msg.text.strip()
    uids = [r["user_id"] for r in
            db.cx().execute("SELECT user_id FROM users WHERE is_banned=0").fetchall()]
    sm   = await msg.answer(f"⚡ <i>Broadcasting to {len(uids)} users...</i>")
    await state.clear(); s = f = 0
    for i in range(0, len(uids), 25):
        res = await asyncio.gather(*[bot.send_message(u, text) for u in uids[i:i + 25]],
                                   return_exceptions=True)
        s += sum(1 for r in res if not isinstance(r, Exception))
        f += sum(1 for r in res if isinstance(r, Exception))
        await asyncio.sleep(1)
    await sm.edit_text(f"📢 <b>Broadcast Done</b>\n✅ Sent: {s} | ❌ Failed: {f}")


# ══════════════ CORE ══════════════════════════════

dp.include_router(router)


async def main():
    asyncio.create_task(auto_resync())
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
