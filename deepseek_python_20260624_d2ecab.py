i# ═══════════════════════════════════════════════════════════════════
#   SHOPSY GAME SESSION BOT
#   Supports: JSON import + OTP login
# ═══════════════════════════════════════════════════════════════════

BOT_TOKEN   = "7786266503:AAEbknozlbGOxUtFy_yNAGL1GZRNLd27Kqw"
ADMIN_IDS   = [6013007573]  # Your Telegram user ID(s)

import os, json, asyncio, logging, uuid, time, re
from typing import List, Dict, Optional
import aiohttp
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

import subprocess, sys
def install(pkg):
    subprocess.check_call([sys.executable, "-m", "pip", "install", pkg, "-q"])
try:
    from aiogram import Bot, Dispatcher, F, Router
except ImportError:
    install("aiogram==3.7.0")
    from aiogram import Bot, Dispatcher, F, Router

from aiogram.client.default import DefaultBotProperties
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, ReplyKeyboardRemove, KeyboardButton
)
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage

# ── Configuration & Storage ──────────────────────────────────────
CONFIG_FILE = "shopsy_accounts.json"

def load_accounts():
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "r") as f:
            return json.load(f)
    return {}

def save_accounts(accounts):
    with open(CONFIG_FILE, "w") as f:
        json.dump(accounts, f, indent=4)

# ── API Interaction Class (unchanged) ────────────────────────────
class ShopsyBot:
    def __init__(self, account_data: dict):
        self.base_url = "https://1.rome.api.flipkart.net/1/shopsy"
        self.games_url = f"{self.base_url}/games"
        self.headers = account_data.get("headers", {})
        self.user_id = account_data.get("user_id")
        self.user_name = account_data.get("user_name")
        self.raw_json = account_data.get("raw_json", {})

    async def start_game(self, session: aiohttp.ClientSession, game_id: str) -> str:
        payload = '{"requestMethod":"POST","routeUri":"game/game-started","payload":{"userId":"' + self.user_id + '","gameId":"' + game_id + '"}}'
        try:
            async with session.post(self.games_url, headers=self.headers, data=payload) as resp:
                data = await resp.json()
                if data.get("success"):
                    return data["data"]["sessionId"]
        except Exception as e:
            logger.error(f"Error starting game: {e}")
        return None

    async def end_game(self, session: aiohttp.ClientSession, game_id: str, game_session_id: str):
        payload = {
            "requestMethod": "POST",
            "routeUri": "game/game-ended",
            "payload": {
                "userId": self.user_id,
                "gameId": game_id,
                "sessionId": game_session_id,
                "gemsEarned": 100,
                "playTimeInSec": 300
            }
        }
        try:
            async with session.post(self.games_url, headers=self.headers, json=payload) as resp:
                data = await resp.json()
                return "✅ Success" if data.get("success") else "❌ Failed"
        except Exception as e:
            logger.error(f"Error ending game session {game_session_id}: {e}")
            return "❌ Error"

    # ── OTP Login Methods ──────────────────────────────────────────
    @staticmethod
    async def request_otp(session: aiohttp.ClientSession, phone: str) -> dict:
        """Request OTP from Shopsy/Flipkart."""
        url = "https://1.rome.api.flipkart.net/1/shopsy/user/request-otp"
        payload = {"phone": phone}
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 (Linux; Android 16; SM-S921B) AppleWebKit/537.36"
        }
        try:
            async with session.post(url, headers=headers, json=payload) as resp:
                data = await resp.json()
                return {"success": data.get("success", False), "data": data}
        except Exception as e:
            return {"success": False, "error": str(e)}

    @staticmethod
    async def verify_otp(session: aiohttp.ClientSession, phone: str, otp: str) -> dict:
        """Verify OTP and get full session data."""
        url = "https://1.rome.api.flipkart.net/1/shopsy/user/verify-otp"
        payload = {"phone": phone, "otp": otp}
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 (Linux; Android 16; SM-S921B) AppleWebKit/537.36"
        }
        try:
            async with session.post(url, headers=headers, json=payload) as resp:
                data = await resp.json()
                if data.get("success"):
                    # Extract cookies from response headers
                    cookies = resp.cookies
                    cookie_dict = {}
                    for key, value in cookies.items():
                        cookie_dict[key] = value.value
                    # Build the full account object from the response
                    # The response likely contains session data similar to your JSON
                    session_data = data.get("data", {}).get("session", {})
                    account_obj = {
                        "phone": phone,
                        "device_id": session_data.get("device_id", str(uuid.uuid4())),
                        "cookies": cookie_dict,
                        "session": session_data,
                        "headers": {
                            "Cookie": "; ".join([f"{k}={v}" for k, v in cookie_dict.items()]),
                            "Content-Type": "application/json",
                            "Accept": "application/json",
                            "User-Agent": session_data.get("device_ua", "Mozilla/5.0")
                        }
                    }
                    return {"success": True, "account": account_obj}
                return {"success": False, "message": data.get("message", "OTP verification failed")}
        except Exception as e:
            return {"success": False, "error": str(e)}

# ── Helper: Parse JSON Session ────────────────────────────────────
def parse_account_from_json(json_text: str):
    try:
        data = json.loads(json_text)
        phone = data.get("phone") or data.get("user_id")
        if not phone:
            return None, "Missing 'phone' or 'user_id' field"
        # Build name
        first = data.get("session", {}).get("firstName", "")
        last = data.get("session", {}).get("lastName", "")
        user_name = f"{first} {last}".strip() or phone
        # Build headers
        cookies = data.get("cookies", {})
        if not cookies:
            return None, "Missing 'cookies' object"
        cookie_str = "; ".join([f"{k}={v}" for k, v in cookies.items()])
        headers = {
            "Cookie": cookie_str,
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": data.get("session", {}).get("device_ua", "Mozilla/5.0"),
        }
        return {
            "user_id": phone,
            "user_name": user_name,
            "headers": headers,
            "raw_json": data
        }, None
    except json.JSONDecodeError as e:
        return None, f"Invalid JSON: {e}"

# ── Helper: Parse Raw HTTP Request (original method) ─────────────
def parse_raw_request(raw_text: str):
    lines = raw_text.strip().split('\n')
    headers = {}
    body = ""
    is_body = False
    EXCLUDED_HEADERS = {
        "content-length", "accept-encoding", "transfer-encoding", "connection"
    }
    for line in lines:
        if not line.strip() and not is_body:
            is_body = True
            continue
        if is_body:
            body += line
        elif ":" in line:
            key, value = line.split(":", 1)
            if key.strip().lower() not in EXCLUDED_HEADERS:
                headers[key.strip()] = value.strip()
    try:
        json_body = json.loads(body)
        user_id = json_body.get("payload", {}).get("userId")
        user_name = json_body.get("payload", {}).get("userName")
        return headers, user_id, user_name
    except:
        return headers, None, None

# ── Telegram Bot Setup ───────────────────────────────────────────
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher(storage=MemoryStorage())
router = Router()

class S(StatesGroup):
    # JSON Import
    waiting_json = State()
    # Raw Request Import
    waiting_raw = State()
    # OTP Login
    waiting_phone = State()
    waiting_otp = State()
    # Run Sessions
    waiting_game_select = State()
    waiting_session_count = State()
    waiting_thread_limit = State()
    # Delete
    waiting_delete_confirm = State()

# ── Persistent Main Keyboard ─────────────────────────────────────
def get_main_keyboard(user_id: int):
    buttons = [
        [KeyboardButton(text="📱 Login with OTP")],
        [KeyboardButton(text="📝 Import JSON Session")],
        [KeyboardButton(text="📋 List Accounts")],
        [KeyboardButton(text="🚀 Run Sessions")],
        [KeyboardButton(text="🗑 Delete Account")],
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True, persistent=True)

# ── Game selection ──────────────────────────────────────────────
GAMES = ["ludo", "match-3", "city-builder", "goods-triple", "nazaria", "runner-3d"]

def game_buttons():
    btns = [[InlineKeyboardButton(text=g.capitalize(), callback_data=f"game_{g}")] for g in GAMES]
    btns.append([InlineKeyboardButton(text="❌ Cancel", callback_data="cancel")])
    return InlineKeyboardMarkup(inline_keyboard=btns)

# ── START ────────────────────────────────────────────────────────
@router.message(CommandStart())
async def cmd_start(msg: Message):
    uid = msg.from_user.id
    if uid not in ADMIN_IDS:
        return await msg.answer("⛔️ Unauthorized.")
    await msg.answer(
        "🤖 <b>Shopsy Game Session Bot</b>\n\n"
        "📱 <b>Login with OTP</b> – Enter phone, receive OTP, auto-login.\n"
        "📝 <b>Import JSON Session</b> – Paste existing session JSON.\n"
        "Then use <b>Run Sessions</b> to start farming games.",
        reply_markup=get_main_keyboard(uid)
    )

# ── OTP LOGIN FLOW ──────────────────────────────────────────────
@router.message(F.text == "📱 Login with OTP")
async def otp_start(msg: Message, state: FSMContext):
    if msg.from_user.id not in ADMIN_IDS: return
    await msg.answer(
        "📱 Enter your phone number with country code.\n"
        "Example: <code>+918357907306</code>",
        reply_markup=ReplyKeyboardRemove()
    )
    await state.set_state(S.waiting_phone)

@router.message(S.waiting_phone)
async def otp_phone(msg: Message, state: FSMContext):
    if msg.from_user.id not in ADMIN_IDS: return
    phone = msg.text.strip()
    # Basic validation
    if not re.match(r'^\+?[0-9]{10,15}$', phone):
        return await msg.answer("❌ Invalid phone number. Use format: +918357907306")
    await state.update_data(phone=phone)
    # Send OTP request
    status = await msg.answer("⏳ Sending OTP...")
    async with aiohttp.ClientSession() as session:
        result = await ShopsyBot.request_otp(session, phone)
    if result.get("success"):
        await status.edit_text(f"✅ OTP sent to {phone}.\n\nEnter the OTP you received:")
        await state.set_state(S.waiting_otp)
    else:
        error = result.get("error") or result.get("data", {}).get("message", "Unknown error")
        await status.edit_text(f"❌ Failed to send OTP: {error}\n\nTry again with /start")
        await state.clear()

@router.message(S.waiting_otp)
async def otp_verify(msg: Message, state: FSMContext):
    if msg.from_user.id not in ADMIN_IDS: return
    otp = msg.text.strip()
    if not otp.isdigit() or len(otp) < 4:
        return await msg.answer("❌ Enter a valid OTP (4-6 digits).")
    data = await state.get_data()
    phone = data.get("phone")
    status = await msg.answer("⏳ Verifying OTP...")
    async with aiohttp.ClientSession() as session:
        result = await ShopsyBot.verify_otp(session, phone, otp)
    if result.get("success"):
        account_json = result["account"]
        parsed, error = parse_account_from_json(json.dumps(account_json))
        if error:
            return await status.edit_text(f"❌ Error parsing account: {error}")
        accounts = load_accounts()
        uid = parsed["user_id"]
        accounts[uid] = parsed
        save_accounts(accounts)
        await status.edit_text(
            f"✅ <b>Login successful!</b>\n"
            f"Account: <b>{parsed['user_name']}</b>\n"
            f"Phone: <code>{uid}</code>\n\n"
            f"Now use <b>Run Sessions</b> to start farming.",
            reply_markup=get_main_keyboard(msg.from_user.id)
        )
    else:
        await status.edit_text(f"❌ OTP verification failed: {result.get('message', 'Unknown error')}")
    await state.clear()

# ── IMPORT JSON SESSION ─────────────────────────────────────────
@router.message(F.text == "📝 Import JSON Session")
async def import_json_start(msg: Message, state: FSMContext):
    if msg.from_user.id not in ADMIN_IDS: return
    await msg.answer(
        "📝 Paste the JSON session data (like the one with 'phone' and 'cookies'):\n\n"
        "Make sure it contains:\n"
        "• 'phone' (user ID)\n"
        "• 'cookies' (object with ud, vd, etc.)",
        reply_markup=ReplyKeyboardRemove()
    )
    await state.set_state(S.waiting_json)

@router.message(S.waiting_json)
async def import_json_receive(msg: Message, state: FSMContext):
    if msg.from_user.id not in ADMIN_IDS: return
    raw = msg.text.strip()
    if not raw:
        return await msg.answer("❌ Empty message.")
    parsed, error = parse_account_from_json(raw)
    if error:
        return await msg.answer(f"❌ Error: {error}")
    accounts = load_accounts()
    uid = parsed["user_id"]
    accounts[uid] = parsed
    save_accounts(accounts)
    await msg.answer(
        f"✅ Account <b>{parsed['user_name']}</b> (ID: {uid}) saved!",
        reply_markup=get_main_keyboard(msg.from_user.id)
    )
    await state.clear()

# ── LIST ACCOUNTS ──────────────────────────────────────────────
@router.message(F.text == "📋 List Accounts")
async def list_accounts(msg: Message):
    if msg.from_user.id not in ADMIN_IDS: return
    accounts = load_accounts()
    if not accounts:
        return await msg.answer("No accounts saved.")
    text = "📋 <b>Saved Accounts</b>\n━━━━━━━━━━━━━━━━━━\n"
    for uid, data in accounts.items():
        text += f"• <b>{data['user_name']}</b> (ID: <code>{uid}</code>)\n"
    await msg.answer(text)

# ── DELETE ACCOUNT ─────────────────────────────────────────────
@router.message(F.text == "🗑 Delete Account")
async def delete_account_start(msg: Message, state: FSMContext):
    if msg.from_user.id not in ADMIN_IDS: return
    accounts = load_accounts()
    if not accounts:
        return await msg.answer("No accounts to delete.")
    btns = []
    for uid, data in accounts.items():
        btns.append([InlineKeyboardButton(
            text=f"🗑 {data['user_name']} ({uid})",
            callback_data=f"delacc_{uid}"
        )])
    btns.append([InlineKeyboardButton(text="❌ Cancel", callback_data="cancel")])
    await msg.answer("Select account to delete:", reply_markup=InlineKeyboardMarkup(inline_keyboard=btns))
    await state.set_state(S.waiting_delete_confirm)

@router.callback_query(F.data.startswith("delacc_"))
async def delete_account_confirm(call: CallbackQuery, state: FSMContext):
    if call.from_user.id not in ADMIN_IDS: return
    uid = call.data.split("_")[1]
    accounts = load_accounts()
    if uid in accounts:
        del accounts[uid]
        save_accounts(accounts)
        await call.answer(f"✅ Account {uid} deleted.", show_alert=True)
        await call.message.delete()
        await call.message.answer("Account deleted.", reply_markup=get_main_keyboard(call.from_user.id))
    else:
        await call.answer("Account not found.", show_alert=True)
    await state.clear()

# ── RUN SESSIONS ─────────────────────────────────────────────────
@router.message(F.text == "🚀 Run Sessions")
async def run_sessions_start(msg: Message, state: FSMContext):
    if msg.from_user.id not in ADMIN_IDS: return
    accounts = load_accounts()
    if not accounts:
        return await msg.answer("No accounts. Add one first with OTP or JSON import.")
    first_uid = list(accounts.keys())[0]
    await state.update_data(account_id=first_uid)
    await msg.answer("🎮 Select game:", reply_markup=game_buttons())
    await state.set_state(S.waiting_game_select)

@router.callback_query(F.data.startswith("game_"), S.waiting_game_select)
async def game_selected(call: CallbackQuery, state: FSMContext):
    if call.from_user.id not in ADMIN_IDS: return
    game = call.data.split("_")[1]
    await state.update_data(game=game)
    await call.message.edit_text(f"Selected: <b>{game}</b>\nEnter number of sessions:")
    await state.set_state(S.waiting_session_count)
    await call.answer()

@router.message(S.waiting_session_count)
async def session_count_input(msg: Message, state: FSMContext):
    if msg.from_user.id not in ADMIN_IDS: return
    if not msg.text.isdigit():
        return await msg.answer("Please enter a number.")
    count = int(msg.text)
    await state.update_data(session_count=count)
    await msg.answer("Enter parallel threads (e.g., 5):")
    await state.set_state(S.waiting_thread_limit)

@router.message(S.waiting_thread_limit)
async def thread_limit_input(msg: Message, state: FSMContext):
    if msg.from_user.id not in ADMIN_IDS: return
    if not msg.text.isdigit():
        return await msg.answer("Please enter a number.")
    threads = int(msg.text)
    data = await state.get_data()
    account_id = data["account_id"]
    game = data["game"]
    count = data["session_count"]

    accounts = load_accounts()
    acc_data = accounts.get(account_id)
    if not acc_data:
        await msg.answer("❌ Account not found.")
        await state.clear()
        return

    bot_obj = ShopsyBot(acc_data)
    status_msg = await msg.answer(f"⏳ Running {count} sessions for <b>{game}</b> with {threads} threads...")

    try:
        connector = aiohttp.TCPConnector(limit=threads)
        async with aiohttp.ClientSession(connector=connector) as session:
            start_tasks = [bot_obj.start_game(session, game) for _ in range(count)]
            sessions_ids = await asyncio.gather(*start_tasks)
            valid = [s for s in sessions_ids if s]
            await status_msg.edit_text(f"✅ Started {len(valid)} sessions. Waiting 2 seconds...")
            await asyncio.sleep(2)
            end_tasks = [bot_obj.end_game(session, game, sid) for sid in valid]
            results = await asyncio.gather(*end_tasks)
            success_count = sum(1 for r in results if "Success" in r)
            fail_count = len(results) - success_count
            await status_msg.edit_text(
                f"✅ <b>Completed</b>\n"
                f"Game: {game}\n"
                f"Total sessions: {count}\n"
                f"Started: {len(valid)}\n"
                f"End results: ✅ {success_count} | ❌ {fail_count}"
            )
    except Exception as e:
        await status_msg.edit_text(f"❌ Error: {e}")
    finally:
        await state.clear()
        await msg.answer("Back to menu.", reply_markup=get_main_keyboard(msg.from_user.id))

@router.callback_query(F.data == "cancel")
async def cancel_cb(call: CallbackQuery, state: FSMContext):
    await state.clear()
    await call.message.edit_text("Cancelled.")
    await call.message.answer("Main menu:", reply_markup=get_main_keyboard(call.from_user.id))
    await call.answer()

# ── MAIN ──────────────────────────────────────────────────────────
async def main():
    dp.include_router(router)
    logger.info("🚀 Shopsy Bot with OTP Login started.")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
