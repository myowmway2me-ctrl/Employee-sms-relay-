# ╔══════════════════════════════════════════════════════╗
#   OFFICE SMS RELAY V6 — CONFIGURATION
#   Groq AI · Telegram-Only · Full Monitoring
# ╚══════════════════════════════════════════════════════╝

import os

# ══════════════ TELEGRAM ══════════════════════════════
BOT_TOKEN        = os.environ.get("TG_BOT_TOKEN", "6379711237:AAEQamc5bWsR-wbF_2s6CdpL6ZKpMIUjG5k")
ADMIN_IDS        = [6013007573]
KEY_PREFIX       = "RELAY-"
KEY_LENGTH       = 30

# ══════════════ GROQ AI (REPLACES OLD api.g0i.ai) ════
GROQ_API_KEY     = os.environ.get("GROQ_API_KEY", "")
GROQ_BASE_URL    = "https://api.groq.com/openai/v1"
GROQ_MODEL       = "llama-3.3-70b-versatile"
GROQ_TIMEOUT     = 30

# ══════════════ FIREBASE SETTINGS ═════════════════════
MAX_FIREBASE_SOURCES = 20
RESYNC_INTERVAL  = 600
PAGE_SIZE        = 15

# ══════════════ PERFORMANCE ═══════════════════════════
SSE_DRAIN_MS     = 1.5
POLL_INTERVAL    = 4.0
DEVICE_CHECK_INTERVAL = 45
GHOST_PROBE_INTERVAL = 30
FIREBASE_HEALTH_CHECK = 30

# ══════════════ THRESHOLDS ════════════════════════════
FIREBASE_QUARANTINE_THRESHOLD = 5
FIREBASE_SLOW_THRESHOLD = 3
BATTERY_WARNING_THRESHOLD = 15
OFFLINE_THRESHOLD_HOURS = 48

# ══════════════ CACHING ════════════════════════════════
HOT_NUMBERS_CACHE_TTL = 10
RADAR_CACHE_TTL = 5
FIREBASE_HEALTH_CACHE_TTL = 30

# ══════════════ DATABASE ═══════════════════════════════
DB_PATH = "office_relay_v6.db"
DB_BACKUP_INTERVAL = 3600

# ══════════════ LOGGING ════════════════════════════════
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s %(levelname)s %(message)s"

# ══════════════ FEATURE FLAGS ══════════════════════════
ENABLE_AI_LEARNING = True
ENABLE_FIREBASE_QUARANTINE = True
ENABLE_AUTO_PROBE = True
ENABLE_STATUS_ALERTS = True
ENABLE_WATCHLIST = True
ENABLE_PATTERN_STATS = True
