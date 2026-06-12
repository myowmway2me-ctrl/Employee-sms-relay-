# ╔══════════════════════════════════════════════════════╗
#   OFFICE SMS RELAY V6 — DATABASE SCHEMAS
#   All tables for Phase 1-10 upgrade
# ╚══════════════════════════════════════════════════════╝

# These SQL scripts create/upgrade all needed tables
# Run via DB.init_schemas()

SCHEMA_SCRIPTS = {
    # ─── EXISTING (from v5) ───────────────────────────
    "users": """
        CREATE TABLE IF NOT EXISTS users(
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            refer_count INTEGER DEFAULT 0,
            is_banned INTEGER DEFAULT 0,
            access_key TEXT DEFAULT NULL,
            is_unlocked INTEGER DEFAULT 0,
            is_suspended INTEGER DEFAULT 0,
            suspended_reason TEXT DEFAULT NULL,
            suspended_at TEXT DEFAULT NULL
        )
    """,
    
    "refer_log": """
        CREATE TABLE IF NOT EXISTS refer_log(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            referrer_id INTEGER,
            referred_id INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """,
    
    "access_keys": """
        CREATE TABLE IF NOT EXISTS access_keys(
            key TEXT PRIMARY KEY,
            owner_id INTEGER,
            created_at TEXT,
            used_by INTEGER DEFAULT NULL,
            key_type TEXT DEFAULT 'temporary',
            expires_at TEXT DEFAULT NULL
        )
    """,
    
    "numbers": """
        CREATE TABLE IF NOT EXISTS numbers(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            number TEXT UNIQUE,
            device_id TEXT,
            device_name TEXT,
            sim_slot TEXT DEFAULT 'sim1',
            carrier TEXT,
            status TEXT DEFAULT 'Active',
            assigned_to INTEGER DEFAULT NULL,
            fb_source TEXT DEFAULT NULL,
            sms_path TEXT DEFAULT NULL,
            status_path TEXT DEFAULT NULL,
            struct_type TEXT DEFAULT NULL,
            is_ghost INTEGER DEFAULT 0,
            last_seen_ts INTEGER DEFAULT NULL,
            last_activity TEXT DEFAULT NULL,
            device_status TEXT DEFAULT 'Unknown'
        )
    """,
    
    "firebase_sources": """
        CREATE TABLE IF NOT EXISTS firebase_sources(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT UNIQUE,
            label TEXT,
            added_at TEXT,
            last_synced TEXT,
            num_count INTEGER DEFAULT 0,
            struct_type TEXT DEFAULT NULL,
            api_key TEXT DEFAULT NULL
        )
    """,
    
    "channels": """
        CREATE TABLE IF NOT EXISTS channels(
            channel_id TEXT PRIMARY KEY,
            channel_link TEXT
        )
    """,
    
    "settings": """
        CREATE TABLE IF NOT EXISTS settings(
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """,
    
    "sms_log": """
        CREATE TABLE IF NOT EXISTS sms_log(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            number TEXT,
            sender TEXT,
            otp TEXT,
            full_msg TEXT,
            received_at TEXT
        )
    """,
    
    # ─── NEW v6 TABLES ─────────────────────────────────
    
    "firebase_health": """
        CREATE TABLE IF NOT EXISTS firebase_health(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fb_source TEXT UNIQUE,
            status TEXT DEFAULT 'Unknown',
            success_count INTEGER DEFAULT 0,
            failure_count INTEGER DEFAULT 0,
            timeout_count INTEGER DEFAULT 0,
            last_check TEXT DEFAULT NULL,
            quarantine_reason TEXT DEFAULT NULL,
            quarantined_at TEXT DEFAULT NULL,
            last_recovery_attempt TEXT DEFAULT NULL,
            consecutive_failures INTEGER DEFAULT 0
        )
    """,
    
    "pattern_stats": """
        CREATE TABLE IF NOT EXISTS pattern_stats(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pattern_type TEXT,
            success_count INTEGER DEFAULT 0,
            failure_count INTEGER DEFAULT 0,
            confidence_score REAL DEFAULT 0.0,
            last_used TEXT DEFAULT NULL
        )
    """,
    
    "learned_patterns": """
        CREATE TABLE IF NOT EXISTS learned_patterns(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            db_host TEXT UNIQUE,
            pattern_id TEXT,
            devices_root TEXT,
            phone_field TEXT,
            sms_root TEXT,
            sms_body_field TEXT,
            sms_sender_field TEXT,
            sms_time_field TEXT,
            status_root TEXT,
            status_field TEXT,
            confidence REAL,
            learned_at TEXT DEFAULT CURRENT_TIMESTAMP,
            used_count INTEGER DEFAULT 0,
            last_used TEXT DEFAULT NULL
        )
    """,
    
    "admin_action_logs": """
        CREATE TABLE IF NOT EXISTS admin_action_logs(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            admin_id INTEGER,
            action_type TEXT,
            target_id TEXT DEFAULT NULL,
            details TEXT DEFAULT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """,
    
    "watchlist": """
        CREATE TABLE IF NOT EXISTS watchlist(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            number TEXT UNIQUE,
            device_id TEXT,
            added_by INTEGER,
            reason TEXT DEFAULT NULL,
            added_at TEXT DEFAULT CURRENT_TIMESTAMP,
            alert_count INTEGER DEFAULT 0
        )
    """,
    
    "ghost_recovery_queue": """
        CREATE TABLE IF NOT EXISTS ghost_recovery_queue(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            device_id TEXT UNIQUE,
            category TEXT DEFAULT 'Recoverable',
            fb_source TEXT,
            probe_attempts INTEGER DEFAULT 0,
            last_probe TEXT DEFAULT NULL,
            suggested_paths TEXT DEFAULT NULL,
            confidence REAL DEFAULT 0.0,
            added_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """,
    
    "device_status_history": """
        CREATE TABLE IF NOT EXISTS device_status_history(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            number TEXT,
            old_status TEXT,
            new_status TEXT,
            changed_at TEXT DEFAULT CURRENT_TIMESTAMP,
            reason TEXT DEFAULT NULL
        )
    """,
    
    "hot_numbers_cache": """
        CREATE TABLE IF NOT EXISTS hot_numbers_cache(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            number TEXT UNIQUE,
            activity_score REAL DEFAULT 0.0,
            recent_sms_count INTEGER DEFAULT 0,
            recent_otp_count INTEGER DEFAULT 0,
            last_activity TEXT DEFAULT NULL,
            cached_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """,
    
    "sms_fingerprints": """
        CREATE TABLE IF NOT EXISTS sms_fingerprints(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fingerprint TEXT UNIQUE,
            number TEXT,
            sender TEXT,
            body_hash TEXT,
            timestamp INTEGER,
            seen_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """,
}

# ══════════════ ALTER TABLE STATEMENTS ═══════════════
SCHEMA_MIGRATIONS = [
    # Add columns to existing tables if they don't exist
    "ALTER TABLE users ADD COLUMN is_suspended INTEGER DEFAULT 0",
    "ALTER TABLE users ADD COLUMN suspended_reason TEXT DEFAULT NULL",
    "ALTER TABLE users ADD COLUMN suspended_at TEXT DEFAULT NULL",
    "ALTER TABLE access_keys ADD COLUMN key_type TEXT DEFAULT 'temporary'",
    "ALTER TABLE access_keys ADD COLUMN expires_at TEXT DEFAULT NULL",
    "ALTER TABLE numbers ADD COLUMN device_status TEXT DEFAULT 'Unknown'",
    "ALTER TABLE numbers ADD COLUMN last_activity TEXT DEFAULT NULL",
    "ALTER TABLE firebase_sources ADD COLUMN quarantine_status TEXT DEFAULT 'active'",
]

# ══════════════ CREATE INDEXES ═════════════════════════
INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_numbers_number ON numbers(number)",
    "CREATE INDEX IF NOT EXISTS idx_numbers_device_id ON numbers(device_id)",
    "CREATE INDEX IF NOT EXISTS idx_numbers_status ON numbers(status)",
    "CREATE INDEX IF NOT EXISTS idx_firebase_sources_label ON firebase_sources(label)",
    "CREATE INDEX IF NOT EXISTS idx_admin_logs_admin ON admin_action_logs(admin_id)",
    "CREATE INDEX IF NOT EXISTS idx_watchlist_number ON watchlist(number)",
    "CREATE INDEX IF NOT EXISTS idx_ghost_queue_category ON ghost_recovery_queue(category)",
    "CREATE INDEX IF NOT EXISTS idx_sms_fingerprint ON sms_fingerprints(fingerprint)",
    "CREATE INDEX IF NOT EXISTS idx_hot_cache_score ON hot_numbers_cache(activity_score DESC)",
]
