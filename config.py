import configparser
from pathlib import Path

CONFIG_FILE = Path("configuration.conf")

if not CONFIG_FILE.exists():
    raise FileNotFoundError("configuration.conf nicht gefunden")

config = configparser.ConfigParser()
config.read(CONFIG_FILE)

# -------------------
# Telegram (erforderlich)
# -------------------
api_id = config.getint("telegram", "api_id")
api_hash = config.get("telegram", "api_hash")
session = config.get("telegram", "session")

# -------------------
# Gruppen (erforderlich)
# -------------------
TARGET_GROUP = config.get("groups", "target_group")

SOURCE_GROUPS = [
    g.strip()
    for g in config.get("groups", "source_groups").splitlines()
    if g.strip()
]

# -------------------
# Deduplication / Laufzeit-Settings (optional, mit Defaults)
# -------------------

# Historische / bestehende Flags (weiterhin lesbar)
# Falls die Sektion/Option fehlt, setzen wir sichere Defaults.
try:
    DRY_RUN = config.getboolean("deduplication", "dry_run")
except Exception:
    DRY_RUN = False  # Standard: kein Dry-Run (du hattest gesagt: Dry-Run nicht nötig)

try:
    DELETE_CROSS_TOPIC = config.getboolean("deduplication", "delete_cross_topic_duplicates")
except Exception:
    DELETE_CROSS_TOPIC = False

try:
    MAX_MESSAGES_PER_TOPIC = config.getint("deduplication", "max_messages_per_topic")
except Exception:
    MAX_MESSAGES_PER_TOPIC = 0  # 0 = kein Limit

try:
    RATE_LIMIT_DELAY = config.getfloat("deduplication", "rate_limit_delay")
except Exception:
    RATE_LIMIT_DELAY = 5.0  # conservative default (Sekunden) — passt gut für große Dateien

try:
    LOG_FILE = config.get("deduplication", "log_file")
    if LOG_FILE.strip() == "":
        LOG_FILE = None
except Exception:
    LOG_FILE = None

# -------------------
# Neue Parameter für media-forwarding / floodwait-reduktion
# -------------------

# Anzahl Nachrichten-IDs, die wir in einem forward/copy-Batch zusammenfassen.
# Für große Dateien ist 1 konservativ; Alben werden als Gruppe forwarded.
try:
    FORWARD_BATCH_SIZE = config.getint("forwarding", "forward_batch_size")
except Exception:
    FORWARD_BATCH_SIZE = 100

# Anzahl paralleler Worker / Semaphore-Size.
# Bei sehr großen Dateien setze 1 (empfohlen). Höher nur, wenn du Netz/Ratenlimits testen kannst.
try:
    CONCURRENCY = config.getint("forwarding", "concurrency")
except Exception:
    CONCURRENCY = 1

# Zusätzliche Sicherheitssekunden, die wir zu einem FloodWait-Wert addieren.
try:
    FLOODWAIT_BUFFER = config.getint("forwarding", "floodwait_buffer")
except Exception:
    FLOODWAIT_BUFFER = 5

# Optional: maximale Größe (in Bytes) die in einem einzelnen ForwardBatch toleriert werden soll.
# Wird nicht automatisch verwendet, ist aber eine nützliche Steuergröße für Erweiterungen.
try:
    MAX_BATCH_BYTES = config.getint("forwarding", "max_batch_bytes")
except Exception:
    MAX_BATCH_BYTES = 0  # 0 = no limit (default)

PERSIST_FORWARDED_UIDS_FILE = config.get("forwarding", "PERSIST_FORWARDED_UIDS_FILE")   

# -------------------
# Sonstiges / Sicherheit
# -------------------

# Timeout für API-Operationen (sekunden). Nutze dies, falls langsame Antworten vorkommen.
try:
    API_CALL_TIMEOUT = config.getint("general", "api_call_timeout")
except Exception:
    API_CALL_TIMEOUT = 300

# Debug-Pause (wenn True, fragt das Skript vor kritischen Aktionen nach Bestätigung).
# Default: False auf Produktions-Servern.
try:
    DEBUG_PAUSE = config.getboolean("general", "debug_pause")
except Exception:
    DEBUG_PAUSE = False


# -------------------
# End of config
# -------------------

