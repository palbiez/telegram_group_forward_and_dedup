# tdg_config.py
# Central configuration loader. Reads configuration.conf and exposes typed variables.

# tgd_config.py
from pathlib import Path
import configparser

# Der config-file name
CONFIG_FILE = Path(__file__).parent / "tgd_configuration.conf"

if not CONFIG_FILE.exists():
    raise FileNotFoundError(f"{CONFIG_FILE} not found")

cfg = configparser.ConfigParser()
cfg.read(CONFIG_FILE)

# dann wie gehabt die Variablen extrahieren...
api_id = cfg.getint("telegram", "api_id")
api_hash = cfg.get("telegram", "api_hash")
session = cfg.get("telegram", "session")

TARGET_GROUP = cfg.get("groups", "target_group")
SOURCE_GROUPS = [
    line.strip()
    for line in cfg.get("groups", "source_groups").splitlines()
    if line.strip()
]


# ---------------- Forwarding ----------------
FORWARD_BATCH_SIZE = cfg.getint("forwarding", "forward_batch_size", fallback=50)
CONCURRENCY = cfg.getint("forwarding", "concurrency", fallback=1)
RATE_LIMIT_DELAY = cfg.getfloat("forwarding", "rate_limit_delay", fallback=1.0)
FLOODWAIT_BUFFER = cfg.getint("forwarding", "floodwait_buffer", fallback=5)
PERSIST_FORWARDED_UIDS_FILE = cfg.get(
    "forwarding",
    "persist_forwarded_uids_file",
    fallback="forwarded_uids.json"
)

# ---------------- Deduplication ----------------
DEDUPLICATION_ENABLED = cfg.getboolean("deduplication", "enabled", fallback=True)

# ---------------- State / Checkpoints ----------------
CHECKPOINT_DIR = cfg.get("state", "checkpoint_dir", fallback="checkpoints")
SNAPSHOT_INTERVAL_MESSAGES = cfg.getint(
    "state", "snapshot_interval_messages", fallback=2000
)

STATE_FILE = cfg.get("forwarding", "state_file", fallback="state.json")


# ---------------- Logging ----------------
LOG_DIR = cfg.get("logging", "log_dir", fallback="logs")
LOG_LEVEL = cfg.get("logging", "log_level", fallback="INFO")

