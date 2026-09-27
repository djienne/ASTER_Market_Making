import os
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).parent.absolute()
SECRETS_ENV_PATH = PROJECT_ROOT / "aster.env"
RUNTIME_ENV_PATH = PROJECT_ROOT / "runtime.env"


def load_project_env():
    """Load project env files, keeping runtime symbol config in a single dedicated file."""
    load_dotenv(SECRETS_ENV_PATH)
    load_dotenv(RUNTIME_ENV_PATH, override=True)


def configured_symbol(default: str = "ETHUSDT") -> str:
    """Return the configured full trading symbol."""
    return (os.getenv("SYMBOL") or default).upper()


load_project_env()
