"""Single place that reads configuration: config/settings.yml (non-secret) and .env (environment/secrets)."""
import os
from pathlib import Path

import yaml
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / '.env')

SETTINGS = yaml.safe_load((ROOT / 'config' / 'settings.yml').read_text(encoding='utf-8'))

DB = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': int(os.getenv('POSTGRES_PORT', '5432')),
    'dbname': os.getenv('POSTGRES_DB', 'dss150p'),
    'user': os.getenv('POSTGRES_USER', 'dss150p'),
    'password': os.getenv('POSTGRES_PASSWORD', ''),  # no default password in code
}


def path_for(key: str) -> Path:
    """Absolute path for a key in settings.yml -> paths (always relative to the project root)."""
    return ROOT / SETTINGS['paths'][key]


def load_yaml(key: str) -> dict:
    return yaml.safe_load(path_for(key).read_text(encoding='utf-8'))
