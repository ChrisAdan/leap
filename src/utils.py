import random
from pathlib import Path

import numpy as np
import pandas as pd

# ==== Constants ====
GRID_BOUNDS = (-100, 100)
HEARTBEAT_INTERVAL = 30  # seconds
SESSION_MAX_DURATION_SECONDS = 1800
COUNTRIES = ['US', 'BR', 'MX', 'FR', 'ES', 'DE']
MIN_DAILY_SESSIONS = 0
MAX_DAILY_SESSIONS = 1
DEFAULT_STARTING_PLAYERS = 1000

MIN_TEAMS = 1
MAX_TEAMS = 5
MIN_PLAYERS_PER_TEAM = 1
MAX_PLAYERS_PER_TEAM = 2

RANDOM_SEED = 42

# Get project root = one level up from the scripts directory
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Paths to important files
DB_PATH = Path("data/synthetic.duckdb")
DIM_PRODUCTS_CSV = PROJECT_ROOT / "dbt_project" / "seeds" / "dim_products.csv"

def ensure_path(path):
    path.mkdir(parents=True, exist_ok=True)

SESSION_PATH = Path("data/sessions")
ensure_path(SESSION_PATH)
TRANSACTION_PATH = Path("data/transactions")
ensure_path(TRANSACTION_PATH)

def generate_player_ids(n_players=DEFAULT_STARTING_PLAYERS, seed=RANDOM_SEED):
    random.seed(seed)
    np.random.seed(seed)
    ids = np.random.choice(range(n_players), size=n_players, replace=False)
    return [str(i).zfill(4) for i in ids]

def model_sign_ons(player_ids, n_days=365, start_date="2025-01-01", seed=RANDOM_SEED):
    random.seed(seed)
    np.random.seed(seed)

    start_dt = pd.to_datetime(start_date)
    all_dates = pd.date_range(start=start_dt, periods=n_days)
    # Removed 'decay' pattern
    patterns = ['daily', 'weekday', 'cyclical']
    behaviors = {pid: random.choice(patterns) for pid in player_ids}

    records = []
    for pid in player_ids:
        behavior = behaviors[pid]
        for i, date in enumerate(all_dates):
            day = date.weekday()
            x = i / n_days
            p = {
                'daily': 0.9,
                'weekday': 0.8 if day < 5 else 0.3,
                'cyclical': 0.5 + 0.4 * np.sin(2 * np.pi * x * 4),
            }[behavior]
            if np.random.rand() < p:
                records.append({"player_id": pid, "date": date.date()})
    return pd.DataFrame(records)


def assign_countries(player_ids, seed=RANDOM_SEED):
    np.random.seed(seed)
    countries = np.random.choice(COUNTRIES, size=len(player_ids))
    return dict(zip(player_ids, countries))

def convert_numpy_types(obj):
    if isinstance(obj, dict):
        return {k: convert_numpy_types(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_numpy_types(i) for i in obj]
    elif isinstance(obj, (np.integer, np.int64, np.int32)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float64, np.float32)):
        return float(obj)
    elif isinstance(obj, np.bool_):
        return bool(obj)
    else:
        return obj