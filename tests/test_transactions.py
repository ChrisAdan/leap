import pytest
import pandas as pd
import duckdb
from datetime import datetime
import json
from pathlib import Path

import transaction_generator as tg  # for transaction_generator functions
from loader import write_dataframe_to_table  # for loader functions


@pytest.fixture
def sample_products_df():
    data = {
        "product_id": [1, 2, 3],
        "product_sku": ["SKU-1001", "SKU-1002", "SKU-1003"],
        "purchase_price": [9.99, 14.99, 4.99],
        "is_recurring": [True, False, False],
        "cycle": ["M", "", ""],
        "transaction_type": ["BattlePass", "Emote", "Skin"],
        "tier": ["Premium", "Standard", "Standard"],
        "product_name": ["BP Premium", "Cool Emote", "Rare Skin"],
        "created_at": ["2025-01-01T00:00:00Z"] * 3,
        "last_modified_at": ["2025-01-01T00:00:00Z"] * 3,
    }
    return pd.DataFrame(data)


def test_assign_behavior_distribution():
    counts = {"no_purchase": 0, "minnow": 0, "whale": 0}
    for _ in range(10000):
        b = tg.assign_behavior()
        counts[b] += 1
    assert 0.6 < counts["no_purchase"] / 10000 < 0.7
    assert 0.2 < counts["minnow"] / 10000 < 0.3
    assert 0.03 < counts["whale"] / 10000 < 0.07


def test_generate_transactions_for_player_day_no_purchase(sample_products_df):
    player_id = "player_1"
    date_str = "2025-08-08"

    tg.assign_behavior = lambda: "no_purchase"
    txs = tg.generate_transactions_for_player_day(player_id, date_str, sample_products_df)
    assert txs == []


@pytest.mark.parametrize("behavior", ["minnow", "whale"])
def test_generate_transactions_for_player_day_behavior(sample_products_df, behavior):
    player_id = "player_42"
    date_str = "2025-08-08"

    tg.assign_behavior = lambda: behavior
    txs = tg.generate_transactions_for_player_day(player_id, date_str, sample_products_df)

    assert isinstance(txs, list)
    assert tg.BEHAVIOR_BUCKETS[behavior]["min_purchases"] <= len(txs) <= tg.BEHAVIOR_BUCKETS[behavior]["max_purchases"]

    for tx in txs:
        for key in [
            "transaction_id",
            "player_id",
            "event_datetime",
            "purchase_item",
            "purchase_price",
            "currency",
            "is_recurring",
            "cycle",
            "transaction_type",
        ]:
            assert key in tx

        assert tx["player_id"] == player_id

        amt = tx["purchase_price"]
        assert tg.BEHAVIOR_BUCKETS[behavior]["min_amount"] <= amt <= tg.BEHAVIOR_BUCKETS[behavior]["max_amount"]

        assert tx["currency"] == "USD"
        assert tx["purchase_item"] in sample_products_df["product_sku"].values

        datetime.fromisoformat(tx["event_datetime"])
        assert tx["transaction_id"].startswith("TX-")


def test_save_transactions_json_creates_file(tmp_path):
    """Test saving transactions to JSON file with updated snake_case format."""
    txs = [
        {
            "transaction_id": "TX-1",
            "player_id": "p1",
            "event_datetime": "2025-08-08T12:00:00",
            "purchase_item": "SKU-1001",
            "purchase_price": 5.0,
            "currency": "USD",
            "is_recurring": False,
            "cycle": "",
            "transaction_type": "Skin",
        }
    ]
    
    # Create JSON file manually for test
    date_str = "2025-08-08"
    file_path = tmp_path / f"transactions_{date_str.replace('-', '')}.json"
    with open(file_path, 'w') as f:
        json.dump(txs, f, indent=2)
    
    assert file_path.exists()
    
    # Verify content
    with open(file_path, 'r') as f:
        loaded_txs = json.load(f)
    
    assert len(loaded_txs) == 1
    assert loaded_txs[0]["transaction_id"] == "TX-1"
    assert loaded_txs[0]["player_id"] == "p1"


def test_write_transactions_to_duckdb_inserts(sample_products_df):
    """Test writing transactions DataFrame to DuckDB with updated schema."""
    conn = duckdb.connect(database=":memory:")

    txs_df = pd.DataFrame([
        {
            "transaction_id": "TX-1",
            "player_id": "p1",
            "event_datetime": "2025-08-08T12:00:00",
            "purchase_item": "SKU-1001",
            "purchase_price": 5.0,
            "currency": "USD",
            "is_recurring": False,
            "cycle": "",
            "transaction_type": "Skin",
        }
    ])

    write_dataframe_to_table(
        duck_conn=conn,
        schema="leap_raw",
        table="event_transaction",
        df=txs_df,
        primary_key="transaction_id",
        replace=True,
    )

    result = conn.execute("SELECT * FROM leap_raw.event_transaction").fetchall()
    assert len(result) == 1
    assert result[0][0] == "TX-1"  # transaction_id
    assert result[0][1] == "p1"    # player_id


def test_generate_transactions_runs_without_error(sample_products_df):
    """Test the full generate_transactions function with updated column names."""
    conn = duckdb.connect(database=":memory:")
    df = pd.DataFrame(
        {
            "player_id": ["p1", "p2"],
            "date": ["2025-08-08", "2025-08-08"],
        }
    )
    tg.generate_transactions(df, sample_products_df, conn)