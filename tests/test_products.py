"""
test_products.py

Common sense test suite for dim_products.csv seed file.
Ensures generated products meet data integrity and business rule expectations.
"""

import os
import pandas as pd
import pytest
import re
import csv

# Path to seed file (adjust if project structure changes)
SEED_PATH = os.path.join(
    os.path.dirname(__file__), "..", "dbt_project", "seeds", "dim_products.csv"
)
CSV_PATH = os.path.join(os.path.dirname(__file__), "..", "dbt_project", "seeds", "dim_products.csv")
SKU_PATTERN = re.compile(r"^SKU-\d{4,}$")

@pytest.fixture(scope="module")
def products_df():
    """Load the dim_products.csv seed into a DataFrame for testing."""
    if not os.path.exists(SEED_PATH):
        pytest.fail(f"Seed file not found at expected location: {SEED_PATH}")
    return pd.read_csv(SEED_PATH)

def test_non_empty(products_df):
    """The seed file must contain at least one product."""
    assert not products_df.empty, "dim_products.csv is empty"

def test_record_count_range(products_df):
    """The seed file should contain between 30 and 50 records."""
    count = len(products_df)
    assert 30 <= count <= 50, f"Expected 30-50 products, found {count}"

def test_unique_product_id(products_df):
    """Product IDs must be unique."""
    assert products_df["product_id"].is_unique, "Duplicate product_id values found"

def test_unique_product_sku(products_df):
    """Product SKUs must be unique."""
    assert products_df["product_sku"].is_unique, "Duplicate product_sku values found"

def test_tier_values(products_df):
    """Tier must be either 'Premium' or 'Standard'."""
    allowed = {"Premium", "Standard"}
    invalid = set(products_df["tier"]) - allowed
    assert not invalid, f"Invalid tier values found: {invalid}"

def test_purchase_price_positive(products_df):
    """Purchase price must be greater than zero."""
    assert (products_df["purchase_price"] > 0).all(), "Found non-positive purchase_price values"

def test_transaction_type_casing(products_df):
    """Transaction type must be CamelCase (BattlePass, Emote, Skin)."""
    allowed = {"BattlePass", "Emote", "Skin"}
    invalid = set(products_df["transaction_type"]) - allowed
    assert not invalid, f"Invalid transaction_type values found: {invalid}"

def test_is_recurring_boolean(products_df):
    """is_recurring must only contain True or False."""
    valid_values = {True, False}
    actual_values = set(products_df["is_recurring"].dropna().unique())
    assert actual_values <= valid_values, f"Invalid is_recurring values found: {actual_values}"

def test_cycle_values(products_df):
    """
    If is_recurring is True, cycle must be 'M' or 'Y'.
    If is_recurring is False, cycle must be empty or NaN.
    """
    recurring_df = products_df[products_df["is_recurring"]]
    non_recurring_df = products_df[~products_df["is_recurring"]]

    assert set(recurring_df["cycle"]) <= {"M", "Y"}, \
        "Recurring products must have cycle 'M' or 'Y'"

    # For non-recurring, allow empty string or NaN
    assert all(cycle in ("", None) or pd.isna(cycle)
               for cycle in non_recurring_df["cycle"]), \
        "Non-recurring products must have empty or null cycle"


def test_product_sku_format():
    with open(CSV_PATH, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            sku = row["product_sku"]
            assert SKU_PATTERN.match(sku), f"Invalid SKU format: {sku}"