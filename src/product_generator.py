"""
product_generator.py

Generates a synthetic dim_products.csv seed file in USD.
This script is idempotent: running it again will overwrite the file.
"""

import os
import csv
import random
from datetime import datetime, timezone
from utils import DIM_PRODUCTS_CSV

# Configuration
NUM_PRODUCTS = 40  # or random.randint(30, 50)
OUTPUT_PATH = DIM_PRODUCTS_CSV


TIERS = ["Premium", "Standard"]
TRANSACTION_TYPES = ["BattlePass", "Emote", "Skin"]

# Example realistic names for each product type
BATTLEPASS_NAMES = [
    "Season of the Eclipse",
    "Nightfall Pass",
    "Guardian's Journey",
    "Legends BattlePass",
    "Shadow Frontier",
    "Iron Vanguard Pass",
    "Celestial War",
    "Dawnbreaker Pass",
]

EMOTE_NAMES = [
    "Victory Dance",
    "Thumbs Up",
    "Laugh Out Loud",
    "Salute",
    "Facepalm",
    "Mic Drop",
    "Air Guitar",
    "Wave Hello",
    "Cheer",
]

SKIN_NAMES = [
    "Crimson Phantom",
    "Obsidian Knight",
    "Solar Flare",
    "Arctic Camo",
    "Golden Hunter",
    "Nebula Striker",
    "Midnight Ranger",
    "Electric Surge",
]

def generate_products(n):
    now_iso = datetime.now(timezone.utc).isoformat()
    products = []
    for i in range(n):
        transaction_type = random.choice(TRANSACTION_TYPES)

        if transaction_type == "BattlePass":
            is_recurring = True
            cycle = random.choice(["M", "Y"])
            product_name = random.choice(BATTLEPASS_NAMES)
        elif transaction_type == "Emote":
            is_recurring = False
            cycle = None
            product_name = random.choice(EMOTE_NAMES)
        else:  # Skin
            is_recurring = False
            cycle = None
            product_name = random.choice(SKIN_NAMES)
        pid = i + 1
        sku = f"SKU-{1000 + i}"
        tier = random.choice(TIERS)
        price = round(random.uniform(1.99, 99.99), 2)
        products.append({
            "product_id": pid,
            "product_sku": sku,
            "product_name": product_name,
            "tier": tier,
            "purchase_price": price,
            "transaction_type": transaction_type,
            "is_recurring": is_recurring,
            "cycle": cycle,
            "created_at": now_iso,
            "last_modified_at": now_iso
        })
        print(f'Generated [{sku}]: {product_name} | {tier} | Recurring: {is_recurring} | Price: {price} | Created at: {now_iso}')
    return products


def write_products_to_csv(products, output_path):
    """
    Write product list to CSV, overwriting any existing file.
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, mode="w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=products[0].keys())
        writer.writeheader()
        writer.writerows(products)


if __name__ == "__main__":
    products = generate_products(NUM_PRODUCTS)
    write_products_to_csv(products, OUTPUT_PATH)
    print(f"✅ Generated {len(products)} products in {OUTPUT_PATH}")