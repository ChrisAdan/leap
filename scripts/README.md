# main.py

## 🗂️ Overview

`main.py` is the primary entrypoint for the Leap data generation pipeline.  
It orchestrates the creation and loading of all core synthetic datasets into DuckDB:

- Player dimension data
- Product dimension data
- Player sign-ons
- Session events
- Transaction events

The script can start from any stage in the pipeline using the `--entrypoint` argument.

---

## 🚀 Usage

From the project root:

```bash
python scripts/main.py --entrypoint all
```

**Available entrypoints:**

- `players` → Generate player dimension (`dim_players`)
- `products` → Generate product dimension (`dim_products`)
- `signons` → Generate sign-on events (`event_signons`)
- `sessions` → Generate session events from sign-ons (`event_session`)
- `transactions` → Generate in-game transactions (`event_transaction`)
- `all` → Run full pipeline in sequence

Example:

```bash
python scripts/main.py --entrypoint sessions
```

---

## 🔄 Workflow

1. **Players**

   - Generates unique player IDs with UUIDs
   - Models growth/decay over `n` days
   - Assigns countries to each player
   - Saves to `leap_dim.dim_players`

2. **Products**

   - Generates `n` in-game products
   - Writes to CSV in `seeds/` for dbt

3. **Sign-ons**

   - Models daily logins over one year
   - Saves to `leap_raw.event_signons`

4. **Sessions**

   - Uses sign-on data + country mapping
   - Generates 3D movement heartbeat data and aggregates session-level stats
   - Saves to `leap_raw.event_session`

5. **Transactions**
   - Uses sign-on data + product dimension
   - Generates realistic purchase behavior
   - Saves to `leap_raw.event_transaction`

---

## 📦 DuckDB Tables Created

**leap_dim**

- dim_players
- dim_products

**leap_raw**

- event_signons
- event_session
- event_transaction

**leap_stage**

- fact_session

---

## 🛠️ Developer Notes

- Prompts will appear if existing data is found, allowing reuse or regeneration.
- All generated datasets are immediately written to DuckDB using `write_dataframe_to_table`.
- `DEFAULT_STARTING_PLAYERS` controls the base player count if no players exist.
- Growth/decay rates in `run_players` can be adjusted for different simulation curves.
