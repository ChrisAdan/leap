import json
from datetime import datetime, timezone
from pathlib import Path
import duckdb
import pandas as pd
import numpy as np
from utils import DB_PATH

def connect_to_duckdb():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(DB_PATH)

def ensure_schema_and_table(
    duck_conn: duckdb.DuckDBPyConnection,
    schema: str,
    table: str,
    columns_def,
):
    """
    columns_def can be a string or dict:
    - If string, use as-is in CREATE TABLE
    - If dict, keys=column names, values=types (e.g. "VARCHAR PRIMARY KEY")
    """
    duck_conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    
    if isinstance(columns_def, dict):
        cols = ", ".join(f"{col} {typ}" for col, typ in columns_def.items())
    else:
        cols = columns_def

    duck_conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            {cols}
        )
    """)

def write_dataframe_to_table(
    duck_conn: duckdb.DuckDBPyConnection,
    schema: str,
    table: str,
    df: pd.DataFrame,
    primary_key: str = None,
    replace: bool = True,
):
    """
    Writes a DataFrame to DuckDB table, optionally replacing existing data.
    Creates the table if missing using df schema inferred as VARCHAR/INTEGER/DOUBLE.
    """

    def infer_duckdb_type(dtype):
        if pd.api.types.is_integer_dtype(dtype):
            return "INTEGER"
        elif pd.api.types.is_float_dtype(dtype):
            return "DOUBLE"
        elif pd.api.types.is_bool_dtype(dtype):
            return "BOOLEAN"
        else:
            return "VARCHAR"

    cols_defs = ", ".join(
        f"{col} {infer_duckdb_type(dtype)}" + (" PRIMARY KEY" if col == primary_key else "")
        for col, dtype in df.dtypes.items()
    )

    ensure_schema_and_table(duck_conn, schema, table, cols_defs)

    if replace:
        duck_conn.execute(f"DELETE FROM {schema}.{table}")

    duck_conn.register("temp_df", df)
    duck_conn.execute(f"INSERT INTO {schema}.{table} SELECT * FROM temp_df")
    print(f"✅ Inserted {len(df)} rows into {schema}.{table}")

def write_json_record_to_duckdb(
    duck_conn: duckdb.DuckDBPyConnection,
    schema: str,
    table: str,
    record_id_col: str,
    record_id: str,
    json_obj: dict,
    directory: Path,
    created_at_col: str = "created_at",
    write_to_db: bool = True,
):
    """
    Saves JSON to disk, and optionally writes a record into DuckDB table 
    with columns: record_id_col (PK), raw_response (JSON string), created_at_col (timestamp).

    If write_to_db=False, only saves JSON file to disk.
    """
    directory.mkdir(parents=True, exist_ok=True)
    json_str = json.dumps(json_obj, indent=2)
    created_at = datetime.now(timezone.utc).isoformat()

    # Safe filename: sanitize colons in ISO timestamp
    timestamp_safe = json_obj.get('end_time', datetime.now(timezone.utc).isoformat()).replace(':', '-')
    filename = f"{record_id}_{timestamp_safe}.json"
    json_path = directory / filename
    json_path.write_text(json_str)

    if write_to_db:
        columns_def = {
            record_id_col: "VARCHAR PRIMARY KEY",
            "raw_response": "VARCHAR",
            created_at_col: "TIMESTAMP"
        }
        ensure_schema_and_table(duck_conn, schema, table, columns_def)
        try:
            duck_conn.execute(
                f"INSERT INTO {schema}.{table} ({record_id_col}, raw_response, {created_at_col}) VALUES (?, ?, ?)",
                (record_id, json_str, created_at)
            )
        except duckdb.ConversionException as e:
            print(f"Error inserting JSON record {record_id} into {schema}.{table}: {e}")

def load_table_to_df(duck_conn: duckdb.DuckDBPyConnection, schema: str, table: str):
    try:
        df = duck_conn.execute(f"SELECT * FROM {schema}.{table}").fetchdf()
        if df.empty:
            return None
        return df
    except Exception:
        return None

def clear_old_data(duck_conn, level="all"):
    # Base level definitions (no overlap)
    base_tables = {
        "players": {
            "leap_dim.dim_players",
            "leap_dim.dim_products",
            "leap_raw.event_signons",
            "leap_raw.event_session",
            "leap_raw.event_transaction",
        },
        "signons": {
            "leap_raw.event_signons",
            "leap_raw.event_session",
            "leap_raw.event_transaction",
        },
        "sessions": {
            "leap_raw.event_session",
            "leap_raw.event_transaction",
            "leap_stage.event_heartbeat",
            "leap_stage.fact_session",
            "leap_stage.stage_centroids",
            "leap_stage.stage_encounters",
        },
        "transactions": {
            "leap_raw.event_transaction",
        },
        "mart": {
            "leap_mart.encounter_summary_daily",
            "leap_mart.player_activity_daily",
        }
    }

    # Cascade definitions: each level includes its own plus lower levels
    cascades = {
        "players": base_tables["players"],
        "signons": base_tables["signons"],
        "sessions": base_tables["sessions"],
        "transactions": base_tables["transactions"],
        "mart": base_tables["mart"],
        "all": set().union(
            base_tables["players"],
            base_tables["signons"],
            base_tables["sessions"],
            base_tables["transactions"],
            base_tables["mart"],
        )
    }

    tables_to_drop = cascades.get(level)
    if not tables_to_drop:
        print(f"No tables configured for level '{level}'")
        return

    for table in tables_to_drop:
        try:
            duck_conn.execute(f"DROP TABLE IF EXISTS {table}")
            print(f"Dropped table {table}")
        except Exception as e:
            print(f"Warning: Could not drop table {table}: {e}")