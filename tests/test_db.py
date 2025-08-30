import json
import uuid
import shutil
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest

# Debug sys.path to ensure PYTHONPATH is correct (esp. for direnv)
import sys
import pprint
print("PYTHONPATH from runtime sys.path:")
pprint.pprint(sys.path)

# ✅ Imports from src/
from loader import connect_to_duckdb, write_json_record_to_duckdb, write_dataframe_to_table
import utils


@pytest.fixture(scope="session")
def session_path():
    """
    Creates a temporary directory for session JSON files,
    overrides utils.SESSION_PATH for test isolation,
    and cleans up after all tests in the session.
    """
    temp_dir = Path(tempfile.mkdtemp())
    utils.SESSION_PATH = temp_dir
    yield temp_dir
    shutil.rmtree(temp_dir)


@pytest.fixture(scope="module")
def duck_conn(session_path):
    """
    Provides a DuckDB connection for testing, ensuring schemas and tables are created.
    Depends on session_path fixture to ensure session path setup.
    """
    conn = connect_to_duckdb()
    # Clean and recreate test tables
    conn.execute("DROP TABLE IF EXISTS leap_raw.event_session")
    conn.execute("DROP TABLE IF EXISTS leap_stage.fact_session")
    conn.execute("CREATE SCHEMA IF NOT EXISTS leap_raw")
    conn.execute("CREATE SCHEMA IF NOT EXISTS leap_stage")
    conn.execute("""
        CREATE TABLE leap_raw.event_session (
            session_id TEXT,
            raw_response TEXT,
            created_at TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE TABLE leap_stage.fact_session (
            player_id TEXT,
            session_id TEXT,
            event_date_time TIMESTAMP,
            country TEXT,
            event_length_seconds INTEGER,
            kills INTEGER,
            deaths INTEGER
        )
    """)
    yield conn
    conn.close()


def test_write_session_to_disk_and_duckdb(duck_conn, session_path):
    """
    Tests writing a game session JSON payload to disk and insertion into DuckDB leap_raw.event_session table.

    Verifies:
    - JSON file created in session_path
    - JSON structure uses snake_case keys (session_id, player_id, team_id, position_x/y/z)
    - DuckDB row exists with matching session_id and correct JSON data
    """
    session_id = str(uuid.uuid4())
    session_start = datetime.now(timezone.utc)
    session_end = session_start + timedelta(minutes=30)

    heartbeat_data = [
        {
            "player_id": "abc123",
            "session_id": session_id,
            "team_id": "redTeam",
            "timestamp": session_start.isoformat(),
            "position_x": 1.1,
            "position_y": 2.2,
            "position_z": 3.3
        }
    ]

    session_json = {
        "session_id": session_id,
        "start_time": session_start.isoformat(),
        "end_time": session_end.isoformat(),
        "heartbeats": heartbeat_data,
    }

    # ✅ Write to disk and DuckDB
    write_json_record_to_duckdb(
        duck_conn=duck_conn,
        schema="leap_raw",
        table="event_session",
        record_id_col="session_id",
        record_id=session_id,
        json_obj=session_json,
        directory=session_path,
        created_at_col="created_at",
        write_to_db=True,
    )

    # ✅ Check JSON file creation
    files = list(session_path.glob(f"{session_id}_*.json"))
    assert len(files) == 1, "Expected exactly one JSON file for the session"
    json_file = files[0]
    assert json_file.exists()

    with open(json_file, "r") as f:
        payload = json.load(f)

    assert "session_id" in payload
    assert payload["session_id"] == session_id
    assert "heartbeats" in payload
    hb = payload["heartbeats"][0]
    assert hb["player_id"] == "abc123"
    assert hb["session_id"] == session_id
    assert hb["team_id"] == "redTeam"
    assert hb["position_x"] == 1.1
    assert hb["position_y"] == 2.2
    assert hb["position_z"] == 3.3

    # ✅ Verify DuckDB insertion
    row = duck_conn.execute(
        "SELECT * FROM leap_raw.event_session WHERE session_id = ?", (session_id,)
    ).fetchone()

    assert row is not None
    assert row[0] == session_id
    stored_json = json.loads(row[1])
    assert stored_json["session_id"] == session_id


def test_stage_session(duck_conn):
    """
    Tests inserting a session summary DataFrame into DuckDB leap_stage.fact_session table.

    Verifies:
    - Table created and data inserted correctly
    - Data matches input snake_case keys and values
    """
    df = pd.DataFrame([
        {
            "player_id": "player001",
            "session_id": "sess001",
            "event_date_time": datetime.now(timezone.utc).isoformat(),
            "country": "US",
            "event_length_seconds": 1800,
            "kills": 7,
            "deaths": 5
        }
    ])

    write_dataframe_to_table(
        duck_conn=duck_conn,
        schema="leap_stage",
        table="fact_session",
        df=df,
        primary_key=None,
        replace=False,
    )

    rows = duck_conn.execute("SELECT * FROM leap_stage.fact_session").fetchall()
    assert len(rows) == 1
    row = rows[0]

    assert row[0] == "player001"
    assert row[1] == "sess001"
    assert row[3] == "US"
    assert row[4] == 1800
    assert row[5] == 7
    assert row[6] == 5