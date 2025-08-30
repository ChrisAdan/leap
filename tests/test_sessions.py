import pytest
from datetime import datetime

from utils import HEARTBEAT_INTERVAL
from heartbeat_generator import simulate_heartbeats


@pytest.fixture
def basic_inputs():
    player_ids = ["p1", "p2"]
    session_id = "session_xyz"
    team_ids = {"p1": "team_red", "p2": "team_blue"}
    session_start = datetime(2025, 1, 1, 12, 0, 0)
    speed_map = {"p1": 1.0, "p2": 2.0}
    durations = {"p1": 180, "p2": 60}  # p1 lasts 3 mins, p2 lasts 1 min
    behavior_map = {"p1": "lorentzian", "p2": "bezier"}

    return player_ids, session_id, team_ids, session_start, speed_map, durations, behavior_map


def test_simulate_heartbeats_structure(basic_inputs):
    """
    Ensure heartbeat structure is correct and includes all required fields.
    """
    result = simulate_heartbeats(*basic_inputs)

    assert isinstance(result, list)
    assert all(isinstance(hb, dict) for hb in result)

    required_keys = {
        "timestamp", "player_id", "session_id", "team_id", "position_x", "position_y", "position_z"
    }
    for hb in result:
        assert required_keys.issubset(hb.keys())


def test_heartbeat_counts_match_duration(basic_inputs):
    """
    Validate that each player's number of heartbeats matches their session duration.
    """
    player_ids, _, _, _, _, durations, _ = basic_inputs
    result = simulate_heartbeats(*basic_inputs)

    counts = {pid: 0 for pid in player_ids}
    for hb in result:
        counts[hb["player_id"]] += 1

    for pid in player_ids:
        expected = durations[pid] // HEARTBEAT_INTERVAL
        assert counts[pid] == expected


def test_positions_within_bounds(basic_inputs):
    """
    Ensure all generated positions are within the GRID_BOUNDS.
    """
    from utils import GRID_BOUNDS
    lower, upper = GRID_BOUNDS

    result = simulate_heartbeats(*basic_inputs)
    for hb in result:
        assert lower <= hb["position_x"] <= upper
        assert lower <= hb["position_y"] <= upper
        assert lower <= hb["position_z"] <= upper


def test_no_duplicate_positions_at_start(basic_inputs):
    """
    Check that players start at different positions (distance ≥ 1.0).
    """
    from heartbeat_generator import clamp_to_bounds
    import numpy as np

    # Patch simulate_heartbeats to only emit the first beat
    player_ids, session_id, team_ids, session_start, speed_map, durations, behavior_map = basic_inputs
    short_durations = {pid: HEARTBEAT_INTERVAL for pid in player_ids}

    heartbeats = simulate_heartbeats(
        player_ids, session_id, team_ids, session_start,
        speed_map, short_durations, behavior_map
    )

    positions = [
        (hb["position_x"], hb["position_y"], hb["position_z"])
        for hb in heartbeats
    ]
    dist = np.linalg.norm(np.array(positions[0]) - np.array(positions[1]))
    assert dist >= 1.0


def test_mixed_behavior_paths(basic_inputs):
    """
    Confirm that players with different behavior functions produce distinct movement paths.
    """
    result = simulate_heartbeats(*basic_inputs)

    p1_positions = [(hb["position_x"], hb["position_y"], hb["position_z"]) for hb in result if hb["player_id"] == "p1"]
    p2_positions = [(hb["position_x"], hb["position_y"], hb["position_z"]) for hb in result if hb["player_id"] == "p2"]

    assert len(set(p1_positions)) > 1
    assert len(set(p2_positions)) > 1
    assert p1_positions != p2_positions