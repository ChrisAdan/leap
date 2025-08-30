from datetime import datetime, timedelta
import numpy as np

from utils import HEARTBEAT_INTERVAL, GRID_BOUNDS
from movement.step import lorentzian, bezier, lissajous, perlin

STEP_FUNCTIONS = {
    "lorentzian": lorentzian.step,
    "bezier": bezier.step,
    "lissajous": lissajous.step,
    "perlin": perlin.step,
}

def clamp_to_bounds(x, y, z):
    lower, upper = GRID_BOUNDS
    x = max(min(x, upper), lower)
    y = max(min(y, upper), lower)
    z = max(min(z, upper), lower)
    return x, y, z

def simulate_heartbeats(
    player_ids: list[str],
    session_id: str,
    team_ids: dict[str, str],
    session_start: "datetime.datetime",
    speed_map: dict[str, int],
    durations: dict[str, int],
    behavior_map: dict[str, str],
) -> list[dict]:
    """
    Simulate heartbeat position logs for all players over their session durations.

    Each heartbeat is emitted every HEARTBEAT_INTERVAL seconds,
    including player id, team id, position (X,Y,Z), timestamp, and session id.

    Args:
        player_ids: List of player UUID strings.
        session_id: UUID string for the session.
        team_ids: Mapping of player_id -> team_id.
        session_start: datetime object marking session start.
        speed_map: player_id -> speed int (units per step).
        durations: player_id -> session length in seconds.
        behavior_map: player_id -> movement type string (must be a key in STEP_FUNCTIONS).

    Returns:
        List of heartbeat dicts.
    """

    heartbeats = []
    positions = {}

    # Assign unique random starting positions avoiding collisions (min 1 unit apart)
    attempts = 0
    while len(positions) < len(player_ids):
        candidate_pos = np.random.uniform(*GRID_BOUNDS, size=3)
        collision = any(
            np.linalg.norm(candidate_pos - np.array(pos)) < 1 for pos in positions.values()
        )
        if not collision:
            pid = player_ids[len(positions)]
            positions[pid] = tuple(candidate_pos)
        else:
            attempts += 1
            if attempts > 1000:
                raise RuntimeError("Failed to assign unique start positions without collision.")

    # Generate heartbeats per player
    for pid in player_ids:
        duration = durations[pid]
        speed = speed_map[pid]
        team_id = team_ids[pid]
        behavior = behavior_map[pid]
        step_fn = STEP_FUNCTIONS[behavior]

        x, y, z = positions[pid]
        num_beats = duration // HEARTBEAT_INTERVAL

        for i in range(num_beats):
            ts = session_start + timedelta(seconds=i * HEARTBEAT_INTERVAL)

            x, y, z = step_fn(x, y, z, speed, i)
            x, y, z = clamp_to_bounds(x, y, z)

            heartbeats.append({
                "timestamp": ts.isoformat(),
                "player_id": pid,
                "session_id": session_id,
                "team_id": team_id,
                "position_x": round(x, 3),
                "position_y": round(y, 3),
                "position_z": round(z, 3),
            })

    return heartbeats