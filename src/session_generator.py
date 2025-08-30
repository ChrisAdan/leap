import uuid
import random
from pathlib import Path
from datetime import datetime, timedelta
import numpy as np
import pandas as pd

from heartbeat_generator import simulate_heartbeats, STEP_FUNCTIONS
from loader import write_json_record_to_duckdb, clear_old_data, write_dataframe_to_table
from utils import (
    SESSION_PATH,
    SESSION_MAX_DURATION_SECONDS,
    MIN_TEAMS,
    MAX_TEAMS,
    MIN_PLAYERS_PER_TEAM,
    MAX_PLAYERS_PER_TEAM,
    MIN_DAILY_SESSIONS,
    MAX_DAILY_SESSIONS
)

def clear_sessions(duck_conn):
    clear_old_data(duck_conn, level="sessions")

def get_players_grouped_by_day(signins_df):
    return signins_df.groupby("date")["player_id"].apply(list)

def assign_sessions_per_player(players, min_sessions=MIN_DAILY_SESSIONS, max_sessions=MAX_DAILY_SESSIONS):
    """
    Assign a random number of sessions per player for the day.
    """
    return {pid: random.randint(min_sessions, max_sessions) for pid in players}

def create_sessions_schedule(player_sessions_map):
    """
    Given a dict player->num_sessions,
    create a schedule mapping session_id -> list_of_players,
    so all players appear in exactly their assigned sessions.
    """
    # Total sessions needed = max number of sessions any player has to do,
    # but can be more to accommodate team sizes, we'll dynamically create session buckets.
    
    # Build a list of players repeated by their session counts
    players_needed = []
    for pid, count in player_sessions_map.items():
        players_needed.extend([pid] * count)
    random.shuffle(players_needed)

    sessions = []  # list of player lists

    # To build sessions, greedily pack players into sessions with max team size constraint:
    max_session_size = MAX_TEAMS * MAX_PLAYERS_PER_TEAM

    # We'll fill sessions one by one:
    while players_needed:
        session_players = players_needed[:max_session_size]
        players_needed = players_needed[max_session_size:]
        sessions.append(session_players)

    # sessions is list of lists, each is one session's players
    return sessions

def generate_team_structure(players):
    """
    Given a list of players assigned to a session,
    create teams randomly with players_per_team.
    """
    num_teams = np.random.randint(MIN_TEAMS, MAX_TEAMS + 1)
    players_per_team = np.random.randint(MIN_PLAYERS_PER_TEAM, MAX_PLAYERS_PER_TEAM + 1)
    total_players_needed = num_teams * players_per_team

    # If not enough players, adjust team count or players per team:
    if len(players) < total_players_needed:
        # Try fewer teams or fewer players per team to fit:
        # Greedy approach: reduce teams first
        while num_teams > 1 and num_teams * players_per_team > len(players):
            num_teams -= 1
        # If still too many, reduce players per team
        while players_per_team > 1 and num_teams * players_per_team > len(players):
            players_per_team -= 1

    total_players = num_teams * players_per_team
    players_selected = players[:total_players]

    team_ids = [str(uuid.uuid4()) for _ in range(num_teams)]
    teams = {
        team_ids[i]: players_selected[i * players_per_team : (i + 1) * players_per_team]
        for i in range(num_teams)
    }

    return players_selected, team_ids, teams

def generate_session_times(date):
    session_start = datetime.combine(pd.to_datetime(date), datetime.min.time()) + timedelta(
        seconds=random.randint(0, 60 * 60 * 12)
    )
    session_end = session_start + timedelta(seconds=SESSION_MAX_DURATION_SECONDS)
    return session_start, session_end

def assign_behavior_and_speed(players):
    behavior_types = list(STEP_FUNCTIONS.keys())
    behavior_map = {pid: random.choice(behavior_types) for pid in players}
    speed_map = {pid: np.random.randint(1, 4) for pid in players}
    durations = {pid: np.random.randint(120, SESSION_MAX_DURATION_SECONDS + 1) for pid in players}
    return behavior_map, speed_map, durations

def generate_kill_death_distribution(players):
    total_kills = np.random.randint(10, 60)
    total_deaths = total_kills
    kill_dist = np.random.multinomial(total_kills, np.random.dirichlet(np.ones(len(players))))
    death_dist = np.random.multinomial(total_deaths, np.random.dirichlet(np.ones(len(players))))
    return kill_dist, death_dist

def write_session_to_disk(
    session_id: str,
    session_start: datetime,
    session_end: datetime,
    heartbeat_data: list,
    duck_conn,
    session_dir: Path,
):
    if session_id is None:
        print("⚠️ No session_id provided, skipping write_session_to_disk.")
        return

    session_json = {
        "session_id": session_id,
        "start_time": session_start.isoformat(),
        "end_time": session_end.isoformat(),
        "heartbeats": heartbeat_data,
    }

    write_json_record_to_duckdb(
        duck_conn=duck_conn,
        schema="leap_raw",
        table="event_session",
        record_id_col="session_id",
        record_id=session_id,
        json_obj=session_json,
        directory=session_dir,
        created_at_col="created_at",
        write_to_db=True,
    )

def save_session_summaries(summary_df, duck_conn):
    write_dataframe_to_table(
        duck_conn=duck_conn,
        schema="leap_stage",
        table="fact_session",
        df=summary_df,
        primary_key=None,
        replace=True,
    )

def generate_sessions(
    signins_df,
    country_map,
    duck_conn,
    session_dir: Path = SESSION_PATH,
    min_sessions_per_player=MIN_DAILY_SESSIONS,
    max_sessions_per_player=MAX_DAILY_SESSIONS,
):
    session_dir.mkdir(parents=True, exist_ok=True)

    clear_sessions(duck_conn)
    summaries = []

    players_by_day = get_players_grouped_by_day(signins_df)

    for date, players_today in players_by_day.items():
        print(f"Generating sessions for {date}: {len(players_today)} players.")

        # Assign how many sessions each player plays this day
        player_sessions_map = assign_sessions_per_player(
            players_today, min_sessions_per_player, max_sessions_per_player
        )

        # Create sessions schedule (list of player lists)
        sessions_schedule = create_sessions_schedule(player_sessions_map)
        print(f"Total sessions to generate: {len(sessions_schedule)}")

        for session_players in sessions_schedule:
            session_start, session_end = generate_session_times(date)

            players_selected, _, teams = generate_team_structure(session_players)
            behavior_map, speed_map, durations = assign_behavior_and_speed(players_selected)

            player_to_team = {pid: tid for tid, players in teams.items() for pid in players}

            session_id = str(uuid.uuid4())

            heartbeat_data = simulate_heartbeats(
                player_ids=players_selected,
                session_id=session_id,
                team_ids=player_to_team,
                session_start=session_start,
                speed_map=speed_map,
                durations=durations,
                behavior_map=behavior_map,
            )

            kill_dist, death_dist = generate_kill_death_distribution(players_selected)

            for i, pid in enumerate(players_selected):
                summaries.append(
                    {
                        "player_id": pid,
                        "session_id": session_id,
                        "event_datetime": session_end.isoformat(),
                        "country": country_map.get(pid, "Unknown"),
                        "event_length_seconds": durations[pid],
                        "kills": kill_dist[i],
                        "deaths": death_dist[i],
                    }
                )

            write_session_to_disk(
                session_id=session_id,
                session_start=session_start,
                session_end=session_end,
                heartbeat_data=heartbeat_data,
                duck_conn=duck_conn,
                session_dir=session_dir,
            )

    summary_df = pd.DataFrame(summaries)
    save_session_summaries(summary_df, duck_conn)