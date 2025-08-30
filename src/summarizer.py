import numpy as np
import pandas as pd

from loader import write_dataframe_to_table


def summarize_session(player_ids, session_id, session_end, durations, country_map, duck_conn):
    """
    Assigns kills/deaths and stages session summary.
    """
    total_kills = np.random.randint(10, 60)
    total_deaths = total_kills  # balance

    kill_dist = np.random.multinomial(total_kills, np.random.dirichlet(np.ones(len(player_ids))))
    death_dist = np.random.multinomial(total_deaths, np.random.dirichlet(np.ones(len(player_ids))))

    summary_rows = []

    for i, pid in enumerate(player_ids):
        summary_rows.append({
            "player_id": pid,
            "session_id": session_id,
            "event_datetime": session_end.isoformat(),
            "country": country_map[pid],
            "event_length_seconds": durations[pid],
            "kills": kill_dist[i],
            "deaths": death_dist[i]
        })

    summary_df = pd.DataFrame(summary_rows)
    write_dataframe_to_table(
        duck_conn=duck_conn,
        schema="leap_stage",
        table="fact_session",
        df=summary_df,
        primary_key=None,
        replace=False,
    )