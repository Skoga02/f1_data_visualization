"""
data/fetch_drivers.py
Fetches driver data for Race and Qualifying sessions at Monza 2023-2025.
"""
import time
import pandas as pd
import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from utils.openf1 import fetch_openf1, add_driver_session_key, get_monza_sessions

os.makedirs("data/csv", exist_ok=True)

# ── 1. Get sessions ───────────────────────────────────────────────────────────
sessions = get_monza_sessions(session_types=["Race", "Qualifying"])
print(sessions[["session_key", "session_name", "year"]])

# ── 2. Fetch drivers per session ──────────────────────────────────────────────
all_drivers = []
for _, session in sessions.iterrows():
    key, year = session["session_key"], session["year"]
    print(f"Fetching drivers for session {key}...")
    df = fetch_openf1("drivers", {"session_key": key}).assign(
        session_key=key,
        session_name=session["session_name"],
        year=year
    )
    all_drivers.append(df)
    time.sleep(1)

drivers_df = pd.concat(all_drivers, ignore_index=True)
drivers_df = add_driver_session_key(drivers_df)

drivers_df.to_csv("data/csv/drivers_clean_updated.csv", index=False)
print(f"Done! {len(drivers_df)} rows saved.")
