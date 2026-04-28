import os
import sys
import time

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from utils.openf1 import fetch_openf1, add_driver_session_key, get_monza_sessions

os.makedirs("data/csv", exist_ok=True)

# ── 1. Get sessions ───────────────────────────────────────────────────────────
sessions = get_monza_sessions(session_types=["Race", "Qualifying"])
print(sessions[["session_key", "session_name", "year"]])

# ── 2. Fetch laps and final positions per session ─────────────────────────────
all_laps = []
all_positions = []

for _, session in sessions.iterrows():
    key = session["session_key"]
    year = session["year"]
    session_name = session["session_name"]
    print(f"Fetching {year} Monza {session_name} (session_key: {key})...")

    # --- Laps ---
    laps = fetch_openf1("laps", {"session_key": key})
    if not laps.empty:
        laps = laps.assign(session_key=key, year=year)
        all_laps.append(laps)
    time.sleep(1.0)

    # --- Final positions: take the last position record per driver in the session ---
    pos = fetch_openf1("position", {"session_key": key})
    if not pos.empty:
        pos["date"] = pd.to_datetime(pos["date"])
        final_pos = (
            pos.sort_values("date")
            .groupby("driver_number", as_index=False)
            .tail(1)[["session_key", "driver_number", "position"]]
            .assign(session_type=session_name, year=year)
        )
        all_positions.append(final_pos)
    time.sleep(1.0)

# ── 3. Combine ────────────────────────────────────────────────────────────────
laps_df = pd.concat(all_laps, ignore_index=True)
positions_df = pd.concat(all_positions, ignore_index=True)

# ── 4. Add driver_session_key to both ─────────────────────────────────────────
laps_df = add_driver_session_key(laps_df)
positions_df = add_driver_session_key(positions_df)

# ── 5. Select the columns we actually need, in a fixed order ──────────────
laps_cols = [
    "meeting_key",
    "session_key",
    "driver_number",
    "lap_number",
    "date_start",
    "duration_sector_1",
    "duration_sector_2",
    "duration_sector_3",
    "i1_speed",
    "i2_speed",
    "is_pit_out_lap",
    "lap_duration",
    "segments_sector_1",
    "segments_sector_2",
    "segments_sector_3",
    "st_speed",
    "driver_session_key",
]
laps_df = laps_df[[c for c in laps_cols if c in laps_df.columns]]

positions_cols = [
    "session_key",
    "driver_number",
    "position",
    "session_type",
    "year",
    "driver_session_key",
]
positions_df = positions_df[positions_cols]

# ── 6. Sort by natural keys (session -> driver ->lap / position)  ────────────────────────────
laps_df = laps_df.sort_values(
    ["session_key", "driver_number", "lap_number"]
).reset_index(drop=True)
positions_df = positions_df.sort_values(
    ["year", "session_type", "position"]
).reset_index(drop=True)

# ── 7. Save ───────────────────────────────────────────────────────────────────
laps_df.to_csv("data/csv/laps_clean_updated.csv", index=False)
positions_df.to_csv("data/csv/final_positions.csv", index=False)

print(f"laps_clean_updated.csv  → {len(laps_df)} rows")
print(f"final_positions.csv     → {len(positions_df)} rows")
