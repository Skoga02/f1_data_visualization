import time
import pandas as pd
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from utils.openf1 import fetch_openf1, get_monza_sessions

os.makedirs("data/csv", exist_ok=True)

# ── 1. Get race sessions ───────────────────────────────────────────────────────
race_sessions = get_monza_sessions(session_types=["Race"])
print(race_sessions[["session_key", "session_name", "date_start", "year"]])

# ── 2. Fetch stints, drivers and pit stop times ───────────────────────────────
all_stints, all_drivers, all_pits = [], [], []

for _, session in race_sessions.iterrows():
    key, year = session["session_key"], session["year"]
    print(f"Fetching {year} Monza Race (session_key: {key})...")

    all_stints.append(fetch_openf1("stints", {"session_key": key}).assign(year=year))
    time.sleep(1.0)

    all_drivers.append(fetch_openf1("drivers", {"session_key": key}).assign(year=year))
    time.sleep(1.0)

    pit = fetch_openf1("pit", {"session_key": key})
    if not pit.empty:
        pit["year"] = year
        all_pits.append(pit)
    time.sleep(1.0)

stints_df = pd.concat(all_stints, ignore_index=True)
drivers_df = pd.concat(all_drivers, ignore_index=True)
pits_df = pd.concat(all_pits, ignore_index=True) if all_pits else pd.DataFrame()

# ── 3. Export CSV files for Power BI ──────────────────────────────────────────
stints_df["stint_length"] = stints_df["lap_end"] - stints_df["lap_start"] + 1


# Per-year driver info — teams change between seasons
def get_drivers_year(year):
    return drivers_df[drivers_df["year"] == year][
        ["driver_number", "name_acronym", "full_name", "team_name"]
    ].drop_duplicates()


# stints.csv — one row per stint
stints_export_all = []
for year in [2023, 2024, 2025]:
    s_year = stints_df[stints_df["year"] == year].merge(
        get_drivers_year(year), on="driver_number", how="left"
    )
    stints_export_all.append(s_year)
stints_export = pd.concat(stints_export_all, ignore_index=True)

# pit_with_compound.csv — one row per pit stop with the compound fitted after.
# Pit on lap N → new stint starts lap N+1.
if not pits_df.empty:
    pits_df["stint_lap_start"] = pits_df["lap_number"] + 1
    pit_compound_df = pits_df.merge(
        stints_df[
            ["driver_number", "session_key", "lap_start", "compound", "stint_number"]
        ],
        left_on=["driver_number", "session_key", "stint_lap_start"],
        right_on=["driver_number", "session_key", "lap_start"],
        how="left",
    )
    pit_compound_all = []
    for year in [2023, 2024, 2025]:
        pc_year = pit_compound_df[pit_compound_df["year"] == year].merge(
            get_drivers_year(year), on="driver_number", how="left"
        )
        pit_compound_all.append(pc_year)
    pit_compound_export = pd.concat(pit_compound_all, ignore_index=True)
    pit_compound_export = pit_compound_export.dropna(
        subset=["pit_duration", "compound"]
    )
    pit_compound_export.to_csv("data/csv/pit_with_compound.csv", index=False)

stints_export.to_csv("data/csv/stints.csv", index=False)

print("CSV files saved in data/csv/:")
print("stints.csv")
print("pit_with_compound.csv")
