import duckdb
import time
import requests
import pandas as pd


# Fetches data from the OpenF1 API and returns it as a pandas DataFrame.
# Retries up to 3 times with increasing wait time if rate-limited (429 error).
def fetch_openf1(endpoint, params={}, retries=3):
    url = f"https://api.openf1.org/v1/{endpoint}"
    for attempt in range(retries):
        r = requests.get(url, params=params)
        if r.status_code == 429:
            wait = 10 * (attempt + 1)
            print(f"Rate limited, waiting {wait}s...")
            time.sleep(wait)
            continue
        r.raise_for_status()
        return pd.DataFrame(r.json())
    raise Exception(f"Failed after {retries} retries: {endpoint}")


# ── 1. GET RACE SESSIONS ───────────────────────────────────────────────────────

# Fetch all Monza sessions and keep only Race sessions up to 2025
sessions_df = fetch_openf1("sessions", {"location": "Monza"})
race_sessions = sessions_df[
    (sessions_df["session_name"] == "Race") & (sessions_df["year"] <= 2025)
].reset_index(drop=True)

print(race_sessions[["session_key", "session_name", "date_start", "year"]])


# ── 2. FETCH STINTS AND DRIVERS PER YEAR ──────────────────────────────────────

all_stints = []
all_drivers = []

for _, session in race_sessions.iterrows():
    key = session["session_key"]
    year = session["year"]
    print(f"Fetching {year} Monza Race (session_key: {key})...")

    # Stints = each driver's time on one set of tyres
    all_stints.append(fetch_openf1("stints", {"session_key": key}).assign(year=year))
    time.sleep(1.0)

    # Drivers = name, team, and number for each driver in the session
    all_drivers.append(fetch_openf1("drivers", {"session_key": key}).assign(year=year))
    time.sleep(1.0)

# Combine all years into one table
stints_df = pd.concat(all_stints, ignore_index=True)
drivers_df = pd.concat(all_drivers, ignore_index=True)


# ── 3. SAVE TO DUCKDB ─────────────────────────────────────────────────────────

con = duckdb.connect("data/pitstops_monza.duckdb")
con.execute(
    """
    CREATE OR REPLACE TABLE stints AS
    SELECT *, lap_end - lap_start + 1 AS stint_length
    FROM stints_df
"""
)
con.execute("CREATE OR REPLACE TABLE drivers AS SELECT * FROM drivers_df")
con.checkpoint()  # Force save to disk
con.close()
print("Saved to pitstops_monza.duckdb")


# ── 4. CALCULATE PIT STOP LAP PER DRIVER (2025 only) ─────────────────────────

stints_df["stint_length"] = stints_df["lap_end"] - stints_df["lap_start"] + 1

# Pit stop lap = last lap of each stint except the final one
# e.g. if a driver pitted after lap 22, lap_end of stint 1 = 22
pit_laps_df = (
    stints_df[stints_df["year"] == 2025]  # filter to 2025 only
    .groupby("driver_number")
    .apply(lambda x: x.sort_values("stint_number").iloc[:-1]["lap_end"])
    .reset_index(level=0)
    .rename(columns={"lap_end": "pit_lap"})
    .merge(
        drivers_df[
            ["driver_number", "name_acronym", "full_name", "team_name"]
        ].drop_duplicates(),
        on="driver_number",
        how="left",
    )
)


# ── 5. EXPORT CSV FILES FOR POWER BI (2025 only) ─────────────────────────────

# Graf 1: Tyre strategy — one row per stint with compound and lap range
stints_export = stints_df[stints_df["year"] == 2025].merge(
    drivers_df[
        ["driver_number", "name_acronym", "full_name", "team_name"]
    ].drop_duplicates(),
    on="driver_number",
    how="left",
)

# Graf 2: Pit stop lap per driver — when each driver made their stop
pit_laps_df.to_csv("data/csv/powerbi_pit_stops.csv", index=False)
stints_export.to_csv("data/csv/powerbi_stints.csv", index=False)

print("CSV files saved in data/csv/:")
print("  powerbi_pit_stops.csv  -> Pit stop lap per driver (2025)")
print("  powerbi_stints.csv     -> Tyre strategy per stint (2025)")
