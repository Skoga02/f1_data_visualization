"""
utils/openf1.py

Central DRY module for all OpenF1 API calls.
Imported by all fetch scripts in the project.

Usage:
    from utils.openf1 import fetch_openf1, add_driver_session_key
"""

import time
import requests
import pandas as pd

BASE_URL = "https://api.openf1.org/v1"


def fetch_openf1(endpoint: str, params: dict = {}, retries: int = 3) -> pd.DataFrame:
    """
    Fetches data from the OpenF1 API and returns a pandas DataFrame.
    Retries up to 3 times with increasing wait time if rate-limited (429).
    """
    url = f"{BASE_URL}/{endpoint}"
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


def add_driver_session_key(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a driver_session_key column.
    Format: driver_number_session_key (e.g. 44_9912)
    """
    df["driver_session_key"] = (
        df["driver_number"].astype(str) + "_" + df["session_key"].astype(str)
    )
    return df


def get_monza_sessions(
    session_types: list = ["Race"], year_max: int = 2025
) -> pd.DataFrame:
    """
    Fetches Monza sessions filtered by type and year.
    session_types: ["Race"], ["Qualifying"], ["Race", "Qualifying"]
    """
    sessions_df = fetch_openf1("sessions", {"location": "Monza"})
    return sessions_df[
        (sessions_df["session_name"].isin(session_types))
        & (sessions_df["year"] <= year_max)
    ].reset_index(drop=True)
