from utils.openf1 import fetch_openf1, add_driver_session_key, get_monza_sessions

# Test 1 — fetch sessions
print("Testing get_monza_sessions...")
sessions = get_monza_sessions(session_types=["Race"])
print(f"Found {len(sessions)} race sessions")
print(sessions[["session_key", "year"]])

# Test 2 — fetch drivers
print("\nTesting fetch_openf1...")
drivers = fetch_openf1("drivers", {"session_key": 9912})
print(f"Found {len(drivers)} drivers")

# Test 3 — add driver_session_key
print("\nTesting add_driver_session_key...")
drivers = add_driver_session_key(drivers)
print(f"driver_session_key added: {drivers['driver_session_key'].iloc[0]}")
