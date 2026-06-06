import sqlite3

conn = sqlite3.connect("rideshare.db")

cursor = conn.cursor()

print("Database Created Successfully")


cursor.execute("""
CREATE TABLE IF NOT EXISTS drivers (
    driver_id INTEGER PRIMARY KEY,
    driver_name TEXT NOT NULL,
    city TEXT NOT NULL,
    vehicle_type TEXT NOT NULL,
    join_date TEXT,
    updated_at TEXT
)
""")
cursor.execute("""
CREATE TABLE IF NOT EXISTS riders (
    rider_id INTEGER PRIMARY KEY,
    rider_name TEXT NOT NULL,
    city TEXT NOT NULL,
    signup_date TEXT,
    updated_at TEXT
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS trips (
    trip_id INTEGER PRIMARY KEY,

    driver_id INTEGER,
    rider_id INTEGER,

    pickup_city TEXT,
    drop_city TEXT,

    trip_type TEXT,

    trip_start_time TEXT,
    trip_end_time TEXT,

    distance_km REAL,

    base_fare REAL,
    surge_multiplier REAL,
    final_fare REAL,

    trip_rating REAL,
    trip_status TEXT,

    updated_at TEXT,

    FOREIGN KEY(driver_id)
        REFERENCES drivers(driver_id),

    FOREIGN KEY(rider_id)
        REFERENCES riders(rider_id)
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS payments (
    payment_id INTEGER PRIMARY KEY,

    trip_id INTEGER,

    payment_method TEXT,
    payment_amount REAL,

    payment_status TEXT,

    payment_timestamp TEXT,

    updated_at TEXT,

    FOREIGN KEY(trip_id)
        REFERENCES trips(trip_id)
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS driver_status_events (
    event_id INTEGER PRIMARY KEY,

    driver_id INTEGER,

    event_type TEXT,
    status TEXT,
    city TEXT,

    event_timestamp TEXT,

    FOREIGN KEY(driver_id)
        REFERENCES drivers(driver_id)
)
""")
conn.commit()

conn.close()

print("All tables created successfully")
