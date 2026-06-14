import sqlite3
import random
from datetime import datetime, timedelta

conn = sqlite3.connect("rideshare.db")

cursor = conn.cursor()

cursor.execute(
    "DELETE FROM driver_status_events"
)

conn.commit()

cities = [
    "Bangalore",
    "Mumbai",
    "Delhi",
    "Hyderabad",
    "Pune",
    "Chennai"
]

start_date = datetime.now() - timedelta(days=90)

for event_id in range(1, 50001):

    driver_id = random.randint(
        1,
        1000
    )

    status = random.choices(
        [
            "AVAILABLE",
            "BUSY",
            "OFFLINE"
        ],
        weights=[
            50,
            35,
            15
        ],
        k=1
    )[0]

    event_type = status

    city = random.choice(
        cities
    )

    event_timestamp = start_date + timedelta(
        seconds=random.randint(
            0,
            90 * 24 * 60 * 60
        )
    )

    cursor.execute(
        """
        INSERT INTO driver_status_events
        (
            event_id,
            driver_id,
            event_type,
            status,
            city,
            event_timestamp
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            event_id,
            driver_id,
            event_type,
            status,
            city,
            str(event_timestamp)
        )
    )

    if event_id % 10000 == 0:

        print(
            f"{event_id} events loaded..."
        )

conn.commit()

cursor.execute(
    "SELECT COUNT(*) FROM driver_status_events"
)

print(
    "Events Loaded:",
    cursor.fetchone()[0]
)

conn.close()

print(
    "Driver status events loaded successfully"
)