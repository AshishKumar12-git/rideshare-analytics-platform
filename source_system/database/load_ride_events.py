import sqlite3
import random
from datetime import datetime, timedelta

conn = sqlite3.connect("rideshare.db")

cursor = conn.cursor()

cursor.execute(
    "DELETE FROM ride_request_events"
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

for request_id in range(1, 100001):

    rider_id = random.randint(
        1,
        10000
    )

    pickup_city = random.choice(
        cities
    )

    ride_type = random.choices(
        [
            "mini",
            "premium"
        ],
        weights=[
            80,
            20
        ]
    )[0]

    request_status = random.choices(
        [
            "REQUESTED",
            "MATCHED",
            "CANCELLED"
        ],
        weights=[
            15,
            75,
            10
        ]
    )[0]

    request_timestamp = start_date + timedelta(
        seconds=random.randint(
            0,
            90 * 24 * 60 * 60
        )
    )

    updated_at = request_timestamp

    cursor.execute(
        """
        INSERT INTO ride_request_events
        (
            request_id,
            rider_id,
            pickup_city,
            ride_type,
            request_status,
            request_timestamp,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            request_id,
            rider_id,
            pickup_city,
            ride_type,
            request_status,
            str(request_timestamp),
            str(updated_at)
        )
    )

    if request_id % 10000 == 0:

        print(
            f"{request_id} ride requests loaded..."
        )

conn.commit()

cursor.execute(
    "SELECT COUNT(*) FROM ride_request_events"
)

print(
    "Ride Requests Loaded:",
    cursor.fetchone()[0]
)

conn.close()

print(
    "Ride request events loaded successfully"
)