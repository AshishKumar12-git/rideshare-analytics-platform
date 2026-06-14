import sqlite3
import random
from datetime import datetime, timedelta

conn = sqlite3.connect("rideshare.db")

cursor = conn.cursor()

cities = [
    "Bangalore",
    "Mumbai",
    "Delhi",
    "Hyderabad",
    "Pune",
    "Chennai"
]
driver_weights = []
for driver_id in range(1, 151):
    driver_weights.append((driver_id, 10))

for driver_id in range(151, 501):
    driver_weights.append((driver_id, 5))

for driver_id in range(501, 1001):
    driver_weights.append((driver_id, 1))

driver_ids = [x[0] for x in driver_weights]

weights = [x[1] for x in driver_weights]

selected_driver = random.choices(
    driver_ids,
    weights=weights,
    k=1
)[0]

sample = random.choices(
    driver_ids,
    weights=weights,
    k=10000
)
start_date = datetime(
    2024,
    1,
    1
)

end_date = datetime(
    2026,
    6,
    30
)
cursor.execute(
    "DELETE FROM trips"
)

conn.commit()

for trip_id in range(1, 100001):

    driver_id = random.choices(
        driver_ids,
        weights=weights,
        k=1
    )[0]

    rider_id = random.randint(
        1,
        10000
    )

    trip_type = random.choices(
        ["mini", "premium"],
        weights=[80, 20]
    )[0]

    pickup_city = random.choices(
        cities,
        weights=[35, 25, 15, 10, 10, 5],
        k=1
    )[0]

    drop_city = pickup_city

    distance_bucket = random.choices(
        ["short", "medium", "long", "very_long"],
        weights=[40, 40, 15, 5],
        k=1
    )[0]

    if distance_bucket == "short":
        distance_km = round(random.uniform(1, 5), 2)

    elif distance_bucket == "medium":
        distance_km = round(random.uniform(5, 15), 2)

    elif distance_bucket == "long":
        distance_km = round(random.uniform(15, 30), 2)

    else:
        distance_km = round(random.uniform(30, 60), 2)

    trip_start_time = start_date + timedelta(
        seconds=random.randint(
            0,
            int(
                (end_date - start_date).total_seconds()
            )
        )
    )

    average_speed = random.uniform(
        20,
        40
    )

    trip_duration_hours = (
        distance_km /
        average_speed
    )

    trip_end_time = (
        trip_start_time +
        timedelta(
            hours=trip_duration_hours
        )
    )

    available_drivers = random.randint(
        100,
        500
    )

    trip_requests = random.randint(
        50,
        600
    )

    if trip_requests > available_drivers:

        if trip_type == "mini":
            surge_multiplier = 1.25

        else:
            surge_multiplier = 1.50

    else:

        surge_multiplier = 1.0

    if trip_type == "mini":

        base_fare = round(
            distance_km * 24,
            2
        )

    else:

        base_fare = round(
            distance_km * 28,
            2
        )

    final_fare = round(
        base_fare * surge_multiplier,
        2
    )

    trip_rating = random.choices(
        [5, 4, 3, 2, 1],
        weights=[60, 25, 10, 4, 1],
        k=1
    )[0]

    trip_status = random.choices(
        [
            "completed",
            "cancelled",
            "in_progress"
        ],
        weights=[
            92,
            6,
            2
        ],
        k=1
    )[0]

    updated_at = str(
        datetime.now()
    )

    if trip_status == "cancelled":

        trip_rating = None

        base_fare = 0

        final_fare = 0

    elif trip_status == "in_progress":

        trip_rating = None
    cursor.execute(
        """
        INSERT INTO trips
        (
            trip_id,
            driver_id,
            rider_id,
            pickup_city,
            drop_city,
            trip_type,
            trip_start_time,
            trip_end_time,
            distance_km,
            base_fare,
            surge_multiplier,
            final_fare,
            trip_rating,
            trip_status,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            trip_id,
            driver_id,
            rider_id,
            pickup_city,
            drop_city,
            trip_type,
            str(trip_start_time),
            str(trip_end_time),
            distance_km,
            base_fare,
            surge_multiplier,
            final_fare,
            trip_rating,
            trip_status,
            updated_at
        )
    )

    if trip_id % 10000 == 0:

        print(
            f"{trip_id} trips loaded..."
        )

conn.commit()

cursor.execute(
    "SELECT COUNT(*) FROM trips"
)

print(
    "Trips Loaded:",
    cursor.fetchone()[0]
)

conn.close()

print(
    "Trip generation completed successfully"
)
