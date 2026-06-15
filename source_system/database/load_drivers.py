import sqlite3
import random
from faker import Faker
from datetime import datetime

fake = Faker("en_IN")
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
for driver_id in range(1, 1001):

    driver_name = fake.name()

    city = random.choice(cities)

    vehicle_type = random.choices(
        ["mini", "premium"],
        weights=[80, 20]
    )[0]

    join_date = fake.date_between(
        start_date="-3y",
        end_date="today"
    )

    updated_at = str(datetime.now())

    cursor.execute(
        """
        INSERT INTO drivers
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            driver_id,
            driver_name,
            city,
            vehicle_type,
            str(join_date),
            updated_at
        )
    )

conn.commit()
conn.close()