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

for rider_id in range(1, 10001):

    rider_name = fake.name()

    city = random.choice(cities)

    signup_date = fake.date_between(
        start_date="-5y",
        end_date="today"
    )

    updated_at = str(datetime.now())

    cursor.execute(
        """
        INSERT INTO riders
        (
            rider_id,
            rider_name,
            city,
            signup_date,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            rider_id,
            rider_name,
            city,
            str(signup_date),
            updated_at
        )
    )
conn.commit()
cursor.execute(
    "SELECT COUNT(*) FROM riders"
)

print(
    "Riders Loaded:",
    cursor.fetchone()[0]
)
cursor.execute("""
SELECT *
FROM riders
LIMIT 5
""")

print(cursor.fetchall())
conn.close()