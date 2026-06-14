import sqlite3
import random
from datetime import datetime
conn = sqlite3.connect("rideshare.db")

cursor = conn.cursor()
cursor.execute(
    "DELETE FROM payments"
)

conn.commit()

cursor.execute(
    """
    SELECT
        trip_id,
        final_fare,
        trip_status,
        trip_end_time
    FROM trips
    """
)

trips = cursor.fetchall()
payment_id = 1

for trip in trips:
    trip_id = trip[0]

    final_fare = trip[1]

    trip_status = trip[2]

    trip_end_time = trip[3]

    payment_method = random.choices(
        [
            "UPI",
            "Credit Card",
            "Debit Card",
            "Cash"
        ],
        weights=[
            65,
            20,
            10,
            5
        ]
    )[0]
    if trip_status == "completed":

        payment_status = "SUCCESS"

        payment_amount = final_fare

    elif trip_status == "cancelled":

        payment_status = "REFUNDED"

        payment_amount = 0

    else:

        payment_status = "PENDING"

        payment_amount = 0
    payment_timestamp = trip_end_time
    updated_at = str(
        datetime.now()
    )
    cursor.execute(
        """
        INSERT INTO payments
        (
            payment_id,
            trip_id,
            payment_method,
            payment_amount,
            payment_status,
            payment_timestamp,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            payment_id,
            trip_id,
            payment_method,
            payment_amount,
            payment_status,
            payment_timestamp,
            updated_at
        )
    )

    if payment_id % 10000 == 0:

        print(
            f"{payment_id} payments loaded..."
        )
    payment_id += 1
conn.commit()

cursor.execute(
    "SELECT COUNT(*) FROM payments"
)

print(
    "Payments Loaded:",
    cursor.fetchone()[0]
)

conn.close()