from fastapi import FastAPI, HTTPException, Depends
from dotenv import load_dotenv
import os

from auth import create_access_token
from database import get_connection
from security import get_current_client

load_dotenv()

CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

app = FastAPI()


@app.get("/")
def main():
    return {
        "message": "Welcome to Rideshare Analytics Platform API"
    }


@app.post("/token")
def generate_token(
    client_id: str,
    client_secret: str
):

    if (
        client_id != CLIENT_ID
        or client_secret != CLIENT_SECRET
    ):
        raise HTTPException(
            status_code=401,
            detail="Invalid Credentials"
        )

    token = create_access_token(client_id)

    return {
        "access_token": token,
        "token_type": "bearer"
    }


@app.get("/drivers")
def get_drivers(
    limit: int = 100,
    offset: int = 0,
    updated_after: str = None,
    client=Depends(get_current_client)
):

    conn = get_connection()
    cursor = conn.cursor()

    if updated_after:

        cursor.execute(
            """
            SELECT *
            FROM drivers
            WHERE updated_at > ?
            ORDER BY updated_at
            LIMIT ?
            OFFSET ?
            """,
            (
                updated_after,
                limit,
                offset
            )
        )

    else:

        cursor.execute(
            """
            SELECT *
            FROM drivers
            LIMIT ?
            OFFSET ?
            """,
            (
                limit,
                offset
            )
        )

    drivers = cursor.fetchall()

    conn.close()

    return drivers


@app.get("/riders")
def get_riders(
    limit: int = 100,
    offset: int = 0,
    updated_after: str = None,
    client=Depends(get_current_client)
):

    conn = get_connection()
    cursor = conn.cursor()

    if updated_after:

        cursor.execute(
            """
            SELECT *
            FROM riders
            WHERE updated_at > ?
            ORDER BY updated_at
            LIMIT ?
            OFFSET ?
            """,
            (
                updated_after,
                limit,
                offset
            )
        )

    else:

        cursor.execute(
            """
            SELECT *
            FROM riders
            LIMIT ?
            OFFSET ?
            """,
            (
                limit,
                offset
            )
        )

    riders = cursor.fetchall()

    conn.close()

    return riders


@app.get("/trips")
def get_trips(
    limit: int = 100,
    offset: int = 0,
    updated_after: str = None,
    client=Depends(get_current_client)
):

    conn = get_connection()
    cursor = conn.cursor()

    if updated_after:

        cursor.execute(
            """
            SELECT *
            FROM trips
            WHERE updated_at > ?
            ORDER BY updated_at
            LIMIT ?
            OFFSET ?
            """,
            (
                updated_after,
                limit,
                offset
            )
        )

    else:

        cursor.execute(
            """
            SELECT *
            FROM trips
            LIMIT ?
            OFFSET ?
            """,
            (
                limit,
                offset
            )
        )

    trips = cursor.fetchall()

    conn.close()

    return trips


@app.get("/payments")
def get_payments(
    limit: int = 100,
    offset: int = 0,
    updated_after: str = None,
    client=Depends(get_current_client)
):

    conn = get_connection()
    cursor = conn.cursor()

    if updated_after:

        cursor.execute(
            """
            SELECT *
            FROM payments
            WHERE updated_at > ?
            ORDER BY updated_at
            LIMIT ?
            OFFSET ?
            """,
            (
                updated_after,
                limit,
                offset
            )
        )

    else:

        cursor.execute(
            """
            SELECT *
            FROM payments
            LIMIT ?
            OFFSET ?
            """,
            (
                limit,
                offset
            )
        )

    payments = cursor.fetchall()

    conn.close()

    return payments