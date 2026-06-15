from fastapi import FastAPI, HTTPException, Depends
from dotenv import load_dotenv
import os 
from auth import create_access_token, verify_token
from database import get_connection
from security import get_current_client

load_dotenv()

CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
print(CLIENT_ID, CLIENT_SECRET)
app = FastAPI()

@app.get('/')
def main():
    return {
        "message": "Welcome to Rideshare Analytics Platform API"
    }

@app.post('/token')
def generate_token(client_id : str, client_secret : str):
    if client_id != CLIENT_ID or client_secret != CLIENT_SECRET:
        raise HTTPException(status_code = 401, detail = "Invalid Credentials")
    token = create_access_token(client_id)
    return {
        "access_token" : token,
        "token_type" : "bearer"
    }
    

@app.get('/drivers')
def get_drivers(limit : int = 100, offset : int = 0, client = Depends(get_current_client)):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        select * from drivers limit ? offset ?
        """,
        (limit,offset)
    )
    drivers = cursor.fetchall()
    conn.close()
    return drivers

@app.get('/riders')
def get_riders(limit : int = 100, offset : int = 0, client = Depends(get_current_client)):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        select * from riders limit ? offset ?
        """, (limit, offset)
    )
    riders = cursor.fetchall()
    conn.close()
    return riders

@app.get('/trips')
def get_trips(limit : int = 100, offset : int =0, client = Depends(get_current_client)):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        select * from trips limit ? offset ? 
        """, (limit, offset))
    trips = cursor.fetchall()
    conn.close()
    return trips

@app.get('/payments')
def get_payments(limit : int =100, offset : int=0, client = Depends(get_current_client)):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""select * from payments limit ? offset ?""", (limit,offset))
    payments = cursor.fetchall()
    conn.close()
    return payments

