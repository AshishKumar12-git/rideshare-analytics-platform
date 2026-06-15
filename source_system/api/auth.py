from jose import jwt 
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

load_dotenv()

JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY")
ALGORITHM = 'HS256'

def create_access_token(client_id : str):
    expire = datetime.utcnow() + timedelta(minutes=30)
    payload = {
        "sub" : client_id,
        "exp" : expire
    }
    token = jwt.encode(payload, JWT_SECRET_KEY, algorithm = ALGORITHM)
    return token 

def verify_token(token : str):
    payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[ALGORITHM])
    return payload
