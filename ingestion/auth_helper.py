import os, requests
from keyvault_helper import get_secret

def get_access_token():
    client_id = get_secret("clientid")
    client_secret = get_secret("clientsecret")
    fastapi_url = os.getenv('FASTAPI_URL')
    response = requests.post(
        f"{fastapi_url}/token",
        params = {
            "client_id" : client_id,
            "client_secret" : client_secret
        }
    )
    response.raise_for_status()
    return response.json()['access_token']