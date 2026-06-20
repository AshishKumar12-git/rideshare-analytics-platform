from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
import os 


def get_secret(secret_name : str):
    key_vault_url = os.getenv("KEY_VAULT_URL")
    credential = DefaultAzureCredential()
    client = SecretClient(vault_url = key_vault_url, credential = credential)
    secret = client.get_secret(secret_name)
    return secret.value
