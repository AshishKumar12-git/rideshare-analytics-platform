from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient
import os 
import json

def read_json_from_adls(
    container_name,
    file_path
):

    storage_url = os.getenv(
        "STORAGE_ACCOUNT_URL"
    )

    credential = DefaultAzureCredential()

    service_client = DataLakeServiceClient(
        account_url=storage_url,
        credential=credential
    )

    file_system_client = (
        service_client.get_file_system_client(
            file_system=container_name
        )
    )

    file_client = (
        file_system_client.get_file_client(
            file_path
        )
    )

    data = (
        file_client.download_file()
        .readall()
    )

    return json.loads(
        data.decode("utf-8")
    )

def write_json_to_adls(
    container_name,
    file_path,
    content
):

    storage_url = os.getenv(
        "STORAGE_ACCOUNT_URL"
    )

    credential = DefaultAzureCredential()

    service_client = DataLakeServiceClient(
        account_url=storage_url,
        credential=credential
    )

    file_system_client = (
        service_client.get_file_system_client(
            file_system=container_name
        )
    )

    file_client = (
        file_system_client.get_file_client(
            file_path
        )
    )

    file_client.upload_data(
        json.dumps(
            content,
            indent=4
        ),
        overwrite=True
    )

def write_adls(container_name : str, file_path : str, local_file_path : str):
    storage_url = os.getenv('STORAGE_ACCOUNT_URL')
    credential = DefaultAzureCredential()
    service_client = DataLakeServiceClient(account_url = storage_url, credential = credential)
    file_system_client = service_client.get_file_system_client(file_system= container_name)
    file_client = file_system_client.get_file_client(file_path)
    with open(local_file_path, 'rb') as f:
        file_client.upload_data(f.read(),overwrite = True)
    print("File uploaded successfully to ADLS")


