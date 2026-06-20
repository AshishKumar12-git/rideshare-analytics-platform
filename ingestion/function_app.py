import azure.functions as func
from datetime import datetime
import json
import logging
import os, requests
import pandas as pd
from auth_helper import get_access_token
from adls_helper import write_adls

app = func.FunctionApp()

PAGE_SIZE = 1000
container = os.getenv('CONTAINER')
MAX_RECORDS_PER_CHUNK = 5000
TABLE = 'drivers'


def fetch_pages(limit : int, offset: int,headers:dict):
    api_url = os.getenv('FASTAPI_URL')
    response = requests.get(f"{api_url}/{TABLE}",
                            params = { "limit" : limit , "offset" : offset}, headers = headers )
    response.raise_for_status()
    return response.json()

def write_parquet(records,chunk_number):
    df = pd.DataFrame(records)
    file_name = f"{TABLE}_part_{chunk_number:03d}.parquet"
    df.to_parquet(file_name,index=False)
    return file_name

def upload_chunk(local_file_path):
    load_time = datetime.utcnow().strftime("%Y-%m-%d")
    adls_path = f"{TABLE}/"f"ingestion_date={load_time}/"f"{local_file_path}"
    write_adls(container_name = container,file_path = adls_path, local_file_path=local_file_path)
    logging.info( f"Uploaded file : {local_file_path}" )
    os.remove(local_file_path)

def process_drivers():
    token = get_access_token() 
    headers = { "Authorization": f"Bearer {token}" }
    offset = 0
    chunk_number = 1
    total_records = 0
    current_chunk = []

    while True:
        logging.info(f"fetching records from {offset}")
        data = fetch_pages(PAGE_SIZE,offset,headers)
        logging.info(f"Retrieved {len(data)} records")

        if not data:
            break
        current_chunk.extend(data)

        total_records += len(data)

        if len(current_chunk) >= MAX_RECORDS_PER_CHUNK:
            file_name = write_parquet(current_chunk,chunk_number)
            logging.info(f"Current Chunk Size = {len(current_chunk)}")
            upload_chunk(file_name)
            current_chunk.clear()
            chunk_number+=1
        offset += PAGE_SIZE
    if current_chunk:
        file_name = write_parquet(current_chunk,chunk_number)
        logging.info(f"Current Chunk Size = {len(current_chunk)}")
        upload_chunk(file_name)
        logging.info(f"Total Records Processed: {total_records}")

@app.route(route="ingest-drivers",auth_level=func.AuthLevel.FUNCTION)

def ingest_drivers(req: func.HttpRequest) -> func.HttpResponse:

    try:

        logging.info(
            "Drivers ingestion started"
        )

        process_drivers()


        logging.info(
            "Drivers ingestion completed"
        )

        return func.HttpResponse(
            "Drivers ingestion completed successfully",
            status_code=200
        )

    except Exception as e:

        logging.error(
            f"Ingestion Failed : {str(e)}"
        )

        return func.HttpResponse(
            f"Error : {str(e)}",
            status_code=500
        )
