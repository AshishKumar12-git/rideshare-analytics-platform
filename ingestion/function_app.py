import azure.functions as func
from datetime import datetime
import json
import logging
import os, requests
import pandas as pd
from auth_helper import get_access_token
from adls_helper import write_adls
from metadata_helper import (
    get_table_config,
    update_offset,
    update_watermark,
    mark_full_load_complete
)

app = func.FunctionApp()

PAGE_SIZE = 1000
container = os.getenv('CONTAINER')
MAX_RECORDS_PER_CHUNK = 5000


def fetch_pages(limit : int, offset: int,headers:dict,table:str):
    api_url = os.getenv('FASTAPI_URL')
    response = requests.get(f"{api_url}/{table}",
                            params = { "limit" : limit , "offset" : offset}, headers = headers )
    response.raise_for_status()
    return response.json()

def write_parquet(records,chunk_number,table):
    df = pd.DataFrame(records)
    file_name = f"{table}_part_{chunk_number:03d}.parquet"
    df.to_parquet(file_name,index=False)
    return file_name

def upload_chunk(local_file_path,table):
    load_time = datetime.utcnow().strftime("%Y-%m-%d")
    adls_path = f"{table}/"f"ingestion_date={load_time}/"f"{local_file_path}"
    write_adls(container_name = container,file_path = adls_path, local_file_path=local_file_path)
    logging.info( f"Uploaded file : {local_file_path}" )
    os.remove(local_file_path)

def fetch_incremental_pages(
    limit: int,
    offset: int,
    headers: dict,
    table: str,
    watermark: str
):

    api_url = os.getenv("FASTAPI_URL")

    response = requests.get(
        f"{api_url}/{table}",
        params={
            "limit": limit,
            "offset": offset,
            "updated_after": watermark
        },
        headers=headers
    )

    response.raise_for_status()

    return response.json()

def process_incremental_load(
    table: str
):

    token = get_access_token()

    headers = {
        "Authorization": f"Bearer {token}"
    }

    config = get_table_config(table)

    watermark = config["last_watermark"]

    offset = 0

    all_records = []

    while True:

        data = fetch_incremental_pages(
            PAGE_SIZE,
            offset,
            headers,
            table,
            watermark
        )

        if not data:
            break

        all_records.extend(data)

        offset += PAGE_SIZE

    if not all_records:

        logging.info(
            f"{table}: No new records found"
        )

        return

    latest_watermark = max(
        row["updated_at"]
        for row in all_records
    )

    file_name = write_parquet(
        all_records,
        1,
        table
    )

    upload_chunk(
        file_name,
        table
    )

    update_watermark(
        table,
        latest_watermark
    )

    logging.info(
        f"{table}: Incremental load completed"
    )


def process_table(table : str):
    token = get_access_token() 
    headers = { "Authorization": f"Bearer {token}" }
    config = get_table_config(table)
    offset = config["last_offset"]
    chunk_number = 1
    total_records = 0
    current_chunk = []

    while True:
        logging.info(f"Table : {table} -> fetching records from {offset}")
        data = fetch_pages(PAGE_SIZE,offset,headers,table)
        logging.info(f"Retrieved {len(data)} records")

        if not data:
            mark_full_load_complete(table,datetime.utcnow().isoformat())
            break
        current_chunk.extend(data)
        total_records += len(data)
        offset += PAGE_SIZE

        if len(current_chunk) >= MAX_RECORDS_PER_CHUNK:
            file_name = write_parquet(current_chunk,chunk_number,table)
            logging.info(f"Current Chunk Size = {len(current_chunk)}")
            upload_chunk(file_name,table)
            update_offset(table, offset)
            current_chunk.clear()
            chunk_number+=1
    if current_chunk:
        file_name = write_parquet(current_chunk,chunk_number,table)
        logging.info(f"Current Chunk Size = {len(current_chunk)}")
        upload_chunk(file_name,table)
        update_offset(table, offset)
        logging.info(f"Table : {table} -> Total Records Processed: {total_records}")

@app.route(
    route="ingest-tables",
    auth_level=func.AuthLevel.FUNCTION
)
def ingest_tables(
    req: func.HttpRequest
) -> func.HttpResponse:

    try:

        TABLES = [
            "drivers",
            "riders",
            "trips",
            "payments"
        ]

        for table in TABLES:

            config = get_table_config(table)

            if config["load_type"] == "full":

                logging.info(
                    f"{table} -> Starting Full Load"
                )

                process_table(table)

            else:

                logging.info(
                    f"{table} -> Starting Incremental Load"
                )

                process_incremental_load(table)

        return func.HttpResponse(
            "Ingestion completed successfully",
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


