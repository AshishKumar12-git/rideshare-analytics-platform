import sys
import os

PROJECT_ROOT = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "..",
        ".."
    )
)

sys.path.append(PROJECT_ROOT)

from ingestion.adls_helper import *
import time
import json
from datetime import datetime
import sqlite3
from event_helper import *

METADATA_FILE = 'ride_event_metadata.json'
CONTAINER = 'bronze'
POLL_INTERVAL = 5

def get_connection():
    return sqlite3.connect('/Users/ashishkumarmahto/Desktop/rideshare-analytics-platform/source_system/database/rideshare.db')


def fetch_records():
    conn = get_connection()
    last_processed_id = get_last_processed_id(METADATA_FILE)
    cursor = conn.cursor()
    cursor.execute(
        """
        select request_id,rider_id,pickup_city, ride_type, request_status, request_timestamp,updated_at from ride_request_events 
        where request_id > ? order by request_id 
        """,(last_processed_id,)
    )
    rows = cursor.fetchall()
    events = []
    for row in rows:
        events.append(
            {
                'request_id' : row[0],
                'rider_id' : row[1],
                'pickup_city' : row[2],
                'ride_type' : row[3],
                'request_status' : row[4],
                'request_timestamp' : row[5],
                'updated_at' : row[6]
            }
        )
    return events

def create_json(events):
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    file_name = f'riders_event_{timestamp}.json'
    with open(file_name,'w') as file:
        json.dump(events,file,indent = 4)
    return file_name

def upload_adls(file_name):
    load_time = datetime.utcnow().strftime('%Y-%m-%d')
    adls_path = f"rider_status_events/"f"ingestion_date={load_time}/"f"{file_name}"
    write_adls(container_name=CONTAINER,file_path = adls_path,local_file_path=file_name)

def process_events():
    while True:
        events = fetch_records()
        if not events:
            print(f"Events not found")
            time.sleep(POLL_INTERVAL)
            continue
        print(f"Events fetched {len(events)}")
        file_name = create_json(events)
        upload_adls(file_name)
        latest_id = max(event['request_id'] for event in events)
        update_last_processed_id(METADATA_FILE,latest_id)
        print(f"latest event id : {latest_id}")
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":

    print(
        "Starting Driver Event Ingestion..."
    )

    process_events()