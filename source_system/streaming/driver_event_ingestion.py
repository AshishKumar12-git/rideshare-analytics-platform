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
import sqlite3
import json
import time
from datetime import datetime
from event_helper import *

METADATA_FILE = 'driver_event_metadata.json'
CONTAINER = 'bronze'
POLL_INTERVAL = 5

def get_connection():
    return sqlite3.connect('/Users/ashishkumarmahto/Desktop/rideshare-analytics-platform/source_system/database/rideshare.db')

def fetch_records():
    last_processed_id = get_last_processed_id(METADATA_FILE)
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT event_id, driver_id, event_type, status, city, event_timestamp FROM driver_status_events WHERE EVENT_ID > ? ORDER BY EVENT_ID
        """, (last_processed_id,)
    )
    rows = cursor.fetchall()
    conn.close()
    events = []
    for row in rows:
        events.append(
            {
                "event_id" : row[0],
                "driver_id" : row[1],
                "event_type" : row[2],
                "status" : row[3],
                "city" : row[4],
                "event_timestamp" : row[5]
            }
        )
    return events

def create_json_file(events):
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    file_name = f"driver_events_{timestamp}.json"
    with open(file_name,'w') as file:
        json.dump(events,file,indent=4)
    return file_name

def upload_adls(file_name):
    load_time = datetime.utcnow().strftime("%Y-%m-%d")
    adls_path = f"driver_status_events/"f"ingestion_date={load_time}/"f"{file_name}"
    write_adls(container_name = CONTAINER,file_path=adls_path,local_file_path=file_name)
    os.remove(file_name)

def process_events():
    while True:
        events = fetch_records()
        if not events:
            print(f" No Events fetched")
            time.sleep(POLL_INTERVAL)
            continue
        print(f"Events fetched : {len(events)}")
        file_name = create_json_file(events)
        upload_adls(file_name)
        latest_event_id = max(event['event_id'] for event in events)
        update_last_processed_id(METADATA_FILE,latest_event_id)
        print(f"latest_event_id : {latest_event_id}")
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":

    print(
        "Starting Driver Event Ingestion..."
    )

    process_events()