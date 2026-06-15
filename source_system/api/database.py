import sqlite3

def get_connection():
    conn = sqlite3.connect('/Users/ashishkumarmahto/Desktop/rideshare-analytics-platform/source_system/database/rideshare.db')

    conn.row_factory = sqlite3.Row
    return conn