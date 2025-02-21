import logging
import sqlite3
from datetime import datetime

import pandas as pd
import requests
from tqdm import tqdm
import configparser

config = configparser.ConfigParser()
config.read('../project.ini')
# Configuration
API_ENDPOINT = config.get("ip_resolver", "api_endpoint")
GEO_API_KEY = config.get("ip_resolver", "geo_api_key")
DB_FILE = '../data/hub_exp.db'
BATCH_SIZE = 100  # Insert in batches to improve performance

# Configure logging
logging.basicConfig(
    # filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)


def resolve_ip(api_key, ip):
    """Resolve IP address using the IP2Location API."""
    try:
        response = requests.get(API_ENDPOINT, params={
            'key': api_key,
            'format': 'json',
            'ip': ip
        })
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logging.error(f"Failed to resolve IP {ip}: {e}")
        return None


def insert_into_db(records):
    """Insert resolved IP data into the SQLite database."""
    if not records:
        return

    insert_query = """
        INSERT INTO hub_addr (
            ip, 
            country_code, 
            country_name, 
            region_name, 
            city_name, 
            latitude, 
            longitude, 
            zip_code, 
            time_zone, 
            as_number, 
            as_name, 
            is_proxy, 
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (ip) DO UPDATE SET 
            country_code = excluded.country_code,
            country_name = excluded.country_name,
            region_name = excluded.region_name,
            city_name = excluded.city_name,
            latitude = excluded.latitude,
            longitude = excluded.longitude,
            zip_code = excluded.zip_code,
            time_zone = excluded.time_zone,
            as_number = excluded.as_number,
            as_name = excluded.as_name,
            is_proxy = excluded.is_proxy,
            updated_at = excluded.updated_at
    """
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.executemany(insert_query, records)
            conn.commit()
            logging.info(f"Inserted {len(records)} records into the database.")
    except sqlite3.Error as e:
        logging.error(f"Database error: {e}")


def get_existed_ip():
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT ip FROM hub_addr GROUP BY ip")
        results = cursor.fetchall()
        return set([result[0] for result in results])


def init_table():
    """Initialize the hub_addr table in the database."""
    create_table_query = """
        CREATE TABLE IF NOT EXISTS hub_addr (
            ip TEXT PRIMARY KEY,
            country_code TEXT,
            country_name TEXT,
            region_name TEXT,
            city_name TEXT,
            latitude REAL,
            longitude REAL,
            zip_code TEXT,
            time_zone TEXT,
            as_number TEXT,
            as_name TEXT,
            is_proxy INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(create_table_query)
            logging.info("Table hub_addr initialized successfully.")
    except sqlite3.Error as e:
        logging.error(f"Error initializing table: {e}")


def find_hubs_without_ip():
    """Find hubs that do not have an IP in the peers table."""
    try:
        with sqlite3.connect('../data/hub_exp.db') as conn:
            query = """
                SELECT rpc_address
                FROM peers t1
                left join hub_addr t2
                ON t1.rpc_address = t2.ip
                WHERE t2.ip IS NULL
                
            """
            cursor = conn.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            return [result[0] for result in results]
    except sqlite3.Error as e:
        logging.error(f"Error finding hubs without IP: {e}")
        return []


def main():
    # Initialize the table
    init_table()

    # Load the API key (replace 'YOUR_API_KEY' with your actual key)
    geo_api_key = GEO_API_KEY

    # Find hubs without IP from the peers table
    # hubs_without_ip = find_hubs_without_ip()
    hubs_without_ip = pd.read_csv('../data/df_top_hubs.csv')['rpc_address'].unique()
    logging.info(f"Found {len(hubs_without_ip)} hubs without IP in the peers table.")

    # Read the addresses from the CSV file
    addresses = hubs_without_ip  # Assume there is an 'ip' column in the DataFrame


    batch_records = []
    for ip in tqdm(hubs_without_ip):
        if ip == '127.0.0.1':  # Skip localhost
            continue

        info = resolve_ip(geo_api_key, ip)
        if info:
            batch_records.append((
                info.get('ip'),
                info.get('country_code'),
                info.get('country_name'),
                info.get('region_name'),
                info.get('city_name'),
                info.get('latitude'),
                info.get('longitude'),
                info.get('zip_code'),
                info.get('time_zone'),
                info.get('asn'),
                info.get('as'),
                info.get('is_proxy'),
                datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            ))

        # Insert in batches
        if len(batch_records) >= BATCH_SIZE:
            insert_into_db(batch_records)
            batch_records.clear()

    # Insert any remaining records
    if batch_records:
        insert_into_db(batch_records)


if __name__ == "__main__":
    main()