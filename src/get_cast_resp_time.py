import asyncio
import traceback
import aiohttp
import sqlite3
from datetime import datetime
from tqdm.asyncio import tqdm


async def submit_msg(addr, fid, session):
    """Submit a message to a hub and record the submission response time."""
    url = f"http://{addr}:2281/v1/submitMessage"
    start_time = datetime.utcnow()
    try:
        # Example message data (replace with your actual message construction logic)
        message_data = {
            "fid": fid,
            "text": f"Test message at {datetime.utcnow()}",
            "timestamp": int(datetime.utcnow().timestamp())
        }
        headers = {"Content-Type": "application/json"}

        async with session.post(url, json=message_data, headers=headers, timeout=600) as response:
            status = response.status
            response_json = await response.json()
            end_time = datetime.utcnow()
            response_time = (end_time - start_time).total_seconds()
            hash_value = response_json.get("hash", None)
            error_info = None if status == 200 else response_json.get("error", None)
            return addr, fid, hash_value, response_time, status, error_info
    except aiohttp.ClientError as e:
        end_time = datetime.utcnow()
        response_time = (end_time - start_time).total_seconds()
        status = None
        error_info = str(e) + str(traceback.format_exc())
        return addr, fid, None, response_time, status, error_info


async def main(rpc_addresses):
    # Connect to SQLite database
    conn = sqlite3.connect('../data/hub_exp.db')
    cursor = conn.cursor()

    # Create table (if it doesn't exist)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS submit_msg_resp_time (
            addr TEXT,
            fid INTEGER,
            hash_value TEXT,
            response_time REAL,
            status_code INTEGER,
            error_info TEXT
        )
    ''')
    conn.commit()

    # Define multiple tasks
    tasks = []
    fid = 1  # Example FID
    for addr in rpc_addresses:
        task = submit_msg(addr, fid, session=aiohttp.ClientSession())
        tasks.append(task)

    # Use tqdm to show progress bar
    results = []
    for future in tqdm(asyncio.as_completed(tasks), total=len(tasks)):
        result = await future
        results.append(result)

    # Batch insert data into SQLite database
    cursor.executemany("""
        INSERT INTO submit_msg_resp_time (addr, fid, hash_value, response_time, status_code, error_info)
        VALUES (?, ?, ?, ?, ?, ?)
    """, results)
    conn.commit()

    # Close the database connection
    conn.close()


if __name__ == "__main__":
    rpc_addresses = [""]  # Example RPC addresses
    asyncio.run(main(rpc_addresses))