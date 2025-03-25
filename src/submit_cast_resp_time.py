import asyncio
import traceback
import aiohttp
import sqlite3
from datetime import datetime

from farcaster.fcproto.message_pb2 import CastAddBody, MessageData, FARCASTER_NETWORK_MAINNET, MESSAGE_TYPE_CAST_ADD
from tqdm.asyncio import tqdm

from src.eventual_consistency import covert_farcaster_timestamp, MSG_BUILDER


async def submit_msg(addr, fid, session):
    """Submit a message to a hub and record the submission response time."""
    url = f"http://{addr}:2281/v1/submitMessage"

    try:
        # Example message data (replace with your actual message construction logic)
        cast_text = "Hello World."
        cast_body = CastAddBody(text=cast_text, embeds=[], mentions=[], mentions_positions=[])
        message_data = MessageData(
            fid=fid,
            network=FARCASTER_NETWORK_MAINNET,
            timestamp=covert_farcaster_timestamp(),
            type=MESSAGE_TYPE_CAST_ADD,
            cast_add_body=cast_body
        )
        submit_msg = MSG_BUILDER.message(data=message_data)
        headers = {
            "Content-Type": "application/octet-stream"
        }
        message_bytes = submit_msg.SerializeToString()
        start_time = datetime.utcnow()
        async with session.post(url, data=message_bytes, headers=headers) as response:
            status = response.status
            response_json = await response.json()
            end_time = datetime.utcnow()
            response_time = (end_time - start_time).total_seconds()
            hash_value = response_json.get("hash", None)
            error_info = None if status == 200 else str(response_json)
            return addr, fid, hash_value, response_time, status, error_info
    except aiohttp.ClientError as e:
        return None


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
    fid = 862653  # Example FID
    for addr in rpc_addresses:
        task = submit_msg(addr, fid, session=aiohttp.ClientSession())
        tasks.append(task)

    # Use tqdm to show progress bar
    results = []
    for future in tqdm(asyncio.as_completed(tasks), total=len(tasks)):
        result = await future
        if result:
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