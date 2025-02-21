import asyncio
import logging
import sqlite3
import time
import traceback
import configparser
from asyncio import Semaphore
from datetime import datetime
from functools import partial
from multiprocessing import Pool, Manager
from queue import Queue
from random import random
from threading import Lock

import aiohttp
from farcaster.Message import MessageBuilder
from farcaster.fcproto.message_pb2 import SIGNATURE_SCHEME_ED25519, HASH_SCHEME_BLAKE3, MessageData, \
    FARCASTER_NETWORK_MAINNET, CastAddBody, MESSAGE_TYPE_CAST_ADD
from tqdm import tqdm


# Create a ConfigParser object
config = configparser.ConfigParser()
config.read('../project.ini')

MSG_BUILDER = MessageBuilder(hash_scheme=HASH_SCHEME_BLAKE3,
                             signature_scheme=SIGNATURE_SCHEME_ED25519,
                             signer_key=bytes.fromhex(config.get("farcaster_user", "private_key"))
                             )

FID = config.get("farcaster_user", "fid")
DB_PATH = "../../database/eventual_consistency.db"

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# SQL Queries
INSERT_MSG_RECEIVE_HUBS = """
INSERT INTO exp_hub_gossip_latency (
round, submit_hub_ip, rcve_rpc_ip, msg_hash, cast_text, op_time, submit_finish_time, submit_time_gap, is_received, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
ON CONFLICT (round, submit_hub_ip, rcve_rpc_ip, msg_hash)
DO UPDATE SET
cast_text = EXCLUDED.cast_text,
op_time = EXCLUDED.op_time,
submit_time_gap = EXCLUDED.submit_time_gap,
is_received = EXCLUDED.is_received,
updated_at = CURRENT_TIMESTAMP;
"""

INSERT_SUBMIT_FAILURES = """
INSERT INTO exp_hub_msg_submit_failures (round, hub_ip, fid, error_message, created_at)
VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
ON CONFLICT (round, hub_ip)
DO UPDATE SET 
    fid = EXCLUDED.fid,
    error_message = EXCLUDED.error_message,
    created_at = CURRENT_TIMESTAMP
;
"""


class SQLiteConnectionPool:
    def __init__(self, db_path, pool_size=5):
        self._pool = Queue(maxsize=pool_size)
        self._lock = Lock()
        for _ in range(pool_size):
            conn = sqlite3.connect(db_path, check_same_thread=False)
            conn.execute("PRAGMA journal_mode=WAL;")  # Enable Write-Ahead Logging
            self._pool.put(conn)

    def get_connection(self):
        with self._lock:
            return self._pool.get()

    def release_connection(self, conn):
        with self._lock:
            self._pool.put(conn)

    def close_all(self):
        while not self._pool.empty():
            conn = self._pool.get()
            conn.close()


async def batch_insert_sqlite(sqlite_db_pool, sql, items):
    if not items:
        return
    retry = 0
    while retry < 5:
        conn = sqlite_db_pool.get_connection()
        try:
            with conn:
                conn.executemany(sql, items)
                break
        except Exception as e:
            raise e
        finally:
            sqlite_db_pool.release_connection(conn)
            retry += 1


def covert_farcaster_timestamp() -> int:
    epoch = 1609459200
    result = int(time.time() + int(600 * random())) - epoch
    return result


async def submit_msg(fid, dst_rpc_ip, session):
    """Submit a message to a hub."""
    cast_text = f"Timestamp {datetime.utcnow()} : {random()} : {random()} : {random()} : {random()} : {random()}"
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
    try:
        message_bytes = submit_msg.SerializeToString()
        submit_start_time = datetime.utcnow()
        resp = await session.post(f'http://{dst_rpc_ip}:2281/v1/submitMessage', headers=headers, data=message_bytes)
        result = await resp.json()
        submit_time_gap = (datetime.utcnow() - submit_start_time).total_seconds()
        if 'hash' in result:
            return dst_rpc_ip, result['hash'], cast_text, submit_time_gap, datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        else:
            return dst_rpc_ip, None, cast_text, None, f"Unexpected error: {traceback.format_exc()}"
    except Exception as e:
        return dst_rpc_ip, None, cast_text, None, f"Unexpected error: {traceback.format_exc()}"


async def check_msg(hub_ip, msg_hash, fid, session, semaphore):
    """Check if a single message is received by a specific hub."""
    url = f"http://{hub_ip}:2281/v1/castById?fid={fid}&hash={msg_hash}"
    start_time = datetime.utcnow()
    max_duration = 24 * 3600  # 12 hour
    retry = 0
    while (datetime.utcnow() - start_time).total_seconds() < max_duration and retry < 10:
        async with semaphore:
            try:
                async with session.get(url, timeout=300) as response:
                    if response.status == 200:
                        return hub_ip, msg_hash, datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"), 1
            except Exception as e:
                retry += 1
                await asyncio.sleep(min(2 ** retry, 64))
        await asyncio.sleep(1)
    return hub_ip, msg_hash, datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"), 0


async def process_chunk(hubs_chunk, round, progress_dict, sqlite_db_path):
    """Process a chunk of hubs for submission."""
    sqlite_db_pool = SQLiteConnectionPool(sqlite_db_path)
    fid = FID
    submitted_msgs = []
    submit_failures = []

    # Submission Progress Bar
    with tqdm(total=len(hubs_chunk), desc="Submitting Messages", unit="hub") as pbar:
        async with aiohttp.ClientSession() as session:
            submit_tasks = [submit_msg(fid, hub, session) for hub in hubs_chunk]
            results = await asyncio.gather(*submit_tasks)

            for result in results:
                if result[1]:  # Assuming result[1] is the msg_hash existence check
                    submitted_msgs.append(result)
                else:
                    submit_failures.append(
                        (round, result[0], fid, result[4]))  # round, hub_ip, fid, error_message, created_at
                pbar.update(1)
                progress_dict["submitted"] += 1

    if submit_failures:
        await batch_insert_sqlite(sqlite_db_pool, INSERT_SUBMIT_FAILURES, submit_failures)

    sqlite_db_pool.close_all()
    return submitted_msgs


async def centralized_check(submitted_msgs, all_hubs, progress_dict):
    """
    Check reception of all submitted messages and return a results map.
    The key is (hub_ip, hub_port, cast_hash), and the value is (receive_time, is_receive).
    """
    fid = FID
    results_map = []
    semaphore = Semaphore(int(len(submitted_msgs) * 100))
    async with aiohttp.ClientSession() as session:
        tasks = [
            check_msg(hub, msg_hash, fid, session, semaphore)
            for hub in all_hubs
            for _, msg_hash, _, _, _ in submitted_msgs
        ]

        total_tasks = len(tasks)
        with tqdm(total=total_tasks, desc="Checking Messages", unit="task") as pbar:
            for task in asyncio.as_completed(tasks):
                try:
                    res = await task
                    if res:
                        receive_hub_ip, msg_hash, receive_time, is_receive = res
                        results_map.append((receive_hub_ip, msg_hash, receive_time, is_receive))
                except Exception as e:
                    logger.error(f"Error in message check: {traceback.format_exc()}")

                pbar.update(1)
                progress_dict["checked"] += 1

    results_dict = {
        (hub_ip, msg_hash): (receive_time, is_receive)
        for hub_ip, msg_hash, receive_time, is_receive in results_map
    }
    return results_dict


def process_chunk_in_process(hubs_chunk, round, all_hubs, sqlite_db_path, progress_dict):
    try:
        sqlite_db_pool = SQLiteConnectionPool(sqlite_db_path)

        # Submit messages
        submitted_msgs = asyncio.run(process_chunk(hubs_chunk, round, progress_dict, sqlite_db_path))

        # Check reception
        results_map = asyncio.run(centralized_check(submitted_msgs, all_hubs, progress_dict))

        # Prepare data for insertion
        insert_data = []

        for dst_rpc_ip, cast_hash, text, submit_time_gap, submit_finish_time in submitted_msgs:
            for hub_ip in all_hubs:
                key = (hub_ip, cast_hash)
                if key in results_map:
                    receive_time, is_receive = results_map[key]
                    insert_data.append((
                        round, dst_rpc_ip, hub_ip,
                        cast_hash, text, receive_time,
                        submit_finish_time, submit_time_gap, is_receive
                    ))
                else:
                    logger.warning(f"No result for key: {key}")

        # Insert into database
        if insert_data:
            asyncio.run(batch_insert_sqlite(sqlite_db_pool, INSERT_MSG_RECEIVE_HUBS, insert_data))
        sqlite_db_pool.close_all()
    except Exception as e:
        logger.error(f"Error processing chunk: {e}\n{traceback.format_exc()}")


def get_hub_addr(file):
    """Get the current round number."""
    try:
        with sqlite3.connect(file) as db:
            cursor = db.cursor()
            cursor.execute(
                "SELECT rpc_address FROM hub_info where round_number = (select max(round_number) from hub_info)")
            results = cursor.fetchall()
            return [result[0] for result in results]
    except Exception as e:
        logger.error(f"Error getting hub address: {e}\n{traceback.format_exc()}")
        return []


def create_exp_hub_gossip_latency_table(file):
    with sqlite3.connect(file) as db:
        cursor = db.cursor()
        create_exp_hub_gossip_latency_sql = """
        CREATE TABLE IF NOT EXISTS exp_hub_gossip_latency (
            id SERIAL PRIMARY KEY,
            round INTEGER NOT NULL,
            submit_hub_ip VARCHAR(45) NOT NULL,
            rcve_rpc_ip VARCHAR(45) NOT NULL,
            msg_hash VARCHAR(64) NOT NULL,
            cast_text TEXT,
            op_time TIMESTAMP NOT NULL,
            submit_finish_time TIMESTAMP,
            submit_time_gap INTERVAL,
            is_received BOOLEAN NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (round, submit_hub_ip, rcve_rpc_ip, msg_hash)
        );
        """
        cursor.execute(create_exp_hub_gossip_latency_sql)
        db.commit()


def create_exp_hub_msg_submit_failures_table(file):
    with sqlite3.connect(file) as db:
        cursor = db.cursor()
        create_exp_hub_msg_submit_failures_sql = """CREATE TABLE IF NOT EXISTS exp_hub_msg_submit_failures (
        id SERIAL PRIMARY KEY,
        round INTEGER NOT NULL,
        hub_ip VARCHAR(45) NOT NULL,
        fid VARCHAR(64) NOT NULL,
        error_message TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (round, hub_ip)
        );
        """
        cursor.execute(create_exp_hub_msg_submit_failures_sql)
        db.commit()


def get_round_number(file):
    """Get the current round number."""
    with sqlite3.connect(file) as db:
        cursor = db.cursor()
        cursor.execute(
            "SELECT max(round) from exp_hub_gossip_latency")
        result = cursor.fetchone()
        return result[0] + 1 if result and result[0] is not None else 1

def main():
    while True:
        manager = Manager()
        progress_dict = manager.dict(submitted=0, checked=0)
        hubs = get_hub_addr("../data/hub_exp.db")
        round = get_round_number(DB_PATH)
        logger.info(f"Processing round {round}")
        logger.info(f"Alive Hub Count {len(hubs)}")
        create_exp_hub_msg_submit_failures_table(DB_PATH)
        create_exp_hub_gossip_latency_table(DB_PATH)
        # Divide hubs into chunks
        chunk_size = int(len(hubs) / 16)
        chunks = [hubs[i:i + chunk_size] for i in range(0, len(hubs), chunk_size)]

        with Pool(processes=len(chunks)) as pool:  # Adjust process count as needed
            for _ in tqdm(
                    pool.imap_unordered(
                        partial(process_chunk_in_process, all_hubs=hubs, round=round, sqlite_db_path=DB_PATH,
                                progress_dict=progress_dict),
                        chunks
                    ),
                    total=len(chunks),
                    desc="Processing Chunks",
                    unit="chunk",
            ):
                pass
        round += 1
        logger.info(f"All chunks processed. Progress: {progress_dict}")


if __name__ == "__main__":
    main()