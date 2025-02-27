import logging
from asyncio import Semaphore
import time
import aiohttp
import traceback
import asyncio
import sqlite3
from datetime import datetime

DB_FILE = "../data/hub_exp.db"

# Configure logging
LOG_FILE = "../log/liveness.log"
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
# liveness_semaphore = Semaphore(2000)


# Setup SQLite tables
def setup_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS hub_info (
            round_number INTEGER,
            rpc_address TEXT,
            version TEXT,
            is_syncing INTEGER,
            nickname TEXT,
            root_hash TEXT,
            num_messages INTEGER,
            num_fid_events INTEGER,
            num_fname_events INTEGER,
            peer_id TEXT,
            hub_operator_fid INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (round_number, rpc_address)
        )
    """)
    cursor.execute("""
        PRAGMA journal_mode=WAL;
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS offline_reason (
            round_number INTEGER,
            rpc_address TEXT,
            http_status INTEGER,
            error_type TEXT,
            reason TEXT,
            error_timestamp TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (round_number, rpc_address)
        )
    """)

    conn.commit()
    conn.close()


async def fetch_info(session, addr):
    """
    Fetch information from a given address.

    Args:
        session: An aiohttp session object.
        addr: The target address as a string.

    Returns:
        A dictionary containing the response data or error details.
    """
    url = f"http://{addr}:2281/v1/info?dbstats=1"
    try:
        async with session.get(url, timeout=60*10 - 10) as response:
            if response.status == 200:
                try:
                    return await response.json()
                except aiohttp.ContentTypeError as e:
                    # Handle cases where the response is not JSON
                    return {
                        "status": response.status,
                        "reason": "Response is not JSON",
                        "error_type": "ContentTypeError",
                        "exception": str(e)
                    }
            else:
                return {
                    "status": response.status,
                    "reason": await response.text(),
                    "error_type": "HTTPError"
                }
    except aiohttp.ClientConnectorError as e:
        # Connection issues (e.g., target address is unreachable)
        return {
            "status": -1,
            "reason": f"Connection error: {str(e)}",
            "error_type": "ClientConnectorError"
        }
    except aiohttp.ClientPayloadError as e:
        # Payload processing issues (e.g., corrupted data)
        return {
            "status": -1,
            "reason": f"Payload error: {str(e)}",
            "error_type": "ClientPayloadError"
        }
    except aiohttp.ServerTimeoutError as e:
        # Timeout on server response
        return {
            "status": -1,
            "reason": f"Server timeout: {str(e)}",
            "error_type": "ServerTimeoutError"
        }
    except aiohttp.ClientResponseError as e:
        # Issues with the client response
        return {
            "status": e.status,
            "reason": f"Client response error: {str(e)}",
            "error_type": "ClientResponseError"
        }
    except aiohttp.InvalidURL as e:
        # Invalid URL format
        return {
            "status": -1,
            "reason": f"Invalid URL: {str(e)}",
            "error_type": "InvalidURL"
        }
    except Exception as e:
        # Catch-all for any other exceptions
        return {
            "status": -1,
            "reason": f"Unexpected error: {str(e)}\nTraceback: {traceback.format_exc()}",
            "error_type": "UnknownError"
        }

async def batch_store_hub_info(conn, hub_info_data):
    retry = 0
    while retry < 5:
        cursor = None
        try:
            cursor = conn.cursor()
            retry += 1
            cursor.executemany("""
                INSERT INTO hub_info (
                    rpc_address, version, is_syncing, nickname, root_hash,
                    num_messages, num_fid_events, num_fname_events, peer_id,
                    hub_operator_fid, round_number, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(rpc_address, round_number) DO UPDATE SET
                    version = excluded.version,
                    is_syncing = excluded.is_syncing,
                    nickname = excluded.nickname,
                    root_hash = excluded.root_hash,
                    num_messages = excluded.num_messages,
                    num_fid_events = excluded.num_fid_events,
                    num_fname_events = excluded.num_fname_events,
                    peer_id = excluded.peer_id,
                    hub_operator_fid = excluded.hub_operator_fid,
                    updated_at = CURRENT_TIMESTAMP
            """, hub_info_data)
            conn.commit()
        except Exception as e:
            conn.rollback()
            if cursor:
                cursor.close()


async def batch_store_offline_reason(conn, offline_reason_data):
    retry = 0
    while retry < 5:
        cursor = None
        try:
            cursor = conn.cursor()
            retry += 1
            cursor.executemany("""
                INSERT INTO offline_reason (
            rpc_address, http_status, error_type, reason, error_timestamp, round_number, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(rpc_address, round_number) DO UPDATE SET
            http_status = excluded.http_status,
            error_type = excluded.error_type,
            reason = excluded.reason,
            error_timestamp = excluded.error_timestamp,
            updated_at = CURRENT_TIMESTAMP
    """, offline_reason_data)
            conn.commit()
        except Exception as e:
            conn.rollback()
            if cursor:
                cursor.close()


async def fetch_and_collect(session, addr, round_number, hub_info_data, offline_reason_data):
    result = await fetch_info(session, addr)
    if isinstance(result, dict) and "version" in result:  # Successful response
        hub_info_data.append((
            addr,
            result.get("version"),
            int(result.get("isSyncing", False)),
            result.get("nickname"),
            result.get("rootHash"),
            result["dbStats"].get("numMessages"),
            result["dbStats"].get("numFidEvents"),
            result["dbStats"].get("numFnameEvents"),
            result.get("peerId"),
            result.get("hubOperatorFid"),
            round_number
        ))
    else:  # Failure
        offline_reason_data.append((
            addr,
            result.get("status", -1),
            result.get("error_type", "Unknown"),
            result.get("reason", "No reason provided"),
            datetime.utcnow().isoformat(),
            round_number
        ))


def get_round_number():
    """Get the current round number."""
    with sqlite3.connect(DB_FILE) as db:
        cursor = db.cursor()
        cursor.execute("SELECT MAX(round_number) FROM hub_info")
        result = cursor.fetchone()
        return (result[0] or 0) + 1


async def main():
    setup_db()
    round_number = get_round_number()

    try:
        async with aiohttp.ClientSession() as session:
            while True:  # Periodically fetch data every 5 minutes
                conn = sqlite3.connect(DB_FILE)
                logging.info(f"Round {round_number} begin")
                begin = time.time()
                cursor = conn.cursor()
                cursor.execute("SELECT rpc_address FROM peers")
                peers = [row[0] for row in cursor.fetchall()]

                hub_info_data = []
                offline_reason_data = []

                tasks = [fetch_and_collect(session, addr, round_number, hub_info_data, offline_reason_data) for addr in
                         peers]
                await asyncio.gather(*tasks)

                if hub_info_data:
                    logging.info(f"Successfully fetched {len(hub_info_data)} hub information")
                    await batch_store_hub_info(conn, hub_info_data)

                if offline_reason_data:
                    logging.info(f"Fail to fetch fetched {len(offline_reason_data)} hub information")
                    await batch_store_offline_reason(conn, offline_reason_data)

                logging.info(f"Round {round_number} ends in {int(time.time() - begin)} seconds")
                round_number += 1  # Increment round number
                await asyncio.sleep(max(0, int(10 * 60 - (time.time() - begin))))  # 5 minutes
    finally:
        conn.close()


# Run the program
if __name__ == "__main__":
    asyncio.run(main())