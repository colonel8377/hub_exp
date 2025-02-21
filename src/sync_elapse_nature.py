import logging

import aiohttp
import asyncio
import sqlite3
from datetime import datetime

DB_FILE = "../data/hub_exp.db"
MAX_FAILURES = 100
SYNC_TIMEOUT = 4 * 3600  # Maximum allowed sync time in seconds

# Configure logging
LOG_FILE = f"../log/sync_elapse_nature_{datetime.utcnow().strftime('%Y%m%d_%H%M')}.log"
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

async def fetch_info(session, addr):
    url = f"http://{addr}:2281/v1/info?dbstats=1"
    try:
        async with session.get(url) as response:
            if response.status == 200:
                return await response.json()
            else:
                return {
                    "status": response.status,
                    "reason": await response.text(),
                    "error_type": "HTTPError"
                }
    except aiohttp.ClientError as e:
        return {
            "status": -1,
            "reason": str(e),
            "error_type": "ClientError"
        }
    except Exception as e:
        return {
            "status": -1,
            "reason": str(e),
            "error_type": "UnknownError"
        }

async def fetch_sync_end_time(session, addr):
    begin_ts = datetime.utcnow()
    retry = 0
    while (datetime.utcnow() - begin_ts).total_seconds() < SYNC_TIMEOUT:
        result = await fetch_info(session, addr)
        if isinstance(result, dict) and "isSyncing" in result:
            if not result.get("isSyncing"):
                return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"), None, result
            await asyncio.sleep(0.5)
        else:
            retry += 1
            if retry >= MAX_FAILURES:
                return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"), result.get("reason"), None
            await asyncio.sleep(6)

    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"), f'Sync Exceeded {SYNC_TIMEOUT} seconds', None

async def fetch_sync_begin_time(session, addr):
    begin_ts = datetime.utcnow()
    retry = 0
    while (datetime.utcnow() - begin_ts).total_seconds() < SYNC_TIMEOUT / 2:
        result = await fetch_info(session, addr)
        if isinstance(result, dict) and "isSyncing" in result:
            if result.get("isSyncing"):
                return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"), result
            await asyncio.sleep(0.5)
        else:
            retry += 1
            if retry >= MAX_FAILURES:
                return None, None
            await asyncio.sleep(0.5)
    return None, None

async def fetch_unsync(session, addr):
    result = await fetch_info(session, addr)
    if isinstance(result, dict) and "isSyncing" in result:
        if result.get("isSyncing"):
            return addr, None, None, None, None, None
        else:
            logging.info(f'Monitor {addr} is unsynced')
            begin_time, begin_snapshot = await fetch_sync_begin_time(session, addr)
            if not begin_time:
                return addr, None, None, None, None, None
            logging.info(f'Monitor {addr} begin to sync at {begin_time}')
            end_time, err_msg, end_snapshot = await fetch_sync_end_time(session, addr)
            logging.info(f'Monitor {addr} end syncing at {end_time}, error msg: {err_msg}')
            return addr, begin_time, end_time, err_msg, begin_snapshot, end_snapshot
    return addr, None, None, None, None, None

def setup_db():
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS hub_sync_elapse_wild_info (
                rpc_address TEXT,
                begin_time TEXT,
                end_time TEXT,
                error_message TEXT,
                begin_version TEXT,
                end_version TEXT,
                begin_is_syncing INTEGER,
                end_is_syncing INTEGER,
                begin_nickname TEXT,
                end_nickname TEXT,
                begin_root_hash TEXT,
                end_root_hash TEXT,
                begin_num_messages INTEGER,
                end_num_messages INTEGER,
                begin_num_fid_events INTEGER,
                end_num_fid_events INTEGER,
                begin_num_fname_events INTEGER,
                end_num_fname_events INTEGER,
                begin_peer_id TEXT,
                end_peer_id TEXT,
                begin_hub_operator_fid TEXT,
                end_hub_operator_fid TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (rpc_address, begin_time)
            )
        """)
        conn.commit()

async def main():
    setup_db()
    round_number = 1  # Initialize round counter
    async with aiohttp.ClientSession() as session:
        while True:
            logging.info(f'Round {round_number} start')
            begin_ts = datetime.utcnow()
            with sqlite3.connect(DB_FILE) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT rpc_address FROM hub_info
                    WHERE round_number = (SELECT MAX(round_number) FROM hub_info)
                """)
                peers = [row[0] for row in cursor.fetchall()]

                tasks = [
                    fetch_unsync(session, addr)
                    for addr in peers
                ]
                task_results = await asyncio.gather(*tasks)

                inserts = []
                for result in task_results:
                    addr, begin_time, end_time, err_msg, begin_snapshot, end_snapshot = result
                    if begin_time:
                        inserts.append((
                            addr,
                            begin_time,
                            end_time,
                            err_msg,
                            begin_snapshot.get("version") if begin_snapshot else None,
                            end_snapshot.get("version") if end_snapshot else None,
                            int(begin_snapshot.get("isSyncing", False)) if begin_snapshot else None,
                            int(end_snapshot.get("isSyncing", False)) if end_snapshot else None,
                            begin_snapshot.get("nickname") if begin_snapshot else None,
                            end_snapshot.get("nickname") if end_snapshot else None,
                            begin_snapshot.get("rootHash") if begin_snapshot else None,
                            end_snapshot.get("rootHash") if end_snapshot else None,
                            begin_snapshot.get('dbStats', {}).get("numMessages") if begin_snapshot else None,
                            end_snapshot.get('dbStats', {}).get("numMessages") if end_snapshot else None,
                            begin_snapshot.get('dbStats', {}).get("numFidEvents") if begin_snapshot else None,
                            end_snapshot.get('dbStats', {}).get("numFidEvents") if end_snapshot else None,
                            begin_snapshot.get('dbStats', {}).get("numFnameEvents") if begin_snapshot else None,
                            end_snapshot.get('dbStats', {}).get("numFnameEvents") if end_snapshot else None,
                            begin_snapshot.get("peerId") if begin_snapshot else None,
                            end_snapshot.get("peerId") if end_snapshot else None,
                            begin_snapshot.get("hubOperatorFid") if begin_snapshot else None,
                            end_snapshot.get("hubOperatorFid") if end_snapshot else None,
                        ))

                if inserts:
                    cursor.executemany("""
                        INSERT OR REPLACE INTO hub_sync_elapse_wild_info (
                            rpc_address, begin_time, end_time, error_message,
                            begin_version, end_version, begin_is_syncing, end_is_syncing,
                            begin_nickname, end_nickname, begin_root_hash, end_root_hash,
                            begin_num_messages, end_num_messages, begin_num_fid_events, end_num_fid_events,
                            begin_num_fname_events, end_num_fname_events, begin_peer_id, end_peer_id,
                            begin_hub_operator_fid, end_hub_operator_fid
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, inserts)
                    conn.commit()

            logging.info(f"Round completed in {(datetime.utcnow() - begin_ts).total_seconds()} seconds")

if __name__ == "__main__":
    asyncio.run(main())