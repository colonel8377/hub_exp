import sqlite3
import asyncio
import aiohttp
import logging
import time
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

# 配置项
DATABASE_SOURCE = '../data/hub_exp.db'
DATABASE_TARGET = '../data/sync_status.db'
ROUND_TS = 60 * 10  # 每轮间隔时间（秒）
CONCURRENCY = 10000  # 并发限制
CHUNK_SIZE = 500  # 批量插入大小
RETRY_LIMIT = 5  # 重试次数
RETRY_DELAY = 1  # 初始重试延迟（秒）

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    filename='../log/sync_status.log'
)
logger = logging.getLogger(__name__)

# SQL语句
INSERT_SQL = """
INSERT OR REPLACE INTO sync_scores (
    peer_id, rpc_address, round_number, is_syncing, in_sync, should_sync, divergence_prefix,
    divergence_seconds_ago, their_messages, our_messages, last_bad_sync, score, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
"""


class SQLiteConnectionPool:
    """SQLite连接池"""

    def __init__(self, db_path, pool_size=5):
        self._pool = asyncio.Queue(pool_size)
        for _ in range(pool_size):
            conn = sqlite3.connect(db_path, check_same_thread=False)
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA busy_timeout=5000;")
            self._pool.put_nowait(conn)

    async def get_connection(self):
        """获取连接"""
        return await self._pool.get()

    async def release_connection(self, conn):
        """释放连接"""
        try:
            conn.execute("ROLLBACK")
        except:
            pass
        await self._pool.put(conn)

    async def close_all(self):
        """关闭所有连接"""
        while not self._pool.empty():
            conn = await self._pool.get()
            conn.close()


@retry(
    wait=wait_exponential(multiplier=1, min=RETRY_DELAY, max=60),
    stop=stop_after_attempt(RETRY_LIMIT),
    retry=retry_if_exception_type(sqlite3.OperationalError)
)
async def safe_insert(conn, chunk):
    """安全插入数据"""
    conn.execute("BEGIN")
    conn.executemany(INSERT_SQL, chunk)
    conn.commit()


async def async_batch_insert(data):
    """异步批量插入"""
    pool = SQLiteConnectionPool(DATABASE_TARGET)

    for i in range(0, len(data), CHUNK_SIZE):
        chunk = data[i:i + CHUNK_SIZE]
        conn = await pool.get_connection()
        try:
            await safe_insert(conn, chunk)
            logger.info(f"Inserted {len(chunk)} records")
        except Exception as e:
            logger.error(f"Insert failed: {str(e)}")
        finally:
            await pool.release_connection(conn)

    await pool.close_all()


def fetch_all_rpc_addresses():
    """获取所有RPC地址"""
    with sqlite3.connect(DATABASE_SOURCE) as db:
        cursor = db.cursor()
        cursor.execute("SELECT rpc_address FROM hub_info where round_number = (select max(round_number) from hub_info) group by rpc_address")
        return [row[0] for row in cursor.fetchall()]


def fetch_all_peer_ids():
    """获取所有Peer ID"""
    with sqlite3.connect(DATABASE_SOURCE) as db:
        cursor = db.cursor()
        cursor.execute("SELECT peer_id FROM hub_info where round_number = (select max(round_number) from hub_info) group by peer_id")
        return [row[0] for row in cursor.fetchall()]


async def fetch_sync_status(session, address, peer_id):
    """获取同步状态"""
    url = f'http://{address}:2281/v1/syncStatus?peerId={peer_id}'
    try:
        async with session.get(url, timeout=10) as response:
            if response.status == 200:
                return await response.json()
    except Exception as e:
        logger.warning(f"Failed to fetch {url}: {str(e)}")
    return None


async def process_peer(peer_id, rpc_address, round_number, session, semaphore):
    """处理单个Peer"""
    async with semaphore:
        sync_status = await fetch_sync_status(session, rpc_address, peer_id)
        if sync_status and 'syncStatus' in sync_status:
            sync_data = sync_status['syncStatus'][0]
            return (
                peer_id,
                rpc_address,
                round_number,
                sync_status.get('isSyncing'),
                sync_data.get('inSync'),
                sync_data.get('shouldSync'),
                sync_data.get('divergencePrefix'),
                sync_data.get('divergenceSecondsAgo'),
                sync_data.get('theirMessages'),
                sync_data.get('ourMessages'),
                sync_data.get('lastBadSync'),
                sync_data.get('score')
            )
    return None


def initialize_tables():
    """初始化数据库表"""
    with sqlite3.connect(DATABASE_TARGET) as db:
        cursor = db.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sync_scores (
                peer_id TEXT NOT NULL,
                rpc_address TEXT NOT NULL,
                round_number INTEGER NOT NULL,
                is_syncing INTEGER,
                in_sync TEXT,
                should_sync INTEGER,
                divergence_prefix TEXT,
                divergence_seconds_ago INTEGER,
                their_messages INTEGER,
                our_messages INTEGER,
                last_bad_sync INTEGER,
                score INTEGER,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (peer_id, rpc_address, round_number)
            )
        """)
        db.commit()


def get_round_number():
    """获取当前轮次号"""
    with sqlite3.connect(DATABASE_TARGET) as db:
        cursor = db.cursor()
        cursor.execute("SELECT MAX(round_number) FROM sync_scores")
        result = cursor.fetchone()
        return (result[0] or 0) + 1


async def main():
    """主函数"""
    initialize_tables()
    round_number = get_round_number()

    while True:
        logger.info(f"Starting round {round_number}")
        start_time = time.time()

        rpc_addresses = fetch_all_rpc_addresses()
        peer_ids = fetch_all_peer_ids()

        async with aiohttp.ClientSession() as session:
            semaphore = asyncio.Semaphore(CONCURRENCY)
            tasks = [
                process_peer(peer_id, rpc_address, round_number, session, semaphore)
                for rpc_address in rpc_addresses
                for peer_id in peer_ids
            ]
            results = await asyncio.gather(*tasks)
            valid_data = list(filter(None, results))

            await async_batch_insert(valid_data)

        elapsed = time.time() - start_time
        logger.info(f"Round {round_number} completed in {elapsed:.2f}s")
        round_number += 1
        await asyncio.sleep(max(0, int(ROUND_TS - elapsed)))


if __name__ == '__main__':
    asyncio.run(main())