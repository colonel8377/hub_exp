import aiohttp
import asyncio
import sqlite3
import random
import logging
import json

# Configure logging
LOG_FILE = "../log/peer_discovery.log"
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# Database setup
DB_FILE = "../data/hub_exp.db"


def setup_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS peers (
            rpc_address TEXT,
            rpc_port INTEGER,
            gossip_address TEXT,
            gossip_port INTEGER,
            gossip_family INTEGER,
            excluded_hashes TEXT,
            count INTEGER,
            hub_version TEXT,
            network TEXT,
            app_version TEXT,
            timestamp INTEGER,
            PRIMARY KEY (rpc_address, rpc_port)
        )
    """)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.commit()
    conn.close()


async def fetch_peer_data(session, target_address):
    url = f"http://{target_address}:2281/v1/currentPeers"
    try:
        async with session.get(url) as response:
            if response.status == 200:
                return await response.json()
            else:
                logging.warning(f"Received status {response.status} from {target_address}")
    except Exception as e:
        logging.error(f"Error fetching from {target_address}: {e}")
    return None


async def store_peer_data(data):
    conn = sqlite3.connect(DB_FILE)
    # cursor = conn.cursor()
    inserts = []
    for peer in data.get("contacts", []):
        gossip = peer["gossipAddress"]
        rpc = peer["rpcAddress"]
        excluded_hashes = json.dumps(peer.get("excludedHashes", []))
        count = peer.get("count", 0)
        hub_version = peer.get("hubVersion", "")
        network = peer.get("network", "")
        app_version = peer.get("appVersion", "")
        timestamp = peer.get("timestamp", 0)
        inserts.append((
            rpc["address"], rpc["port"], gossip["address"], gossip["port"], gossip["family"],
            excluded_hashes, count, hub_version, network, app_version, timestamp
        ))
    try:
        conn.executemany("""
            INSERT OR REPLACE INTO peers (
                rpc_address, rpc_port, gossip_address, gossip_port, gossip_family,
                excluded_hashes, count, hub_version, network, app_version, timestamp
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            inserts
        ))
        conn.commit()
        logging.info(f"Stored {len(inserts)} peer data")

    except Exception as e:
        logging.error(f"Error inserting data: {e}")
    finally:
        if conn:
            conn.close()


def fetch_known_peers():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT rpc_address from peers group by rpc_address
    """)
    results = cursor.fetchall()
    return [result[0] for result in results]


async def main():
    setup_db()
    discovered_hubs = fetch_known_peers()
    seen_hubs = set(discovered_hubs)

    async with aiohttp.ClientSession() as session:
        while True:
            target_hub = random.choice(discovered_hubs)
            seen_hubs.add(target_hub)

            logging.info(f"Fetching peer data from {target_hub}")
            data = await fetch_peer_data(session, target_hub)
            if data:
                await store_peer_data(data)

                # Add new hubs to the list
                for peer in data.get("contacts", []):
                    rpc = peer["rpcAddress"]["address"]
                    if rpc not in seen_hubs and rpc not in discovered_hubs:
                        discovered_hubs.append(rpc)
                        seen_hubs.add(rpc)
                        logging.info(f"Discovered new hub: {rpc}")

            # Wait to avoid spamming requests
            await asyncio.sleep(1)


# Run the program
if __name__ == "__main__":
    asyncio.run(main())