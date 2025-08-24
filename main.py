import requests
import time
import json
import logging
from datetime import datetime
import clickhouse_connect
from dotenv import load_dotenv
import os
import socket

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename='logs/app.log',
    filemode='a'
)

URL = "http://api.open-notify.org/astros.json"
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost") 
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", 8123))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "user")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "pass")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "space_db")

def check_port(host, port):
    """Проверка доступности порта"""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(2)
    try:
        result = sock.connect_ex((host, port))
        if result == 0:
            logging.info(f"Port {port} on {host} is open")
            return True
        else:
            logging.error(f"Port {port} on {host} is closed")
            return False
    except Exception as e:
        logging.error(f"Port check failed: {e}")
        return False
    finally:
        sock.close()

def fetch_data(url, max_retries=5):
    retries = 0
    backoff = 1
    while retries < max_retries:
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            return json.dumps(response.json())
        except requests.exceptions.HTTPError as e:
            status = response.status_code if 'response' in locals() else 0
            logging.warning(f"HTTP error {status}. Retry {retries+1}/{max_retries} in {backoff}s")
            retry_after = int(response.headers.get("Retry-After", backoff)) if 'response' in locals() else backoff
            time.sleep(retry_after)
            backoff *= 2
            retries += 1
        except Exception as e:
            logging.error(f"Request error: {e}")
            time.sleep(backoff)
            backoff *= 2
            retries += 1
    raise Exception("Max retries exceeded after 5 attempts")

def save_to_clickhouse(data):
    try:
        if not check_port(CLICKHOUSE_HOST, CLICKHOUSE_PORT):
            raise Exception(f"Cannot connect to {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}. Ensure Docker is running and ports are open.")
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            database=CLICKHOUSE_DATABASE
        )
        logging.info("Connected to Clickhouse successfully")
        insert_time = datetime.utcnow()
        client.insert("people_in_space_raw", [(data, insert_time)], column_names=['json_data', '_inserted_at'])
        client.command("OPTIMIZE TABLE people_in_space_raw FINAL DEDUPLICATE")
        logging.info(f"Inserted data: size={len(data)} bytes at {insert_time}")
    except Exception as e:
        logging.error(f"Clickhouse connect/insert error: {e}. Check if Docker is running (docker ps) and port 8123 is open (curl http://localhost:8123).")
        raise

if __name__ == "__main__":
    try:
        data = fetch_data(URL)
        save_to_clickhouse(data)
        logging.info("Data insertion successful")
    except Exception as e:
        logging.error(f"Process failed: {e}")
        raise