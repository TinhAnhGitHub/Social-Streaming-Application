import os
import json
import time
import shutil
from elasticsearch import Elasticsearch, helpers

# ========== COLORS ==========
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
RESET = "\033[0m"

# ========== CONFIG ==========
es = Elasticsearch("http://localhost:9200")
INDEX_NAME = "social_stream"
WATCH_DIR = "/home/tinhanhnguyen/Desktop/HK7/BigData/big_project/data/spark/kafka/output/reddit.submissions"
PROCESSED_DIR = os.path.join(WATCH_DIR, "processed")
SLEEP_INTERVAL = 5  # seconds between checks

os.makedirs(PROCESSED_DIR, exist_ok=True)

# ========== FUNCTIONS ==========
def generate_actions(path):
    """Yield Elasticsearch bulk actions with deduplication by 'created_utc'."""
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                doc = json.loads(line)
                # Try to locate unique field
                doc_id = None
                # For safety, handle both "created_utc" and nested "payload.created_utc"
                if "created_utc" in doc:
                    doc_id = str(doc["created_utc"])
                elif "payload" in doc and "created_utc" in doc["payload"]:
                    doc_id = str(doc["payload"]["created_utc"])
                else:
                    print(f"{YELLOW}[WARN]{RESET} No 'created_utc' field found in {path}")

                action = {
                    "_index": INDEX_NAME,
                    "_source": doc
                }
                if doc_id:
                    action["_id"] = doc_id  # ensures idempotent insert
                yield action
            except json.JSONDecodeError:
                print(f"{YELLOW}[WARN]{RESET} Skipping invalid JSON line in {path}")
            except Exception as e:
                print(f"{RED}[ERROR]{RESET} Failed parsing {path}: {e}")

def process_file(file_path):
    """Inject data from a JSON file into Elasticsearch."""
    try:
        helpers.bulk(es, generate_actions(file_path))
        print(f"{GREEN}[SUCCESS]{RESET} Indexed data from: {file_path}")
        # Move file to processed/
        dest_path = os.path.join(PROCESSED_DIR, os.path.basename(file_path))
        shutil.move(file_path, dest_path)
    except Exception as e:
        print(f"{RED}[ERROR]{RESET} Failed to index {file_path}: {e}")

def watch_directory():
    """Continuously watch the folder and inject new JSON files."""
    print(f"{YELLOW}Watching folder:{RESET} {WATCH_DIR}")
    while True:
        try:
            files = [f for f in os.listdir(WATCH_DIR) if f.endswith((".json", ".jsonl"))]
            if not files:
                time.sleep(SLEEP_INTERVAL)
                continue
            for filename in files:
                file_path = os.path.join(WATCH_DIR, filename)
                process_file(file_path)
        except KeyboardInterrupt:
            print(f"\n{YELLOW}Stopped by user.{RESET}")
            break
        except Exception as e:
            print(f"{RED}[ERROR]{RESET} Unexpected error: {e}")
            time.sleep(SLEEP_INTERVAL)

# ========== MAIN ==========
if __name__ == "__main__":
    watch_directory()
