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
INDEX_NAME = "reddit_submissions"  # FIXED: Match the index from create_index.py
WATCH_DIR = "/home/tinhanhnguyen/Desktop/HK7/BigData/big_project/data/spark/kafka/output/reddit.submissions"
PROCESSED_DIR = os.path.join(WATCH_DIR, "processed")
SLEEP_INTERVAL = 5  # seconds between checks

os.makedirs(PROCESSED_DIR, exist_ok=True)

# ========== FUNCTIONS ==========
def generate_actions(path):
    """Yield Elasticsearch bulk actions with deduplication by 'payload.id'."""
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                doc = json.loads(line)
                
                # FIXED: Use payload.id as the unique identifier (matches your schema)
                doc_id = None
                if "payload" in doc and "id" in doc["payload"]:
                    doc_id = doc["payload"]["id"]
                elif "id" in doc:
                    doc_id = doc["id"]
                else:
                    print(f"{YELLOW}[WARN]{RESET} No 'id' field found in {path}, using auto-generated ID")

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
        success, failed = helpers.bulk(es, generate_actions(file_path), stats_only=True)
        print(f"{GREEN}[SUCCESS]{RESET} Indexed {success} documents from: {file_path}")
        if failed > 0:
            print(f"{YELLOW}[WARN]{RESET} {failed} documents failed to index")
        
        # Move file to processed/
        dest_path = os.path.join(PROCESSED_DIR, os.path.basename(file_path))
        shutil.move(file_path, dest_path)
    except Exception as e:
        print(f"{RED}[ERROR]{RESET} Failed to index {file_path}: {e}")

def watch_directory():
    """Continuously watch the folder and inject new JSON files."""
    print(f"{YELLOW}Watching folder:{RESET} {WATCH_DIR}")
    print(f"{YELLOW}Target index:{RESET} {INDEX_NAME}")
    print(f"{YELLOW}Elasticsearch:{RESET} {es.info()['cluster_name']}\n")
    
    while True:
        try:
            files = [f for f in os.listdir(WATCH_DIR) 
                    if f.endswith((".json", ".jsonl")) and os.path.isfile(os.path.join(WATCH_DIR, f))]
            
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
    # Verify index exists
    if not es.indices.exists(index=INDEX_NAME):
        print(f"{RED}[ERROR]{RESET} Index '{INDEX_NAME}' does not exist!")
        print(f"{YELLOW}Please run create_index.py first{RESET}")
        exit(1)
    
    watch_directory()