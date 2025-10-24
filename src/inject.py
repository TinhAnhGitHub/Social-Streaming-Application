import json
from elasticsearch import Elasticsearch, helpers

GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
RESET = "\033[0m"

es = Elasticsearch("http://localhost:9200")

INDEX_NAME = "social_stream"
DATA_PATH = "data/sample.json"  # đường dẫn đến file JSONL

def generate_actions(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                doc = json.loads(line.strip())
                yield {
                    "_index": INDEX_NAME,
                    "_source": doc
                }
            except json.JSONDecodeError:
                print(f"{YELLOW} [WARN] Skipping one JSON error line {RESET}")

if __name__ == "__main__":
    try:
        helpers.bulk(es, generate_actions(DATA_PATH))
        print(f"{GREEN}[SUCCESS] Data successfully injected into Elasticsearch!{RESET}")
    except Exception as e:
        print(f"{RED}[ERROR] Error while injecting data: {e}{RESET}")
