from elasticsearch import Elasticsearch
from elasticsearch.exceptions import BadRequestError, NotFoundError

GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
RESET = "\033[0m"

es = Elasticsearch("http://localhost:9200")
INDEX_NAME = "social_stream"

try:
    if es.indices.exists(index=INDEX_NAME):
        es.indices.delete(index=INDEX_NAME)
        print(f"[INFO] Deleted old index: {INDEX_NAME}")
except NotFoundError:
    print(f"[INFO] Index {INDEX_NAME} does not exist yet.")

mapping = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0
    },
    "mappings": {
        "properties": {
            "user_id": {"type": "keyword"},
            "username": {"type": "text"},
            "timestamp": {"type": "date"},
            "content": {"type": "text"},
            "likes": {"type": "integer"},
            "comments": {"type": "integer"},
            "tags": {"type": "keyword"}
        }
    }
}

try:
    response = es.indices.create(index=INDEX_NAME, body=mapping)
    print(f"{GREEN}[SUCCESS] Index '{INDEX_NAME}' created successfully!{RESET}")
    print(response)
except BadRequestError as e:
    print(f"{RED}[ERROR] BadRequestError: Invalid mapping or syntax.{RESET}")
    print(e.info)
except Exception as e:
    print(f"{RED}[ERROR] Unexpected error: {e}{RESET}")
