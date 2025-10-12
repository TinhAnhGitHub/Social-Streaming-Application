# vector_search.py
import sys
import time
from pymilvus import connections, Collection
from pymilvus.exceptions import MilvusException

COLLECTION = "reddit_vectors"

def connect_with_retry(host="127.0.0.1", port="19530", retries=30, wait=2):
    for i in range(retries):
        try:
            connections.connect(host=host, port=port)
            return
        except MilvusException as e:
            print(f"⏳ Waiting Milvus... ({i+1}/{retries}) {e}")
            time.sleep(wait)
    raise RuntimeError("❌ Không thể kết nối Milvus.")

def main():
    connect_with_retry()

    col = Collection(COLLECTION)
    col.load()  # load index vào RAM

    vectors = None

    if vectors is None:
        sample = col.query(expr="", output_fields=["embedding"], limit=1)
        if not sample:
            print("Collection rỗng, không có gì để search.")
            return
        vectors = [sample[0]["embedding"]]

    params = {"metric_type": "COSINE", "params": {"ef": 128}}
    results = col.search(
        data=vectors,
        anns_field="embedding",
        param=params,
        limit=5,
        output_fields=["id", "subreddit", "title", "score", "num_comments", "created_utc"]
    )

    print(f"🔎 Top-{len(results[0])} kết quả:")
    for rank, hit in enumerate(results[0], start=1):
        ent = hit.entity
        print(
            f"{rank:02d}. id={ent.get('id')} | subreddit={ent.get('subreddit')} | "
            f"score={ent.get('score')} | sim={hit.score:.3f}\n"
            f"    title={ent.get('title')[:120] if ent.get('title') else ''}"
        )

if __name__ == "__main__":
    main()
