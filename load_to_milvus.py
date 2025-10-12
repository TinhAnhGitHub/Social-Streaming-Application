import base64, pickle, orjson, time
from pathlib import Path
from datetime import datetime
import numpy as np
from pymilvus import (
    connections, FieldSchema, CollectionSchema, DataType, Collection, utility
)
from pymilvus.exceptions import MilvusException

INPUT = "part-00000-0b3cee1d-e0a3-4407-ade8-d6548df15c7c-c000.json"
COLLECTION = "reddit_vectors"
BATCH = 512

def decode_vec(s):
    arr = pickle.loads(base64.b64decode(s))
    if isinstance(arr, np.ndarray):
        return arr.astype("float32").ravel()
    return np.array(arr, dtype="float32").ravel()

def to_epoch(dt_str):
    if isinstance(dt_str, str) and dt_str.endswith("Z"):
        dt_str = dt_str.replace("Z", "+00:00")
    return int(datetime.fromisoformat(dt_str).timestamp())

raw = orjson.loads(Path(INPUT).read_bytes())
print(f"Records in JSON: {len(raw)}")
if not raw:
    print("File dữ liệu rỗng. Kiểm tra lại output Spark.")
    exit(0)

dim = decode_vec(raw[0]["embedding"]).shape[0]
print("Embedding dim:", dim)

for i in range(30):
    try:
        connections.connect(host="127.0.0.1", port="19530")
        break
    except MilvusException as e:
        print(f"Waiting Milvus... ({i+1}/30) {e}")
        time.sleep(3)
else:
    raise RuntimeError("Không thể kết nối Milvus sau 90s")

if utility.has_collection(COLLECTION):
    utility.drop_collection(COLLECTION)

fields = [
    FieldSchema(name="id",           dtype=DataType.VARCHAR, max_length=32, is_primary=True),
    FieldSchema(name="entity_type",  dtype=DataType.VARCHAR, max_length=32),
    FieldSchema(name="source",       dtype=DataType.VARCHAR, max_length=16),
    FieldSchema(name="subreddit",    dtype=DataType.VARCHAR, max_length=64),
    FieldSchema(name="title",        dtype=DataType.VARCHAR, max_length=512),
    FieldSchema(name="body",         dtype=DataType.VARCHAR, max_length=4096),
    FieldSchema(name="created_utc",  dtype=DataType.INT64),
    FieldSchema(name="score",        dtype=DataType.INT32),
    FieldSchema(name="num_comments", dtype=DataType.INT32),
    FieldSchema(name="embedding",    dtype=DataType.FLOAT_VECTOR, dim=dim),
]
schema = CollectionSchema(fields, description="Reddit events with embeddings")
col = Collection(name=COLLECTION, schema=schema)

#Tạo index vector
col.create_index(
    field_name="embedding",
    index_params={"metric_type": "COSINE", "index_type": "HNSW",
                  "params": {"M": 32, "efConstruction": 200}}
)

buf = {f.name: [] for f in fields}
inserted = 0

def flush_batch(force=False):
    global inserted
    if buf["id"] and (len(buf["id"]) >= BATCH or force):
        entities = [buf[k] for k in [f.name for f in fields]]
        mr = col.insert(entities)
        try:
            inserted += sum(mr.insert_count) if hasattr(mr, "insert_count") else len(buf["id"])
        except Exception:
            inserted += len(buf["id"])
        for k in buf: buf[k].clear()

for rec in raw:
    p = rec["payload"]
    vec = decode_vec(rec["embedding"])

    buf["id"].append(p["id"])
    buf["entity_type"].append(rec.get("entity_type",""))
    buf["source"].append(rec.get("source",""))
    buf["subreddit"].append(p.get("subreddit",""))
    buf["title"].append(p.get("title","") if rec["entity_type"]=="reddit_submission" else "")
    buf["body"].append(p.get("body",""))
    buf["created_utc"].append(to_epoch(p.get("created_utc")))
    buf["score"].append(int(p.get("score",0)))
    buf["num_comments"].append(int(p.get("num_comments",0)))
    buf["embedding"].append(vec.tolist())

    flush_batch()

flush_batch(force=True)

col.flush()

try:
    from pymilvus import utility
    if hasattr(utility, "wait_for_flush_complete"):
        utility.wait_for_flush_complete([col.name])
    elif hasattr(utility, "wait_for_flushed"):
        utility.wait_for_flushed([col.name])
    else:
        print("Không tìm thấy hàm wait_for_flush*, bỏ qua bước chờ flush đồng bộ.")
except Exception as e:
    print(f"Bỏ qua flush sync: {e}")

print(f"Inserted (reported): {inserted}")
print("Entities in collection (after flush):", col.num_entities)
