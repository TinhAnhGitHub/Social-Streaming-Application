from pymilvus import connections, Collection

connections.connect(host="127.0.0.1", port="19530")

col = Collection("reddit_vectors")

from pymilvus import DataType

type_map = {
    DataType.BOOL: "BOOL",
    DataType.INT8: "INT8",
    DataType.INT16: "INT16",
    DataType.INT32: "INT32",
    DataType.INT64: "INT64",
    DataType.FLOAT: "FLOAT",
    DataType.DOUBLE: "DOUBLE",
    DataType.VARCHAR: "VARCHAR",
    DataType.JSON: "JSON",
    DataType.FLOAT_VECTOR: "FLOAT_VECTOR",
    DataType.BINARY_VECTOR: "BINARY_VECTOR"
}

print("Collection name:", col.name)
print("Description:", col.description)
print("\nFields:")
for f in col.schema.fields:
    dtype_name = type_map.get(f.dtype, f.dtype)
    dim = f.params.get("dim") if hasattr(f, "params") and "dim" in f.params else None
    print(f"- {f.name:15} | {dtype_name:13} | is_primary={f.is_primary} | dim={dim}")
