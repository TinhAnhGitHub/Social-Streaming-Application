from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import base64, pickle
from sentence_transformers import SentenceTransformer

model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')

def embed_text(text):
    embedding = model.encode(text)
    return base64.b64encode(pickle.dumps(embedding)).decode('utf-8')

embedder_udf = udf(embed_text, StringType())