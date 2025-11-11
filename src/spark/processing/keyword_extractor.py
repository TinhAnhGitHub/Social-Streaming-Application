from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, ArrayType
from keybert import KeyBERT

kw_model = KeyBERT()

def extract_keywords(text, top_k=20):
    kws = kw_model.extract_keywords(text)
    return [k[0] for k in kws[:top_k]]

keyword_extractor_udf = udf(extract_keywords, ArrayType(StringType()))
