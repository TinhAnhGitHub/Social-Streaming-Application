from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import re

def clean_text(text):
    if not text:
        return ""
    
    text = text.lower()
    text = re.sub(r"http\S+", "", text)
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text

clean_text_udf = udf(clean_text, StringType())