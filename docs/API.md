## Clean raw text
- Endpoint: `clean_text_udf()`.
- Input: Raw text (`string`).
- Ouput: Cleaned text.

## Extract keywords
- Endpoint: `keyword_extractor_udf()`.
- Input: Cleaned text.
- Output: List of top 20 keywords.

## Sentence embedding
- Endpoint: `embedder_udf()`.
- Input: Cleaned text.
- Output: 384 dimension array embedding vector of sentence.