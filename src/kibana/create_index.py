"""
Elasticsearch Index Creation Script
Creates indices with optimized mappings before streaming starts.
"""

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import RequestError
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
ES_HOST = "http://elasticsearch:9200"
es = Elasticsearch(ES_HOST)


def create_submissions_index():
    """Create reddit_submissions index with optimized mapping."""
    
    index_name = "reddit_submissions"
    
    mapping = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0,
            "refresh_interval": "5s"  
        },
        "mappings": {
            "properties": {
                "entity_type": {
                    "type": "keyword"
                },
                "source": {
                    "type": "keyword"
                },
                "mode": {
                    "type": "keyword"
                },
                "emitted_at": {
                    "type": "date",
                    "format": "strict_date_optional_time||epoch_millis"
                },
                
                "payload": {
                    "properties": {
                        "id": {
                            "type": "keyword"
                        },
                        "subreddit": {
                            "type": "keyword"  # For aggregations (top subreddits)
                        },
                        "author": {
                            "type": "keyword",  # For aggregations (top authors)
                            "ignore_above": 256
                        },
                        "title": {
                            "type": "text",  # Full-text search
                            "fields": {
                                "keyword": {  # For sorting, exact match
                                    "type": "keyword",
                                    "ignore_above": 256
                                }
                            }
                        },
                        "body": {
                            "type": "text",  # Full-text search
                            "analyzer": "english"  # Better English text analysis
                        },
                        "created_utc": {
                            "type": "date"
                        },
                        "score": {
                            "type": "integer"
                        },
                        "num_comments": {
                            "type": "integer"
                        },
                        "url": {
                            "type": "keyword",
                            "index": False  # Don't index URLs (just store)
                        },
                        "permalink": {
                            "type": "keyword",
                            "index": False
                        },
                        "flair": {
                            "type": "keyword"
                        }
                    }
                },
                
                "metadata": {
                    "properties": {
                        "subreddit": {
                            "type": "keyword"
                        },
                        "post_sort": {
                            "type": "keyword"
                        }
                    }
                },
                
                # Spark processing outputs
                "body": {
                    "type": "text"
                },
                "clean_body": {
                    "type": "text",
                    "analyzer": "english"
                },
                "keywords": {
                    "type": "keyword"  # Array of keywords
                },
                "embedding": {
                    "type": "keyword",  # Base64 encoded, don't analyze
                    "index": False      # Don't index (just store for retrieval)
                },
                "es_id": {
                    "type": "keyword"
                }
            }
        }
    }
    
    try:
        if es.indices.exists(index=index_name):
            logger.info(f"Index '{index_name}' already exists, deleting...")
            es.indices.delete(index=index_name)
        
        es.indices.create(index=index_name, body=mapping)
        logger.info(f"✅ Created index: {index_name}")
        
        mapping_result = es.indices.get_mapping(index=index_name)
        logger.info(f"Index created with {len(mapping_result[index_name]['mappings']['properties'])} top-level fields")
        
        return True
        
    except RequestError as e:
        logger.error(f"❌ Failed to create index: {e}")
        return False


def create_comments_index():
    """Create reddit_comments index with optimized mapping."""
    
    index_name = "reddit_comments"
    
    mapping = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0,
            "refresh_interval": "5s"
        },
        "mappings": {
            "properties": {
                # Top-level fields
                "entity_type": {
                    "type": "keyword"
                },
                "source": {
                    "type": "keyword"
                },
                "mode": {
                    "type": "keyword"
                },
                "emitted_at": {
                    "type": "date",
                    "format": "strict_date_optional_time||epoch_millis"
                },
                
                # Payload (Reddit comment data)
                "payload": {
                    "properties": {
                        "id": {
                            "type": "keyword"
                        },
                        "submission_id": {
                            "type": "keyword"  # For joining with submissions
                        },
                        "parent_id": {
                            "type": "keyword"
                        },
                        "subreddit": {
                            "type": "keyword"
                        },
                        "author": {
                            "type": "keyword",
                            "ignore_above": 256
                        },
                        "body": {
                            "type": "text",
                            "analyzer": "english"
                        },
                        "created_utc": {
                            "type": "date"
                        },
                        "score": {
                            "type": "integer"
                        },
                        "permalink": {
                            "type": "keyword",
                            "index": False
                        },
                        "controversiality": {
                            "type": "integer"
                        }
                    }
                },
                
                # Metadata
                "metadata": {
                    "properties": {
                        "subreddit": {
                            "type": "keyword"
                        },
                        "submission_id": {
                            "type": "keyword"
                        }
                    }
                },
                
                # Spark processing outputs
                "body": {
                    "type": "text"
                },
                "clean_body": {
                    "type": "text",
                    "analyzer": "english"
                },
                "keywords": {
                    "type": "keyword"
                },
                "embedding": {
                    "type": "keyword",
                    "index": False
                },
                "es_id": {
                    "type": "keyword"
                }
            }
        }
    }
    
    try:
        if es.indices.exists(index=index_name):
            logger.info(f"Index '{index_name}' already exists, deleting...")
            es.indices.delete(index=index_name)
        
        es.indices.create(index=index_name, body=mapping)
        logger.info(f"Created index: {index_name}")
        
        return True
        
    except RequestError as e:
        logger.error(f"Failed to create index: {e}")
        return False


def ensure_index_exists(index_name: str, topic: str):
    if not es.indices.exists(index=index_name):
        print(f"Index '{index_name}' doesn't exist, creating...")
        if topic == "reddit.submissions":
            create_submissions_index()
        elif topic == "reddit.comments":
            create_comments_index()
        print(f"Index '{index_name}' created")
    else:
        print(f"Index '{index_name}' already exists, skipping creation")
