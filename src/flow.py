from prefect import flow
import sys
import os
ROOT_DIR = os.path.abspath(
    os.path.join(__file__, '../..')
)
print(ROOT_DIR)
sys.path.insert(0, ROOT_DIR)

from src.config import load_settings
from src.clients.reddit_client import RedditClient, RedditEvent
from confluent_kafka import Producer
from src.kafka.publisher import KafkaPublisher
from src.task.reddit import fetch_and_publish_reddit_events
from prefect.logging import get_logger
from prefect import serve



logger = get_logger()
logger.setLevel('INFO')
@flow(
    name="Reddit Trending Ingestion",
    description="Fetch trending Reddit posts and comments, publish to Kafka",
    retries=1,
    retry_delay_seconds=60,
)
def streaming_flow():
    """
    Main Prefect flow for ingesting trending Reddit data.
    
    This flow:
    1. Initializes Reddit and Kafka clients
    2. Fetches trending subreddits
    3. Fetches posts and comments from each subreddit
    4. Publishes all events to Kafka topics
    5. Implements rate limiting to respect Reddit API limits
    
    Returns:
        Statistics about the ingestion run
    """
    settings = load_settings()
    reddit_client = RedditClient(settings.reddit)
    print("Reddit client initialized successfully")
    
    producer_config = settings.kafka.build_producer_config()
    print(f"{producer_config=}")
    
    producer = Producer(producer_config)
    kafka_publisher = KafkaPublisher(
        producer=producer,
        max_attempts=5,
        wait_initial=32.0,
        wait_max=1,
        delivery_timeout=5.0,
    )
    print("Kafka publisher initialized successfully")


    fetch_and_publish_reddit_events(reddit_client, kafka_publisher)

    return


if __name__ == "__main__":
    streaming_flow()
