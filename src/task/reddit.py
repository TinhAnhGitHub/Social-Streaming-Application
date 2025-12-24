import time
from datetime import datetime, timezone
from confluent_kafka import Producer
from prefect import task
from src.kafka.publisher import KafkaPublisher, build_envelope
from src.clients.reddit_client import RedditClient
from prefect.cache_policies import NO_CACHE
from src.config import load_settings
from prefect.logging import get_logger


logger = get_logger()

@task(
    name="Fetch and Publish Reddit Events",
    retries=3,
    retry_delay_seconds=10,
    timeout_seconds=3600, 
    cache_policy=NO_CACHE
)
def fetch_and_publish_reddit_events(
    reddit_client: RedditClient,
    kafka_publisher: KafkaPublisher
):
    """
    Fetch reddit events and publish them to kafka with rate limit
    Rate limiting strategy:
    - Base delay between API calls: 2 seconds (Reddit rate limit: ~60 requests/min)
    - After fetching subreddits: 1 second delay
    - After each post fetch: 2 seconds delay
    - After each comment batch: 1 second delay


    """
    settings = load_settings()

    emitted_at = datetime.now(timezone.utc)

    try:
        logger.info("Fetching trending subreddits...")
        subreddits = reddit_client.fetch_trending_subreddits(
            limit=settings.reddit.REDDIT_SUBREDDIT_LIMIT
        )

        time.sleep(1)
        logger.info(f"Found {len(subreddits)} trending subreddits: {subreddits}")




        for subreddit_idx, subreddit_name in enumerate(subreddits, 1):
            logger.info(
                f"Processing subreddit {subreddit_idx}/{len(subreddits)}: r/{subreddit_name}"
            )
            posts = list(
                reddit_client._iter_subreddit_posts(
                    subreddit_name,
                    limit=settings.reddit.REDDIT_POSTS_PER_SUBREDDIT,
                    post_sort=settings.reddit.REDDIT_POST_SORT,
                )
            )

            logger.info(f"Fetched {len(posts)} posts from r/{subreddit_name}")
            time.sleep(1)
            for  submission in posts:
                try:
                    submission_event = reddit_client._serialize_submission(submission)
                    created_utc = submission_event.created_utc
                   
                            
                    envelope = build_envelope(
                        entity_type="reddit_submission",
                        source="reddit",
                        payload=submission_event,
                        emitted_at=emitted_at,
                        metadata={
                            "subreddit": subreddit_name,
                            "post_sort": settings.reddit.REDDIT_POST_SORT,
                        }
                    )
                    logger.info(f"publising...")

                    kafka_publisher.publish(
                        topic=settings.topics.TOPIC_REDDIT_SUBMISSIONS,
                        key=submission_event.id,
                        payload=envelope,
                        headers={"source": "reddit", "entity_type": "submission"},
                    )

                    logger.info(
                        f"Published submission {submission_event.id} from r/{subreddit_name}"
                    )

                    if settings.reddit.REDDIT_COMMENT_LIMIT > 0:
                        logger.info(
                            f"Fetching comments for submission {submission_event.id}..."
                        )
                        comments = list(
                            reddit_client._iter_submission_comments(
                                submission,
                                limit=settings.reddit.REDDIT_COMMENT_LIMIT,
                            )
                        )

                        for comment_event in comments:
                            try:

                                logger.debug(f"publising in coments...")
                                comment_envelope = build_envelope(
                                    entity_type="reddit_comment",
                                    source="reddit",
                                    payload=comment_event,
                                    emitted_at=emitted_at,
                                    metadata={
                                        "subreddit": subreddit_name,
                                        "submission_id": submission_event.id,
                                    }
                                )
                                
                                kafka_publisher.publish(
                                    topic=settings.topics.TOPIC_REDDIT_COMMENTS,
                                    key=comment_event.id,
                                    payload=comment_envelope,
                                    headers={"source": "reddit", "entity_type": "comment"},
                                )
                            except Exception as e:
                                logger.info(
                                    f"Failed to publish comment {comment_event.id}: {e}"
                                )
                        time.sleep(1)
                        logger.info(
                            f"Published {len(comments)} comments for submission {submission_event.id}"
                        )
                except Exception as e:
                    raise e
                    
            logger.info(
                f"Completed r/{subreddit_name}: "
            )
        kafka_publisher.flush(timeout=30.0)


        logger.info(
            "Reddit ingestion completed successfully",
        )
    except Exception as e:
        logger.exception(f"Reddit ingestion failed: {e}")
        raise
    
    finally:
        try:
            kafka_publisher.close(timeout=10.0)
        except Exception as e:
            logger.info(f"Error closing Kafka publisher: {e}")
    

