from __future__ import annotations
from functools import lru_cache
from typing import Any, Literal
from pydantic_settings import BaseSettings, SettingsConfigDict
from dotenv import load_dotenv
from pydantic import BaseModel

load_dotenv(dotenv_path='./.env.kafka', verbose=True)


class KafkaSettings(BaseSettings):

    KAFKA_BOOTSTRAP_SERVERS: str
    KAFKA_CLIENT_ID: str

    def build_producer_config(self) -> dict[str, Any]:
        cfg = {
            "bootstrap.servers": self.KAFKA_BOOTSTRAP_SERVERS,
            "client.id": self.KAFKA_CLIENT_ID,
        }
        return cfg


class RedditSettings(BaseSettings):
    REDDIT_CLIENT_ID: str
    REDDIT_CLIENT_SECRET: str
    REDDIT_USER_AGENT: str
    REDDIT_SUBREDDIT_LIMIT: int = 5
    REDDIT_POSTS_PER_SUBREDDIT: int = 10
    REDDIT_COMMENT_LIMIT: int = 20
    REDDIT_POST_SORT: Literal["hot", "new", "top", "rising"] = "hot"


class TopicSettings(BaseSettings):
    TOPIC_REDDIT_SUBMISSIONS: str = "reddit.submissions"
    TOPIC_REDDIT_COMMENTS: str = "reddit.comments"


class AppSettings(BaseModel):
    model_config = SettingsConfigDict(extra="ignore")

    kafka: KafkaSettings
    reddit: RedditSettings
    topics: TopicSettings

@lru_cache(maxsize=1)
def load_settings() -> AppSettings:
    kafka = KafkaSettings() #type:ignore
    reddit = RedditSettings() #type:ignore
    topics = TopicSettings()
    settings = AppSettings(
        kafka=kafka,
        reddit=reddit,
        topics=topics
    )
    return settings


__all__ = [
    "AppSettings",
    "KafkaSettings",
    "RedditSettings",
    "TopicSettings",
    "load_settings",
]
