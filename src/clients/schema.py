from __future__ import annotations
from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional, Union, TypeVar, Generic, Literal, Any


class RedditSubmissionEvent(BaseModel):
    id: str = Field(..., description="Unique Reddit ID of the submission (e.g., 'abc123').")
    subreddit: str = Field(..., description="Name of the subreddit where the submission was posted.")
    author: Optional[str] = Field(
        None, description="Username of the post author, or None if the account was deleted or suspended."
    )
    title: str = Field(..., description="Title of the submission.")
    body: Optional[str] = Field(
        "", description="Text content of the submission (selftext). Empty for link, image, or video posts."
    )
    created_utc: datetime = Field(..., description="UTC datetime when the post was created.")
    score: int = Field(..., description="Current score (upvotes - downvotes) of the post.")
    num_comments: int = Field(..., description="Number of comments on the post.")
    url: str = Field(..., description="External URL or media link associated with the post, if applicable.")
    permalink: str = Field(..., description="Relative Reddit permalink (e.g., '/r/.../comments/...').")
    flair: Optional[str] = Field(None, description="Flair text assigned to the post, if any.")


class RedditCommentEvent(BaseModel):
    id: str = Field(..., description="Unique Reddit ID of the comment (e.g., 'def456').")
    submission_id: str = Field(..., description="ID of the parent submission this comment belongs to.")
    parent_id: str = Field(
        ..., description="Fullname of the parent (e.g., 't3_<submission_id>' for post or 't1_<comment_id>' for another comment)."
    )
    subreddit: str = Field(..., description="Name of the subreddit where the comment was posted.")
    author: Optional[str] = Field(
        None, description="Username of the comment author, or None if deleted or suspended."
    )
    body: str = Field(..., description="Text content of the comment.")
    created_utc: datetime = Field(..., description="UTC datetime when the comment was created.")
    score: int = Field(..., description="Current score (upvotes - downvotes) of the comment.")
    permalink: str = Field(..., description="Relative Reddit permalink for the comment (e.g., '/r/.../comments/...').")
    controversiality: Optional[int] = Field(
        None, description="0 if normal, 1 if the comment is flagged as controversial by Reddit."
    )
   

EventPayload = TypeVar(
    "EventPayload",
    RedditSubmissionEvent,
    RedditCommentEvent,
)

class KafkaEnvelope(Generic[EventPayload]):
    """
    Common architectural pattern used for sending event payload
    """
    entity_type: Literal['reddit_submission', 'reddit_comment']
    source: Literal['reddit']
    mode: Literal['trending']
    payload: EventPayload
    emitted_at: datetime
    metadata: dict[str,str] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "entity_type": self.entity_type,
            "source": self.source,
            "mode": self.mode,
            "emitted_at": self.emitted_at.isoformat(),
            "payload": self.payload.model_dump(mode='json'),
            "metadata": dict(self.metadata) if self.metadata else None,
        }


 
__all__ = [
    "KafkaEnvelope",
    "RedditCommentEvent",
    "RedditSubmissionEvent"
]