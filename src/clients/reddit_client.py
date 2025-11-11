from __future__ import annotations
from datetime import datetime, timezone
from typing import Callable, Iterator, Literal, Union, Any
import praw
import praw.exceptions
import praw.models
import prawcore
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from .schema import RedditCommentEvent, RedditSubmissionEvent
from src.config import RedditSettings
from prefect.logging import get_logger

logger = get_logger()


RedditEvent = Union[RedditSubmissionEvent, RedditCommentEvent]


class RedditClientError(RuntimeError):
    """Raised when the Reddit client encounters an API failure."""



def _parse_submission_id(comment: praw.models.Comment) -> str:
    link_id = getattr(comment, "link_id", "")
    if link_id.startswith("t3_"):
        return link_id.split("_", maxsplit=1)[-1]

    try:
        return comment.submission.id  # type: ignore[return-value]
    except Exception:  
        return ""


def _to_datetime(value: float | None) -> datetime:
    if value is None:
        raise RedditClientError("Expected timestamp value from Reddit API but received None")
    return datetime.fromtimestamp(value, tz=timezone.utc)


class RedditClient:
    """Thin wrapper around PRAW focused on trending subreddit discovery."""

    def __init__(
        self,
        config: RedditSettings,
        *,
        max_attempts: int = 5,
        wait_initial: float = 1.0,
        wait_max: float = 32.0,
    ) -> None:
        self.config = config
        self.max_attempts = max_attempts
        self.wait_initial = wait_initial
        self.wait_max = wait_max

        try:
            self.api = praw.Reddit(
                client_id=config.REDDIT_CLIENT_ID,
                client_secret=config.REDDIT_CLIENT_SECRET,
                user_agent=config.REDDIT_USER_AGENT,
            )
            self.api.read_only = True
        except Exception as exc:  
            raise RedditClientError("Failed to initialise PRAW client") from exc

        self._retry_exceptions = (
            prawcore.exceptions.PrawcoreException,
            praw.exceptions.PRAWException,
        )

   
    
    def fetch_trending_subreddits(self, *, limit: int = 10) -> list[str]:

        def _call() -> list[str]:
            logger.debug(f"Fetching {limit} trending subreddits")
            return [sub.display_name for sub in self.api.subreddits.popular(limit=limit)]

        return self._call_with_retry("fetch trending subreddits", _call)

    def iter_trending_events(
        self,
        *,
        subreddit_limit: int = 5,
        posts_per_subreddit: int = 10,
        comment_limit: int = 20,
        post_sort: Literal["new", "hot", "top", "rising"] = "hot",
    ) -> Iterator[RedditEvent]:
        """Yield submissions and comments discovered through the trending workflow."""

        subreddits = self.fetch_trending_subreddits(limit=subreddit_limit)
        logger.debug(
            "Beginning trending crawl", extra={"subreddits": subreddits, "post_sort": post_sort}
        )
        for subreddit_name in subreddits:
            for submission in self._iter_subreddit_posts(
                subreddit_name,
                limit=posts_per_subreddit,
                post_sort=post_sort,
            ):
                submission_event = self._serialize_submission(submission)
                yield submission_event

                if comment_limit <= 0:
                    continue

                for comment_event in self._iter_submission_comments(
                    submission,
                    limit=comment_limit,
                ):
                    yield comment_event

    def _iter_subreddit_posts(
        self,
        subreddit_name: str,
        *,
        limit: int,
        post_sort: Literal["new", "hot", "top", "rising"],
    ) -> Iterator[praw.models.Submission]:
        def _fetch_posts() -> list[praw.models.Submission]:
            subreddit = self.api.subreddit(subreddit_name)
            fetcher = getattr(subreddit, post_sort)
            logger.debug(
                f"Fetching {limit} posts for r/{subreddit_name}"
            )
            return list(fetcher(limit=limit))

        submissions = self._call_with_retry(
            f"fetch {post_sort} posts for r/{subreddit_name}",
            _fetch_posts,
        )

        for submission in submissions:
            yield submission

    def _iter_submission_comments(
        self,
        submission: praw.models.Submission,
        *,
        limit: int,
    ) -> Iterator[RedditCommentEvent]:
        def _load_comments() -> list:
            logger.debug(f"Fetching comments for submission {submission.id}")
            submission.comments.replace_more(limit=0)
            return submission.comments.list()

        comments = self._call_with_retry(
            f"fetch comments for submission {submission.id}",
            _load_comments,
        )

        emitted = 0
        for comment in comments:
            yield self._serialize_comment(comment)
            emitted += 1
            if limit and emitted >= limit:
                break

    def _call_with_retry(self, action: str, func: Callable) -> Any:
        @retry(
            reraise=True,
            stop=stop_after_attempt(self.max_attempts),
            wait=wait_exponential(multiplier=self.wait_initial, max=self.wait_max),
            retry=retry_if_exception_type(self._retry_exceptions),
        )
        def _wrapped():
            return func()

        try:
            return _wrapped()
        except self._retry_exceptions as exc:
            raise RedditClientError(f"{action} failed after {self.max_attempts} attempts") from exc

    @staticmethod
    def _serialize_submission(submission: praw.models.Submission) -> RedditSubmissionEvent:
        author = getattr(submission.author, "name", None)
        subreddit = getattr(submission.subreddit, "display_name", "")

        return RedditSubmissionEvent(
            id=submission.id,
            subreddit=subreddit,
            author=author,
            title=submission.title or "",
            body=(getattr(submission, "selftext", "") or ""),
            created_utc=_to_datetime(getattr(submission, "created_utc", None)),
            score=int(getattr(submission, "score", 0) or 0),
            num_comments=int(getattr(submission, "num_comments", 0) or 0),
            url=getattr(submission, "url", "") or "",
            permalink=submission.permalink or "",
            flair=getattr(submission, "link_flair_text", None),
        )

    @staticmethod
    def _serialize_comment(comment: praw.models.Comment) -> RedditCommentEvent:
        submission_id = _parse_submission_id(comment)

        return RedditCommentEvent(
            id=comment.id,
            submission_id=submission_id,
            parent_id=getattr(comment, "parent_id", ""),
            subreddit=getattr(comment.subreddit, "display_name", ""),
            author=getattr(comment.author, "name", None),
            body=comment.body,
            created_utc=_to_datetime(getattr(comment, "created_utc", None)),
            score=int(getattr(comment, "score", 0) or 0),
            permalink=comment.permalink,
            controversiality=getattr(comment, "controversiality", None),
        )





