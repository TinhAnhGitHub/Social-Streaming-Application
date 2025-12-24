import json
import threading
from dataclasses import dataclass

from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential
from typing import Any, Callable, Mapping, MutableMapping, Optional, Sequence, Literal
from datetime import datetime
from src.clients.schema import KafkaEnvelope, RedditCommentEvent, RedditSubmissionEvent
from confluent_kafka import Producer, KafkaError, KafkaException
from prefect.logging import get_logger

logger = get_logger()

class KafkaPublishError(RuntimeError):
    """Raised when a non-retriable Kafka delivery failure occurs."""
 
 
class KafkaPublishRetriableError(KafkaPublishError):
    """Raised for retryable Kafka delivery failures."""



@dataclass(frozen=True)
class PublishResult: 
    topic: str
    partition: str
    offset: int

def default_serializer(payload: Any):
    try:
        return json.dumps(payload, ensure_ascii=False).encode('utf-8')
    except Exception as e:
        raise KafkaPublishError(f"Failed to serialise payload to JSON: {e}") from e
    


 
def build_envelope(
    *,
    entity_type: Literal["reddit_submission", "reddit_comment"],
    source: Literal["reddit", "x"],
    payload: RedditSubmissionEvent | RedditCommentEvent ,
    emitted_at: datetime,
    metadata: Optional[dict[str, str]] = None,
) -> dict[str, object]:
    payload_dict = payload.model_dump(mode="json", exclude_none=True)
    envelope = {
        "entity_type": entity_type,
        "source": source,
        "mode": "trending",
        "payload": payload_dict,
        "emitted_at": emitted_at.isoformat(),
    }
    if metadata:
        envelope["metadata"] = metadata
    return envelope


class KafkaPublisher:
    def __init__(self, *, producer: Producer, max_attempts: int =5, wait_initial: float=1.0, wait_max: float=32.0, delivery_timeout: float = 32.0):
        self._producer = producer 
        self.max_attempts = max_attempts
        self.wait_initial = wait_initial
        self.wait_max = wait_max
        self.delivery_timeout = delivery_timeout
        self.serializer = lambda x: default_serializer(x)
    

    

    def _prepare_payload(self, payload:Any) -> bytes:
        if isinstance(payload, KafkaEnvelope) :
            payload = payload.to_dict()
        elif hasattr(payload, "model_dump"):
            payload = payload.model_dump()
        elif hasattr(payload, "dict"):
            payload = payload.dict()
        return self.serializer(payload)

    def _prepare_headers(self, headers) -> Sequence[tuple[str, bytes]] | None:
        if headers is None:
            return None
        
        if isinstance(headers, Sequence):
            return [(key, value.encode('utf-8') )for key, value in headers]
        return [(key, value.encode('utf-8')) for key, value in headers.items()]

    def publish(
        self,
        *,
        topic: str,
        key: str,
        payload: Any,
        headers = None
    ):

        prepared_payload = self._prepare_payload(payload)
        prepared_header = self._prepare_headers(headers)

        @retry(
            reraise=True,
            stop=stop_after_attempt(self.max_attempts),
            wait=wait_exponential(multiplier=self.wait_initial, max=self.wait_max),
            retry=retry_if_exception_type(KafkaPublishRetriableError),
        )
        def _send() -> PublishResult:
            deliver_event = threading.Event()
            deliver_error: list = []
            deliver_metadata: MutableMapping[str, Any] = {}

            def _deliver_callback(err: KafkaError | None, msg) -> None:
                if err is not None:
                    exception_cls = KafkaPublishError
                    if not err.fatal():
                        exception_cls = KafkaPublishRetriableError
                    deliver_error.append(exception_cls(f"Kafka delivery error: {err}"))
                else:
                    deliver_metadata.update(
                        {
                            'topic': msg.topic(),
                            'partition': msg.partition(),
                            'offset': msg.offset()
                        }
                    )

                deliver_event.set()
            
            try:
                logger.debug("Before producing...")
                self._producer.produce( #type:ignore
                    topic=topic,
                    key=key,
                    value=prepared_payload,
                    headers=prepared_header,
                    on_delivery=_deliver_callback
                )
            except BufferError as e:
                self._producer.poll(0) #type:ignore
                raise KafkaPublishRetriableError("Local producer queue is full, backing off...") from e
            
            except KafkaException as e:
                kafka_error = e.args[0] if e.args else None
                if isinstance(kafka_error, KafkaError) and kafka_error.fatal(): #type:ignore
                    raise KafkaPublishError(f"Kafka produce failed: {kafka_error}") from e
                raise KafkaPublishRetriableError(f"Kafka produce failed: {kafka_error}") from e
            
            logger.debug("Call polling...")
            self._producer.poll(0.5)  #type:ignore

            if not deliver_event.wait(self.delivery_timeout):
                raise KafkaPublishRetriableError(f"timeout waiting for delivery report after {self.delivery_timeout}")
            
            if deliver_error:
                raise deliver_error[0]
            

            return PublishResult(**deliver_metadata)
        
        result = _send()

        return result
    

    def flush(self, timeout: float | None = None):
        remaining = self._producer.flush(timeout) #type:ignore
        if remaining > 0:
            raise KafkaPublishError(f"Failed to flush kafka producer; {remaining} messages (s) pending")
        

    def close(self, timeout: float | None = None) -> None:
        try:
            self.flush(timeout)
        finally:
            self._producer = None
    
    
