# 🚀 Reddit Prefect Producer

> **Stream trending Reddit content to Kafka with orchestrated reliability**

A production-ready data ingestion pipeline that discovers trending Reddit content and publishes structured events to Apache Kafka, orchestrated by Prefect for reliability and observability.

---

## 🎯 What It Does

```
Reddit API → Prefect Tasks → Kafka Topics → Your Data Pipeline
   📡            ⚙️              📨              🔮
```

- **Discovers** trending subreddits and hot content using Reddit's public API
- **Normalizes** posts and comments into strongly-typed data models
- **Publishes** events to Kafka with retry logic and delivery guarantees
- **Orchestrates** execution with Prefect for monitoring and scheduling
- **Tracks** progress with watermarking to avoid duplicate processing

---

## 🏗️ Architecture

### System Layers

```
┌─────────────────────────────────────────────┐
│          Orchestration Layer                │
│            (Prefect Flow)                   │
└─────────────────┬───────────────────────────┘
                  │
┌─────────────────▼───────────────────────────┐
│             Task Layer                      │
│   (Rate Limiting, Logic)     │
└─────┬───────────────────────────┬───────────┘
      │                           │
┌─────▼─────────┐         ┌───────▼──────────┐
│ Reddit Client │         │ Kafka Publisher  │
│   (+ Retry)   │         │  (+ Callbacks)   │
└───────────────┘         └──────────────────┘
```

### Key Components

| Component | Purpose | Location |
|-----------|---------|----------|
| 🌐 **Reddit Client** | Fetches trending content with retry logic | `app/clients/reddit_client.py` |
| 📊 **Schema Models** | Pydantic models for type safety | `app/clients/schema.py` |
| 📤 **Kafka Publisher** | Publishes with delivery callbacks | `app/kafka/publisher.py` |
| ⚡ **Ingestion Task** | Core fetch-publish loop | `app/task/reddit.py` |
| 🎭 **Prefect Flow** | Orchestration and dependency wiring | `app/flow.py` |
| ⚙️ **Configuration** | Environment-driven settings | `app/config.py` |

---

## 📦 Data Model

### Message Envelope

Every event is wrapped in a consistent envelope structure:

```json
{
  "entity_type": "reddit_submission",
  "source": "reddit",
  "mode": "trending",
  "emitted_at": "2025-01-01T12:00:00+00:00",
  "payload": { /* event data */ },
  "metadata": {
    "subreddit": "python",
    "post_sort": "hot"
  }
}
```

### Event Types

#### 📝 Submission Event
```json
{
  "id": "abc123",
  "subreddit": "python",
  "author": "someuser",
  "title": "Interesting post",
  "body": "Post content...",
  "created_utc": "2025-01-01T11:59:00+00:00",
  "score": 100,
  "num_comments": 5,
  "url": "https://reddit.com/...",
  "permalink": "/r/python/comments/abc123/...",
  "flair": "Discussion"
}
```

#### 💬 Comment Event
Similar structure with parent linkage and comment-specific fields.

---

## 🎨 Architectural Patterns

- **🧩 Layered Architecture** - Clear separation of concerns
- **📮 Envelope Pattern** - Consistent message structure
- **🌊 Event-Driven Streaming** - Decoupled producer/consumer
- **🔄 Retry with Exponential Backoff** - Resilience to failures
- **💉 Dependency Injection** - Testable and maintainable
- **📋 Configuration as Code** - Type-safe environment settings

---

## ⚙️ Configuration

### Environment Variables

Create a `.env` file from `.env.example`:

#### Reddit Settings
```bash
REDDIT_CLIENT_ID=your_client_id
REDDIT_CLIENT_SECRET=your_secret
REDDIT_USER_AGENT=MyApp/1.0
REDDIT_SUBREDDIT_LIMIT=5          # Trending subreddits to fetch
REDDIT_POSTS_PER_SUBREDDIT=10     # Posts per subreddit
REDDIT_COMMENT_LIMIT=20           # Comments per post (0=disabled)
REDDIT_POST_SORT=hot              # hot, new, top, or rising
```

#### Kafka Settings
```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_CLIENT_ID=reddit-producer
TOPIC_REDDIT_SUBMISSIONS=reddit.submissions
TOPIC_REDDIT_COMMENTS=reddit.comments
```

---

## 🚀 Quick Start

### Installation

```bash
# Install dependencies
uv sync
```

### Run Locally

```bash
# Execute the flow
uv run app/flow.py
```

### Validate with Consumer

```bash
# Run the reference consumer to see messages
uv run consumer/consumer.py
```

---

## 🔄 Control Flow

```
1. Load Configuration
        ↓
2. Initialize Clients (Reddit + Kafka)
        ↓
3. Discover Trending Subreddits
        ↓
4. For Each Subreddit:
   ├─ Fetch Posts (sorted by hot/new/top/rising)
   ├─ Filter by Watermark
   ├─ Serialize to Submission Event
   ├─ Wrap in Envelope
   ├─ Publish to Kafka
   └─ For Each Post:
      ├─ Fetch Comments (limited)
      ├─ Serialize to Comment Events
      └─ Publish to Kafka
        ↓
5. Flush & Close Publisher
```

---

## 🛡️ Reliability Features

### Error Handling

| Error Type | Strategy | Implementation |
|------------|----------|----------------|
| 🌐 Reddit API Failures | Exponential backoff retry | `tenacity` with max attempts |
| 📤 Kafka Publish Failures | Delivery callbacks + retry | Retriable vs. fatal error classification |
| 💾 Buffer Full | Backpressure detection | Treated as retriable, automatic retry |

### Observability

- **📊 Prefect Logs** - Flow and task execution tracking
- **🔍 Structured Logging** - Event metadata and errors
- **✅ Delivery Callbacks** - Per-message confirmation
- **🏷️ Consumer Validation** - Reference implementation for testing

---

## 🎛️ Operations & Tuning

### Rate Limiting

Conservative sleep intervals between API calls respect Reddit's public API limits. Adjust in `app/task/reddit.py` as needed.

### Throughput

Scale up ingestion by increasing:
- `REDDIT_SUBREDDIT_LIMIT` - More subreddits
- `REDDIT_POSTS_PER_SUBREDDIT` - More posts per subreddit
- `REDDIT_COMMENT_LIMIT` - More comments per post

⚠️ Ensure Kafka and downstream systems can handle the increased load.
---

## 🔧 Extensibility

### Add New Event Types

1. Define Pydantic model in `app/clients/schema.py`
2. Extend envelope builder in `app/kafka/publisher.py`
3. Route events through the task

### Add New Sources

1. Create client module (similar to `RedditClient`)
2. Use same envelope structure
3. Add dedicated task or extend existing one

### Custom Metadata & Headers

The publisher accepts headers and metadata for:
- Downstream routing
- Filtering and indexing
- Audit trails

---

## 📁 Project Structure

```
reddit-prefect-producer/
├── app/
│   ├── flow.py                  # 🎭 Prefect flow definition
│   ├── config.py                # ⚙️ Configuration management
│   ├── clients/
│   │   ├── reddit_client.py     # 🌐 Reddit API wrapper
│   │   └── schema.py            # 📊 Pydantic models
│   ├── kafka/
│   │   └── publisher.py         # 📤 Kafka publishing logic
│   └── task/
│       └── reddit.py            # ⚡ Main ingestion task
│       
├── consumer/
│   └── consumer.py              # ✅ Reference consumer
├── .env.example                 # 📋 Configuration template
└── README.md                    # 📖 This file
```

---

## ⚠️ Limitations & Considerations

### Ordering
Kafka preserves order **per partition**, not globally. Events for the same key remain ordered within a partition.

### Delivery Semantics
- **Current**: At-least-once delivery
- **Exactly-once**: Requires additional Kafka configuration (idempotent producer, transactions)

### API Variability
Reddit's public API may change or enforce rate limits. Retry policies mitigate but don't eliminate this risk.

---



## 📚 Additional Resources

- [Prefect Documentation](https://docs.prefect.io/)
- [Confluent Kafka Python](https://docs.confluent.io/kafka-clients/python/current/overview.html)
- [PRAW (Reddit API)](https://praw.readthedocs.io/)
- [Pydantic](https://docs.pydantic.dev/)

---
