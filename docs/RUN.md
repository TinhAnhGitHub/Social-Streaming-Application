# Running the Application

## Start Services

Start all services using Docker Compose with the Kafka environment configuration:
```bash
docker compose --env-file .env.kafka up -d --build
```

## Monitor Logs

### Prefect Worker and Kafka

Monitor the Prefect custom worker and Kafka logs:
```bash
docker compose logs -f prefect-custom-worker
```

### Spark Streaming Jobs

Watch Spark consume from Kafka and stream data to Elasticsearch:

**Submissions streaming:**
```bash
docker compose logs -f spark-streaming-submissions
```

**Comments streaming:**
```bash
docker compose logs -f spark-streaming-comments
```

## Verify Elasticsearch Indexing

### Check Document Counts

**Submissions index:**
```bash
curl -s http://localhost:9200/reddit_submissions/_count
```

**Comments index:**
```bash
curl -s http://localhost:9200/reddit_comments/_count
```

### Check Indexing Statistics
```bash
curl -s http://localhost:9200/reddit_submissions/_stats/indexing
```