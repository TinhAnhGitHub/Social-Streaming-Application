# 🧠 Social Streaming Application. Elasticsearch + Kibana Setup Guide

This document provides a complete step-by-step guide to set up and run the **Social Streaming Application** using **Docker**, **Elasticsearch**, and **Kibana**, as well as how to create indices and inject data.

---

## 📦 Prerequisites

Before starting, make sure you have the following installed:

- [Docker](https://www.docker.com/get-started)
- [Python 3.10+](https://www.python.org/downloads/)
- Python packages:
  ```bash
  pip install elasticsearch
  ```

---

## ⚙️ Step 1: Run Elasticsearch and Kibana with Docker

Use the following **`docker-compose.yml`** file (already included in this project) to start the services:

```bash
docker-compose up -d
```

Check that both containers are running:

```bash
docker ps
```

Expected output:
```
CONTAINER ID   IMAGE                                                  PORTS
abcd1234...    docker.elastic.co/elasticsearch/elasticsearch:8.13.4   0.0.0.0:9200->9200/tcp
efgh5678...    docker.elastic.co/kibana/kibana:8.13.4                 0.0.0.0:5601->5601/tcp
```

Now you can open:
- **Elasticsearch:** http://localhost:9200  
- **Kibana:** http://localhost:5601

---

## 🧩 Step 2: Create an Index in Elasticsearch

Run the provided script to create an index with the correct mapping:

```bash
python src/create_index_1.py
```

Example output:
```
Deleted old index: social_stream
Index 'social_stream' created successfully!
{'acknowledged': True, 'shards_acknowledged': True, 'index': 'social_stream'}
```

✅ If you see green messages — your index was created successfully.

---

## 📤 Step 3: Inject Sample Data

To populate Elasticsearch with example data, run:

```bash
python src/inject_1.py
```

Expected output:
```
Injecting data from data/sample.json into index 'social_stream' ...
Data successfully injected into Elasticsearch!
```

Your data is now ready for visualization and analysis in Kibana.

---

## 📊 Step 4: Explore Data in Kibana

1. Open **Kibana** at [http://localhost:5601](http://localhost:5601).
2. Go to **"Discover"**.
3. When prompted to create an *Index Pattern*, enter:
   ```
   social_stream*
   ```
   Then click **Next**.
4. Choose the `timestamp` field as the time filter and click **Create index pattern**.
5. You can now explore your indexed data in Kibana!

---

## 🧠 Step 5: Verify Data via Python Console

You can test your connection manually:

```python
from elasticsearch import Elasticsearch
es = Elasticsearch("http://localhost:9200")
print(es.info())
```

Expected output (simplified):
```json
{
  "name": "elasticsearch",
  "cluster_name": "docker-cluster",
  "version": { "number": "8.13.4" }
}
```

---

## 🧹 Step 6: Stop or Clean Up

To stop all containers:
```bash
docker-compose down
```

To remove containers, networks, and volumes:
```bash
docker-compose down -v
```

---

## ✅ Troubleshooting

| Issue | Cause | Solution |
|-------|--------|-----------|
| `BadRequestError(400)` | Wrong mapping format | Check JSON syntax in `create_index_1.py` |
| Kibana asks for enrollment token | Security is still enabled | Ensure `xpack.security.enabled=false` in `docker-compose.yml` |
| Cannot connect to `localhost:9200` | Container not running or port conflict | Run `docker ps` and ensure port `9200` is not used by another service |
| “Name must match one or more data streams” in Kibana | Wrong index pattern | Use `social_stream*` instead of `filebeat-*` |

---

## 🧾 File Overview

| File | Description |
|------|-------------|
| `docker-compose.yml` | Docker setup for Elasticsearch & Kibana |
| `src/create_index_1.py` | Creates a clean index and mapping |
| `src/inject_1.py` | Loads sample JSON data into Elasticsearch |
| `data/sample.json` | Example dataset for ingestion |

---

## 🎯 Final Notes

- This setup uses **Elasticsearch 8.13.4** and **Kibana 8.13.4** in single-node mode.
- Security is **disabled** for simplicity in local development.
- For production, **enable authentication and SSL**.

---