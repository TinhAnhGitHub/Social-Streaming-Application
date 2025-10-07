## Docker

```bash
docker compose build
docker compose up -d
docker exec -it spark-master spark-submit --master spark://spark-master:7077 /opt/spark-apps/main.py
```

After finishing
```bash
docker compose down
```