## Docker

```bash
docker compose build
docker compose up -d
```

To run sample with mock data
```bash
docker exec -it spark-master spark-submit --master spark://spark-master:7077 /opt/spark-apps/main.py
```

To listen to reddit submissions
```bash
docker exec -it spark-master spark-submit --master spark://spark-master:7077 /opt/spark-apps/main_kafka.py --topic reddit.submissions
```

To listen to reddit comments
```bash
docker exec -it spark-master spark-submit --master spark://spark-master:7077 /opt/spark-apps/main_kafka.py --topic reddit.comments
```

After finishing
```bash
docker compose down
```