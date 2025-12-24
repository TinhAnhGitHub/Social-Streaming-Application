docker exec -it broker-1 env KAFKA_OPTS="" kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 3 --partitions 3 --topic reddit.submissions

docker exec -it broker-1 env KAFKA_OPTS="" kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 3 --partitions 3 --topic reddit.comments

curl -X PUT "localhost:9200/_all/_settings" -H 'Content-Type: application/json' -d'
{
  "index.number_of_replicas": 0
}'

