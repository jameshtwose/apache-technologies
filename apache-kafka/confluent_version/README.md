## Apache-kafka
Apache Kafka is a distributed streaming platform. It is used for building real-time data pipelines and streaming apps. It is horizontally scalable, fault-tolerant, wicked fast, and runs in production in thousands of companies.

## Installation and usage (local version)
- `docker compose up -d` to start the kafka cluster and zookeeper locally
- `
docker compose exec broker \ 
kafka-topics --create \
--topic purchases \
--bootstrap-server localhost:9092 \
--replication-factor 1 \
--partitions 1
`
Create a new topic, purchases, which we will use to produce and consume events. We'll use the kafka-topics command located inside the local running Kafka broker.
- `python producer.py getting_started.ini` to run the producer script and pass the getting_started.ini file as an argument
- `python consumer.py getting_started.ini` to run the consumer script and pass the getting_started.ini file as an argument