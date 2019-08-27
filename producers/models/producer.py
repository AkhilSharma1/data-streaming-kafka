"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)

BROKER_URL = "PLAINTEXT://localhost:9092,PLAINTEXT://localhost:9093,PLAINTEXT://localhost:9094"



class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        self.producer = AvroProducer(
            {
                "bootstrap.servers": BROKER_URL,
                "schema.registry.url": "http://localhost:8081"
            },
            default_key_schema=key_schema, default_value_schema=value_schema
        )

    def topic_exists(self, client, topic_name):
        """Checks if the given topic exists"""
        topic_metadata = client.list_topics(timeout=5)
        return topic_name in set(
            t.topic for t in iter(topic_metadata.topics.values())
        )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        client = AdminClient({"bootstrap.servers": BROKER_URL})

        if self.topic_exists(client, self.topic_name):
            logger.info("topic already exists: %s", self.topic_name)
            return

        futures = client.create_topics(
            [
                NewTopic(
                    topic=self.topic_name,
                    num_partitions=self.num_partitions,
                    replication_factor=self.num_replicas,
                    # config={
                    #     "cleanup.policy": "delete",
                    #     "compression.type": "lz4",
                    # },
                )
            ]
        )

        for topic, future in futures.items():
            try:
                future.result()
                print("topic created")
            except Exception as e:
                logger.fatal(f"failed to create topic {topic}: {e}")

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""

        # logger.info("producer close incomplete - skipping")
        logger.info("preparing for producer close")
        self.producer.flush()

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
