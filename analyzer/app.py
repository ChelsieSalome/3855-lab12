import connexion
from connexion import FlaskApp
from flask_cors import CORS
import json
import logging
import logging.config
import yaml
import time
import random
import threading
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# Load configuration
with open('/config/analyzer_config.yml', 'r') as f:
    CONFIG = yaml.safe_load(f)

# Load logging configuration
with open('/config/analyzer_log_config.yml', 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('basicLogger')


class KafkaConsumerWrapper:
    """
    Wraps a KafkaConsumer with automatic reconnection logic.
    If Kafka goes down after startup, the wrapper keeps retrying
    until it reconnects - no container restart needed.
    """

    def __init__(self, topic, hostname, port):
        self.topic = topic
        self.bootstrap_servers = f"{hostname}:{port}"
        self.consumer = None
        self.connect()

    def connect(self):
        """Retry loop - keeps trying until a consumer is established."""
        while True:
            logger.info("Attempting to connect to Kafka...")
            if self._make_consumer():
                logger.info("Kafka consumer connected successfully")
                break
            # Random sleep between 0.5s and 1.5s to avoid hammering Kafka
            sleep_time = random.randint(500, 1500) / 1000
            logger.warning(f"Retrying Kafka connection in {sleep_time:.1f}s...")
            time.sleep(sleep_time)

    def _make_consumer(self):
        """
        Attempts to create a KafkaConsumer once.
        Returns True on success, False on failure.
        """
        try:
            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id=None,
                auto_offset_reset='earliest',
                enable_auto_commit=False,
                consumer_timeout_ms=100,
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            return True
        except KafkaError as e:
            logger.warning(f"Kafka connection failed: {e}")
            self.consumer = None
            return False
        except Exception as e:
            logger.warning(f"Unexpected error connecting to Kafka: {e}")
            self.consumer = None
            return False

    def get_consumer(self):
        """
        Returns the active consumer, reconnecting first if needed.
        Call this inside every endpoint instead of using kafka_consumer directly.
        """
        if self.consumer is None:
            logger.warning("Consumer is None - reconnecting...")
            self.connect()
        return self.consumer

    def reset(self):
        """
        Called when a Kafka error occurs mid-operation.
        Closes the broken consumer and triggers reconnection.
        """
        logger.warning("Resetting Kafka consumer after error...")
        try:
            if self.consumer:
                self.consumer.close()
        except Exception:
            pass
        self.consumer = None
        self.connect()


# Create ONE global wrapper at startup - replaces the old create_consumer()
kafka_wrapper = KafkaConsumerWrapper(
    topic=CONFIG['kafka']['topic'],
    hostname=CONFIG['kafka']['hostname'],
    port=CONFIG['kafka']['port']
)

# Lock to prevent concurrent access to the consumer
consumer_lock = threading.Lock()

# Flag to track if consumer has been initialized (seek to beginning on first use)
consumer_initialized = False


def get_performance_event(index):
    """Gets a performance event at a specific index from the Kafka topic."""
    logger.info(f"Request for performance event at index {index}")

    global consumer_initialized

    try:
        with consumer_lock:
            consumer = kafka_wrapper.get_consumer()

            # Seek to beginning on first use so we read all historical messages
            if not consumer_initialized:
                logger.info("Initializing consumer - seeking to beginning")
                consumer.poll(timeout_ms=1000)
                consumer.seek_to_beginning()
                consumer_initialized = True

            consumer.seek_to_beginning()
            performance_count = 0

            for msg in consumer:
                data = msg.value
                if data.get('type') == 'performance_metric':
                    if performance_count == index:
                        logger.info(f"Found performance event at index {index}")
                        return data['payload'], 200
                    performance_count += 1

        logger.error(f"No performance event found at index {index}")
        return {"message": f"No performance event at index {index}"}, 404

    except KafkaError as e:
        # Kafka dropped mid-read - reset and let next request reconnect
        logger.error(f"Kafka error retrieving performance event: {e}")
        kafka_wrapper.reset()
        return {"message": "Kafka unavailable, reconnecting - please retry"}, 503

    except Exception as e:
        logger.error(f"Error retrieving performance event: {e}")
        return {"message": f"Error retrieving event: {str(e)}"}, 400


def get_error_event(index):
    """Gets an error event at a specific index from the Kafka topic."""
    logger.info(f"Request for error event at index {index}")

    global consumer_initialized

    try:
        with consumer_lock:
            consumer = kafka_wrapper.get_consumer()

            if not consumer_initialized:
                logger.info("Initializing consumer - seeking to beginning")
                consumer.poll(timeout_ms=1000)
                consumer.seek_to_beginning()
                consumer_initialized = True

            consumer.seek_to_beginning()
            error_count = 0

            for msg in consumer:
                data = msg.value
                if data.get('type') == 'error_metric':
                    if error_count == index:
                        logger.info(f"Found error event at index {index}")
                        return data['payload'], 200
                    error_count += 1

        logger.error(f"No error event found at index {index}")
        return {"message": f"No error event at index {index}"}, 404

    except KafkaError as e:
        logger.error(f"Kafka error retrieving error event: {e}")
        kafka_wrapper.reset()
        return {"message": "Kafka unavailable, reconnecting - please retry"}, 503

    except Exception as e:
        logger.error(f"Error retrieving error event: {e}")
        return {"message": f"Error retrieving event: {str(e)}"}, 400


def get_stats():
    """Gets statistics about events in the Kafka topic."""
    logger.info("Request for event statistics")

    global consumer_initialized

    try:
        with consumer_lock:
            consumer = kafka_wrapper.get_consumer()

            if not consumer_initialized:
                logger.info("Initializing consumer - seeking to beginning")
                consumer.poll(timeout_ms=1000)
                consumer.seek_to_beginning()
                consumer_initialized = True

            consumer.seek_to_beginning()
            performance_count = 0
            error_count = 0

            for msg in consumer:
                data = msg.value
                if data.get('type') == 'performance_metric':
                    performance_count += 1
                elif data.get('type') == 'error_metric':
                    error_count += 1

        stats = {
            "num_performance_events": performance_count,
            "num_error_events": error_count
        }
        logger.info(f"Statistics: {stats}")
        return stats, 200

    except KafkaError as e:
        logger.error(f"Kafka error retrieving statistics: {e}")
        kafka_wrapper.reset()
        return {"message": "Kafka unavailable, reconnecting - please retry"}, 503

    except Exception as e:
        logger.error(f"Error retrieving statistics: {e}")
        return {"message": f"Error retrieving statistics: {str(e)}"}, 400


def health():
    """Health check endpoint - always returns 200 if the service is running."""
    return {"status": "healthy"}, 200


app = FlaskApp(__name__, specification_dir='')

# Enable CORS
CORS(app.app)

# Add API with base path
app.add_api(
    'openapi.yaml',
    base_path='/analyzer',
    strict_validation=True,
    validate_responses=True
)

if __name__ == '__main__':
    app.run(
        host='0.0.0.0',
        port=CONFIG['app']['port']
    )