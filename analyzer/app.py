"""
Analyzer Service - app.py

ROLE IN THE SYSTEM:
-------------------
The Analyzer reads DIRECTLY from Kafka (bypassing MySQL entirely) to provide
real-time event lookup and counting. It can retrieve a specific event by index
and count total events in the topic.

DATA FLOW:
    Kafka topic "events" → Analyzer (reads on each request) → JSON response to Dashboard

KAFKA ROLE HERE: CONSUMER (without a group_id — stateless reader)
    The Analyzer is also a Kafka consumer, but fundamentally different from Storage:

    STORAGE consumer:  group_id="storage_group", tracks offset, reads each message ONCE
    ANALYZER consumer: group_id=None, no offset tracking, reads ALL messages from
                       the BEGINNING on every single request

    WHY NO GROUP ID?
    The Analyzer is a READ-ONLY query tool. It doesn't process or store anything,
    it just counts and retrieves. By not using a group_id:
    - Kafka does not track any read position for this consumer
    - Every request starts fresh from the beginning of the topic
    - Multiple Analyzer instances can all read the same data independently
    - There's no risk of "stealing" messages from the Storage consumer's group

    ANALOGY: Storage is like a worker reading their inbox and marking emails as read.
    Analyzer is like a manager browsing the same inbox but never marking anything —
    every time they open it, they see everything from the start.

    TRADE-OFF:
    Reading ALL messages from the beginning every request is O(n) — it gets slower
    as the topic grows. For large topics this would be very slow, but for this lab
    it's acceptable.
"""

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
import os


# Load configuration from shared /config volume
with open('/config/analyzer_config.yml', 'r') as f:
    CONFIG = yaml.safe_load(f)

with open('/config/analyzer_log_config.yml', 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('basicLogger')


class KafkaConsumerWrapper:
    """
    Wraps a KafkaConsumer with automatic reconnection logic.
    
    WHY A WRAPPER CLASS?
    If Kafka goes down after startup and comes back up, a plain KafkaConsumer
    object will remain broken — it won't reconnect automatically. This wrapper
    detects failures and transparently creates a new consumer without needing
    to restart the container.
    
    LIFECYCLE:
    1. __init__: called once at startup, immediately calls connect()
    2. connect(): retry loop that keeps trying until a consumer is created
    3. get_consumer(): called by each API endpoint to get the active consumer
    4. reset(): called when a Kafka error happens mid-operation — closes the
                broken consumer and reconnects
    """

    def __init__(self, topic, hostname, port):
        self.topic = topic
        self.bootstrap_servers = f"{hostname}:{port}"
        self.consumer = None
        self.connect()   # block here until connected

    def connect(self):
        """
        Retry loop — keeps attempting until a consumer is successfully created.
        Uses random jitter (0.5s–1.5s) to avoid the "thundering herd" problem:
        if 10 services all restart at once and retry at exactly the same interval,
        they'd all hammer Kafka simultaneously. Random jitter spreads them out.
        """
        while True:
            logger.info("Attempting to connect to Kafka...")
            if self._make_consumer():
                logger.info("Kafka consumer connected successfully")
                break
            # Random sleep between 0.5s and 1.5s
            sleep_time = random.randint(500, 1500) / 1000
            logger.warning(f"Retrying Kafka connection in {sleep_time:.1f}s...")
            time.sleep(sleep_time)

    def _make_consumer(self):
        """
        Attempts to create a KafkaConsumer ONCE.
        Returns True on success, False on any failure (caller will retry).
        
        KEY SETTINGS:
        group_id=None
            No consumer group. Kafka won't track this consumer's read position.
            Every call to seek_to_beginning() truly starts from message #0.
            
        auto_offset_reset='earliest'
            When no saved offset exists (which is always, since group_id=None),
            start reading from the oldest available message in the topic.
            
        enable_auto_commit=False
            Don't automatically save read position. Since group_id=None,
            there's no position to save anyway, but this avoids any
            accidental offset commits.
            
        consumer_timeout_ms=100
            After 100ms of no new messages, the consumer stops iterating
            (raises StopIteration to exit the for loop). This is how we know
            we've read ALL current messages — when the topic goes quiet.
            Without this, the for loop would block forever waiting for new messages.
        """
        try:
            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id=None,               # stateless — no offset tracking
                auto_offset_reset='earliest',
                enable_auto_commit=False,
                consumer_timeout_ms=100,     # stop iterating after 100ms of silence
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
        Returns the active consumer. If it's None (was reset after an error),
        reconnects first. Call this at the start of every API endpoint.
        """
        if self.consumer is None:
            logger.warning("Consumer is None - reconnecting...")
            self.connect()
        return self.consumer

    def reset(self):
        """
        Called when a KafkaError occurs MID-OPERATION (e.g., while iterating).
        Safely closes the broken consumer and reconnects.
        After reset(), the next call to get_consumer() will return a fresh consumer.
        """
        logger.warning("Resetting Kafka consumer after error...")
        try:
            if self.consumer:
                self.consumer.close()
        except Exception:
            pass
        self.consumer = None
        self.connect()   # block until reconnected


# Create ONE global wrapper at startup — all endpoints share this single consumer.
# WHY SINGLE CONSUMER? Creating a new KafkaConsumer per request would be very
# slow (TCP handshake, group coordination, etc.). Reusing one is much faster.
kafka_wrapper = KafkaConsumerWrapper(
    topic=CONFIG['kafka']['topic'],
    hostname=CONFIG['kafka']['hostname'],
    port=CONFIG['kafka']['port']
)

# Threading lock to prevent concurrent access to the consumer.
# WHY A LOCK? The consumer is NOT thread-safe. If two HTTP requests come in
# simultaneously and both call consumer.seek_to_beginning() and iterate,
# they'd interfere with each other producing wrong results or crashes.
# The lock ensures only ONE request uses the consumer at a time.
# TRADE-OFF: other requests must WAIT while one is iterating — this is why
# the health endpoint can appear slow under heavy load (it gets blocked too).
consumer_lock = threading.Lock()

# Flag to track whether we've done the initial seek_to_beginning().
# On very first use, we need to poll() once to get partition assignment
# before we can seek. After that, seek_to_beginning() works directly.
consumer_initialized = False


def get_performance_event(index):
    """
    GET /analyzer/performance?index=N
    
    Returns the Nth performance event (0-based) from the Kafka topic.
    
    HOW IT WORKS:
    1. Acquire the lock (wait if another request is using the consumer)
    2. Seek the consumer to the beginning of the topic (message #0)
    3. Iterate through ALL messages, counting only performance_metric types
    4. When our counter reaches the requested index, return that payload
    5. If we exhaust all messages without reaching index N, return 404
    
    WHY READ FROM BEGINNING EVERY TIME?
    Because Kafka is an append-only log, not a random-access database.
    There's no "get message at index N" operation in Kafka — you have to
    scan from the start and count. This is O(n) complexity.
    
    The consumer_timeout_ms=100 setting causes the for loop to automatically
    stop after 100ms of no new messages, so we don't hang forever.
    """
    logger.info(f"Request for performance event at index {index}")

    global consumer_initialized

    try:
        with consumer_lock:   # block until we have exclusive access
            consumer = kafka_wrapper.get_consumer()

            # First-time initialization: poll() triggers partition assignment
            # (Kafka assigns topic partitions to this consumer), then seek to start
            if not consumer_initialized:
                logger.info("Initializing consumer - seeking to beginning")
                consumer.poll(timeout_ms=1000)   # triggers partition assignment
                consumer.seek_to_beginning()
                consumer_initialized = True

            # Rewind to message #0 for this request
            consumer.seek_to_beginning()
            performance_count = 0

            # Iterate messages until consumer_timeout_ms triggers (topic exhausted)
            for msg in consumer:
                data = msg.value   # already deserialized dict by value_deserializer
                if data.get('type') == 'performance_metric':
                    if performance_count == index:
                        logger.info(f"Found performance event at index {index}")
                        return data['payload'], 200
                    performance_count += 1
            # Loop ended without finding index N — topic has fewer events than requested

        logger.error(f"No performance event found at index {index}")
        return {"message": f"No performance event at index {index}"}, 404

    except KafkaError as e:
        # Kafka dropped mid-iteration — reset consumer so next request reconnects
        logger.error(f"Kafka error retrieving performance event: {e}")
        kafka_wrapper.reset()
        return {"message": "Kafka unavailable, reconnecting - please retry"}, 503

    except Exception as e:
        logger.error(f"Error retrieving performance event: {e}")
        return {"message": f"Error retrieving event: {str(e)}"}, 400


def get_error_event(index):
    """
    GET /analyzer/error?index=N
    
    Same pattern as get_performance_event but counts error_metric type messages.
    Scans from beginning, counts only error events, returns the Nth one.
    """
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
    """
    GET /analyzer/stats
    
    Counts ALL performance and error events in the entire Kafka topic.
    Scans every message from beginning to end and increments the appropriate counter.
    
    This is called by the Dashboard every 3 seconds to display total event counts.
    It's also used by the Dashboard to know the valid index range before requesting
    a specific event (e.g., if num_performance_events=100, random index is 0-99).
    
    Returns: {"num_performance_events": N, "num_error_events": M}
    """
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

            # Read every message and count by type
            for msg in consumer:
                data = msg.value
                if data.get('type') == 'performance_metric':
                    performance_count += 1
                elif data.get('type') == 'error_metric':
                    error_count += 1
            # Loop ends automatically when consumer_timeout_ms triggers

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
    """
    GET /analyzer/health — liveness probe.
    
    IMPORTANT NOTE ON PERFORMANCE UNDER LOAD:
    This endpoint does NOT use the consumer_lock, but it shares Flask worker
    threads with the other endpoints that DO hold the lock for extended periods
    (while iterating Kafka). Under heavy concurrent load, Flask may be busy
    serving other requests, causing this endpoint to respond slowly, which
    causes the Health Check service to mark Analyzer as "Down" momentarily.
    This is a known limitation of the single-consumer + lock design.
    """
    return {"status": "healthy"}, 200


# ── App Setup ─────────────────────────────────────────────────────────────────

app = FlaskApp(__name__, specification_dir='')

# Only enable CORS when CORS_ALLOW_ALL=yes environment variable is set.
# In production (behind Nginx), CORS is not needed because all requests
# come from the same origin (the Nginx server). CORS_ALLOW_ALL=yes is only
# used during development when hitting the service directly by port.
if os.environ.get("CORS_ALLOW_ALL") == "yes":
    CORS(app.app)

app.add_api(
    'openapi.yaml',
    base_path='/analyzer',       # Nginx routes /analyzer/* to this service
    strict_validation=True,
    validate_responses=True
)

if __name__ == '__main__':
    app.run(
        host='0.0.0.0',
        port=CONFIG['app']['port']   # 5005
    )