"""
Receiver Service - app.py

ROLE IN THE SYSTEM:
-------------------
The Receiver is the ENTRY POINT for all incoming data. It accepts HTTP POST
requests from external clients (like jMeter) and forwards each event to Kafka.
It does NOT store anything to a database — its only job is to receive and publish.

DATA FLOW:
    jMeter → POST /receiver/monitoring/performance → Receiver → Kafka topic "events"
    jMeter → POST /receiver/monitoring/errors      → Receiver → Kafka topic "events"

KAFKA ROLE HERE: PRODUCER
    This service is a Kafka PRODUCER. A producer WRITES messages TO a Kafka topic.
    Think of Kafka like a post office, and the producer is someone dropping off letters.
    The letters (messages) sit in the topic until consumers come to read them.
"""

import connexion
from connexion import NoContent
import uuid
import yaml
import logging
import logging.config
import json
import datetime
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ── Config & Logging ──────────────────────────────────────────────────────────
# Load YAML config files from the shared /config volume (mounted via docker-compose).
# These files live outside the container so they can be changed without rebuilding.

with open('/config/receiver_config.yml', 'r') as f:
    app_config = yaml.safe_load(f)

with open('/config/receiver_log_config.yml', 'r') as f:
    logging.config.dictConfig(yaml.safe_load(f))

logger = logging.getLogger('basicLogger')

# Suppress noisy kafka-python internal debug logs — we only want our own logs
logging.getLogger('kafka').setLevel(logging.WARNING)

# Build the Kafka broker address string from config, e.g. "kafka:29092"
KAFKA_SERVER = f"{app_config['events']['hostname']}:{app_config['events']['port']}"
KAFKA_TOPIC  = app_config['events']['topic']   # "events"

# ── Global Kafka Producer ─────────────────────────────────────────────────────
#
# WHAT IS A KAFKA PRODUCER?
# A KafkaProducer is an object that connects to the Kafka broker and can SEND
# (publish) messages to a topic. We create ONE producer at startup and reuse it
# for every single HTTP request — this is efficient because creating a new
# connection for every request would be very slow.
#
# WHY RETRY LOGIC?
# When Docker Compose starts all containers simultaneously, Kafka takes ~30 seconds
# to be ready. Without retries, the Receiver would crash immediately on startup
# because Kafka isn't ready yet. The retry loop keeps attempting to connect until
# it succeeds, using exponential backoff to avoid hammering Kafka.
#
# EXPONENTIAL BACKOFF:
# Instead of retrying every second forever (wasteful), we wait longer each time:
# 1s → 2s → 4s → 8s → 16s → 32s (then stays at 32s max)
# This is a standard pattern for distributed systems.

def _create_producer():
    """
    Connect to Kafka with exponential backoff. Never gives up.
    Returns a ready-to-use KafkaProducer once Kafka is available.
    
    value_serializer: automatically converts Python dicts to JSON bytes
                      before sending. Kafka only understands bytes, not dicts.
    """
    delay = 1
    attempt = 0
    while True:
        attempt += 1
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_SERVER,
                # Automatically serialize Python dict → JSON string → bytes
                # This runs on every message before it's sent
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                # Keep TCP connection alive so it doesn't drop between requests
                connections_max_idle_ms=540000,
                request_timeout_ms=30000,
                retries=5,
            )
            logger.info(f"Kafka producer connected on attempt {attempt}")
            return producer
        except NoBrokersAvailable:
            # Kafka broker not up yet — wait and retry
            logger.warning(
                f"Kafka not available (attempt {attempt}). Retrying in {delay}s …"
            )
            time.sleep(delay)
            delay = min(delay * 2, 32)   # double the wait, cap at 32s
        except Exception as e:
            logger.error(f"Unexpected error connecting to Kafka: {e}. Retrying in {delay}s …")
            time.sleep(delay)
            delay = min(delay * 2, 32)


logger.info("Connecting to Kafka …")
# Create the single global producer at module load time.
# All request handlers will reuse this same producer object.
_producer = _create_producer()


def _send(msg: dict):
    """
    Send one message to the Kafka topic, reconnecting if the connection dropped.
    
    WHY RECONNECT HERE?
    Even after initial connection, a producer can go stale if Kafka restarts
    or the TCP connection drops. Instead of crashing, we transparently recreate
    the producer and resend the message.
    
    producer.send()  → puts the message in a local buffer (non-blocking)
    producer.flush() → blocks until all buffered messages are actually sent
                       to Kafka. Without flush(), messages might be lost if
                       the process exits before the buffer drains.
    """
    global _producer
    try:
        _producer.send(KAFKA_TOPIC, value=msg)
        _producer.flush()   # wait until Kafka acknowledges receipt
    except Exception as e:
        logger.error(f"Kafka send failed: {e}. Reconnecting …")
        _producer = _create_producer()   # replace the broken producer
        _producer.send(KAFKA_TOPIC, value=msg)
        _producer.flush()


# ── API Handlers ──────────────────────────────────────────────────────────────

def report_performance_metrics(body):
    """
    POST /receiver/monitoring/performance
    
    Receives a BATCH of performance metrics from one server and publishes
    each individual metric as a SEPARATE Kafka message.
    
    WHY SPLIT INTO INDIVIDUAL MESSAGES?
    The client sends a batch (e.g., 5 CPU readings from one server), but
    Kafka works best with individual, independent events. Splitting now means
    downstream consumers (Storage, Analyzer) can process each reading
    independently without knowing about batches.
    
    TRACE ID:
    Each individual event gets a UUID trace_id generated HERE. This allows
    you to track one specific event across all services — the same trace_id
    will appear in Receiver logs, Kafka, Storage logs, and the database.
    Think of it like a tracking number on a package.
    
    MESSAGE FORMAT published to Kafka:
    {
        "type": "performance_metric",      ← consumers use this to identify the event type
        "datetime": "2026-04-15T...",      ← when the receiver processed it
        "payload": {                        ← the actual data
            "trace_id": "uuid...",
            "server_id": "server-123",
            "cpu": 72.5,
            "memory": 64.2,
            "disk_io": 120.5,
            "reporting_timestamp": "..."
        }
    }
    """
    try:
        server_id           = body['server_id']
        reporting_timestamp = body['reporting_timestamp']

        # Loop through each metric in the batch and publish as individual event
        for metric in body['metrics']:
            trace_id = str(uuid.uuid4())   # unique ID for THIS specific reading
            logger.info(
                f"RECEIVED performance metric | server={server_id} trace={trace_id}"
            )

            payload = {
                "trace_id":            trace_id,
                "server_id":           server_id,
                "cpu":                 metric['cpu'],
                "memory":              metric['memory'],
                "disk_io":             metric['disk_io'],
                "reporting_timestamp": reporting_timestamp,
            }

            # Wrap the payload in an envelope with type and timestamp
            # The "type" field is how Storage and Analyzer know what kind of event this is
            _send({
                "type":     "performance_metric",
                "datetime": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                "payload":  payload,
            })

            logger.info(f"SENT performance metric to Kafka | trace={trace_id}")

        return NoContent, 201   # 201 Created — standard for successful POST

    except Exception as e:
        logger.error(f"Error processing performance metrics: {e}")
        return {"error": str(e)}, 500


def report_error_metrics(body):
    """
    POST /receiver/monitoring/errors
    
    Same pattern as report_performance_metrics but for error events.
    Each error in the batch becomes its own Kafka message with type "error_metric".
    Storage will store these in the error_reading table.
    Analyzer will count these separately from performance events.
    """
    try:
        server_id           = body['server_id']
        reporting_timestamp = body['reporting_timestamp']

        for error in body['errors']:
            trace_id = str(uuid.uuid4())
            logger.info(
                f"RECEIVED error metric | server={server_id} trace={trace_id}"
            )

            payload = {
                "trace_id":            trace_id,
                "server_id":           server_id,
                "error_code":          error['error_code'],
                "severity_level":      error['severity_level'],
                "avg_response_time":   error['avg_response_time'],
                "error_message":       error.get('error_message', ''),
                "reporting_timestamp": reporting_timestamp,
            }

            _send({
                "type":     "error_metric",
                "datetime": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                "payload":  payload,
            })

            logger.info(
                f"SENT error metric to Kafka | trace={trace_id} code={error['error_code']}"
            )

        return NoContent, 201

    except Exception as e:
        logger.error(f"Error processing error metrics: {e}")
        return {"error": str(e)}, 500


def health():
    """
    GET /receiver/health — liveness probe.
    Always returns 200 as long as the service process is running.
    Used by the Health Check service to determine if Receiver is "Up".
    Does NOT check Kafka connectivity — just confirms the process is alive.
    """
    return {"status": "healthy"}, 200


# ── App Setup ─────────────────────────────────────────────────────────────────
# Connexion wraps Flask and adds OpenAPI validation.
# base_path="/receiver" means all routes are prefixed: /receiver/monitoring/performance
# This prefix is required because Nginx routes /receiver/* to this service.

app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api(
    "receiver_openapi.yaml",
    base_path="/receiver",       # must match Nginx location block
    strict_validation=True,      # reject requests that don't match the OpenAPI spec
    validate_responses=True,     # also validate outgoing responses
)

flask_app = app.app

from flask_cors import CORS
# CORS is always enabled here for the receiver.
# In other services it's behind an env variable check,
# but since receiver is the public entry point it needs it.
CORS(flask_app)


@flask_app.route('/')
def home():
    return (
        "<h1>Receiver Service</h1>"
        "<ul>"
        "<li>POST /monitoring/performance</li>"
        "<li>POST /monitoring/errors</li>"
        "<li>GET  /health</li>"
        "</ul>"
    )


if __name__ == "__main__":
    logger.info("Starting Receiver Service on port 8080")
    app.run(host='0.0.0.0', port=8080)