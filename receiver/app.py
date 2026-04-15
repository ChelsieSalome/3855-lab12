"""
Receiver Service - app.py
Receives performance and error metric events, publishes them to Kafka.
Lab 11 fixes:
  - Single global KafkaProducer (created once at startup, reused for all events)
  - Retry logic with exponential backoff so service survives Kafka restarts
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

with open('/config/receiver_config.yml', 'r') as f:
    app_config = yaml.safe_load(f)

with open('/config/receiver_log_config.yml', 'r') as f:
    logging.config.dictConfig(yaml.safe_load(f))

logger = logging.getLogger('basicLogger')

# Suppress noisy kafka-python internal logs
logging.getLogger('kafka').setLevel(logging.WARNING)

KAFKA_SERVER = f"{app_config['events']['hostname']}:{app_config['events']['port']}"
KAFKA_TOPIC  = app_config['events']['topic']

# ── Global Kafka Producer (created ONCE, reused for every request) ────────────

def _create_producer():
    """
    Keep retrying with exponential backoff until Kafka is reachable.
    Waits: 1s, 2s, 4s, 8s, 16s, 32s … (caps at 32s per attempt).
    Never gives up – the container stays alive and reconnects automatically.
    """
    delay = 1
    attempt = 0
    while True:
        attempt += 1
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_SERVER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                # Keep the connection alive
                connections_max_idle_ms=540000,
                request_timeout_ms=30000,
                retries=5,
            )
            logger.info(f"Kafka producer connected on attempt {attempt}")
            return producer
        except NoBrokersAvailable:
            logger.warning(
                f"Kafka not available (attempt {attempt}). Retrying in {delay}s …"
            )
            time.sleep(delay)
            delay = min(delay * 2, 32)
        except Exception as e:
            logger.error(f"Unexpected error connecting to Kafka: {e}. Retrying in {delay}s …")
            time.sleep(delay)
            delay = min(delay * 2, 32)


logger.info("Connecting to Kafka …")
_producer = _create_producer()


def _send(msg: dict):
    """Send one message, reconnecting if the producer has gone stale."""
    global _producer
    try:
        _producer.send(KAFKA_TOPIC, value=msg)
        _producer.flush()
    except Exception as e:
        logger.error(f"Kafka send failed: {e}. Reconnecting …")
        _producer = _create_producer()
        _producer.send(KAFKA_TOPIC, value=msg)
        _producer.flush()


# ── API Handlers ──────────────────────────────────────────────────────────────

def report_performance_metrics(body):
    """POST /monitoring/performance – receive a batch of performance metrics."""
    try:
        server_id           = body['server_id']
        reporting_timestamp = body['reporting_timestamp']

        for metric in body['metrics']:
            trace_id = str(uuid.uuid4())
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

            _send({
                "type":     "performance_metric",
                "datetime": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                "payload":  payload,
            })

            logger.info(f"SENT performance metric to Kafka | trace={trace_id}")

        return NoContent, 201

    except Exception as e:
        logger.error(f"Error processing performance metrics: {e}")
        return {"error": str(e)}, 500


def report_error_metrics(body):
    """POST /monitoring/errors – receive a batch of error metrics."""
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
    """GET /health – liveness probe."""
    return {"status": "healthy"}, 200


# ── App Setup ─────────────────────────────────────────────────────────────────

app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api(
    "receiver_openapi.yaml",
    strict_validation=True,
    validate_responses=True,
)

flask_app = app.app

from flask_cors import CORS
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