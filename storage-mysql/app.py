"""
Storage Service - app.py
Consumes events from Kafka and persists them to MySQL via SQLAlchemy.
Lab 11 fixes:
  - Single persistent KafkaConsumer running in a daemon thread
  - Retry / reconnect logic with exponential backoff
  - DB connection pooling via create_tables engine (pool_pre_ping, pool_recycle)
"""

import connexion
from connexion import NoContent
from datetime import datetime
import json
import yaml
import logging
import logging.config
import threading
import time

from models import PerformanceReading, ErrorReading
from create_tables import make_session, init_db
from sqlalchemy import select
from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable
from flask_cors import CORS

# ── Config & Logging ──────────────────────────────────────────────────────────

with open('/config/storage_config.yml', 'r') as f:
    app_config = yaml.safe_load(f)

with open('/config/storage_log_config.yml', 'r') as f:
    logging.config.dictConfig(yaml.safe_load(f))

logger = logging.getLogger('basicLogger')

logging.getLogger('kafka').setLevel(logging.WARNING)
logging.getLogger('kafka.conn').setLevel(logging.WARNING)

KAFKA_SERVER = f"{app_config['events']['hostname']}:{app_config['events']['port']}"
KAFKA_TOPIC  = app_config['events']['topic']

logger.info(f"Storage service starting | Kafka={KAFKA_SERVER} topic={KAFKA_TOPIC}")

# ── DB helpers ────────────────────────────────────────────────────────────────

def _store_performance(payload: dict):
    """Write one performance reading to the database."""
    session = make_session()
    try:
        reading = PerformanceReading(
            trace_id=payload['trace_id'],
            server_id=payload['server_id'],
            cpu=payload['cpu'],
            memory=payload['memory'],
            disk_io=payload['disk_io'],
            reporting_timestamp=datetime.strptime(
                payload['reporting_timestamp'], "%Y-%m-%dT%H:%M:%SZ"
            ),
        )
        session.add(reading)
        session.commit()
        logger.info(
            f"STORED performance | trace={payload['trace_id']} server={payload['server_id']}"
        )
    except Exception as e:
        session.rollback()
        logger.error(f"DB error storing performance: {e}")
        raise
    finally:
        session.close()


def _store_error(payload: dict):
    """Write one error reading to the database."""
    session = make_session()
    try:
        reading = ErrorReading(
            trace_id=payload['trace_id'],
            server_id=payload['server_id'],
            error_code=payload['error_code'],
            severity_level=payload['severity_level'],
            avg_response_time=payload['avg_response_time'],
            error_message=payload.get('error_message', ''),
            reporting_timestamp=datetime.strptime(
                payload['reporting_timestamp'], "%Y-%m-%dT%H:%M:%SZ"
            ),
        )
        session.add(reading)
        session.commit()
        logger.info(
            f"STORED error | trace={payload['trace_id']} code={payload['error_code']}"
        )
    except Exception as e:
        session.rollback()
        logger.error(f"DB error storing error event: {e}")
        raise
    finally:
        session.close()


# ── Kafka Consumer Thread ─────────────────────────────────────────────────────

def _create_consumer() -> KafkaConsumer:
    """
    Keep retrying with exponential backoff until Kafka is reachable.
    Returns a ready-to-use KafkaConsumer.
    """
    delay = 1
    attempt = 0
    while True:
        attempt += 1
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_SERVER,
                group_id='storage_group',
                auto_offset_reset='earliest',
                enable_auto_commit=False,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000,
                max_poll_interval_ms=300000,
            )
            logger.info(f"Kafka consumer connected on attempt {attempt}")
            return consumer
        except (NoBrokersAvailable, KafkaError) as e:
            logger.warning(
                f"Kafka not available (attempt {attempt}): {e}. Retrying in {delay}s …"
            )
            time.sleep(delay)
            delay = min(delay * 2, 32)
        except Exception as e:
            logger.error(f"Unexpected error connecting consumer: {e}. Retrying in {delay}s …")
            time.sleep(delay)
            delay = min(delay * 2, 32)


def process_messages():
    """
    Background daemon thread.
    Consumes Kafka messages forever, reconnecting automatically on failure.
    """
    logger.info("Kafka consumer thread started")

    while True:                         # outer loop: reconnect on fatal errors
        consumer = _create_consumer()
        try:
            for msg in consumer:        # inner loop: process messages
                try:
                    data      = msg.value
                    msg_type  = data.get('type')
                    payload   = data.get('payload', {})
                    trace_id  = payload.get('trace_id', 'unknown')

                    logger.info(f"RECEIVED from Kafka | type={msg_type} trace={trace_id}")

                    if msg_type == 'performance_metric':
                        _store_performance(payload)
                    elif msg_type == 'error_metric':
                        _store_error(payload)
                    else:
                        logger.warning(f"Unknown message type: {msg_type}")

                    consumer.commit()

                except Exception as e:
                    logger.error(f"Error processing message: {e} – skipping, not committing")

        except Exception as e:
            logger.error(f"Kafka consumer loop error: {e}. Reconnecting …")
            try:
                consumer.close()
            except Exception:
                pass
            time.sleep(2)


# ── API Endpoints ─────────────────────────────────────────────────────────────

def get_performance_readings(start_timestamp, end_timestamp):
    """GET /monitoring/performance?start_timestamp=…&end_timestamp=…"""
    logger.debug(f"GET performance | {start_timestamp} → {end_timestamp}")

    session = make_session()
    try:
        start_dt = datetime.strptime(start_timestamp.strip(), "%Y-%m-%dT%H:%M:%SZ")
        end_dt   = datetime.strptime(end_timestamp.strip(),   "%Y-%m-%dT%H:%M:%SZ")

        rows = session.execute(
            select(PerformanceReading)
            .where(PerformanceReading.date_created >= start_dt)
            .where(PerformanceReading.date_created <  end_dt)
        ).scalars().all()

        results = [r.to_dict() for r in rows]
        logger.info(f"GET performance: returned {len(results)} rows")
        return results, 200

    except Exception as e:
        logger.error(f"Error fetching performance readings: {e}")
        return {"message": str(e)}, 400
    finally:
        session.close()


def get_error_readings(start_timestamp, end_timestamp):
    """GET /monitoring/errors?start_timestamp=…&end_timestamp=…"""
    logger.debug(f"GET errors | {start_timestamp} → {end_timestamp}")

    session = make_session()
    try:
        start_dt = datetime.strptime(start_timestamp.strip(), "%Y-%m-%dT%H:%M:%SZ")
        end_dt   = datetime.strptime(end_timestamp.strip(),   "%Y-%m-%dT%H:%M:%SZ")

        rows = session.execute(
            select(ErrorReading)
            .where(ErrorReading.date_created >= start_dt)
            .where(ErrorReading.date_created <  end_dt)
        ).scalars().all()

        results = [r.to_dict() for r in rows]
        logger.info(f"GET errors: returned {len(results)} rows")
        return results, 200

    except Exception as e:
        logger.error(f"Error fetching error readings: {e}")
        return {"message": str(e)}, 400
    finally:
        session.close()


def health():
    """GET /health – liveness probe."""
    return {"status": "healthy"}, 200


# ── App Setup ─────────────────────────────────────────────────────────────────

app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api(
    "storage_openapi.yaml" ,
    base_path="/storage",
    strict_validation=True,
    validate_responses=True,
)

flask_app = app.app
CORS(flask_app)


@flask_app.route('/')
def home():
    return "<h1>Storage Service</h1><p>See /monitoring/performance and /monitoring/errors</p>"


if __name__ == "__main__":
    logger.info("Starting Storage Service on port 8091")

    # Wait for DB and create tables
    init_db()

    # Start Kafka consumer in background
    t = threading.Thread(target=process_messages, daemon=True)
    t.start()

    app.run(host='0.0.0.0', port=8091)