"""
Storage Service - app.py

ROLE IN THE SYSTEM:
-------------------
Storage is the PERSISTENCE LAYER. It reads events from Kafka and saves them
to a MySQL database. It also exposes GET endpoints so other services
(like Processing) can query the stored data by time range.

DATA FLOW:
    Kafka topic "events" → Storage consumer thread → MySQL database
    Processing service   → GET /storage/monitoring/performance → MySQL → JSON response

KAFKA ROLE HERE: CONSUMER (with a group_id)
    This service is a Kafka CONSUMER. A consumer READS messages FROM a Kafka topic.
    
    CONSUMER GROUP ("storage_group"):
    The group_id="storage_group" is critical. Kafka tracks which messages each
    consumer GROUP has already read using "offsets" (like a bookmark).
    
    - If Storage crashes and restarts, Kafka knows where it left off and resumes
      from the last committed offset — NO messages are lost or re-processed.
    - auto_offset_reset='earliest' means: if this group has never read before
      (first startup), start from the very beginning of the topic.
    - enable_auto_commit=False means: WE manually call consumer.commit() after
      successfully processing each message. This guarantees at-least-once delivery:
      if we crash mid-processing, the message will be re-read on restart because
      we never committed the offset.
    
    CONTRAST WITH ANALYZER:
    The Analyzer uses group_id=None (no group). This means Kafka gives it its
    own independent read position that is NOT tracked/saved. The Analyzer always
    reads ALL messages from the beginning on every request — it's read-only and
    doesn't need persistence of its position.
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

# Suppress verbose kafka-python internal logs
logging.getLogger('kafka').setLevel(logging.WARNING)
logging.getLogger('kafka.conn').setLevel(logging.WARNING)

KAFKA_SERVER = f"{app_config['events']['hostname']}:{app_config['events']['port']}"
KAFKA_TOPIC  = app_config['events']['topic']

logger.info(f"Storage service starting | Kafka={KAFKA_SERVER} topic={KAFKA_TOPIC}")

# ── DB helpers ────────────────────────────────────────────────────────────────
#
# WHY SEPARATE SESSION PER FUNCTION?
# SQLAlchemy sessions are not thread-safe. The Kafka consumer runs in a
# background thread while HTTP requests run in the main thread. By creating
# a new session for each operation and closing it immediately after, we avoid
# thread-safety issues. make_session() creates a new session bound to the
# shared engine (which IS thread-safe).

def _store_performance(payload: dict):
    """
    Write one performance reading to the performance_reading table in MySQL.
    
    Creates a PerformanceReading ORM object (defined in models.py),
    adds it to the session, and commits (writes to disk).
    If anything goes wrong, rollback() undoes the incomplete write
    so the database stays in a consistent state.
    """
    session = make_session()
    try:
        reading = PerformanceReading(
            trace_id=payload['trace_id'],
            server_id=payload['server_id'],
            cpu=payload['cpu'],
            memory=payload['memory'],
            disk_io=payload['disk_io'],
            # Convert string timestamp "2026-04-15T00:00:00Z" → Python datetime object
            # MySQL cannot store raw strings as DateTime columns
            reporting_timestamp=datetime.strptime(
                payload['reporting_timestamp'], "%Y-%m-%dT%H:%M:%SZ"
            ),
        )
        session.add(reading)
        session.commit()    # actually writes to MySQL
        logger.info(
            f"STORED performance | trace={payload['trace_id']} server={payload['server_id']}"
        )
    except Exception as e:
        session.rollback()  # undo partial write to keep DB consistent
        logger.error(f"DB error storing performance: {e}")
        raise               # re-raise so the Kafka consumer knows not to commit offset
    finally:
        session.close()     # always release the connection back to the pool


def _store_error(payload: dict):
    """
    Write one error reading to the error_reading table in MySQL.
    Same pattern as _store_performance but for error events.
    """
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
    Create a Kafka consumer with exponential backoff retry.
    
    KEY CONSUMER SETTINGS EXPLAINED:
    
    group_id='storage_group'
        Kafka tracks this group's read position (offset) on the broker.
        If Storage restarts, Kafka resumes from where it left off.
        Multiple consumers with the SAME group_id share the work (load balancing).
        
    auto_offset_reset='earliest'
        On FIRST startup (no saved offset yet), read from the very beginning
        of the topic so no historical messages are missed.
        
    enable_auto_commit=False
        We manually call consumer.commit() after successful processing.
        If we crash before committing, Kafka will re-deliver the message
        on restart — ensuring we never silently lose an event.
        
    session_timeout_ms=30000
        If Kafka doesn't hear from this consumer within 30 seconds,
        it assumes the consumer died and reassigns its partitions.
        
    heartbeat_interval_ms=10000
        Consumer sends a heartbeat every 10 seconds to prove it's alive.
        Must be less than session_timeout_ms.
        
    max_poll_interval_ms=300000
        Maximum time between calls to poll(). If processing one batch takes
        longer than 5 minutes, Kafka assumes the consumer is dead.
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
    Background daemon thread that consumes Kafka messages forever.
    
    WHY A SEPARATE THREAD?
    The HTTP server (Flask/Connexion) blocks the main thread handling web requests.
    If we tried to read Kafka in the same thread, the web server would freeze.
    Running Kafka consumption in a daemon thread means both happen simultaneously.
    daemon=True means this thread will automatically stop when the main process exits.
    
    TWO-LEVEL LOOP STRUCTURE:
    
    OUTER while True loop → handles reconnection
        If Kafka crashes mid-operation (network issue, Kafka restart),
        the inner for loop will raise an exception. The outer loop catches it,
        closes the broken consumer, and calls _create_consumer() to get a fresh one.
    
    INNER for msg in consumer loop → processes messages one by one
        Kafka delivers messages one at a time. For each message:
        1. Deserialize the JSON value (done automatically by value_deserializer)
        2. Check the "type" field to know which table to write to
        3. Call the appropriate _store_* function
        4. Call consumer.commit() to tell Kafka "I've processed this message"
        
        If _store_* raises an exception (e.g., DB error), we LOG and SKIP
        without committing. This means Kafka will re-deliver the message
        on the next restart — a safety net, but could cause duplicates
        if the DB write partially succeeded.
    
    KAFKA MESSAGE STRUCTURE (what arrives in msg.value after deserialization):
    {
        "type": "performance_metric",    ← tells us which handler to call
        "datetime": "2026-04-15T...",
        "payload": {
            "trace_id": "uuid...",
            "server_id": "server-123",
            "cpu": 72.5, ...
        }
    }
    """
    logger.info("Kafka consumer thread started")

    while True:                          # outer loop: reconnect on fatal errors
        consumer = _create_consumer()
        try:
            for msg in consumer:         # inner loop: blocks until a message arrives
                try:
                    data      = msg.value          # already deserialized to dict
                    msg_type  = data.get('type')   # "performance_metric" or "error_metric"
                    payload   = data.get('payload', {})
                    trace_id  = payload.get('trace_id', 'unknown')

                    logger.info(f"RECEIVED from Kafka | type={msg_type} trace={trace_id}")

                    if msg_type == 'performance_metric':
                        _store_performance(payload)
                    elif msg_type == 'error_metric':
                        _store_error(payload)
                    else:
                        logger.warning(f"Unknown message type: {msg_type}")

                    # Only commit AFTER successful processing.
                    # If _store_* raised an exception above, we skip this line,
                    # so Kafka will re-deliver this message on next startup.
                    consumer.commit()

                except Exception as e:
                    # Log the error but continue to next message.
                    # NOT committing means this message may be re-processed.
                    logger.error(f"Error processing message: {e} – skipping, not committing")

        except Exception as e:
            # The consumer itself broke (Kafka went down, network issue, etc.)
            # Close the broken consumer and let the outer loop reconnect.
            logger.error(f"Kafka consumer loop error: {e}. Reconnecting …")
            try:
                consumer.close()
            except Exception:
                pass
            time.sleep(2)   # brief pause before reconnecting


# ── API Endpoints ─────────────────────────────────────────────────────────────

def get_performance_readings(start_timestamp, end_timestamp):
    """
    GET /storage/monitoring/performance?start_timestamp=...&end_timestamp=...
    
    Queries MySQL for all performance readings where date_created falls
    within the given time range. Used by the Processing service to fetch
    new readings since its last run.
    
    WHY date_created AND NOT reporting_timestamp?
    date_created is when WE stored the record — it's set by our code and
    always present. reporting_timestamp is when the client says the event
    happened — it could be in the past or malformed. Querying by date_created
    gives us reliable, consistent time-based filtering.
    
    Returns a JSON array of reading dicts (via the to_dict() method on each ORM object).
    """
    logger.debug(f"GET performance | {start_timestamp} → {end_timestamp}")

    session = make_session()
    try:
        # Parse string timestamps into Python datetime objects for SQLAlchemy comparison
        start_dt = datetime.strptime(start_timestamp.strip(), "%Y-%m-%dT%H:%M:%SZ")
        end_dt   = datetime.strptime(end_timestamp.strip(),   "%Y-%m-%dT%H:%M:%SZ")

        # SQLAlchemy SELECT query with WHERE clauses
        # .scalars().all() returns a list of ORM objects (not raw SQL rows)
        rows = session.execute(
            select(PerformanceReading)
            .where(PerformanceReading.date_created >= start_dt)
            .where(PerformanceReading.date_created <  end_dt)
        ).scalars().all()

        results = [r.to_dict() for r in rows]   # convert ORM objects → dicts → JSON
        logger.info(f"GET performance: returned {len(results)} rows")
        return results, 200

    except Exception as e:
        logger.error(f"Error fetching performance readings: {e}")
        return {"message": str(e)}, 400
    finally:
        session.close()


def get_error_readings(start_timestamp, end_timestamp):
    """
    GET /storage/monitoring/errors?start_timestamp=...&end_timestamp=...
    
    Same as get_performance_readings but queries the error_reading table.
    Called by Processing service to count new errors and find max severity.
    """
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
    """
    GET /storage/health — liveness probe.
    Returns 200 as long as the process is running.
    Does NOT verify MySQL or Kafka connectivity — just confirms the service is alive.
    """
    return {"status": "healthy"}, 200


# ── App Setup ─────────────────────────────────────────────────────────────────

app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api(
    "storage_openapi.yaml",
    base_path="/storage",        # Nginx routes /storage/* to this service
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

    # init_db() connects to MySQL and creates tables if they don't exist.
    # Must happen BEFORE starting the Kafka consumer thread, because
    # the consumer immediately tries to write to the database.
    init_db()

    # Start the Kafka consumer in a background thread.
    # daemon=True means this thread dies automatically when the main process exits,
    # so we don't need to manually shut it down on container stop.
    t = threading.Thread(target=process_messages, daemon=True)
    t.start()

    # Start the HTTP server. This blocks the main thread, handling web requests,
    # while the Kafka consumer runs independently in the background thread.
    app.run(host='0.0.0.0', port=8091)