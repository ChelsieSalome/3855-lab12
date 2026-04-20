"""
Health Check Service - app.py

ROLE IN THE SYSTEM:
-------------------
The Health Check service is a MONITOR. It periodically polls every other service's
/health endpoint and records whether each is "Up" or "Down". It stores this status
in a JSON file and exposes it via an API for the Dashboard to display.

DATA FLOW:
    APScheduler (every 5s) → GET each service's /health → write health_status.json
    Dashboard              → GET /healthcheck/health-status → read JSON → response

KEY DESIGN:
- Completely independent — it doesn't share any data with other services
- Uses a JSON file as its datastore (simple key-value: service_name → status)
- Uses APScheduler (same as Processing) for periodic polling
- Timeout on health requests is critical: if a service hangs, we don't want to
  wait forever — we mark it as Down after the timeout expires
"""

import connexion
from connexion.middleware import MiddlewarePosition
from starlette.middleware.cors import CORSMiddleware
import json
import logging
import logging.config
import yaml
from datetime import datetime, timezone
import requests
from apscheduler.schedulers.background import BackgroundScheduler
import os

# ── Config & Logging ──────────────────────────────────────────────────────────

with open('/config/healthcheck_config.yml', 'r') as f:
    CONFIG = yaml.safe_load(f)

with open('/config/healthcheck_log_config.yml', 'r') as f:
    logging.config.dictConfig(yaml.safe_load(f))

logger = logging.getLogger('basicLogger')

# Extract frequently-used config values into variables
DATASTORE_LOCATION    = CONFIG['datastore']['location']           # /data/healthcheck/health_status.json
HEALTH_CHECK_INTERVAL = CONFIG['health_check']['interval_seconds']  # 5
TIMEOUT               = CONFIG['health_check']['timeout_seconds']    # 15
SERVICES              = CONFIG['services']   # dict of service_name → {hostname, port, url}

logger.info("Health Check Service configuration loaded")

# ── Service health check ──────────────────────────────────────────────────────

def check_service_health(service_name, service_config):
    """
    Make a single GET request to one service's /health endpoint.
    Returns "Up" if we get HTTP 200 within the timeout, "Down" for everything else.
    
    THREE FAILURE MODES WE HANDLE:
    
    1. Timeout: Service is running but too slow/busy to respond within TIMEOUT seconds.
       → Mark as Down. Common cause: Analyzer busy iterating Kafka under heavy load.
       
    2. ConnectionError: Service is completely unreachable (container stopped, network issue).
       → Mark as Down. Most common when a container crashes or is stopped manually.
       
    3. Any other exception: Unexpected error (DNS failure, SSL issue, etc.)
       → Mark as Down. Better to be cautious.
    
    WHY TIMEOUT MATTERS:
    Without a timeout, if a service hangs, this function would block indefinitely,
    preventing the scheduler from checking OTHER services. The timeout ensures
    we give up quickly and move on.
    """
    try:
        response = requests.get(service_config['url'], timeout=TIMEOUT)
        if response.status_code == 200:
            logger.info(f"✓ {service_name.upper()} is UP")
            return "Up"
        logger.warning(f"✗ {service_name.upper()} returned HTTP {response.status_code}")
        return "Down"
    except requests.exceptions.Timeout:
        logger.warning(f"✗ {service_name.upper()} TIMEOUT (>{TIMEOUT}s)")
        return "Down"
    except requests.exceptions.ConnectionError as e:
        logger.warning(f"✗ {service_name.upper()} CONNECTION ERROR: {e}")
        return "Down"
    except Exception as e:
        logger.error(f"✗ {service_name.upper()} ERROR: {e}")
        return "Down"

# ── Periodic status update ────────────────────────────────────────────────────

def update_health_status():
    """
    Called by APScheduler every 5 seconds. Polls all services and saves results.
    
    ALGORITHM:
    1. Try to load existing status from JSON file (to preserve last known state)
    2. For each service in config, call check_service_health()
    3. Update the dict with new status and current timestamp
    4. Write the entire dict back to the JSON file
    
    JSON FILE STRUCTURE:
    {
        "receiver":   {"status": "Up",   "last_check": "2026-04-15T12:00:00Z"},
        "storage":    {"status": "Down", "last_check": "2026-04-15T12:00:00Z"},
        "processing": {"status": "Up",   "last_check": "2026-04-15T12:00:00Z"},
        "analyzer":   {"status": "Up",   "last_check": "2026-04-15T12:00:00Z"}
    }
    
    Note: all services in one cycle get the SAME timestamp (current_time is set
    once before the loop) so you can tell at a glance which checks were from the
    same polling cycle.
    
    LOGGING REQUIREMENT:
    The assignment requires logging each time a service status is recorded.
    The logger.info(f"RECORDED: ...") line satisfies this requirement.
    """
    logger.info("=" * 50)
    logger.info("STARTING HEALTH CHECK CYCLE")
    logger.info("=" * 50)

    # Load existing state — if file doesn't exist yet, start with empty dict
    try:
        with open(DATASTORE_LOCATION, 'r') as f:
            health_status = json.load(f)
    except FileNotFoundError:
        health_status = {}
    except Exception as e:
        logger.error(f"Error loading datastore: {e}")
        health_status = {}

    # Use one timestamp for the entire polling cycle
    current_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Poll each service and record result
    for service_name, service_config in SERVICES.items():
        status = check_service_health(service_name, service_config)
        health_status[service_name] = {"status": status, "last_check": current_time}
        # REQUIRED LOG: must log each time we record a service status
        logger.info(f"RECORDED: {service_name.upper():12} = {status} at {current_time}")

    # Persist to JSON file
    try:
        # Create the directory if it doesn't exist yet (e.g., first startup)
        os.makedirs(os.path.dirname(DATASTORE_LOCATION), exist_ok=True)
        with open(DATASTORE_LOCATION, 'w') as f:
            json.dump(health_status, f, indent=2)
    except Exception as e:
        logger.error(f"Failed to write datastore: {e}")

    logger.info("HEALTH CHECK CYCLE COMPLETE")

# ── API endpoint ──────────────────────────────────────────────────────────────

def get_health_status():
    """
    GET /healthcheck/health-status
    
    Reads the JSON file and transforms the internal storage format into the
    API response format expected by the Dashboard.
    
    INTERNAL FORMAT (in file):
    {"receiver": {"status": "Up", "last_check": "2026-04-15T..."}, ...}
    
    API RESPONSE FORMAT:
    {"receiver": "Up", "storage": "Down", ..., "last_update": "2026-04-15T..."}
    
    WHY TRANSFORM?
    The internal format stores per-service timestamps (when was each one checked).
    The API format is simpler — just status strings plus one overall last_update
    timestamp (the most recent check time across all services).
    
    last_update = max of all individual last_check timestamps
    This tells the Dashboard "how fresh is this data overall".
    
    LOGGING REQUIREMENT:
    The assignment requires logging each time health status is retrieved via API.
    Both the logger.info("API REQUEST: ...") and logger.info("API RESPONSE: ...")
    lines satisfy this requirement.
    """
    logger.info("API REQUEST: GET /health-status")

    try:
        with open(DATASTORE_LOCATION, 'r') as f:
            health_status = json.load(f)

        response = {}
        latest_timestamp = None

        for service_name, service_data in health_status.items():
            # Flatten: {"status": "Up", "last_check": "..."} → "Up"
            response[service_name] = service_data['status']
            # Track the most recent timestamp across all services
            if latest_timestamp is None or service_data['last_check'] > latest_timestamp:
                latest_timestamp = service_data['last_check']

        response['last_update'] = latest_timestamp or "Unknown"
        logger.info(f"API RESPONSE: {response}")
        return response, 200

    except FileNotFoundError:
        # Scheduler hasn't run yet (service just started)
        logger.error("Datastore not found")
        return {"message": "Health status not available yet"}, 503
    except Exception as e:
        logger.error(f"Error retrieving health status: {e}")
        return {"message": "Error retrieving health status"}, 500

# ── Scheduler ─────────────────────────────────────────────────────────────────

def init_scheduler():
    """
    Start APScheduler to run update_health_status() every HEALTH_CHECK_INTERVAL seconds.
    
    Same pattern as Processing service — BackgroundScheduler runs in a daemon thread
    so it doesn't block the Flask web server and stops automatically on exit.
    """
    logger.info(f"Initializing scheduler with {HEALTH_CHECK_INTERVAL}s interval")
    scheduler = BackgroundScheduler(daemon=True)
    scheduler.add_job(
        update_health_status,
        'interval',
        seconds=HEALTH_CHECK_INTERVAL,
        id='health_check_job',
    )
    scheduler.start()
    logger.info("Scheduler started")

# ── App setup ─────────────────────────────────────────────────────────────────

app = connexion.FlaskApp(__name__, specification_dir='')

# CORS via Starlette middleware (connexion 3.x style).
# Flask-CORS works with Flask directly; this service uses the newer Starlette
# middleware approach that integrates with Connexion's ASGI layer.
# Only enabled when CORS_ALLOW_ALL=yes — behind Nginx, CORS is not needed.
if os.environ.get("CORS_ALLOW_ALL") == "yes":
    app.add_middleware(
        CORSMiddleware,
        position=MiddlewarePosition.BEFORE_EXCEPTION,  # handle CORS before any errors
        allow_origins=["*"],        # allow requests from any origin
        allow_credentials=True,
        allow_methods=["*"],        # allow GET, POST, etc.
        allow_headers=["*"],
    )

app.add_api(
    'openapi.yaml',
    base_path='/healthcheck',    # Nginx routes /healthcheck/* to this service
    strict_validation=True,
    validate_responses=True,
)

# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == '__main__':
    logger.info("=" * 60)
    logger.info("STARTING HEALTH CHECK SERVICE")
    logger.info(f"Listening on 0.0.0.0:{CONFIG['app']['port']}")
    logger.info("=" * 60)

    # Start scheduler BEFORE the web server so health checks begin immediately.
    # If we started the web server first, there'd be a window where the API
    # returns 503 (file not found) because the scheduler hasn't run yet.
    init_scheduler()

    app.run(host='0.0.0.0', port=CONFIG['app']['port'])   # port 8120