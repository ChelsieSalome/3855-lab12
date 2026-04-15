"""
Health Check Service - app.py
Polls all backend services and exposes their status via API.
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

DATASTORE_LOCATION   = CONFIG['datastore']['location']
HEALTH_CHECK_INTERVAL = CONFIG['health_check']['interval_seconds']
TIMEOUT              = CONFIG['health_check']['timeout_seconds']
SERVICES             = CONFIG['services']

logger.info("Health Check Service configuration loaded")

# ── Service health check ──────────────────────────────────────────────────────

def check_service_health(service_name, service_config):
    """Call a service's /health endpoint. Returns 'Up' or 'Down'."""
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
    """Poll all services and write results to the JSON datastore."""
    logger.info("=" * 50)
    logger.info("STARTING HEALTH CHECK CYCLE")
    logger.info("=" * 50)

    try:
        with open(DATASTORE_LOCATION, 'r') as f:
            health_status = json.load(f)
    except FileNotFoundError:
        health_status = {}
    except Exception as e:
        logger.error(f"Error loading datastore: {e}")
        health_status = {}

    current_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    for service_name, service_config in SERVICES.items():
        status = check_service_health(service_name, service_config)
        health_status[service_name] = {"status": status, "last_check": current_time}
        logger.info(f"RECORDED: {service_name.upper():12} = {status} at {current_time}")

    try:
        os.makedirs(os.path.dirname(DATASTORE_LOCATION), exist_ok=True)
        with open(DATASTORE_LOCATION, 'w') as f:
            json.dump(health_status, f, indent=2)
    except Exception as e:
        logger.error(f"Failed to write datastore: {e}")

    logger.info("HEALTH CHECK CYCLE COMPLETE")

# ── API endpoint ──────────────────────────────────────────────────────────────

def get_health_status():
    """GET /healthcheck/health-status"""
    logger.info("API REQUEST: GET /health-status")

    try:
        with open(DATASTORE_LOCATION, 'r') as f:
            health_status = json.load(f)

        response = {}
        latest_timestamp = None

        for service_name, service_data in health_status.items():
            response[service_name] = service_data['status']
            if latest_timestamp is None or service_data['last_check'] > latest_timestamp:
                latest_timestamp = service_data['last_check']

        response['last_update'] = latest_timestamp or "Unknown"
        logger.info(f"API RESPONSE: {response}")
        return response, 200

    except FileNotFoundError:
        logger.error("Datastore not found")
        return {"message": "Health status not available yet"}, 503
    except Exception as e:
        logger.error(f"Error retrieving health status: {e}")
        return {"message": "Error retrieving health status"}, 500

# ── Scheduler ─────────────────────────────────────────────────────────────────

def init_scheduler():
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

# CORS via Starlette middleware — works correctly with connexion 3.x
app.add_middleware(
    CORSMiddleware,
    position=MiddlewarePosition.BEFORE_EXCEPTION,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_api(
    'openapi.yaml',
    base_path='/healthcheck',
    strict_validation=True,
    validate_responses=True,
)

# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == '__main__':
    logger.info("=" * 60)
    logger.info("STARTING HEALTH CHECK SERVICE")
    logger.info(f"Listening on 0.0.0.0:{CONFIG['app']['port']}")
    logger.info("=" * 60)

    init_scheduler()

    app.run(host='0.0.0.0', port=CONFIG['app']['port'])