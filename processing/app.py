"""
Processing Service - app.py

ROLE IN THE SYSTEM:
-------------------
Processing is the AGGREGATION/STATISTICS layer. It periodically queries Storage
for new events, computes running statistics (counts, max values), and saves them
to a local JSON file. It exposes these stats via an API for the Dashboard.

DATA FLOW:
    APScheduler (every 5s) → GET storage/monitoring/performance → compute stats → data.json
    Dashboard              → GET /processing/stats              → read data.json → response

KEY DESIGN DECISIONS:
- Does NOT read Kafka directly — delegates data storage to the Storage service
- Uses a JSON file as its datastore (simple, no database needed for aggregated stats)
- Runs on a scheduler so stats are always fresh without needing request-triggered updates
- Tracks last_updated timestamp to only fetch NEW data each interval (incremental updates)
"""

import connexion
import json
import logging
import logging.config
import yaml
from flask_cors import CORS
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timezone
import requests
import os


# Load config from shared /config volume
with open('/config/processing_config.yml', 'r') as f:
    app_config = yaml.safe_load(f)

with open('/config/processing_log_config.yml', 'r') as f:
    log_config = yaml.safe_load(f)
    logging.config.dictConfig(log_config)

logger = logging.getLogger('basicLogger')

# Read config values into variables for convenience
filename         = app_config['datastore']['filename']       # /data/data.json
performance_url  = app_config['eventstores']['performance_url']  # Storage URL
errors_url       = app_config['eventstores']['errors_url']        # Storage URL
scheduler_interval = app_config['scheduler']['interval']    # 5 seconds


def get_stats():
    """
    GET /processing/stats
    
    Simply reads the current stats from the JSON file and returns them.
    The stats file is maintained by populate_stats() running in the background.
    
    WHY READ FROM FILE instead of computing on demand?
    Computing stats requires calling Storage, which queries MySQL. Doing this
    on every Dashboard request (every 3 seconds *  many users) would be expensive.
    Instead, we pre-compute on a schedule and serve the cached result instantly.
    """
    logger.info("Request for statistics received")
    
    try:
        with open(filename, 'r') as f:
            stats = json.load(f)
        
        logger.debug(f"Statistics contents: {stats}")
        logger.info("Request has completed")
        return stats, 200
    except FileNotFoundError:
        logger.error("Statistics file does not exist")
        return {"message": "Statistics do not exist"}, 404


def health():
    """
    GET /processing/health — liveness probe.
    Returns 200 as long as the process is running.
    """
    return {"status": "healthy"}, 200


# Global flag: True only on the very first run after startup.
# Used to reset stale stats from a previous run before accumulating new ones.
first_run = True

def populate_stats():
    """
    Called by APScheduler every 5 seconds. Core logic of the Processing service.
    
    ALGORITHM (incremental update):
    1. Read current stats from JSON file (or create defaults if file missing)
    2. On first_run: reset all counters to 0 (discard stale data from last session)
    3. Fetch ONLY NEW events from Storage using last_updated as the start timestamp
    4. Add new counts to existing totals
    5. Update max values if new data exceeds current maximums
    6. Save updated stats back to JSON file
    
    WHY INCREMENTAL (not recompute from scratch)?
    If we re-counted ALL events in MySQL every 5 seconds, it would get slower
    as data grows. Instead, we track last_updated and only ask Storage for events
    since then. This keeps each interval O(new data) not O(all data).
    
    EXAMPLE:
    Interval 1: last_updated=2026-01-01, fetches 50 events, saves count=50
    Interval 2: last_updated=2026-01-01T00:00:05, fetches 10 new events, saves count=60
    
    first_run RESET LOGIC:
    When Processing starts, data.json might have stats from a previous run (e.g., count=500).
    Without resetting, we'd add new events on top of old totals, causing inflated counts.
    first_run=True resets counters to 0 on startup, then sets first_run=False so
    subsequent intervals accumulate normally.
    """
    global first_run
    logger.info("Periodic processing has started")
    
    # ── Step 1: Load existing stats or create defaults ─────────────────────────
    try:
        with open(filename, 'r') as f:
            content = f.read().strip()
        
        if not content:
            # File exists but is empty (e.g., just created by Docker volume mount)
            logger.info("File exists but is empty, using default values")
            stats = {
                "num_performance_readings": 0,
                "max_cpu_reading": 0,
                "num_error_readings": 0,
                "max_severity_level": 0,
                "last_updated": "2026-01-01T00:00:00Z"
            }
        else:
            stats = json.loads(content)
            logger.debug("Successfully loaded existing statistics")
            
            # ── Step 2: Reset on first run ─────────────────────────────────────
            # If stats have non-zero values from a previous session, reset them.
            # This ensures we don't carry over stale data across container restarts.
            if first_run and (stats['num_performance_readings'] > 0 or
                              stats['max_cpu_reading'] > 0 or
                              stats['num_error_readings'] > 0 or
                              stats['max_severity_level'] > 0):
                logger.info("Resetting statistics to zero before updating with new values")
                stats['num_performance_readings'] = 0
                stats['max_cpu_reading'] = 0
                stats['num_error_readings'] = 0
                stats['max_severity_level'] = 0
                first_run = False   # only reset once per startup

    except FileNotFoundError:
        logger.info("File not found, creating with default values")
        stats = {
            "num_performance_readings": 0,
            "max_cpu_reading": 0,
            "num_error_readings": 0,
            "max_severity_level": 0,
            "last_updated": "2026-01-01T00:00:00Z"
        }
    except Exception as e:
        logger.error(f"Unexpected error reading statistics file: {e}")
        stats = {
            "num_performance_readings": 0,
            "max_cpu_reading": 0,
            "num_error_readings": 0,
            "max_severity_level": 0,
            "last_updated": "2026-01-01T00:00:00Z"
        }
    
    # ── Step 3: Fetch only NEW events from Storage ──────────────────────────────
    # Use last_updated as start and now as end — only events in this window are new.
    last_updated     = datetime.strptime(stats['last_updated'], "%Y-%m-%dT%H:%M:%SZ")
    current_datetime = datetime.now(timezone.utc)
    
    params = {
        'start_timestamp': last_updated.strftime("%Y-%m-%dT%H:%M:%SZ"),
        'end_timestamp':   current_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")
    }
    
    # ── Step 4a: Process performance readings ───────────────────────────────────
    # NOTE: performance_url goes through Nginx (http://dashboard/storage/monitoring/performance)
    # Nginx routes /storage → storage-service:8091, which handles the query.
    performance_response = requests.get(performance_url, params=params)
    
    if performance_response.status_code == 200:
        performance_readings = performance_response.json()
        logger.info(f"Number of performance events received: {len(performance_readings)}")
        
        # ADD to existing total (incremental accumulation)
        stats['num_performance_readings'] += len(performance_readings)
        
        # UPDATE max only if this batch has a higher value
        if performance_readings:
            max_cpu = max(reading['cpu'] for reading in performance_readings)
            if max_cpu > stats['max_cpu_reading']:
                stats['max_cpu_reading'] = max_cpu
    else:
        logger.error(f"Did not get 200 response code for performance data: {performance_response.status_code}")
    
    # ── Step 4b: Process error readings ────────────────────────────────────────
    error_response = requests.get(errors_url, params=params)
    
    if error_response.status_code == 200:
        error_readings = error_response.json()
        logger.info(f"Number of error events received: {len(error_readings)}")
        
        stats['num_error_readings'] += len(error_readings)
        
        if error_readings:
            max_severity = max(reading['severity_level'] for reading in error_readings)
            if max_severity > stats['max_severity_level']:
                stats['max_severity_level'] = max_severity
    else:
        logger.error(f"Did not get 200 response code for error data: {error_response.status_code}")
    
    # ── Step 5: Update timestamp and save ─────────────────────────────────────
    # Update last_updated to NOW so next interval only fetches events after this point
    stats['last_updated'] = current_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")
    
    with open(filename, 'w') as f:
        json.dump(stats, f, indent=4)
    
    logger.debug(f"Updated statistics: {stats}")
    logger.info("Periodic processing has ended")


def init_scheduler():
    """
    Start the APScheduler background scheduler.
    
    BackgroundScheduler runs jobs in a separate thread without blocking the
    Flask web server. daemon=True means the scheduler thread stops automatically
    when the main process exits.
    
    The job runs populate_stats() every `scheduler_interval` seconds (5s from config).
    """
    sched = BackgroundScheduler(daemon=True)
    sched.add_job(populate_stats,
                  'interval',
                  seconds=app_config['scheduler']['interval'])
    sched.start()


# ── App Setup ─────────────────────────────────────────────────────────────────

app = connexion.FlaskApp(__name__, specification_dir='')

# Only enable CORS when explicitly requested via environment variable.
# Behind Nginx, CORS is not needed — all traffic comes through port 80.
if os.environ.get("CORS_ALLOW_ALL") == "yes":
    CORS(app.app)

app.add_api("processing_openapi.yaml",
            base_path="/processing",     # Nginx routes /processing/* here
            strict_validation=True,
            validate_responses=True)


if __name__ == "__main__":
    init_scheduler()    # start background stats computation before serving requests
    app.run(port=8100)