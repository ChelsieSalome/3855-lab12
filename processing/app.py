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


with open('/config/processing_config.yml', 'r') as f:
    app_config = yaml.safe_load(f)

with open('/config/processing_log_config.yml', 'r') as f:
    log_config = yaml.safe_load(f)
    logging.config.dictConfig(log_config)

logger = logging.getLogger('basicLogger')

filename = app_config['datastore']['filename']
performance_url = app_config['eventstores']['performance_url']
errors_url = app_config['eventstores']['errors_url']
scheduler_interval = app_config['scheduler']['interval']


def get_stats():
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
    """Health check endpoint"""
    return {"status": "healthy"}, 200


first_run = True

def populate_stats():
    global first_run  
    logger.info("Periodic processing has started")  
    
    try:
        with open(filename, 'r') as f:
            content = f.read().strip()
        
        if not content:
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
            
            if first_run and (stats['num_performance_readings'] > 0 or 
                              stats['max_cpu_reading'] > 0 or 
                              stats['num_error_readings'] > 0 or 
                              stats['max_severity_level'] > 0):
                logger.info("Resetting statistics to zero before updating with new values")
                stats['num_performance_readings'] = 0
                stats['max_cpu_reading'] = 0
                stats['num_error_readings'] = 0
                stats['max_severity_level'] = 0
                first_run = False  

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
    
    last_updated = datetime.strptime(stats['last_updated'], "%Y-%m-%dT%H:%M:%SZ")
    current_datetime = datetime.now(timezone.utc)
    
    params = {
        'start_timestamp': last_updated.strftime("%Y-%m-%dT%H:%M:%SZ"),
        'end_timestamp': current_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")
    }
    
    performance_response = requests.get(performance_url, params=params)
    
    if performance_response.status_code == 200:
        performance_readings = performance_response.json()
        logger.info(f"Number of performance events received: {len(performance_readings)}")  
        
        stats['num_performance_readings'] += len(performance_readings)
        
        if performance_readings:
            max_cpu = max(reading['cpu'] for reading in performance_readings)
            if max_cpu > stats['max_cpu_reading']:
                stats['max_cpu_reading'] = max_cpu
    else:
        logger.error(f"Did not get 200 response code for performance data: {performance_response.status_code}")  
    
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
    
    stats['last_updated'] = current_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")
    
    with open(filename, 'w') as f:
        json.dump(stats, f, indent=4)
    
    logger.debug(f"Updated statistics: {stats}")  
    logger.info("Periodic processing has ended")


def init_scheduler():  
    sched = BackgroundScheduler(daemon=True)
    sched.add_job(populate_stats,
                  'interval',
                  seconds=app_config['scheduler']['interval'])
    sched.start()


# CREATE THE APP FIRST (but don't add API yet)
app = connexion.FlaskApp(__name__, specification_dir='')
if os.environ.get("CORS_ALLOW_ALL") == "yes":
    CORS(app.app)

# NOW ADD THE API AFTER ALL FUNCTIONS ARE DEFINED
app.add_api("processing_openapi.yaml", base_path="/processing", strict_validation=True, validate_responses=True)


if __name__ == "__main__":
    init_scheduler() 
    app.run(port=8100)