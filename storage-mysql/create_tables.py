"""
Storage Service - create_tables.py
Creates the MySQL schema and provides a session factory.
Lab 11 fix:
  - Connection pooling: pool_size, max_overflow, pool_recycle, pool_pre_ping
    prevents "Lost connection to MySQL server" errors after hours of uptime.
"""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Base
import yaml
import logging
import logging.config
import time

# ── Config & Logging ──────────────────────────────────────────────────────────

with open('/config/storage_config.yml', 'r') as f:
    app_config = yaml.safe_load(f)

db_conf = app_config['datastore']

with open('/config/storage_log_config.yml', 'r') as f:
    logging.config.dictConfig(yaml.safe_load(f))

logger = logging.getLogger('basicLogger')


DATABASE_URL = (
    f"mysql+pymysql://{db_conf['user']}:{db_conf['password']}"
    f"@{db_conf['hostname']}:{db_conf['port']}/{db_conf['db']}"
)

engine = create_engine(
    DATABASE_URL,
    echo=False,
    #  Pool settings (Lab 11 fix) 
    pool_size=10,           # keep 10 connections open and ready
    max_overflow=20,        # allow up to 10+20=30 under burst load
    pool_recycle=3600,      # recycle connections after 1 h (before MySQL's 8-h timeout)
    pool_pre_ping=True,     # test each connection before use; replace dead ones silently
    pool_timeout=30,        # wait up to 30 s for a connection from the pool
)

logger.info(
    f"Database engine created | host={db_conf['hostname']}:{db_conf['port']} "
    f"db={db_conf['db']}"
)


_SessionFactory = sessionmaker(bind=engine)


def make_session():
    """Return a new SQLAlchemy session."""
    return _SessionFactory()


# Table creation with retry 

def init_db(retries: int = 15, delay: int = 5):
    """
    Wait for MySQL to be ready, then create all tables.
    Call this once at service startup before starting the Kafka thread.
    """
    for attempt in range(1, retries + 1):
        try:
            logger.info(f"MySQL connection attempt {attempt}/{retries} …")
            Base.metadata.create_all(engine)
            logger.info("MySQL connected and tables ready")
            return
        except Exception as e:
            logger.warning(f"MySQL not ready (attempt {attempt}/{retries}): {e}")
            if attempt < retries:
                time.sleep(delay)
            else:
                logger.error("Could not connect to MySQL after all retries – exiting")
                raise


if __name__ == "__main__":
    init_db()