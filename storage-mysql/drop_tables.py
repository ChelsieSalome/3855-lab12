from sqlalchemy import create_engine
from models import Base
import yaml
import logging
import logging.config

# Load configuration
with open('/config/storage_config.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

db_conf = app_config['datastore']


with open('/config/storage_log_config.yml', 'r') as f:
    LOG_CONFIG = yaml.safe_load(f.read())
    logging.config.dictConfig(LOG_CONFIG)

logger = logging.getLogger('basicLogger')



# MySQL connection string
DATABASE_URL = f"mysql+pymysql://{db_conf['user']}:{db_conf['password']}@{db_conf['hostname']}:{db_conf['port']}/{db_conf['db']}"

engine = create_engine(DATABASE_URL, echo=True)
Base.metadata.drop_all(engine)


logger.info("All tables dropped successfully")
 
