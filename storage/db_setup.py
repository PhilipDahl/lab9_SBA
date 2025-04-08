from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.exc import OperationalError
from db_class import Base
import yaml
import time

# Load the configuration from app_conf.yml
with open("/config/storage_config.yml", "r") as config_file:
    config = yaml.safe_load(config_file)

# Database connection string from the YAML configuration
db_config = config["database"]
DATABASE_URL = f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['db']}"

# Retry logic for MySQL connection
MAX_RETRIES = 10
for attempt in range(MAX_RETRIES):
    try:
        engine = create_engine(DATABASE_URL, echo=True)
        Base.metadata.create_all(engine)
        break
    except OperationalError as e:
        print(f"MySQL not available (attempt {attempt+1}/{MAX_RETRIES}): {e}")
        time.sleep(2 ** (attempt + 1))
else:
    print("Could not connect to MySQL after multiple retries.")
    raise SystemExit(1)

# Set up session (scoped session to handle multiple requests properly)
Session = scoped_session(sessionmaker(bind=engine))

# Use this instead of `session = Session()`
def get_session():
    return Session()
