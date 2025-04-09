from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from db_class import Base
import yaml

# Load the configuration from app_conf.yml
with open("/app/config/storage/storage_conf.yaml", "r") as config_file:
    config = yaml.safe_load(config_file)

# Database connection string from the YAML configuration
db_config = config["datastore"]
DATABASE_URL = f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['hostname']}:{db_config['port']}/{db_config['db']}"

# Create engine
engine = create_engine(DATABASE_URL, echo=True)

# Create tables (if they don't exist)
Base.metadata.create_all(engine)

# Set up session (scoped session to handle multiple requests properly)
Session = scoped_session(sessionmaker(bind=engine))

# Use this instead of `session = Session()`
def get_session():
    return Session()
