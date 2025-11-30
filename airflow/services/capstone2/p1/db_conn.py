from sqlalchemy import create_engine
import logging

logger = logging.getLogger(__name__)

def get_engine():
    db_url = (
        "postgresql+psycopg2://your_db_user:your_db_password"
        "@retail_postgres_31:5432/retail_db2"
    )
    return create_engine(db_url, future=True) 


