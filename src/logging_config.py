import os
import logging

DB_USER = os.getenv("dbuser", "unknown_user")

logging.basicConfig(
    filename="logs/etl.log",
    level=logging.INFO,
    format=f"%(asctime)s - {DB_USER} - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)