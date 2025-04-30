import logging

# Configure logging to write to a file
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("application.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# Example usage of logger
logger.info("Application started")

# ...existing code...