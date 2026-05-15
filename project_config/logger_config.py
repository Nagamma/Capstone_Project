import logging
#Logging to both console and file
logging.basicConfig(
    filename="./logs/etljob.log",
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO,
)

logger = logging.getLogger(__name__)