import logging
from datetime import datetime
import os

os.makedirs("Logs", exist_ok=True)
time = datetime.now()

formatted_time = time.strftime("%y-%m-%d")

logging.basicConfig(
    level=logging.DEBUG, 
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt= '%y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(f"Logs/{formatted_time}_app.log", mode='a'),  
        logging.StreamHandler()  
    ]
)

logger = logging.getLogger("MyApp")