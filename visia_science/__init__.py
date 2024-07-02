import os
from datetime import datetime

from visia_science.logger.basic_logger import BasicLogger

root_path = os.path.dirname(os.path.abspath(__file__))
data_time_today = datetime.now().strftime("%Y-%m-%d")
app_logger = BasicLogger(
                    log_file=os.path.join(root_path, "logs", f"experiment_{data_time_today}.log")
                ).get_logger()