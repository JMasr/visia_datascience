import json
from pathlib import Path
from typing import Union

from visia_science import app_logger


def read_json_as_a_dict(file_path: Path) -> dict:
    try:
        with open(file_path, "r") as file:
            json_file_as_dict = json.load(file)
    except Exception as e:
        app_logger.error(f"Error reading JSON file: {e}")
        json_file_as_dict = {}
    return json_file_as_dict
