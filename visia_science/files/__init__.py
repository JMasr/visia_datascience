import json
import os

from visia_science import app_logger


def is_file_a_valid_ext(path: str, extension: str = None) -> bool:
    if not os.path.exists(path):
        return False

    if extension is not None:
        if not path.endswith(extension):
            return False

    return True


def load_json_as_dict(path: str) -> dict:
    if not is_file_a_valid_ext(path, ".json"):
        raise ValueError(f"File {path} is not a valid JSON file")

    try:
        with open(path, "r", encoding="utf-8") as file:
            json_as_dict = json.load(file)
    except Exception as e:
        raise RuntimeError(f"Error loading JSON file: {e}")

    return json_as_dict


def save_dict_as_json(data: dict, path: str) -> None:
    if os.path.dirname(path) != "" and not os.path.exists(os.path.dirname(path)):
        app_logger.warning(f"Creating directory {os.path.dirname(path)}")
        os.makedirs(os.path.dirname(path))

    try:
        with open(path, "w", encoding="utf-8") as file:
            json.dump(data, file, indent=4)
    except Exception as e:
        raise RuntimeError(f"Error saving JSON file: {e}")
