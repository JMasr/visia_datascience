import json
import os


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
        with open(path, "r") as file:
            json_as_dict = json.load(file)
    except Exception as e:
        raise RuntimeError(f"Error loading JSON file: {e}")

    return json_as_dict
