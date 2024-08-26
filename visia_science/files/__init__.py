import json
import os

from visia_science import app_logger
import random


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


def scramble_file(input_path: str, output_path: str, percentage_to_scramble: float = 0.2) -> bool:
    """
    Scramble the bytes of a file to create a corrupted version.
    Flow
    ----
    1. Read the bytes from the input file as a list
    2. Get a percentage of the bytes to scramble
    2. Shuffle the bytes selected using random
    3. Write the shuffled bytes to the output file

    :param input_path: The path to the input file
    :param output_path: The path to save the scrambled file
    :param percentage_to_scramble: The percentage of bytes to scramble as a float number between 0 and 1 (default 0.2)
    :return: A boolean indicating if the file was successfully scrambled
    """
    try:
        with open(input_path, "rb") as f_in, open(output_path, "wb") as f_out:
            data = f_in.read()
            scrambled_data = bytearray(data[: int(len(data) * percentage_to_scramble)])
            random.shuffle(scrambled_data)
            f_out.write(bytes(scrambled_data))
        return True
    except Exception as e:
        app_logger.error(f"Error scrambling file: {e}")
        return False
