import argparse
import os.path
from pathlib import Path

from visia_science import app_logger
from visia_science.data.make_dataset import download_a_list_of_files_from_gdrive
from visia_science.utils.files import read_json_as_a_dict

ROOT_PATH = Path(__file__).parent
DEFAULT_CONFIG_FILE = ROOT_PATH / "config" / "pipeline_config.json"
DEFAULT_RAW_DATA_PATH = ROOT_PATH / "data" / "raw"

if __name__ == "__main__":
    arguments = argparse.ArgumentParser(description="Run the VISIA's data-pipeline")
    arguments.add_argument(
        "--config",
        type=str,
        default=DEFAULT_CONFIG_FILE,
        help="Path to the configuration file",
    )
    args = arguments.parse_args()
    config_file = Path(args.config)

    if config_file.exists():
        dict_config = read_json_as_a_dict(config_file)
    else:
        app_logger.error(f"Config file not found: {config_file}")
        exit(1)

    app_logger.info("MAIN - Pipeline Execution Started")

    raw_data_name_2_urls = dict_config["raw_data_urls"]
    download_list = raw_data_name_2_urls.values()
    download_paths = [os.path.join(DEFAULT_RAW_DATA_PATH, file_name) for file_name in raw_data_name_2_urls.keys()]

    download_a_list_of_files_from_gdrive(download_list, download_paths)