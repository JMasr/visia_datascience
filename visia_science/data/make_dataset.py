import logging
import os
import re
from pathlib import Path
from typing import List

import gdown
from tqdm import tqdm

from visia_science import app_logger


def download_a_single_file_from_gdrive(gdrive_url: str, output_path: str) -> bool:
    """
    Download a file from Google Drive.

    Parameters:
    gdrive_url (str): The Google Drive URL to download the file from.
    output_path (str): The local path where the downloaded file will be saved.

    Returns:
    bool: True if the download is successful, False otherwise.
    """
    if Path(output_path).exists():
        app_logger.warning(f"DOWNLOADER - File {output_path} already exists")
        return True

    if not re.match(r"^https:\/\/drive\.google\.com\/.*", gdrive_url):
        app_logger.error("DOWNLOADER - Invalid Google Drive URL format")
        return False

    if not Path(output_path).parent.exists():
        app_logger.warning(
            f"DOWNLOADER - Output directory {Path(output_path).parent} does not exist"
        )
        os.makedirs(Path(output_path).parent, exist_ok=True)

    try:
        gdown.download(gdrive_url, output_path, quiet=False)
    except Exception as e:
        app_logger.error(f"DOWNLOADER - Failed to download from {gdrive_url}: {e}")
        return False

    return True


def download_a_list_of_files_from_gdrive(gdrive_urls: List[str], output_paths: List[str]) -> bool:
    """
    Download a list of files from Google Drive and save them to local paths.
    :param gdrive_urls: List of Google Drive URLs to download the files from.
    :param output_paths: List of local paths where the downloaded files will be saved.
    :return:
    """
    if len(gdrive_urls) != len(output_paths):
        logging.error("The number of URLs and output paths do not match")
        return False

    confirmations = []
    for gdrive_url, output_path in tqdm(zip(gdrive_urls, output_paths), total=len(gdrive_urls)):
        is_file_downloaded = download_a_single_file_from_gdrive(gdrive_url, output_path)
        if is_file_downloaded:
            logging.info(f"DATASET - File downloaded to {output_path}")
        else:
            logging.warning(f"DATASET - Failed to download file from {gdrive_url}")

        confirmations.append(is_file_downloaded)

    return all(confirmations)
