import os

import dotenv

from visia_science.data.make_dataset import download_a_single_file_from_gdrive
from visia_science.data.questionary import VisiaQuestionary
from visia_science.files import load_json_as_dict


def pipeline_get_visia_q(q_path: str, config_path: str, q_process_path: str) -> list:
    visia_metadata: dict = load_json_as_dict(config_path)
    visia_q_metadata = visia_metadata.get("VISIA_Q")

    # Download data
    visia_questionaries = []
    for questionary in visia_q_metadata.keys():
        raw_data_url = visia_q_metadata[questionary]["q_url"]
        raw_data_path = os.path.join(q_path, questionary)
        is_file_donwload = download_a_single_file_from_gdrive(
            gdrive_url=raw_data_url, output_path=raw_data_path
        )

        if not is_file_donwload:
            raise ValueError(f"Failed to download {questionary} from {raw_data_url}")

        visia_q = VisiaQuestionary(
            path_to_raw_data=raw_data_path,
            path_to_save_data=q_process_path,
            q_name=visia_q_metadata[questionary]["q_name"],
            column_with_id=visia_q_metadata[questionary]["column_with_id"],
            column_with_date=visia_q_metadata[questionary]["column_with_date"],
            columns_with_items=visia_q_metadata[questionary]["columns_with_items"],
            columns_with_scores=visia_q_metadata[questionary]["columns_with_scores"],
        )
        visia_q.load_raw_data()
        visia_q.save_q_processed()
        visia_questionaries.append(visia_q)
    return visia_questionaries


def pipeline_clean_visia_q(visia_q: list) -> list:
    for visia_q in visia_q:
        visia_q.clean()
    return visia_q


if __name__ == "__main__":
    project_dir = os.path.join(os.path.dirname(__file__))
    dotenv_path = os.path.join(project_dir, ".env")
    dotenv.load_dotenv(dotenv_path)

    exp_name = os.getenv("EXP_NAME")
    CONFIG_PATH = os.getenv("CONFIG_PATH")
    VISIA_Q_PATH = os.getenv("VISIA_Q_PATH")
    VISIA_Q_PROCESS_PATH = os.getenv("VISIA_Q_PROCESS_PATH")
    print(f"Experiment name: {exp_name}")

    visia_questionaries = pipeline_get_visia_q(
        q_path=VISIA_Q_PATH, config_path=CONFIG_PATH, q_process_path=VISIA_Q_PROCESS_PATH
    )

    visia_questionaries = pipeline_clean_visia_q(visia_questionaries)
