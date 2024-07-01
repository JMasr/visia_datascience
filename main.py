import os

import dotenv

from visia_science.data.questionary import VisiaQuestionary
from visia_science.files import load_json_as_dict


def pipeline_2_process_visia_q(q_path: str, config_path: str, q_process_path: str):
    visia_raw_q_paths = [os.path.join(q_path, file) for file in os.listdir(q_path)]
    visia_metadata: dict = load_json_as_dict(config_path)
    visia_q_metadata = visia_metadata.get("VISIA_Q")

    for questionary in visia_q_metadata.keys():
        path_with_q = [
            path
            for path in visia_raw_q_paths
            if visia_q_metadata[questionary]["q_name"].lower() in path
        ]
        visia_q_metadata[questionary]["q_path"] = path_with_q[0]

        visia_q = VisiaQuestionary(
            path_to_raw_data=visia_q_metadata[questionary]["q_path"],
            path_to_save_data=q_process_path,
            q_name=visia_q_metadata[questionary]["q_name"],
            column_with_id=visia_q_metadata[questionary]["column_with_id"],
            column_with_date=visia_q_metadata[questionary]["column_with_date"],
            columns_with_items=visia_q_metadata[questionary]["columns_with_items"],
            columns_with_scores=visia_q_metadata[questionary]["columns_with_scores"],
        )
        visia_q.load_raw_data()
        visia_q.save_q_processed()


if __name__ == "__main__":
    project_dir = os.path.join(os.path.dirname(__file__))
    dotenv_path = os.path.join(project_dir, ".env")
    dotenv.load_dotenv(dotenv_path)

    exp_name = os.getenv("EXP_NAME")
    CONFIG_PATH = os.getenv("CONFIG_PATH")
    VISIA_Q_PATH = os.getenv("VISIA_Q_PATH")
    VISIA_Q_PROCESS_PATH = os.getenv("VISIA_Q_PROCESS_PATH")
    print(f"Experiment name: {exp_name}")

    pipeline_2_process_visia_q(VISIA_Q_PATH, CONFIG_PATH, VISIA_Q_PROCESS_PATH)
