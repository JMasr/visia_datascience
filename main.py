import os
import dotenv

from visia_science.data.questionary import BaseQuestionary

if __name__ == '__main__':
    project_dir = os.path.join(os.path.dirname(__file__))
    dotenv_path = os.path.join(project_dir, '.env')
    dotenv.load_dotenv(dotenv_path)

    exp_name = os.getenv("EXP_NAME")
    print(f"Experiment name: {exp_name}")

    basic_questionary = BaseQuestionary(
        path_to_raw_data="RAW_DATA_PATH",
        path_to_save_data="SAVE_DATA_PATH",
        column_with_id="COLUMN_WITH_ID",
        column_with_date="COLUMN_WITH_DATE",
        columns_with_items=["COLUMNS_WITH_ITEMS"],
        columns_with_scores=["COLUMNS_WITH_SCORES"]
    )
    basic_questionary.load_raw_data()