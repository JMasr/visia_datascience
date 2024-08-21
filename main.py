import os

import dotenv

from visia_science import app_logger
from visia_science.pipelines.questionaries import visia_questionaries_pipeline
from visia_science.pipelines.videos import pipeline_videos, merge_processed_qv

if __name__ == "__main__":
    # Load environment variables
    project_dir = os.path.join(os.path.dirname(__file__))
    dotenv_path = os.path.join(project_dir, ".env")
    dotenv.load_dotenv(dotenv_path)
    # Get environment variables
    EXP_NAME = os.getenv("EXP_NAME")
    EXP_PATH = os.getenv("EXP_PATH")
    CONFIG_PATH = os.getenv("CONFIG_PATH")
    # Get Questionaries paths
    VISIA_Q_PATH = os.getenv("QUESTIONARIES_PATH")
    VISIA_Q_PROCESS_PATH = os.getenv("QUESTIONARIES_PROCESS_PATH")
    # Get Video paths
    VISIA_V_PATH = os.getenv("VIDEO_PATH")
    VISIA_V_PROCESS_PATH = os.getenv("VIDEO_PROCESS_PATH")

    app_logger.info(f"Starting pipeline for {EXP_PATH}")
    visia_q_processed = visia_questionaries_pipeline(
        exp_name=EXP_NAME,
        q_path=VISIA_Q_PATH,
        config_path=CONFIG_PATH,
        q_process_path=VISIA_Q_PROCESS_PATH,
    )

    visia_v_processed = pipeline_videos(path_to_raw_video=VISIA_V_PATH,
                                        path_to_save_processed_video=VISIA_V_PROCESS_PATH)

    # Merge processed questionaries and videos
    visia_qv_processed = merge_processed_qv(visia_q_processed, visia_v_processed)
