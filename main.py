import os

import dotenv

from visia_science import app_logger
from visia_science.data.multimedia import Multimedia

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
    # visia_questionaries_pipeline(
    #     exp_name=EXP_NAME,
    #     q_path=VISIA_Q_PATH,
    #     config_path=CONFIG_PATH,
    #     q_process_path=VISIA_Q_PROCESS_PATH,
    # )

    for video_file_path in os.listdir(VISIA_V_PATH):
        video_file_path = os.path.join(VISIA_V_PATH, video_file_path)
        visia_video = Multimedia(
            path_to_raw_data=video_file_path,
            path_to_save_data=VISIA_V_PROCESS_PATH
        )

        load_video_response = None
        try:
            load_video_response = visia_video.load_multimedia()
            load_video_response.log_response(module="Multimedia", action="LoadRawData")
        except Exception as e:
            app_logger.error(f"Error loading multimedia data: {e}")

        if load_video_response.success:
            try:
                video_response = visia_video.transcribe()
                video_response.log_response(module="Multimedia", action="Transcribe")
            except Exception as e:
                app_logger.error(f"Error transcribing multimedia data: {e}")
        else:
            app_logger.error(f"Error loading multimedia data: {load_video_response.message}")

        app_logger.info(f"Video {video_file_path} processed successfully")
