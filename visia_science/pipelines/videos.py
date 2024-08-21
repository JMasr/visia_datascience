import os

import pandas as pd

from visia_science import app_logger
from visia_science.data.multimedia import Multimedia
from visia_science.responses.http import BasicResponse


def pipeline_videos(path_to_raw_video: str, path_to_save_processed_video: str):
    """
    This function processes all video files in a specified directory, extracts their metadata,
    and saves the combined metadata to a CSV file.

    Flow
    ----
    1. Initialize an empty DataFrame to store metadata for all videos.
    2. Iterate over each video file in the specified raw video directory.
    3. Create a Multimedia object for each video file.
    4. Extract metadata and log the response.
    5. Concatenate the metadata of each video to the main DataFrame.
    6. Save the combined metadata to a CSV file.

    :param path_to_raw_video: The directory path containing raw video files
    :param path_to_save_processed_video: The directory path where processed video metadata will be saved
    :return: A DataFrame containing metadata for all processed videos
    """
    df_metadata_all_videos = pd.DataFrame()

    for video_file_path in os.listdir(path_to_raw_video):
        video_file_path = os.path.join(path_to_raw_video, video_file_path)
        visia_video = Multimedia(
            path_to_raw_data=video_file_path,
            path_to_save_data=path_to_save_processed_video
        )

        try:
            metadata_video: BasicResponse = visia_video.get_metadata_as_dataframe()
            metadata_video.log_response("Video Pipeline", "GetMetadata")

            metadata_video_df = metadata_video.data
            app_logger.info(f"Video {video_file_path} processed successfully")

            # Concatenate with all metadata
            df_metadata_all_videos = pd.concat([df_metadata_all_videos, metadata_video_df])
        except Exception as e:
            app_logger.error(f"Error processing video {video_file_path}: {e}")

    df_metadata_all_videos.to_csv(os.path.join(path_to_save_processed_video, "metadata_all_videos.csv"), index=False)
    return df_metadata_all_videos


def merge_processed_qv(processed_q: pd.DataFrame, processed_v: pd.DataFrame) -> pd.DataFrame:
    """
    Populate the questionaries dataframe with the videos information when the 'id' is the same in both dataframes.

    Args:
        processed_q (pd.DataFrame): Processed questionaries.
        processed_v (pd.DataFrame): Processed videos.

    Returns:
        pd.DataFrame: Merged processed questionaries and videos.
    """
    # Get the numbers of videos of id in processed_q and put 0 if there is no video
    processed_q["video_count"] = processed_q["id"].map(processed_v["id"].value_counts()).fillna(0)
    # Get the total duration of videos of id in processed_q
    processed_q["video_duration"] = processed_q["id"].map(processed_v.groupby("id")["audio-duration"].sum()).fillna(0)
    return processed_q
