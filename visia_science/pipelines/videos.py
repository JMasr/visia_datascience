import os

import pandas as pd

from visia_science import app_logger
from visia_science.data.multimedia import Multimedia
from visia_science.responses.http import BasicResponse


def pipeline_videos(path_to_raw_video: str, path_to_save_processed_video: str):
    """
    This function processes all video files in a specified directory, extracts their metadata,
    and saves the combined metadata to a CSV file.

    Important Cases:
     1. Corrupted Video Files: If the video file is empty, the function will insert a row with the video file path with
      the metadata available, the confidence, and video/audio duration as 0.

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
            path_to_raw_data=video_file_path, path_to_save_data=path_to_save_processed_video
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

        # Clean Up
        del visia_video

    # Save the metadata of all videos to a CSV file
    os.makedirs(path_to_save_processed_video, exist_ok=True)
    df_metadata_all_videos.to_csv(
        os.path.join(path_to_save_processed_video, "metadata_all_videos.csv"), index=False
    )
    return df_metadata_all_videos


def merge_processed_qv(
    processed_q: pd.DataFrame, processed_v: pd.DataFrame, path_to_save: str
) -> pd.DataFrame:
    """
    This function merges the processed questionaries and videos DataFrames, calculates the number of videos, and the
    total duration of videos for each questionary.

    Flow
    ----
    1. Calculate the number of videos for each questionary.
    2. Calculate the total duration of videos for each questionary.
    3. Save the merged DataFrame to a CSV file.

    :param processed_q: The processed questionaries DataFrame
    :param processed_v: The processed videos DataFrame
    :param path_to_save: The directory path where the merged DataFrame will be saved
    :return: The merged DataFrame
    """
    # Get only videos with valid audio-duration and with ffmpeg_confidence > 0.5
    list_with_valid_videos = processed_v[
        (processed_v["audio-duration"] > 0) & (processed_v["ffmpeg_confidence"] > 0.5)
    ]
    # Get the numbers of videos of id in processed_q and put 0 if there is no video
    processed_q["video_count"] = (
        processed_q["id"].map(list_with_valid_videos["id"].value_counts()).fillna(0)
    )
    # Get the total duration of videos of id in processed_q
    processed_q["video_duration"] = (
        processed_q["id"]
        .map(list_with_valid_videos.groupby("id")["audio-duration"].sum())
        .fillna(0)
    )

    processed_q.to_csv(os.path.join(path_to_save, "VISIA_QV_CRD.csv"), index=False)
    return processed_q
