from pathlib import Path
from typing import Tuple

import librosa
import numpy as np
import ffmpeg
import pandas as pd
import torch
import whisper
from pydantic import BaseModel

from visia_science import app_logger
from visia_science.responses.http import DataResponse, BasicResponse, DataFrameResponse


def preprocess_audio_ffmpeg_stream(stream_with_audio_metadata: dict) -> dict:
    processed_audio_stream = {
        "codec_name": str(stream_with_audio_metadata["codec_name"]),
        "codec_long_name": str(stream_with_audio_metadata["codec_long_name"]),
        "sample_rate": int(stream_with_audio_metadata["sample_rate"]),
        "channels": int(stream_with_audio_metadata["channels"]),
        "bits_per_sample": int(stream_with_audio_metadata["bits_per_sample"]),
        "initial_padding": int(stream_with_audio_metadata["initial_padding"]),
        "duration": float(stream_with_audio_metadata["duration"]),
        "bit_rate": int(stream_with_audio_metadata["bit_rate"]),
    }
    # Add "audio-" prefix to all keys
    processed_audio_stream = {f"audio-{k}": v for k, v in processed_audio_stream.items()}

    return processed_audio_stream


def preprocess_video_ffmpeg_stream(stream_with_video_metadata: dict) -> dict:
    processed_video_stream = {
        "codec_name": str(stream_with_video_metadata["codec_name"]),
        "codec_long_name": str(stream_with_video_metadata["codec_long_name"]),
        "codec_tag_string": str(stream_with_video_metadata["codec_tag_string"]),
        "width": int(stream_with_video_metadata["width"]),
        "height": int(stream_with_video_metadata["height"]),
        "pix_fmt": str(stream_with_video_metadata["pix_fmt"]),
        "color_range": str(stream_with_video_metadata["color_range"]),
        "color_space": str(stream_with_video_metadata["color_space"]),
        "color_transfer": str(stream_with_video_metadata["color_transfer"]),
        "color_primaries": str(stream_with_video_metadata["color_primaries"]),
        "chroma_location": str(stream_with_video_metadata["chroma_location"]),
        "field_order": str(stream_with_video_metadata["field_order"]),
        "nal_length_size": int(stream_with_video_metadata["nal_length_size"]),
        "avg_frame_rate": str(stream_with_video_metadata["avg_frame_rate"]),
        "duration": float(stream_with_video_metadata["duration"]),
        "bit_rate": int(stream_with_video_metadata["bit_rate"]),
        "bits_per_raw_sample": int(stream_with_video_metadata["bits_per_raw_sample"]),
        "nb_frames": int(stream_with_video_metadata["nb_frames"]),
        "extradata_size": int(stream_with_video_metadata["extradata_size"]),
    }
    # Add "video-" prefix to all keys
    processed_video_stream = {f"video-{k}": v for k, v in processed_video_stream.items()}

    return processed_video_stream


class MediaObject:
    def __init__(self, file_path, metadata=None):
        self.file_path = file_path

        if metadata is None:
            self.metadata = self.get_metadata()
        else:
            self.metadata = metadata

    def get_metadata(self):
        try:
            probe = ffmpeg.probe(self.file_path)
            return probe
        except ffmpeg.Error as e:
            raise RuntimeError(f"Error probing file: {e.stderr.decode()}")


class AudioObject(MediaObject):
    def __init__(self, file_path):
        super().__init__(file_path)

        self.audio_stream = self.get_audio_stream()
        self.audio_data, self.sample_rate = self.get_audio_data()

    def get_audio_stream(self) -> dict:
        audio_stream = {}
        for stream in self.metadata["streams"]:
            if stream["codec_type"] == "audio":
                audio_stream = stream

        audio_stream = preprocess_audio_ffmpeg_stream(audio_stream)
        return audio_stream

    def get_audio_data(self) -> Tuple[np.ndarray, int]:
        try:
            audio_array, sample_rate = librosa.load(self.file_path, sr=None)
            return audio_array, sample_rate
        except Exception as e:
            raise RuntimeError(f"Error converting audio to ndarray: {e}")


class VideoObject(MediaObject):
    def __init__(self, file_path):
        super().__init__(file_path)
        self.video_stream = self.get_video_stream()
        self.video_data = self.get_video_data()

    def get_video_stream(self) -> dict:
        video_stream = {}
        for stream in self.metadata["streams"]:
            if stream["codec_type"] == "video":
                video_stream = stream

        return video_stream

    def get_video_data(self) -> np.ndarray:
        try:
            out, _ = (
                ffmpeg.input(self.file_path)
                .output("pipe:", format="rawvideo", pix_fmt="rgb24")
                .run(capture_stdout=True, capture_stderr=True)
            )
            width = int(self.video_stream["width"])
            height = int(self.video_stream["height"])
            video_array = np.frombuffer(out, np.uint8).reshape([-1, height, width, 3])
            return video_array
        except ffmpeg.Error as e:
            raise RuntimeError(f"Error converting video to ndarray: {e}")


class Multimedia(BaseModel):
    path_to_raw_data: Path
    path_to_save_data: Path = None

    multimedia_metadata: dict = None

    audio_data: np.ndarray = None
    threshold_snr: int = 95
    hop_size_s: float = 0.010
    window_size_s: float = 0.025

    video_data: np.ndarray = None

    transcription: str = None

    class Config:
        arbitrary_types_allowed = True

    @staticmethod
    def _validate_media(file_path) -> BasicResponse:
        try:
            probe = ffmpeg.probe(file_path)
            probe_score = probe.get("format", {}).get("probe_score", 0)

            if probe_score > 80 and probe:
                status_code = 200
                message = f"Operation successful with ffmpeg confidence of {probe_score}%"
            else:
                status_code = 204
                message = f"Confidence of ffmpeg probe is below 80%. Review the file: {file_path}"

            response = DataResponse(
                success=True, status_code=status_code, message=message, data=probe
            )
        except ffmpeg.Error as e:
            response = BasicResponse(success=False, status_code=500, message=str(e))

        return response

    def is_multimedia(self) -> bool:
        validation_response = self._validate_media(self.path_to_raw_data)
        validation_response.log_response(module="Multimedia", action="IsMultimedia")
        return validation_response.success

    def is_audio(self) -> bool:
        validation_response = self._validate_media(self.path_to_raw_data)
        validation_response.log_response(module="Multimedia", action="IsVideo")

        if not validation_response.success:
            return False
        else:
            validation_data = validation_response.data
            return validation_data.get("streams")[0].get("codec_type") == "audio"

    def is_video(self) -> bool:
        validation_response = self._validate_media(self.path_to_raw_data)
        validation_response.log_response(module="Multimedia", action="IsVideo")

        if not validation_response.success:
            return False
        else:
            validation_data = validation_response.data
            return validation_data.get("streams")[0].get("codec_type") == "video"

    def _standardize_multimedia_metadata(
            self, input_metadata: dict,
    ) -> dict:  # Assuming a unique stream for audio and video
        file_id = Path(input_metadata["format"]["filename"]).stem
        # Standardize metadata using ffmpeg format info
        multimedia_metadata = {
            "file_id": file_id,
            "id": file_id.split("_")[0],
            "file_path": input_metadata["format"]["filename"],
            "date_created": input_metadata["format"].get("tags", {}).get("creation_time", None),
            "format_name": input_metadata["format"]["format_name"],
            "size(bytes)": input_metadata["format"]["size"],
            "ffmpeg_confidence": input_metadata["format"]["probe_score"],
        }

        # Standardize audio and video streams metadata
        for stream_unk in input_metadata["streams"]:
            if stream_unk["codec_type"] == "audio":
                stream_processed = preprocess_audio_ffmpeg_stream(stream_unk)
                multimedia_metadata.update(stream_processed)
            elif stream_unk["codec_type"] == "video":
                stream_processed = preprocess_video_ffmpeg_stream(stream_unk)
                multimedia_metadata.update(stream_processed)
            else:
                message = (f"No audio or video stream found in metadata."
                           f" Skipping stream {self.path_to_raw_data} file")
                app_logger.error(message)

        return multimedia_metadata

    def load_multimedia(self) -> BasicResponse:
        validation_response = self._validate_media(self.path_to_raw_data)
        validation_response.log_response(module="Multimedia", action="LoadRawData")

        if validation_response.success:
            try:
                self.multimedia_metadata = self._standardize_multimedia_metadata(
                    validation_response.data
                )

                if self.is_multimedia():
                    audio_object = AudioObject(self.path_to_raw_data)
                    self.audio_data = audio_object.audio_data

                if self.is_video():
                    video_object = VideoObject(self.path_to_raw_data)
                    self.video_data = video_object.video_data

            except Exception as e:
                validation_response = BasicResponse(
                    success=False,
                    status_code=500,
                    message=f"Error loading multimedia data: {e}",
                )

        return validation_response

    def _estimate_snr_librosa(self) -> float:
        """Estimates SNR of an audio file using librosa."""
        if self.audio_data is None:
            self.load_multimedia()

        # Short-Time Energy (STE) Calculation
        sample_rate = self.multimedia_metadata["audio-sample_rate"]
        hop_length = int(sample_rate * self.hop_size_s)
        frame_length = int(sample_rate * self.window_size_s)
        energy = librosa.feature.rms(
            y=self.audio_data, frame_length=frame_length, hop_length=hop_length
        )

        # Thresholding for Signal vs. Noise
        threshold = np.percentile(
            energy, self.threshold_snr
        )  # Adjust percentile based on your audio characteristics
        signal_frames = energy > threshold
        noise_frames = energy <= threshold

        # Estimate SNR in dB
        signal_energy = np.mean(energy[signal_frames])
        noise_energy = np.mean(energy[noise_frames])
        snr = 10 * np.log10(signal_energy / noise_energy)

        return snr

    def _estimate_zero_crossings(self) -> np.ndarray:
        if self.audio_data is None:
            self.load_multimedia()

        audio_as_ndarray = self.audio_data
        zero_crossings = librosa.feature.zero_crossing_rate(audio_as_ndarray)

        return zero_crossings

    def calculate_audio_quality(self) -> BasicResponse:
        if self.audio_data is None:
            self.load_multimedia()

        # Calculate SNR
        try:
            snr = self._estimate_snr_librosa()

            dict_audio_quality = {"audio-SNR(dB)": snr}
        except Exception as e:
            dict_audio_quality = {"audio-SNR(dB)": None}

            app_logger.error(f"Error calculating SNR: {e}")

        # Calculate zero crossings
        try:
            zero_crossings = self._estimate_zero_crossings()

            dict_audio_quality["audio-ZCR_max "] = np.max(zero_crossings)
            dict_audio_quality["audio-ZCR_min"] = np.min(zero_crossings)
            dict_audio_quality["audio-ZCR_avg"] = np.mean(zero_crossings)
        except Exception as e:
            app_logger.error(f"Error calculating zero crossings: {e}")
            dict_audio_quality["audio-ZCR"] = None

        # Updata metadata only if audio quality is calculated
        self.multimedia_metadata.update(dict_audio_quality)

        return DataResponse(
            success=True,
            status_code=200,
            message="Audio quality calculated",
            data=dict_audio_quality,
        )

    def transcribe(self, language="es") -> BasicResponse:
        if self.audio_data is None:
            self.load_multimedia()

        # Check for GPU availability
        device = "cuda" if torch.cuda.is_available() else "cpu"

        if language == "en":
            model = whisper.load_model("base.en", device=device)
        else:
            model = whisper.load_model("large", device=device)
        try:
            result = model.transcribe(str(self.path_to_raw_data))
        except Exception as e:
            return BasicResponse(success=False, status_code=500, message=str(e))

        self.multimedia_metadata["transcription"] = result["text"]
        self.multimedia_metadata["transcription_language"] = result["language"]
        return DataResponse(
            success=True, status_code=200, message="Transcription successful", data=result
        )

    def set_multimedia_speaker(self, speaker: str) -> BasicResponse:
        try:
            self.multimedia_metadata["speaker_id"] = speaker
            response = BasicResponse(
                success=True, status_code=200, message=f"Speaker set to {speaker}"
            )
        except Exception as e:
            app_logger.error(f"Error setting speaker: {e}")
            response = BasicResponse(success=False, status_code=500, message=str(e))

        return response

    def _calculate_all_possible_metadata(self):
        try:
            # Get all possible metadata
            if self.multimedia_metadata is None:
                self.load_multimedia()
            if self.multimedia_metadata.get("transcription", False):
                self.transcribe()
            if self.multimedia_metadata.get("audio-SNR(dB)", False):
                self.calculate_audio_quality()
            app_logger.info("All possible metadata calculated")
        except Exception as e:
            app_logger.error(f"Error calculating all possible metadata: {e}")
            return False

        return True

    def get_metadata(self) -> dict:
        self._calculate_all_possible_metadata()
        return self.multimedia_metadata

    def get_metadata_as_dataframe(self) -> BasicResponse:
        self._calculate_all_possible_metadata()

        try:
            df_multimedia = pd.DataFrame(self.multimedia_metadata, index=[0])
            response = DataFrameResponse(
                success=True,
                status_code=200,
                message="Multimedia metadata extracted",
                data=df_multimedia,
            )
        except Exception as e:
            response = BasicResponse(success=False, status_code=500, message=str(e))

        return response
