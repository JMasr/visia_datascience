from pathlib import Path
from typing import Tuple

import librosa
import numpy as np
import ffmpeg
import whisper
from pydantic import BaseModel

from visia_science.responses.http import DataResponse, BasicResponse


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

    audio_stream: dict = None
    audio_data: np.ndarray = None

    video_stream: dict = None
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

    def load_multimedia(self) -> BasicResponse:
        validation_response = self._validate_media(self.path_to_raw_data)
        validation_response.log_response(module="Multimedia", action="LoadRawData")

        if validation_response.success:
            self.multimedia_metadata = validation_response.data
            try:
                if self.is_audio():
                    audio_object = AudioObject(self.path_to_raw_data)
                    self.audio_data = audio_object.audio_data
                    self.audio_stream = audio_object.audio_stream
                elif self.is_video():
                    video_object = VideoObject(self.path_to_raw_data)
                    self.video_data = video_object.get_video_data()
                    self.video_stream = video_object.get_video_stream()
            except Exception as e:
                validation_response = BasicResponse(
                    success=False,
                    status_code=500,
                    message=f"Error loading multimedia data: {e}",
                )
        return validation_response

    def transcribe(self, language="es") -> BasicResponse:
        if language == "en":
            model = whisper.load_model("base.en")
        else:
            model = whisper.load_model("large")
        try:
            result = model.transcribe(str(self.path_to_raw_data))
        except Exception as e:
            return BasicResponse(success=False, status_code=500, message=str(e))

        self.transcription = result["text"]
        return DataResponse(success=True, status_code=200, message="Transcription successful", data=result)
