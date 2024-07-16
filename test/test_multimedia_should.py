import os

from pathlib import Path

import pytest
import torch
import torchaudio

from test import ROOT_TEST_PATH
from visia_science.data.make_dataset import download_a_single_file_from_gdrive
from visia_science.data.multimedia import Multimedia


class TestQuestionaryShould:

    @classmethod
    def setup_class(cls):
        # Create a temporary folder
        cls.temp_folder = os.path.join(ROOT_TEST_PATH, "temp_folder")
        os.makedirs(cls.temp_folder, exist_ok=True)

        # Download YESNO
        yesno_data = torchaudio.datasets.YESNO(cls.temp_folder, download=True)
        cls.yesno_data = yesno_data._path

        # Create a dummy temp file
        cls.temp_dummy_file = os.path.join(ROOT_TEST_PATH, cls.temp_folder, "temp_dump.dummy")
        Path(cls.temp_dummy_file).touch()

        # Download a test video file
        media_valid_without_a_person = "valid_video_without_person.mp4"
        cls.path_to_media_valid_without_a_person = os.path.join(cls.temp_folder, media_valid_without_a_person)

        g_drive_id = "18tkPHyBncRmxK1gEumBQ-Xj_fg62W7Un"
        download_a_single_file_from_gdrive(g_drive_id, cls.path_to_media_valid_without_a_person)

        # Download a test wav file
        media_valid_audio = "valid_audio.wav"
        cls.path_to_media_valid_audio = os.path.join(cls.temp_folder, media_valid_audio)

        g_drive_id = "1SEHN2WzubO3-HoJ8X78oCYIBJg3dRzkl"
        download_a_single_file_from_gdrive(g_drive_id, cls.path_to_media_valid_audio)

        # Path to save processed data
        cls.path_to_save_data = os.path.join(cls.temp_folder, f"raw_data_processed")

        cls.audio = Multimedia(
            path_to_raw_data=cls.path_to_media_valid_audio,
            path_to_save_data=cls.path_to_save_data,
        )

        cls.video = Multimedia(
            path_to_raw_data=cls.path_to_media_valid_without_a_person,
            path_to_save_data=cls.path_to_save_data,
        )

        cls.dummy = Multimedia(
            path_to_raw_data=cls.temp_dummy_file,
            path_to_save_data=cls.path_to_save_data,
        )

    @classmethod
    def teardown_class(cls):
        """Remove the temporary folder after testing manually to avoid Google Drive API quota limit"""
        # shutil.rmtree(cls.temp_folder, ignore_errors=True)
        pass

    def test_is_a_multimedia_should(self):
        # Arrange
        audio = self.audio
        video = self.video
        dummy = self.dummy

        # Act
        is_audio_a_multimedia = audio.is_multimedia()
        is_video_a_multimedia = video.is_multimedia()
        is_dummy_a_multimedia = dummy.is_multimedia()

        # Assert
        assert is_audio_a_multimedia is True
        assert is_video_a_multimedia is True
        assert is_dummy_a_multimedia is False

    def test_is_an_audio_should(self):
        # Arrange
        audio = self.audio
        video = self.video
        dummy = self.dummy

        # Act
        is_audio_an_audio = audio.is_audio()
        is_video_an_audio = video.is_audio()
        is_dummy_an_audio = dummy.is_audio()

        # Assert
        assert is_audio_an_audio is True
        assert is_video_an_audio is False
        assert is_dummy_an_audio is False

    def test_is_a_video_should(self):
        # Arrange
        audio = self.audio
        video = self.video
        dummy = self.dummy

        # Act
        is_audio_a_video = audio.is_video()
        is_video_a_video = video.is_video()
        is_dummy_a_video = dummy.is_video()

        # Assert
        assert is_audio_a_video is False
        assert is_video_a_video is True
        assert is_dummy_a_video is False

    def test_load_a_multimedia_should(self):
        # Arrange
        audio = self.audio
        video = self.video
        dummy = self.dummy

        # Act
        load_audio = audio.load_multimedia()
        load_video = video.load_multimedia()
        load_dummy = dummy.load_multimedia()

        # Assert
        assert load_audio.success is True
        assert load_video.success is True
        assert load_dummy.success is False

    def test_transcribe_multimedia_should(self):
        # Arrange
        audio = self.audio
        audio.transcribe_multimedia(language="en")



if __name__ == "__main__":
    # Run all tests in the module
    pytest.main()
