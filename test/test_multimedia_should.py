import os
from pathlib import Path

import pandas as pd
import pytest
import torchaudio

from test import ROOT_TEST_PATH
from visia_science.data.multimedia import Multimedia


class TestQuestionaryShould:
    @classmethod
    def setup_class(cls):
        # Create a temporary folder
        cls.temp_folder = os.path.join(ROOT_TEST_PATH, "temp_folder")
        os.makedirs(cls.temp_folder, exist_ok=True)

        # Download YESNO
        yesno_data = torchaudio.datasets.YESNO(cls.temp_folder, download=True)
        cls.yesno_data_path = yesno_data._path

        # Create a dummy temp file
        cls.temp_dummy_file = os.path.join(ROOT_TEST_PATH, cls.temp_folder, "temp_dump.dummy")
        Path(cls.temp_dummy_file).touch()

    @classmethod
    def teardown_class(cls):
        """Remove the temporary folder after testing manually to avoid quota limit of downloading YESNO dataset."""
        # shutil.rmtree(cls.temp_folder, ignore_errors=True)

    @pytest.fixture(scope="class")
    def yesno_file_paths(self, num_audios=2):
        """Fixture to get a list of YESNO audio file paths."""
        paths = []
        for filename in os.listdir(self.yesno_data_path):  # Iterate over the downloaded files
            if filename.endswith(".wav"):
                file_path = os.path.join(self.yesno_data_path, filename)
                paths.append(file_path)
        return paths[:num_audios]

    def test_is_multimedia_should(self, yesno_file_paths):  # Inject the fixture
        """Test if YESNO audio files are correctly identified as multimedia."""
        # Arrange and Act
        for file_path in yesno_file_paths:
            audio = Multimedia(
                path_to_raw_data=file_path,
                path_to_save_data=self.temp_folder)

            # Assert
            assert audio.is_multimedia()

    def test_is_audio_should(self, yesno_file_paths):  # Inject the fixture
        """Test if YESNO audio files are correctly identified as audio."""
        # Arrange and Act
        for file_path in yesno_file_paths:
            audio = Multimedia(
                path_to_raw_data=file_path,
                path_to_save_data=self.temp_folder)

            # Assert
            assert audio.is_multimedia()
            assert audio.is_audio()

    def test_load_a_multimedia_should(self, yesno_file_paths):  # Inject the fixture
        # Arrange
        for file_path in yesno_file_paths:
            audio = Multimedia(
                path_to_raw_data=file_path,
                path_to_save_data=self.temp_folder)

            # Act
            load_audio_response = audio.load_multimedia()

            # Assert
            assert load_audio_response.success is True

    def test_calculate_audio_quality(self, yesno_file_paths):
        # Arrange
        for file_path in yesno_file_paths:
            audio = Multimedia(
                path_to_raw_data=file_path,
                path_to_save_data=self.temp_folder)

            # Act
            audio_quality = audio.calculate_audio_quality()
            audio_quality.log_response(module="Multimedia", action="CalculateAudioQuality")

            # Assert
            assert audio_quality is not None

            audio_quality_data = audio_quality.data
            for key in audio_quality_data.keys():
                quality_parameter = audio_quality_data[key]
                assert quality_parameter is not None

    def test_transcribe_multimedia_should(self, yesno_file_paths):  # Inject the fixture
        # Arrange
        for file_path in yesno_file_paths:
            audio = Multimedia(
                path_to_raw_data=file_path,
                path_to_save_data=self.temp_folder)

            # Act
            transcribe_audio_response = audio.transcribe(language="en")

            # Assert
            assert transcribe_audio_response.success is True
            assert transcribe_audio_response.data is not None

            transcription = transcribe_audio_response.data.get("text")
            assert transcription is not None
            assert len(transcription) > 0

    def test_multimedia_as_questionary_should(self, yesno_file_paths):
        # Arrange
        for file_path in yesno_file_paths:
            audio = Multimedia(
                path_to_raw_data=file_path,
                path_to_save_data=self.temp_folder)
            audio.load_multimedia()
            audio.transcribe(language="en")
            audio.calculate_audio_quality()

            # Act
            audio_as_questionary_response = audio.get_metadata_as_dataframe()

            # Assert
            assert audio_as_questionary_response.success is True
            assert audio_as_questionary_response.data is not None
            assert isinstance(audio_as_questionary_response.data, pd.DataFrame)


if __name__ == "__main__":
    # Run all tests in the module
    pytest.main()
