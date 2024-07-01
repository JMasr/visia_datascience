import os
import shutil

from faker import Faker
import numpy as np
import pandas as pd
import pytest

from visia_science.data import QuestionaryError
from visia_science.data.questionary import BaseQuestionary
from test import ROOT_TEST_PATH


def mock_a_questionary_as_dataframe(number_of_rows: int = 100) -> pd.DataFrame:
    fake = Faker()
    ids = [fake.uuid4() for _ in range(number_of_rows)]
    labels = [fake.boolean() for _ in range(number_of_rows)]
    scores = [fake.random_int(0, 100) for _ in range(number_of_rows)]
    dates = [fake.date_this_decade() for _ in range(number_of_rows)]
    data = np.random.randint(0, 100, size=number_of_rows)
    items = [fake.random_element(["yes", "no"]) for _ in range(number_of_rows)]
    genders = [fake.random_element(["Male", "Female"]) for _ in range(number_of_rows)]
    return pd.DataFrame(
        {"id": ids, "label": labels, "data": data, "gender": genders, "date": dates, "items": items, "scores": scores})


class TestQuestionaryShould:

    @classmethod
    def setup_class(cls):
        # Create a temporary folder
        cls.temp_folder = ROOT_TEST_PATH / "temp_folder"
        os.mkdir(cls.temp_folder)
        # Create a temporary CSV file for testing
        cls.temp_file = ROOT_TEST_PATH / "temp.csv"
        cls.mock_questionary = mock_a_questionary_as_dataframe()
        cls.mock_questionary.to_csv(cls.temp_file, index=False)

    @classmethod
    def teardown_class(cls):
        # Remove the temporary CSV file after testing
        cls.temp_file.unlink()
        # Remove the temporary folder after testing
        shutil.rmtree(cls.temp_folder, ignore_errors=True)

    def test_valid_init_should(self):
        # Arrange
        testing_q = BaseQuestionary(path_to_raw_data=self.temp_file,
                                    path_to_save_data=self.temp_folder,
                                    column_with_id="id",
                                    column_with_date="date",
                                    columns_with_items=["items"],
                                    columns_with_scores=["scores"])
        # Act
        testing_q._check_raw_data_file()

        # Assert
        assert isinstance(testing_q, BaseQuestionary)

    @pytest.mark.parametrize("path_to_raw_data", [ROOT_TEST_PATH, "non_existent_file.csv"])
    def test_invalid_init_should(self, path_to_raw_data: str):
        # Arrange
        testing_q = BaseQuestionary(path_to_raw_data=path_to_raw_data,
                                    path_to_save_data=self.temp_folder,
                                    column_with_id="id",
                                    column_with_date="date",
                                    columns_with_items=["items"],
                                    columns_with_scores=["scores"])
        # Act and Assert
        with pytest.raises(QuestionaryError):
            testing_q._check_raw_data_file()

    def test_valid_load_raw_data_should(self):
        # Arrange
        testing_q = BaseQuestionary(path_to_raw_data=self.temp_file,
                                    path_to_save_data=self.temp_folder,
                                    column_with_id="id",
                                    column_with_date="date",
                                    columns_with_items=["items"],
                                    columns_with_scores=["scores"])
        # Act
        testing_q.load_raw_data()

        # Assert
        assert isinstance(testing_q.df_raw_data, pd.DataFrame)
        assert not testing_q.df_raw_data.empty
        assert len(testing_q.df_raw_data) == len(self.mock_questionary)
        assert all(testing_q.df_raw_data.columns == self.mock_questionary.columns)


if __name__ == "__main__":
    # Run all tests in the module
    pytest.main()
