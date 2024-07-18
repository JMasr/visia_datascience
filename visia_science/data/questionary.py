import os
from datetime import datetime
from pathlib import Path

import pandas as pd
from pydantic import BaseModel

from visia_science import app_logger
from visia_science.data import QuestionaryError
from visia_science.data.multimedia import Multimedia
from visia_science.files import load_json_as_dict, save_dict_as_json

SUPPORTED_QUESTIONARIES_EXTENSIONS = [".csv"]


class BaseQuestionary(BaseModel):
    path_to_raw_data: Path
    path_to_save_data: Path = None

    q_name: str
    column_with_id: str
    column_with_date: str
    columns_with_items: list
    columns_with_scores: list

    df_raw_data: pd.DataFrame = None
    df_post_processed_data: pd.DataFrame = None

    class Config:
        arbitrary_types_allowed = True

    def _make_post_processed_data(self) -> pd.DataFrame:
        # Create a copy of the raw data
        df_with_desired_columns = self.df_raw_data.copy(deep=False)

        # Remove columns that are not in columns_with_items or columns_with_scores
        columns_to_keep = (
                self.columns_with_items
                + self.columns_with_scores
                + [self.column_with_id, self.column_with_date]
        )
        columns_to_drop = [
            col for col in df_with_desired_columns.columns if col not in columns_to_keep
        ]
        df_with_desired_columns.drop(columns=columns_to_drop, inplace=True)
        return df_with_desired_columns

    def create_simple_post_processed_if_dont_exits(self):
        if self.df_post_processed_data is None or self.df_post_processed_data.empty:
            app_logger.info("Questionary - No post-processed data found. Creating it now.")
            self.df_post_processed_data = self._make_post_processed_data()

    def load_raw_data(self, loading_arguments: dict = None) -> pd.DataFrame:
        if loading_arguments is None:
            loading_arguments = {}

        try:
            if self.path_to_raw_data.suffix == ".csv":
                self.df_raw_data = pd.read_csv(self.path_to_raw_data, **loading_arguments)
            else:
                message = (
                    f"Unsupported extension {self.path_to_raw_data.suffix}."
                    f" Supported extensions are: {SUPPORTED_QUESTIONARIES_EXTENSIONS}"
                )
                app_logger.error(message)
                raise QuestionaryError(
                    message,
                    error_type="UnsupportedExtensionError",
                )
        except Exception as e:
            message = f"Error while reading file {self.path_to_raw_data}: {e}"
            app_logger.error(message)
            raise QuestionaryError(
                message,
                error_type="FileReadError",
            )

        return self.df_raw_data

    @staticmethod
    def _standardize_datetime(
            date_string: str, month_mapping: dict, date_time_format: str
    ) -> datetime:
        date_string_desire = date_string.replace(date_string[:3], month_mapping[date_string[:3]])
        date_object = datetime.strptime(date_string_desire, date_time_format)
        return date_object

    def _standardize_date_column_to_datetime(
            self, df_with_dates: pd.DataFrame, month_mapping: dict, date_time_format: str
    ) -> pd.DataFrame:
        for index, date_string in df_with_dates[self.column_with_date].items():
            date_object = self._standardize_datetime(date_string, month_mapping, date_time_format)
            df_with_dates.loc[index, self.column_with_date] = date_object

        df_with_dates[self.column_with_date] = pd.to_datetime(df_with_dates[self.column_with_date])
        return df_with_dates

    def _standardize_empty_values(self, df_with_empty_values: pd.DataFrame) -> pd.DataFrame:
        # Check columns with NaN values & NULL values
        columns_with_empty_rows = df_with_empty_values.columns[
            df_with_empty_values.isna().any()
        ].tolist()
        columns_with_empty_rows.append(
            df_with_empty_values.columns[df_with_empty_values.isnull().any()].tolist()
        )

        # Fill NaN values with -1 if the column is a score column and with "No answer" if it is an item column
        for column in columns_with_empty_rows:
            if column in self.columns_with_scores:
                df_with_empty_values[column] = df_with_empty_values[column].fillna(-1)
            else:
                df_with_empty_values[column] = df_with_empty_values[column].fillna("No answer")
        return df_with_empty_values

    def _remove_entries_from_a_given_date(
            self, df_with_q: pd.DataFrame, column_with_date: str = None, date: str = None
    ) -> pd.DataFrame:
        if column_with_date is None:
            column_with_date = self.column_with_date

        if date is None:
            app_logger.warning("Questionary - Date is required. No action will be taken")
            return df_with_q

        date_object = datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
        df_with_q = df_with_q[df_with_q[column_with_date] >= date_object]
        return df_with_q

    def remove_entries_that_dont_match_a_given_id_format(
            self, df_with_q: pd.DataFrame, column_with_id: str = None, id_formats: list = None
    ) -> pd.DataFrame:
        if column_with_id is None:
            column_with_id = self.column_with_id

        if id_formats is None:
            app_logger.warning("Questionary - Id formats are required. No action will be taken")
            return df_with_q

        final_visia_q = pd.DataFrame()
        for id_format in id_formats:
            visia_q_with_format_i = df_with_q[df_with_q[column_with_id].str.contains(id_format)]
            final_visia_q = pd.concat([final_visia_q, visia_q_with_format_i])
        return final_visia_q

    def add_number_of_word_of_a_columns_into_questionary(self, column_to_count: str) -> bool:
        self.create_simple_post_processed_if_dont_exits()
        if column_to_count not in self.df_post_processed_data.columns:
            app_logger.warning(
                f"Questionary - Column {column_to_count} not found in the questionary."
            )
            return False

        try:
            # Create a new column to store the number of words. Default value is 0
            name_of_new_column = f"Words - {column_to_count}"
            self.df_post_processed_data[name_of_new_column] = 0

            # Iterate over the column and count the number of words
            number_of_words = []
            for index, row in self.df_post_processed_data[column_to_count].items():
                number_of_words.append(len(row.split()))

            self.df_post_processed_data[name_of_new_column] = number_of_words

            # Cast the column as int
            self.df_post_processed_data[name_of_new_column] = self.df_post_processed_data[
                name_of_new_column
            ].astype(int)

        except RuntimeError as e:
            app_logger.error(f"Questionary - Error while adding the number of words: {e}")
            return False
        except Exception as e:
            app_logger.error(
                f"Questionary - Unexpected error while adding the number of words: {e}"
            )
            return False

        return True

    def add_questionary_name_to_all_columns(self) -> bool:
        self.create_simple_post_processed_if_dont_exits()

        try:
            for column in self.df_post_processed_data.columns:
                self.df_post_processed_data.rename(
                    columns={column: f"{self.q_name} - {column}"}, inplace=True
                )
        except Exception as e:
            app_logger.error(f"Questionary - Error while adding the questionary name: {e}")
            return False

        # Update the column names in the columns_with_items and columns_with_scores and column_with_date
        self.column_with_id = f"{self.q_name} - {self.column_with_id}"
        self.column_with_date = f"{self.q_name} - {self.column_with_date}"
        self.columns_with_items = [f"{self.q_name} - {col}" for col in self.columns_with_items]
        self.columns_with_scores = [f"{self.q_name} - {col}" for col in self.columns_with_scores]
        return True

    def transform_metadata(self, transformations: list = None) -> pd.DataFrame:
        self.create_simple_post_processed_if_dont_exits()

        if transformations is None:
            app_logger.warning(
                "Questionary - No transformations were provided. Data will be returned as is."
            )
        else:
            try:
                app_logger.info(
                    f"Questionary - Starting {len(transformations)} transformations over metadata"
                )

                df_ = self.df_post_processed_data.copy(deep=False)
                for index, transformation in enumerate(transformations):
                    df_ = transformation(df_)

                    self.app_logger.debug(
                        f"Questionary - Transformation #{index + 1} finished successfully"
                    )

                self.df_post_processed_data = df_
            except Exception as e:
                message = f"An runtime error occurred during the transformation process: {e}"
                raise QuestionaryError(message, error_type="RuntimeError")

        return self.df_post_processed_data

    def save_q_processed(self):
        self.create_simple_post_processed_if_dont_exits()

        path_to_save = os.path.join(self.path_to_save_data, f"{self.q_name}_processed.csv")
        self.df_post_processed_data.to_csv(path_to_save, index=False)

    def get_ids(self):
        return self.df_raw_data[self.column_with_id].unique()

    def get_scores(self):
        return self.df_raw_data[self.columns_with_scores]

    def get_items(self):
        return self.df_raw_data[self.columns_with_items]


class VisiaQuestionary(BaseQuestionary):
    ID_WITH_WRONG_FORMAT: dict = None

    DATE_TIME_FORMAT: str = "%b %d, %Y @ %I:%M %p"
    MONTH_MAPPING_ENG_SP: dict = {
        "Jan": "Ene",
        "Feb": "Feb",
        "Mar": "Mar",
        "Apr": "Abr",
        "May": "May",
        "Jun": "Jun",
        "Jul": "Jul",
        "Aug": "Ago",
        "Sep": "Sep",
        "Oct": "Oct",
        "Nov": "Nov",
        "Dec": "Dic",
    }
    MONTH_MAPPING_SP_ENG: dict = {v: k for k, v in MONTH_MAPPING_ENG_SP.items()}

    def validate(self, **kwargs):
        pass

    def extract_metadata(self):
        pass

    def _load_json_with_wrong_ids(self) -> dict:
        path_to_ids_with_wrong_format = os.path.join(
            os.path.dirname(self.path_to_raw_data),
            "visia_ids_with_wrong_format.json"
        )

        try:
            if self.ID_WITH_WRONG_FORMAT is None:
                dict_with_wrong_ids = load_json_as_dict(path_to_ids_with_wrong_format)
                app_logger.info("Questionary - Loaded the json with wrong IDs")
            else:
                dict_with_wrong_ids = self.ID_WITH_WRONG_FORMAT
        except Exception as e:
            app_logger.warning(f"Questionary - Error while creating the json with wrong IDs: {e}")
            app_logger.info("Questionary - Creating a empty json with wrong IDs")
            save_dict_as_json({}, path_to_ids_with_wrong_format)
            dict_with_wrong_ids = {}

        return dict_with_wrong_ids

    def _standardize_ids(self, df_with_ids, id_examples: str = "CUNQ-0"):
        self.ID_WITH_WRONG_FORMAT = self._load_json_with_wrong_ids()

        # Check if the IDs are in the correct format
        path_to_ids_with_wrong_format = os.path.join(
            os.path.dirname(self.path_to_raw_data),
            "visia_ids_with_wrong_format.json"
        )
        for index, raw_id in df_with_ids[self.column_with_id].items():
            if not raw_id.startswith(id_examples):
                # Check if the ID is already in the wrong format
                if raw_id in self.ID_WITH_WRONG_FORMAT:
                    correct_id = self.ID_WITH_WRONG_FORMAT[raw_id]
                else:
                    # Ask the user if the ID is correct
                    usr_answer = input(f"Is the ID {raw_id} correct? (y/n): ")
                    if usr_answer.lower() == "n":
                        correct_id = input("Please enter the correct ID: ")
                    else:
                        correct_id = raw_id

                    # Save the correct ID for future reference
                    self.ID_WITH_WRONG_FORMAT[raw_id] = correct_id
                    # Save the ID with the wrong format as a json file
                    save_dict_as_json(
                        self.ID_WITH_WRONG_FORMAT,
                        path_to_ids_with_wrong_format,
                    )

                df_with_ids.loc[index, self.column_with_id] = f"{correct_id}"
        return df_with_ids

    def clean(self):
        self.create_simple_post_processed_if_dont_exits()

        df_ = self.df_post_processed_data.copy(deep=False)

        df_ = self._standardize_date_column_to_datetime(
            df_, self.MONTH_MAPPING_SP_ENG, self.DATE_TIME_FORMAT
        )

        df_ = self._standardize_empty_values(df_)

        df_ = self._remove_entries_from_a_given_date(df_, date="2024-02-22 00:00:00")

        df_ = self.remove_entries_that_dont_match_a_given_id_format(
            df_, id_formats=["CUNQ-", "CHOX-"]
        )

        df_ = self._standardize_ids(df_)

        self.df_post_processed_data = df_

    def get_all_the_responses_of_one_patient(self, id_patient: str):
        return self.df_post_processed_data[
            self.df_post_processed_data[self.column_with_id] == id_patient
            ]

