import os
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path

import pandas as pd
from pydantic import BaseModel

from visia_science.data import QuestionaryError

SUPPORTED_QUESTIONARIES_EXTENSIONS = [".csv"]


class BaseQuestionary(ABC, BaseModel):
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

    def load_raw_data(self, loading_arguments: dict = None) -> pd.DataFrame:
        if loading_arguments is None:
            loading_arguments = {}

        try:
            if self.path_to_raw_data.suffix == ".csv":
                self.df_raw_data = pd.read_csv(self.path_to_raw_data, **loading_arguments)
            else:
                raise QuestionaryError(
                    f"Unsupported extension {self.path_to_raw_data.suffix}",
                    error_type="UnsupportedExtensionError",
                )
        except Exception as e:
            raise QuestionaryError(
                f"Error while reading file {self.path_to_raw_data}: {e}",
                error_type="FileReadError",
            )

        return self.df_raw_data

    def transform_metadata(self, transformations: list) -> pd.DataFrame:
        if self.df_raw_data.empty:
            message = "LocalDataset - Metadata is empty. Please, load a metadata first."
            self.QuestionaryError.error(message, error_type="EmptyMetadataError")
        if not transformations:
            message = "LocalDataset - No transformations were provided."
            self.app_logger.error(message)
            raise ValueError(message)
        else:
            try:
                self.app_logger.info(
                    f"LocalDataset - Starting {len(transformations)} transformations over metadata"
                )
                df_post_processed = self.raw_metadata.copy(deep=False)
                for index, transformation in enumerate(transformations):
                    df_post_processed = transformation(df_post_processed)
                    self.app_logger.debug(
                        f"LocalDataset - Transformation #{index + 1} finished successfully"
                    )

                self.df_post_processed_data = df_post_processed
            except Exception as e:
                message = f"An runtime error occurred during the transformation process: {e}"
                raise QuestionaryError(message, error_type="RuntimeError")

        return self.df_post_processed_data

    def save_q_processed(self):
        if self.df_post_processed_data is None:
            self.df_post_processed_data = self.df_raw_data

        path_to_save = os.path.join(self.path_to_save_data, f"{self.q_name}_processed.csv")
        self.df_post_processed_data.to_csv(path_to_save, index=False)

    def get_ids(self):
        return self.df_raw_data[self.column_with_id].unique()

    def get_scores(self):
        return self.df_raw_data[self.columns_with_scores]

    def get_items(self):
        return self.df_raw_data[self.columns_with_items]

    @abstractmethod
    def validate(self):
        pass

    @abstractmethod
    def extract_metadata(self):
        pass

    @abstractmethod
    def clean(self):
        pass


class VisiaQuestionary(BaseQuestionary, ABC):
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

    def validate(self):
        pass

    def extract_metadata(self):
        pass

    def clean(self):
        df_ = self.df_raw_data.copy()

        for index, date_string in df_[self.column_with_date].items():
            # Change month using the mapping
            date_string_eng = date_string.replace(
                date_string[:3], self.MONTH_MAPPING_SP_ENG[date_string[:3]]
            )
            date_object = datetime.strptime(date_string_eng, "%b %d, %Y @ %I:%M %p")

            # Update the date column with the new date
            df_.loc[index, self.column_with_date] = date_object

        df_[self.column_with_date] = pd.to_datetime(df_[self.column_with_date])

        # Check columns with NaN values
        columns_with_nan = df_.columns[df_.isna().any()].tolist()
        # Fill NaN values with -1 if the column is a score column and with "No answer" if it is an item column
        for column in columns_with_nan:
            if column in self.columns_with_scores:
                df_[column] = df_[column].fillna(-1)
            else:
                df_[column] = df_[column].fillna("No answer")

        self.df_raw_data = df_

    def process_using_functions(self, functions: list = lambda x: x):
        for function_to_applied in functions:
            self.df_raw_data = function_to_applied(self.df_raw_data)
        return self.df_raw_data
