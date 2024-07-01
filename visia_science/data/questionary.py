import os
from abc import ABC, abstractmethod
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

    def is_raw_data_file_valid(self) -> None:
        if not self.path_to_raw_data.exists():
            raise QuestionaryError(
                f"File '{self.path_to_raw_data}" f"' not found", error_type="FileNotFoundError"
            )
        elif not self.path_to_raw_data.is_file():
            raise QuestionaryError(
                f"'{self.path_to_raw_data}' is not a file", error_type="NotAFileError"
            )
        elif self.path_to_raw_data.suffix not in SUPPORTED_QUESTIONARIES_EXTENSIONS:
            raise QuestionaryError(
                f"File '{self.path_to_raw_data}' has unsupported extension",
                error_type="UnsupportedExtensionError",
            )

    def load_raw_data(self, loading_arguments: dict = None) -> pd.DataFrame:
        if loading_arguments is None:
            loading_arguments = {}

        self.is_raw_data_file_valid()

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


class VisiaQuestionary(BaseQuestionary):
    def validate(self):
        pass

    def extract_metadata(self):
        pass
