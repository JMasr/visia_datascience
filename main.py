import os

import dotenv
import pandas as pd

from visia_science import app_logger
from visia_science.data.make_dataset import download_a_single_file_from_gdrive
from visia_science.data.patient import Patient, return_education_level, return_sex, return_visia_group
from visia_science.data.questionary import VisiaQuestionary
from visia_science.files import load_json_as_dict


def pipeline_get_visia_q(q_path: str, config_path: str, q_process_path: str) -> list:
    visia_metadata: dict = load_json_as_dict(config_path)
    visia_q_metadata = visia_metadata.get("VISIA_Q")

    # Download data
    questionaries = []
    for questionary in visia_q_metadata.keys():
        raw_data_url = visia_q_metadata[questionary]["q_url"]
        raw_data_path = os.path.join(q_path, questionary)
        is_file_download = download_a_single_file_from_gdrive(
            gdrive_url=raw_data_url, output_path=raw_data_path
        )

        if not is_file_download:
            raise ValueError(f"Failed to download {questionary} from {raw_data_url}")

        visia_q = VisiaQuestionary(
            path_to_raw_data=raw_data_path,
            path_to_save_data=q_process_path,
            q_name=visia_q_metadata[questionary]["q_name"],
            column_with_id=visia_q_metadata[questionary]["column_with_id"],
            column_with_date=visia_q_metadata[questionary]["column_with_date"],
            columns_with_items=visia_q_metadata[questionary]["columns_with_items"],
            columns_with_scores=visia_q_metadata[questionary]["columns_with_scores"],
        )
        visia_q.load_raw_data()
        visia_q.save_q_processed()
        questionaries.append(visia_q)
    return questionaries


def pipeline_clean_visia_q(visia_q: list) -> dict:
    cleaned_visia_q = {}

    questionary: VisiaQuestionary
    for questionary in visia_q:
        questionary.clean()
        questionary.save_q_processed()

        cleaned_visia_q[questionary.q_name] = questionary

    return cleaned_visia_q


def pipeline_get_visia_patients(visia_q_with_patients: VisiaQuestionary) -> dict:
    visia_output_patients: dict = {}

    df_patient: pd.DataFrame = visia_q_with_patients.df_post_processed_data
    # Create a new column with the age of the patient from the birth_date
    df_patient["Edad"] = 0
    df_patient["Edad"] = df_patient["Edad"].astype(int)

    visia_q_patient: VisiaQuestionary
    for index, row in df_patient.iterrows():
        # Calculate age from birth_date
        birth_date = row["Fecha de nacimiento"]
        birth_date_obj = pd.to_datetime(birth_date, dayfirst=True)
        age_obj = pd.Timestamp.now() - birth_date_obj
        age = int(age_obj.days // 365)

        lv_education = return_education_level(row["Nivel educativo"])
        clinical_group = return_visia_group(row["Grupo clínico"])
        biological_sex = return_sex(row["Sexo (biológico)"])
        saliva_sample = True if row["Checkbox"] == "saliva" else False

        df_patient.loc[index, "Edad"] = age
        df_patient.loc[index, "Nivel educativo"] = lv_education
        df_patient.loc[index, "Grupo clínico"] = clinical_group
        df_patient.loc[index, "Sexo (biológico)"] = biological_sex
        df_patient.loc[index, "Checkbox"] = saliva_sample

        visia_patient = Patient(
            id=row[visia_q_with_patients.column_with_id],
            age=age,
            sex=biological_sex,
            education_level=lv_education,
            clinical_group=clinical_group,
            diagnosis=row["Diagnóstico"],
            treatment=row["Tratamiento"],
            saliva_sample=saliva_sample,
        )
        visia_output_patients[visia_patient.id] = visia_patient

    visia_q_with_patients.df_post_processed_data = df_patient
    visia_q_with_patients.save_q_processed()
    return visia_output_patients


if __name__ == "__main__":
    project_dir = os.path.join(os.path.dirname(__file__))
    dotenv_path = os.path.join(project_dir, ".env")
    dotenv.load_dotenv(dotenv_path)

    EXP_NAME = os.getenv("EXP_NAME")
    CONFIG_PATH = os.getenv("CONFIG_PATH")
    VISIA_Q_PATH = os.getenv("VISIA_Q_PATH")
    VISIA_Q_PROCESS_PATH = os.getenv("VISIA_Q_PROCESS_PATH")

    app_logger.info(f"Starting pipeline for {EXP_NAME}")

    # Get questionaries
    visia_questionaries: list = pipeline_get_visia_q(
        q_path=VISIA_Q_PATH, config_path=CONFIG_PATH, q_process_path=VISIA_Q_PROCESS_PATH
    )
    # Clean questionaries
    visia_questionaries: dict = pipeline_clean_visia_q(visia_questionaries)

    # Add number of words to the questionary PREGUNTAS
    visia_q_preguntas: VisiaQuestionary = visia_questionaries.get("PREGUNTAS")
    visia_q_preguntas.add_number_of_word_of_a_columns_into_questionary("¿Puedes decirnos cómo eres? ¿Cómo te ves a tí mismo/a?")
    visia_q_preguntas.add_number_of_word_of_a_columns_into_questionary("¿Cómo crees que te ven los demás?")

    visia_questionaries["PREGUNTAS"] = visia_q_preguntas
    visia_q_preguntas.save_q_processed()

    visia_q_patients: VisiaQuestionary = visia_questionaries.pop("VSC")
    visia_all_patients: dict = pipeline_get_visia_patients(visia_q_patients)

    visia_patient_with_all_responses = []
    for patient_id, patient in visia_all_patients.items():
        # Patient data as DataFrame
        df_patient = pd.DataFrame(patient.model_dump(), index=[0])

        # Get all the responses of the patient as DataFrame
        visia_ = []
        for visia_q_name, visia_q in visia_questionaries.items():
            visia_q_response_of_patient: pd.DataFrame = visia_q.get_all_the_responses_of_one_patient(patient_id)

            # Drop columns that are not needed id
            visia_q_response_of_patient = visia_q_response_of_patient.drop(columns=[visia_q.column_with_id])

            # Add q-name to each column
            visia_q_response_of_patient.columns = [f"{visia_q_name} - {column}" for column in visia_q_response_of_patient.columns]

            # Add to each row the df_patient
            visia_q_response_of_patient.reset_index(drop=True, inplace=True)
            visia_q_response_of_patient = pd.merge(df_patient, visia_q_response_of_patient, left_index=True, right_index=True)
            visia_.append(visia_q_response_of_patient)

        # Concatenate all the responses of the patient into a single DataFrame
        visia_df = pd.concat(visia_, axis=1)
        visia_df.reset_index(drop=True, inplace=True)
        visia_patient_with_all_responses.append(visia_df)

    visia_all_patients_df = pd.concat(visia_patient_with_all_responses, ignore_index=True)

    # Remove all the duplicated columns
    visia_all_patients_df = visia_all_patients_df.loc[:,~visia_all_patients_df.columns.duplicated()]

    # Replace all the NaN values in a string column with "No answer"
    for column in visia_all_patients_df.columns:
        if visia_all_patients_df[column].dtype in ["object", "string"]:
            visia_all_patients_df[column] = visia_all_patients_df[column].fillna("No answer")
        elif visia_all_patients_df[column].dtype in ["int", "float"]:
            visia_all_patients_df[column] = visia_all_patients_df[column].fillna(0)
        else:
            visia_all_patients_df[column] = visia_all_patients_df[column].fillna("UNK")

    visia_all_patients_df.to_csv(os.path.join(VISIA_Q_PROCESS_PATH, "VISIA_CRDs.csv"), index=False)
    app_logger.info(f"Pipeline finished for {EXP_NAME}")