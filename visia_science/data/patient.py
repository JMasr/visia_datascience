from datetime import date
from enum import Enum

import pandas as pd
from pandas import DataFrame
from pydantic import BaseModel

from visia_science.data.questionary import VisiaQuestionary


class VisiaGroup(Enum):
    GROUP_1: str = "G1-clínico-S"
    GROUP_2: str = "G2-clínico-N"
    GROUP_3: str = "G3-general"
    UNK: str = "UNK"


def return_visia_group(input_value: str):
    if input_value.lower() in VisiaGroup.GROUP_1.value.lower():
        return VisiaGroup.GROUP_1
    elif input_value.lower() in VisiaGroup.GROUP_2.value.lower():
        return VisiaGroup.GROUP_2
    elif input_value.lower() in VisiaGroup.GROUP_3.value.lower():
        return VisiaGroup.GROUP_3
    else:
        return VisiaGroup.UNK


class Education(Enum):
    PRIMARY: str = "1-Primaria"
    SECONDARY: str = "2-Secundaria"
    BACHELOR: str = "3-Bachiller-ciclo-medio-FP-básica"
    PROFESSIONAL: str = "4-Certificado-profesional"
    UNK: str = "UNK"


def return_education_level(input_value: str):
    if input_value.lower() in Education.PRIMARY.value.lower():
        return Education.PRIMARY
    elif input_value.lower() in Education.SECONDARY.value.lower():
        return Education.SECONDARY
    elif input_value.lower() in Education.BACHELOR.value.lower():
        return Education.BACHELOR
    elif input_value.lower() in Education.PROFESSIONAL.value.lower():
        return Education.PROFESSIONAL
    else:
        return Education.UNK


class Sex(Enum):
    FEMALE: str = "mujer"
    MALE: str = "hombre"
    UNK: str = "UNK"


def return_sex(input_value: str):
    if input_value.lower() in Sex.MALE.value.lower():
        return Sex.MALE
    elif input_value.lower() in Sex.FEMALE.value.lower():
        return Sex.FEMALE
    else:
        return Sex.UNK


class Patient(BaseModel):
    id: str
    age: int
    sex: Sex
    education_level: Education
    clinical_group: VisiaGroup
    diagnosis: str
    treatment: str
    saliva_sample: bool

    class Config:
        arbitrary_types_allowed = True