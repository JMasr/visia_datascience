from enum import Enum

from pydantic import BaseModel


class VisiaGroup(Enum):
    GROUP_1: str = "G1-clínico-S"
    GROUP_2: str = "G2-clínico-N"
    GROUP_3: str = "G3-general"
    UNK: str = "UNK"


def return_visia_group(input_value: str):
    if input_value.lower() in VisiaGroup.GROUP_1.value.lower():
        return VisiaGroup.GROUP_1.value
    elif input_value.lower() in VisiaGroup.GROUP_2.value.lower():
        return VisiaGroup.GROUP_2.value
    elif input_value.lower() in VisiaGroup.GROUP_3.value.lower():
        return VisiaGroup.GROUP_3.value
    else:
        return VisiaGroup.UNK.value


class Education(Enum):
    PRIMARY: str = "1-Primaria"
    SECONDARY: str = "2-Secundaria"
    BACHELOR: str = "3-Bachiller-ciclo-medio-FP-básica"
    PROFESSIONAL: str = "4-Certificado-profesional"
    UNK: str = "UNK"


def return_education_level(input_value: str):
    if input_value.lower() in Education.PRIMARY.value.lower():
        return Education.PRIMARY.value.upper()
    elif input_value.lower() in Education.SECONDARY.value.lower():
        return Education.SECONDARY.value.upper()
    elif input_value.lower() in Education.BACHELOR.value.lower():
        return Education.BACHELOR.value.upper()
    elif input_value.lower() in Education.PROFESSIONAL.value.lower():
        return Education.PROFESSIONAL.value.upper()
    else:
        return Education.UNK.value.upper()


class Sex(Enum):
    FEMALE: str = "mujer"
    MALE: str = "hombre"
    UNK: str = "UNK"


def return_sex(input_value: str):
    if input_value.lower() in Sex.MALE.value.lower():
        return Sex.MALE.value.upper()
    elif input_value.lower() in Sex.FEMALE.value.lower():
        return Sex.FEMALE.value.upper()
    else:
        return Sex.UNK.value.upper()


class Patient(BaseModel):
    id: str
    age: int
    sex: str
    education_level: str
    clinical_group: str
    city: str
    diagnosis: str
    treatment: str
    saliva_sample: bool

    class Config:
        arbitrary_types_allowed = True
