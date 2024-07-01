from datetime import date
from enum import Enum

from pydantic import BaseModel


class VisiaGroup(Enum):
    GROUP_1: str = "G1-clinical S"
    GROUP_2: str = "G2-clinical non-S"
    GROUP_3: str = "G3-general"


class Education(Enum):
    PRIMARY: str = "primary"
    SECONDARY: str = "secondary"
    BACHELOR: str = "bachelor"
    PROFESSIONAL: str = "professional"


class Sex(Enum):
    FEMALE: str = "female"
    MALE: str = "male"


class Patient(BaseModel):
    id: str
    age: int
    sex: Sex
    birth_date: date
    education_level: Education
    clinical_group: VisiaGroup
    extra_info: dict = None

