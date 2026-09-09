"""Schemas Pydantic de Customer - request/response del formulario."""

from datetime import date
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

CustomerStatus = Literal["A", "S", "X"]


class CustomerEntrada(BaseModel):
    """Comun a alta y edicion - el formulario reenvia el registro completo."""

    document_number: str = Field(min_length=1, max_length=25)
    full_name: str = Field(min_length=1, max_length=180)
    email_address: str | None = Field(default=None, max_length=180)
    mobile_phone: str | None = Field(default=None, max_length=35)
    mailing_address: str | None = Field(default=None, max_length=240)
    birth_date: date | None = None
    customer_status: CustomerStatus = "A"

    @field_validator("document_number", "full_name")
    @classmethod
    def _not_blank_after_trim(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("must not be blank")
        return v

    @field_validator("email_address")
    @classmethod
    def _basic_email(cls, v: str | None) -> str | None:
        if v is None or v == "":
            return None
        if "@" not in v:
            raise ValueError("invalid email address")
        return v


class CustomerOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    customer_code: str
    document_number: str
    full_name: str
    email_address: str | None
    mobile_phone: str | None
    mailing_address: str | None
    birth_date: date | None
    customer_status: str
