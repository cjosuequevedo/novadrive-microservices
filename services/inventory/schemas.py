"""Schemas Pydantic de Inventory - request/response del formulario.

`chassis_number` valida el charset real del estandar VIN (17
caracteres, excluye I/O/Q) desde el primer momento - a diferencia de
Andes, que dejo esa validacion solo en Silver y documento el hueco
como deuda tecnica (ver el propio CLAUDE.md de Andes, "Hueco de
validacion de VIN entre capas"). Como NovaDrive no tiene Silver,
dejar pasar un VIN con una letra I/O/Q nunca se detectaria rio abajo -
por eso se cierra aca, no se hereda el mismo hueco."""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from common.schemas.validators import NonNegativeAmount

AvailabilityCode = Literal["AVL", "HOLD", "SOLD"]

_VIN_CHARSET = set("ABCDEFGHJKLMNPRSTUVWXYZ0123456789")  # sin I, O, Q


class InventoryEntrada(BaseModel):
    """Comun a alta y edicion - el formulario reenvia el registro completo."""

    # `model_name`/`model_year` son nombres de campo legitimos del
    # contrato (seccion 7 del PDF), no relacionados con los metodos
    # `model_*` de Pydantic - se desactiva el namespace protegido para
    # que no emita un warning en cada arranque.
    model_config = ConfigDict(protected_namespaces=())

    branch_code: str = Field(min_length=1, max_length=8)
    chassis_number: str = Field(min_length=17, max_length=17)
    brand_name: str = Field(min_length=1, max_length=60)
    model_name: str = Field(min_length=1, max_length=100)
    model_year: int = Field(ge=1900, le=2100)
    exterior_colour: str | None = Field(default=None, max_length=50)
    list_amount: NonNegativeAmount
    availability_code: AvailabilityCode = "AVL"

    @field_validator("branch_code", "brand_name", "model_name")
    @classmethod
    def _not_blank_after_trim(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("must not be blank")
        return v

    @field_validator("chassis_number")
    @classmethod
    def _valid_vin_charset(cls, v: str) -> str:
        v = v.strip().upper()
        invalid_chars = set(v) - _VIN_CHARSET
        if invalid_chars:
            raise ValueError(
                f"invalid VIN characters {sorted(invalid_chars)} - VIN never uses I, O or Q"
            )
        return v


class InventoryOut(BaseModel):
    model_config = ConfigDict(from_attributes=True, protected_namespaces=())

    chassis_number: str
    branch_code: str
    brand_name: str
    model_name: str
    model_year: int
    exterior_colour: str | None
    list_amount: NonNegativeAmount
    availability_code: str


class BranchOption(BaseModel):
    """Solo lectura, para poblar el dropdown de Branch - nunca un campo
    de texto libre para un ID que el usuario tendria que adivinar."""

    model_config = ConfigDict(from_attributes=True)

    branch_code: str
    display_name: str
