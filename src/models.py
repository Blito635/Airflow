from pydantic import BaseModel, Field, validator
from datetime import datetime
from typing import Optional

class TransactionSchema(BaseModel):
    # Definimos los campos esperados
    transaction_id: str
    source_system_id: str
    amount: float = Field(..., gt=0)  # El monto debe ser mayor a 0
    currency: str = Field(..., min_length=3, max_length=3) # Ej: USD, COP
    transaction_date: datetime
    category: Optional[str] = "General"

    # Ejemplo de validación extra: asegurarnos que la fecha no sea futura
    @validator('transaction_date')
    def date_must_be_past(cls, v):
        if v > datetime.now():
            raise ValueError('La fecha de transacción no puede ser futura')
        return v

    class Config:
        # Permite que el modelo funcione bien con objetos de SQLAlchemy si es necesario
        orm_mode = True