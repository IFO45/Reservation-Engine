from uuid import UUID
from datetime import datetime
from pydantic import BaseModel, Field

class HoldRequest(BaseModel):
    event_id: UUID
    user_id: UUID
    seats: int = Field(gt=0, description="Seats requested must be greater than zero")

class HoldResponse(BaseModel):
    reservation_id: UUID
    expires_at: datetime
    message: str

class ConfirmRequest(BaseModel):
    reservation_id: UUID
    user_id: UUID
    amount_cents: int = Field(ge=0)

class ConfirmResponse(BaseModel):
    order_id: UUID
    reservation_id: UUID
    seats: int
    status: str