from __future__ import annotations

from typing import Literal

from sqlmodel import Field, SQLModel


class AntiDengueRunRequest(SQLModel):
    dry_run: bool = Field(default=True)
    login_mode: Literal["auto", "manual", "remote_approve"] = Field(default="auto")
