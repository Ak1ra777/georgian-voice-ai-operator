from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field


class IspTicketPayload(BaseModel):
    session_id: str
    ticket_type: str
    service_type: str | None = None
    subscriber_identifier: str | None = None
    customer_context: dict[str, Any] = Field(default_factory=dict)
    callback_number: str | None = None
    no_action_reason: str | None = None
    caller_uri: str | None = None
    caller_number: str | None = None
    contact_phone: str | None = None
    customer_name: str | None = None
    service_address: str | None = None
    handoff_summary: str = ""


def save_isp_ticket_payload(
    ticket: IspTicketPayload,
    *,
    out_dir: Path | str | None = None,
) -> Path:
    if out_dir is None:
        target_dir = Path("outputs/calls") / ticket.session_id
        out_path = target_dir / "ticket_payload.json"
        tmp_path = target_dir / "ticket_payload.json.tmp"
    else:
        target_dir = Path(out_dir)
        out_path = target_dir / f"{ticket.session_id}.json"
        tmp_path = target_dir / f"{ticket.session_id}.json.tmp"
    target_dir.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(ticket.model_dump(), ensure_ascii=False, indent=2)

    with open(tmp_path, "w", encoding="utf-8") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())

    os.replace(tmp_path, out_path)

    dir_fd = os.open(target_dir, os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)

    return out_path
