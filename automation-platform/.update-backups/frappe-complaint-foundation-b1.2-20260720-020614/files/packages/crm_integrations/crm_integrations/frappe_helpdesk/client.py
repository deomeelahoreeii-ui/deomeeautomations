from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping, Sequence
from urllib.parse import quote

import requests


class FrappeHelpdeskError(RuntimeError):
    """A structured error returned by Frappe or raised while reaching it."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        payload: Any = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.payload = payload


def _message_from_payload(payload: Any, fallback: str) -> str:
    if not isinstance(payload, dict):
        return fallback
    if isinstance(payload.get("message"), str) and payload["message"].strip():
        return payload["message"].strip()
    server_messages = payload.get("_server_messages")
    if isinstance(server_messages, str):
        try:
            decoded = json.loads(server_messages)
            messages: list[str] = []
            for item in decoded if isinstance(decoded, list) else [decoded]:
                if isinstance(item, str):
                    try:
                        item = json.loads(item)
                    except json.JSONDecodeError:
                        messages.append(item)
                        continue
                if isinstance(item, dict):
                    text = item.get("message") or item.get("title")
                    if text:
                        messages.append(str(text))
            if messages:
                return " · ".join(messages)
        except json.JSONDecodeError:
            pass
    for key in ("exception", "exc_type"):
        if payload.get(key):
            return str(payload[key])
    return fallback


class FrappeHelpdeskClient:
    """Small, testable REST client for Frappe resources used by CRM sync."""

    def __init__(
        self,
        *,
        base_url: str,
        api_key: str,
        api_secret: str,
        timeout_seconds: float = 15.0,
        verify_ssl: bool = True,
        ca_bundle: Path | str | None = None,
        session: requests.Session | None = None,
    ) -> None:
        self.base_url = base_url.strip().rstrip("/")
        if not self.base_url:
            raise ValueError("Frappe Helpdesk base URL is required")
        self.api_key = api_key.strip()
        self.api_secret = api_secret.strip()
        self.timeout_seconds = timeout_seconds
        self.verify: bool | str = str(ca_bundle) if ca_bundle else verify_ssl
        self.session = session or requests.Session()

    @property
    def configured(self) -> bool:
        return bool(self.api_key and self.api_secret)

    def _headers(self) -> dict[str, str]:
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        if self.configured:
            headers["Authorization"] = f"token {self.api_key}:{self.api_secret}"
        return headers

    def request(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        payload: Mapping[str, Any] | None = None,
        expected: Sequence[int] = (200,),
    ) -> dict[str, Any]:
        url = f"{self.base_url}/{path.lstrip('/')}"
        try:
            response = self.session.request(
                method,
                url,
                params=params,
                json=payload,
                headers=self._headers(),
                timeout=self.timeout_seconds,
                verify=self.verify,
            )
        except requests.RequestException as exc:
            raise FrappeHelpdeskError(f"Unable to reach Frappe Helpdesk: {exc}") from exc

        try:
            body: Any = response.json()
        except ValueError:
            body = {"raw": response.text[:4000]}
        if response.status_code not in expected:
            fallback = f"Frappe Helpdesk returned HTTP {response.status_code}"
            raise FrappeHelpdeskError(
                _message_from_payload(body, fallback),
                status_code=response.status_code,
                payload=body,
            )
        if not isinstance(body, dict):
            return {"data": body}
        return body

    def authenticated_user(self) -> str:
        body = self.request("GET", "/api/method/frappe.auth.get_logged_user")
        return str(body.get("message") or "")

    def health(self) -> dict[str, Any]:
        user = self.authenticated_user()
        return {
            "status": "ok",
            "base_url": self.base_url,
            "authenticated_user": user,
            "credentials_configured": self.configured,
        }

    def get_resource(self, doctype: str, name: str) -> dict[str, Any]:
        path = f"/api/resource/{quote(doctype, safe='')}/{quote(name, safe='')}"
        body = self.request("GET", path)
        data = body.get("data")
        return data if isinstance(data, dict) else {}

    def list_resources(
        self,
        doctype: str,
        *,
        filters: Sequence[Any] | Mapping[str, Any] | None = None,
        fields: Sequence[str] | None = None,
        limit: int = 20,
        order_by: str | None = None,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"limit_page_length": max(1, min(limit, 1000))}
        if filters is not None:
            params["filters"] = json.dumps(filters, separators=(",", ":"))
        if fields is not None:
            params["fields"] = json.dumps(list(fields), separators=(",", ":"))
        if order_by:
            params["order_by"] = order_by
        path = f"/api/resource/{quote(doctype, safe='')}"
        body = self.request("GET", path, params=params)
        data = body.get("data")
        return [item for item in data if isinstance(item, dict)] if isinstance(data, list) else []

    def create_resource(self, doctype: str, values: Mapping[str, Any]) -> dict[str, Any]:
        path = f"/api/resource/{quote(doctype, safe='')}"
        body = self.request("POST", path, payload=dict(values), expected=(200, 201))
        data = body.get("data")
        return data if isinstance(data, dict) else {}

    def update_resource(
        self,
        doctype: str,
        name: str,
        values: Mapping[str, Any],
    ) -> dict[str, Any]:
        path = f"/api/resource/{quote(doctype, safe='')}/{quote(name, safe='')}"
        body = self.request("PUT", path, payload=dict(values), expected=(200, 201))
        data = body.get("data")
        return data if isinstance(data, dict) else {}

    def find_ticket_by_deomee_case(self, case_id: str) -> dict[str, Any] | None:
        rows = self.list_resources(
            "HD Ticket",
            filters=[["HD Ticket", "custom_deomee_case_id", "=", case_id]],
            fields=[
                "name",
                "modified",
                "status",
                "subject",
                "custom_deomee_case_id",
                "custom_deomee_payload_hash",
            ],
            limit=2,
        )
        return rows[0] if rows else None
