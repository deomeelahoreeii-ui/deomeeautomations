from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from typing import Any


class SignedBatchError(RuntimeError):
    """Raised when an operator-approved batch token cannot be trusted."""


class SignedBatchCodec:
    def __init__(self, *, secret: str, purpose: str, ttl_seconds: int = 1800) -> None:
        if not secret.strip():
            raise SignedBatchError("A configured Helpdesk API secret is required to sign batches")
        self.key = hashlib.sha256(f"deomee:{purpose}:{secret}".encode()).digest()
        self.purpose = purpose
        self.ttl_seconds = ttl_seconds

    @staticmethod
    def _encode(value: bytes) -> str:
        return base64.urlsafe_b64encode(value).decode().rstrip("=")

    @staticmethod
    def _decode(value: str) -> bytes:
        """Decode a canonical, unpadded Base64URL component only."""
        text = str(value or "")
        if not text or "=" in text:
            raise ValueError("Base64URL value must be non-empty and unpadded")
        try:
            encoded = text.encode("ascii")
        except UnicodeEncodeError as exc:
            raise ValueError("Base64URL value must be ASCII") from exc
        padded = encoded + b"=" * (-len(encoded) % 4)
        decoded = base64.b64decode(padded, altchars=b"-_", validate=True)
        canonical = base64.urlsafe_b64encode(decoded).decode("ascii").rstrip("=")
        if not hmac.compare_digest(text, canonical):
            raise ValueError("Base64URL value is not canonical")
        return decoded

    def encode(self, payload: dict[str, Any]) -> str:
        now = int(time.time())
        data = {**payload, "purpose": self.purpose, "issued_at": now, "expires_at": now + self.ttl_seconds}
        encoded = self._encode(json.dumps(data, sort_keys=True, separators=(",", ":")).encode())
        signature = self._encode(hmac.new(self.key, encoded.encode(), hashlib.sha256).digest())
        return f"{encoded}.{signature}"

    def decode(self, token: str) -> dict[str, Any]:
        try:
            encoded, signature = token.strip().split(".", 1)
        except ValueError as exc:
            raise SignedBatchError("Invalid signed batch token") from exc
        expected = self._encode(hmac.new(self.key, encoded.encode(), hashlib.sha256).digest())
        if not hmac.compare_digest(signature, expected):
            raise SignedBatchError("Signed batch token signature is invalid")
        try:
            data = json.loads(self._decode(encoded))
        except (ValueError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise SignedBatchError("Signed batch token payload is invalid") from exc
        if not isinstance(data, dict) or data.get("purpose") != self.purpose:
            raise SignedBatchError("Signed batch token has the wrong purpose")
        if int(data.get("expires_at") or 0) < int(time.time()):
            raise SignedBatchError("Signed batch token has expired")
        return data
