from __future__ import annotations

import httpx

from .config import GatewayConfig


class SmsGateClient:
    def __init__(self, config: GatewayConfig, timeout: float = 30):
        self.config = config
        self.client = httpx.Client(auth=(config.username, config.password), timeout=timeout)

    def close(self) -> None:
        self.client.close()

    def devices(self):
        response = self.client.get(f"{self.config.base_url}/3rdparty/v1/devices")
        response.raise_for_status()
        return response.json()

    def configured_device(self) -> dict:
        payload = self.devices()
        devices = payload if isinstance(payload, list) else [payload]
        for device in devices:
            if isinstance(device, dict) and str(device.get("id") or "") == self.config.device_id:
                return device
        raise ValueError(f"Configured SMSGate device was not found: {self.config.device_id}")

    def send(self, phone: str, text: str, sim_number: int, ttl: int, message_id: str = "") -> str:
        payload = {
            "textMessage": {"text": text},
            "phoneNumbers": [phone],
            "deviceId": self.config.device_id,
            "simNumber": sim_number,
            "ttl": ttl,
            "withDeliveryReport": True,
            "priority": 0,
        }
        if message_id:
            payload["id"] = message_id
        response = self.client.post(
            f"{self.config.base_url}/3rdparty/v1/messages",
            json=payload,
        )
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, list) and payload:
            payload = payload[0]
        if not isinstance(payload, dict):
            raise ValueError("Gateway returned an unexpected response")
        return str(payload.get("id") or payload.get("messageId") or "")

    def status(self, message_id: str) -> dict:
        response = self.client.get(f"{self.config.base_url}/3rdparty/v1/messages/{message_id}")
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError("Gateway returned an unexpected status response")
        return payload
