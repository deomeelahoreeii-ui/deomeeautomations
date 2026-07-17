from __future__ import annotations

import hashlib
import hmac
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, BinaryIO
from urllib.parse import quote, urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from automation_core.config import Settings


class ObjectStorageError(RuntimeError):
    """Raised when durable object storage cannot complete an operation."""


@dataclass(frozen=True)
class StoredObjectResult:
    bucket: str
    object_key: str
    size_bytes: int
    sha256: str
    content_type: str | None
    etag: str | None
    version_id: str | None
    reused: bool


class S3ObjectStorage:
    """Provider-neutral path-style S3 adapter.

    It implements the small common subset required by the inbound archive using
    SigV4 and the already-installed ``requests`` dependency. No vendor SDK is
    required, so RustFS, SeaweedFS and other S3-compatible servers can be
    selected only through environment variables.
    """

    def __init__(self, settings: Settings, *, session: requests.Session | None = None) -> None:
        self.settings = settings
        self.session = session or self._build_session()

    @property
    def enabled(self) -> bool:
        return bool(self.settings.object_storage_enabled)

    def _build_session(self) -> requests.Session:
        session = requests.Session()
        retry = Retry(
            total=4,
            connect=4,
            read=2,
            status=3,
            backoff_factor=0.4,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset({"HEAD", "GET", "PUT"}),
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=8, pool_maxsize=8)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    @property
    def verify(self) -> bool | str:
        if self.settings.object_storage_ca_bundle:
            return str(self.settings.object_storage_ca_bundle)
        return self.settings.object_storage_verify_ssl

    @property
    def timeout(self) -> tuple[float, float]:
        return (
            self.settings.object_storage_connect_timeout_seconds,
            self.settings.object_storage_read_timeout_seconds,
        )

    def _endpoint(self) -> str:
        endpoint = str(self.settings.object_storage_endpoint_url or "").strip().rstrip("/")
        if not endpoint:
            raise ObjectStorageError("OBJECT_STORAGE_ENDPOINT_URL is required")
        parsed = urlparse(endpoint)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ObjectStorageError("OBJECT_STORAGE_ENDPOINT_URL must be an absolute HTTP(S) URL")
        if not self.settings.object_storage_path_style:
            raise ObjectStorageError("Bundle 4A supports path-style S3 endpoints only")
        return endpoint

    @staticmethod
    def _sha256_bytes(value: bytes) -> str:
        return hashlib.sha256(value).hexdigest()

    @staticmethod
    def _sign(key: bytes, message: str) -> bytes:
        return hmac.new(key, message.encode("utf-8"), hashlib.sha256).digest()

    def _authorization_headers(
        self,
        *,
        method: str,
        canonical_uri: str,
        payload_hash: str,
        headers: dict[str, str],
        now: datetime,
    ) -> dict[str, str]:
        access_key = self.settings.object_storage_access_key.strip()
        secret_key = self.settings.object_storage_secret_key
        if not access_key and not secret_key:
            return headers
        if not access_key or not secret_key:
            raise ObjectStorageError(
                "Both OBJECT_STORAGE_ACCESS_KEY and OBJECT_STORAGE_SECRET_KEY are required"
            )

        amz_date = now.strftime("%Y%m%dT%H%M%SZ")
        date_stamp = now.strftime("%Y%m%d")
        region = self.settings.object_storage_region or "us-east-1"
        signed_headers = {k.lower().strip(): " ".join(str(v).strip().split()) for k, v in headers.items()}
        signed_headers["x-amz-date"] = amz_date
        signed_headers["x-amz-content-sha256"] = payload_hash
        canonical_header_text = "".join(
            f"{name}:{signed_headers[name]}\n" for name in sorted(signed_headers)
        )
        signed_header_names = ";".join(sorted(signed_headers))
        canonical_request = "\n".join(
            [
                method.upper(),
                canonical_uri,
                "",
                canonical_header_text,
                signed_header_names,
                payload_hash,
            ]
        )
        scope = f"{date_stamp}/{region}/s3/aws4_request"
        string_to_sign = "\n".join(
            [
                "AWS4-HMAC-SHA256",
                amz_date,
                scope,
                hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
            ]
        )
        date_key = self._sign(("AWS4" + secret_key).encode("utf-8"), date_stamp)
        region_key = self._sign(date_key, region)
        service_key = self._sign(region_key, "s3")
        signing_key = self._sign(service_key, "aws4_request")
        signature = hmac.new(
            signing_key, string_to_sign.encode("utf-8"), hashlib.sha256
        ).hexdigest()
        output = dict(headers)
        output["x-amz-date"] = amz_date
        output["x-amz-content-sha256"] = payload_hash
        output["Authorization"] = (
            f"AWS4-HMAC-SHA256 Credential={access_key}/{scope}, "
            f"SignedHeaders={signed_header_names}, Signature={signature}"
        )
        return output

    def _request(
        self,
        method: str,
        *,
        bucket: str,
        object_key: str | None = None,
        body: bytes | BinaryIO | None = None,
        payload_hash: str | None = None,
        content_type: str | None = None,
        metadata: dict[str, str] | None = None,
        stream: bool = False,
    ) -> requests.Response:
        endpoint = self._endpoint()
        path = "/" + quote(bucket, safe="")
        if object_key is not None:
            path += "/" + quote(object_key, safe="/")
        parsed = urlparse(endpoint)
        base_path = parsed.path.rstrip("/")
        canonical_uri = quote((base_path.rstrip("/") + path) or "/", safe="/-_.~")
        url = endpoint + path
        headers: dict[str, str] = {"host": parsed.netloc}
        if content_type:
            headers["content-type"] = content_type
        for key, value in sorted((metadata or {}).items()):
            headers[f"x-amz-meta-{key.lower().replace('_', '-')}"] = quote(str(value), safe="-_.~")[:1024]
        if payload_hash is None:
            if isinstance(body, bytes):
                payload_hash = self._sha256_bytes(body)
            elif body is None:
                payload_hash = self._sha256_bytes(b"")
            else:
                raise ObjectStorageError("A payload hash is required for streamed S3 uploads")
        headers = self._authorization_headers(
            method=method,
            canonical_uri=canonical_uri,
            payload_hash=payload_hash,
            headers=headers,
            now=datetime.now(timezone.utc),
        )
        try:
            request_kwargs: dict[str, Any] = {
                "headers": headers,
                "data": body,
                "timeout": self.timeout,
                "verify": self.verify,
            }
            if stream:
                request_kwargs["stream"] = True
            return self.session.request(method, url, **request_kwargs)
        except requests.RequestException as exc:
            raise ObjectStorageError(f"S3 request failed for {url}: {exc}") from exc

    @staticmethod
    def _raise_for_response(response: requests.Response, action: str) -> None:
        if response.ok:
            return
        detail = response.text[:1000].strip()
        raise ObjectStorageError(
            f"{action} returned HTTP {response.status_code}"
            + (f": {detail}" if detail else "")
        )

    def ensure_bucket(self, bucket: str) -> None:
        if not bucket:
            raise ObjectStorageError("Object storage bucket name is empty")
        response = self._request("HEAD", bucket=bucket)
        if response.ok:
            return
        if response.status_code not in {404}:
            self._raise_for_response(response, f"Checking bucket {bucket!r}")
        if not self.settings.object_storage_auto_create_buckets:
            raise ObjectStorageError(
                f"Object storage bucket {bucket!r} does not exist and automatic creation is disabled"
            )
        body = b""
        content_type = None
        region = self.settings.object_storage_region
        if region and region != "us-east-1":
            body = (
                "<CreateBucketConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
                f"<LocationConstraint>{region}</LocationConstraint>"
                "</CreateBucketConfiguration>"
            ).encode("utf-8")
            content_type = "application/xml"
        created = self._request(
            "PUT",
            bucket=bucket,
            body=body,
            content_type=content_type,
        )
        if created.status_code not in {200, 201, 204, 409}:
            self._raise_for_response(created, f"Creating bucket {bucket!r}")

    def head(self, bucket: str, object_key: str) -> dict[str, Any] | None:
        response = self._request("HEAD", bucket=bucket, object_key=object_key)
        if response.status_code == 404:
            return None
        self._raise_for_response(response, f"Checking s3://{bucket}/{object_key}")
        metadata = {
            key.lower().removeprefix("x-amz-meta-"): value
            for key, value in response.headers.items()
            if key.lower().startswith("x-amz-meta-")
        }
        return {
            "ContentLength": int(response.headers.get("content-length") or 0),
            "ContentType": response.headers.get("content-type"),
            "ETag": response.headers.get("etag"),
            "VersionId": response.headers.get("x-amz-version-id"),
            "Metadata": metadata,
        }


    def download_file(self, *, bucket: str, object_key: str, destination: Path) -> dict[str, Any]:
        destination.parent.mkdir(parents=True, exist_ok=True)
        response = self._request("GET", bucket=bucket, object_key=object_key, stream=True)
        self._raise_for_response(response, f"Downloading s3://{bucket}/{object_key}")
        temp_path = destination.with_name(destination.name + ".part")
        size = 0
        digest = hashlib.sha256()
        try:
            with temp_path.open("wb") as handle:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if not chunk:
                        continue
                    handle.write(chunk)
                    digest.update(chunk)
                    size += len(chunk)
            temp_path.replace(destination)
        finally:
            response.close()
            if temp_path.exists():
                temp_path.unlink(missing_ok=True)
        return {
            "path": str(destination),
            "size_bytes": size,
            "sha256": digest.hexdigest(),
            "content_type": response.headers.get("content-type"),
            "etag": str(response.headers.get("etag") or "").strip('"') or None,
        }

    def put_file_if_absent(
        self,
        *,
        bucket: str,
        object_key: str,
        source_path: Path,
        sha256: str,
        size_bytes: int,
        content_type: str | None,
        metadata: dict[str, str] | None = None,
    ) -> StoredObjectResult:
        self.ensure_bucket(bucket)
        existing = self.head(bucket, object_key)
        if existing is not None:
            existing_size = int(existing.get("ContentLength") or 0)
            existing_sha = str((existing.get("Metadata") or {}).get("sha256") or "")
            if existing_size != size_bytes or (existing_sha and existing_sha != sha256):
                raise ObjectStorageError(
                    f"Existing object s3://{bucket}/{object_key} does not match expected content"
                )
            return StoredObjectResult(
                bucket=bucket,
                object_key=object_key,
                size_bytes=existing_size,
                sha256=existing_sha or sha256,
                content_type=existing.get("ContentType") or content_type,
                etag=str(existing.get("ETag") or "").strip('"') or None,
                version_id=existing.get("VersionId"),
                reused=True,
            )
        with source_path.open("rb") as handle:
            response = self._request(
                "PUT",
                bucket=bucket,
                object_key=object_key,
                body=handle,
                payload_hash=sha256,
                content_type=content_type,
                metadata={"sha256": sha256, **(metadata or {})},
            )
        self._raise_for_response(response, f"Uploading s3://{bucket}/{object_key}")
        verified = self.head(bucket, object_key)
        if verified is None:
            raise ObjectStorageError(
                f"Object storage did not return s3://{bucket}/{object_key} after upload"
            )
        verified_size = int(verified.get("ContentLength") or 0)
        verified_sha = str((verified.get("Metadata") or {}).get("sha256") or "")
        if verified_size != size_bytes or verified_sha != sha256:
            raise ObjectStorageError(
                f"Object verification failed for s3://{bucket}/{object_key}"
            )
        return StoredObjectResult(
            bucket=bucket,
            object_key=object_key,
            size_bytes=verified_size,
            sha256=verified_sha,
            content_type=verified.get("ContentType") or content_type,
            etag=str(verified.get("ETag") or "").strip('"') or None,
            version_id=verified.get("VersionId"),
            reused=False,
        )

    def put_bytes(
        self,
        *,
        bucket: str,
        object_key: str,
        body: bytes,
        content_type: str,
        metadata: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        self.ensure_bucket(bucket)
        response = self._request(
            "PUT",
            bucket=bucket,
            object_key=object_key,
            body=body,
            content_type=content_type,
            metadata=metadata,
        )
        self._raise_for_response(response, f"Writing s3://{bucket}/{object_key}")
        return {
            "etag": str(response.headers.get("etag") or "").strip('"') or None,
            "version_id": response.headers.get("x-amz-version-id"),
        }

    def health(self) -> dict[str, Any]:
        if not self.enabled:
            return {
                "enabled": False,
                "reachable": False,
                "provider": self.settings.object_storage_provider,
                "message": "Object storage is disabled; inbound files remain in the local compatibility archive.",
            }
        buckets = [
            self.settings.object_storage_raw_bucket,
            self.settings.object_storage_manifest_bucket,
            self.settings.object_storage_derived_bucket,
        ]
        try:
            for bucket in buckets:
                self.ensure_bucket(bucket)
            return {
                "enabled": True,
                "reachable": True,
                "provider": self.settings.object_storage_provider,
                "endpoint_url": self.settings.object_storage_endpoint_url,
                "buckets": buckets,
            }
        except ObjectStorageError as exc:
            return {
                "enabled": True,
                "reachable": False,
                "provider": self.settings.object_storage_provider,
                "endpoint_url": self.settings.object_storage_endpoint_url,
                "buckets": buckets,
                "error": str(exc),
            }
