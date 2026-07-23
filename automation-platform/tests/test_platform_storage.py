from __future__ import annotations

import uuid
from io import BytesIO
from urllib.parse import unquote, urlparse

import requests
from sqlalchemy.pool import StaticPool
from sqlmodel import SQLModel, Session, create_engine

import automation_core.models  # noqa: F401
import master_data.models  # noqa: F401
import whatsapp_gateway.models  # noqa: F401
from automation_core.config import Settings
from automation_core.models import Artifact, Job, JobStatus, JobType, StoredObject
from automation_core.object_storage import S3ObjectStorage
from automation_core.job_service import record_artifact
from automation_core.object_storage import ObjectStorageError
from automation_core.storage_catalog import archive_artifact, hydrate_stored_object, sha256_file


class FakeS3Session:
    def __init__(self) -> None:
        self.buckets: set[str] = set()
        self.objects: dict[tuple[str, str], dict] = {}
        self.put_count = 0

    @staticmethod
    def response(status: int, *, headers: dict[str, str] | None = None, body: bytes = b"") -> requests.Response:
        response = requests.Response()
        response.status_code = status
        response.headers.update(headers or {})
        response._content = body
        return response

    def request(self, method, url, *, headers, data, timeout, verify, stream=False):
        del timeout, verify, stream
        parts = [unquote(part) for part in urlparse(url).path.split("/") if part]
        bucket = parts[0]
        key = "/".join(parts[1:]) if len(parts) > 1 else None
        if method == "HEAD" and key is None:
            return self.response(200 if bucket in self.buckets else 404)
        if method == "PUT" and key is None:
            self.buckets.add(bucket)
            return self.response(200)
        if method == "HEAD":
            item = self.objects.get((bucket, key))
            return self.response(404) if item is None else self.response(200, headers=item["headers"])
        if method == "GET":
            item = self.objects.get((bucket, key))
            if item is None:
                return self.response(404)
            response = self.response(200, headers=item["headers"])
            response.raw = BytesIO(item["body"])
            response._content = False
            return response
        if method == "PUT":
            body = data if isinstance(data, bytes) else data.read()
            self.put_count += 1
            metadata = {name.lower(): value for name, value in headers.items() if name.lower().startswith("x-amz-meta-")}
            response_headers = {
                "content-length": str(len(body)),
                "content-type": headers.get("content-type", "application/octet-stream"),
                "etag": '"fake-etag"',
                **metadata,
            }
            self.objects[(bucket, key)] = {"body": body, "headers": response_headers}
            return self.response(200, headers={"etag": '"fake-etag"'})
        raise AssertionError(f"Unexpected S3 request {method} {url}")


def test_antidengue_artifact_is_content_addressed_deduplicated_and_hydratable(tmp_path):
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    settings = Settings(
        object_storage_enabled=True,
        object_storage_endpoint_url="http://storage.test:9000",
        object_storage_access_key="test",
        object_storage_secret_key="secret",
        platform_storage_derived_bucket="automation-derived-test",
        artifact_root=tmp_path / "cache",
    )
    fake = FakeS3Session()
    storage = S3ObjectStorage(settings, session=fake)
    source = tmp_path / "report.xlsx"
    source.write_bytes(b"verified anti-dengue report bytes")

    with Session(engine) as session:
        job = Job(
            id=uuid.uuid4(),
            type=JobType.antidengue_report.value,
            title="Storage test",
            status=JobStatus.succeeded.value,
        )
        artifact = Artifact(
            job_id=job.id,
            module_key="antidengue",
            kind="report",
            name="report.xlsx",
            path=str(source),
            size_bytes=source.stat().st_size,
        )
        session.add_all([job, artifact])
        session.commit()
        assert archive_artifact(session, artifact, settings=settings, storage=storage)
        session.commit()
        session.refresh(artifact)
        stored = session.get(StoredObject, artifact.stored_object_id)
        assert stored is not None
        assert stored.bucket == "automation-derived-test"
        assert stored.object_key.startswith("antidengue/derived/sha256/")
        assert stored.sha256 == sha256_file(source)
        assert artifact.storage_status == "ready"
        assert fake.put_count == 1

        second = Artifact(
            job_id=job.id,
            module_key="antidengue",
            kind="report",
            name="same-report.xlsx",
            path=str(source),
            size_bytes=source.stat().st_size,
        )
        session.add(second)
        session.commit()
        assert archive_artifact(session, second, settings=settings, storage=storage)
        session.commit()
        assert fake.put_count == 1
        assert second.stored_object_id == stored.id

        destination = tmp_path / "hydrated" / "report.xlsx"
        hydrate_stored_object(stored, destination, settings=settings, storage=storage)
        assert destination.read_bytes() == source.read_bytes()
        assert sha256_file(destination) == stored.sha256


def test_registered_artifact_rejects_mutated_bytes_and_preserves_local_file(tmp_path):
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    settings = Settings(object_storage_enabled=True)
    source = tmp_path / "report.xlsx"
    source.write_bytes(b"registered bytes")

    class StorageMustNotRun:
        def put_file_if_absent(self, **_kwargs):
            raise AssertionError("mutation must be rejected before upload")

    with Session(engine) as session:
        job = Job(
            type=JobType.antidengue_report.value,
            title="Mutation guard",
            status=JobStatus.succeeded.value,
        )
        session.add(job)
        session.commit()
        artifact = record_artifact(
            session,
            job.id,
            source,
            module_key="antidengue",
            kind="report",
        )
        original_sha256 = artifact.sha256
        source.write_bytes(b"different bytes")

        assert not archive_artifact(
            session,
            artifact,
            settings=settings,
            storage=StorageMustNotRun(),
        )
        session.commit()
        assert artifact.storage_status == "error"
        assert "changed after registration" in (artifact.storage_error or "")
        assert artifact.sha256 == original_sha256
        assert source.is_file()


def test_rustfs_outage_leaves_artifact_retryable_and_local(tmp_path):
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    settings = Settings(object_storage_enabled=True)
    source = tmp_path / "report.xlsx"
    source.write_bytes(b"keep me until RustFS recovers")

    class OfflineStorage:
        def put_file_if_absent(self, **_kwargs):
            raise ObjectStorageError("RustFS unavailable")

    with Session(engine) as session:
        job = Job(
            type=JobType.antidengue_report.value,
            title="Outage guard",
            status=JobStatus.succeeded.value,
        )
        session.add(job)
        session.commit()
        artifact = record_artifact(
            session,
            job.id,
            source,
            module_key="antidengue",
            kind="report",
        )
        assert not archive_artifact(
            session,
            artifact,
            settings=settings,
            storage=OfflineStorage(),
        )
        session.commit()
        assert artifact.storage_status == "error"
        assert "RustFS unavailable" in (artifact.storage_error or "")
        assert source.read_bytes() == b"keep me until RustFS recovers"


def test_artifact_registration_is_idempotent_for_interruption_recovery(tmp_path):
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    source = tmp_path / "report.xlsx"
    source.write_bytes(b"one immutable result")
    with Session(engine) as session:
        job = Job(type=JobType.antidengue_report.value, title="Retry discovery")
        session.add(job)
        session.commit()
        first = record_artifact(
            session, job.id, source, module_key="antidengue", kind="report"
        )
        second = record_artifact(
            session, job.id, source, module_key="antidengue", kind="report"
        )
        assert second.id == first.id
