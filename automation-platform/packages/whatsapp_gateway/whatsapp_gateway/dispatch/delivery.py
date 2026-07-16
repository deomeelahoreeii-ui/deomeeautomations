from __future__ import annotations

from fastapi import APIRouter

from whatsapp_gateway.api_common import *

router = APIRouter(prefix="/api/v1/whatsapp", tags=["whatsapp"])

@router.post("/test-message", status_code=status.HTTP_202_ACCEPTED)
async def send_test_message(
    data: TestMessageInput,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    account, gateway_settings = ensure_defaults(session)
    if not gateway_settings.live_delivery_enabled:
        raise HTTPException(
            status_code=409,
            detail="Live delivery is disabled in WhatsApp Settings",
        )
    health = await refresh_health(session, account)
    if not health.get("ready"):
        raise HTTPException(status_code=503, detail="WhatsApp gateway is not ready")

    target = normalize_target(data.target)
    delivery = WhatsAppDelivery(
        account_id=account.id,
        recipient_type="test",
        recipient_name=data.recipient_name,
        target=target,
        message=data.message,
        status_subject=f"whatsapp.status.platform.{uuid.uuid4().hex}",
    )
    session.add(delivery)
    session.flush()
    activity(
        session,
        account,
        "test_message_queued",
        f"Queued a test message for {data.recipient_name}",
        details={"delivery_id": str(delivery.id), "target": target},
    )
    session.commit()

    client = None
    try:
        config = get_settings()
        client = await nats.connect(config.whatsapp_nats_url, connect_timeout=2)
        subscription = await client.subscribe(delivery.status_subject)
        await client.flush()
        payload = {
            "job_id": str(delivery.id),
            "target": target,
            "type": "group" if target.endswith("@g.us") else "contact",
            "recipient_name": data.recipient_name,
            "text": data.message,
            "delay_ms": gateway_settings.send_delay_ms,
            "status_subject": delivery.status_subject,
        }
        acknowledgement = await client.jetstream().publish(
            account.command_subject,
            json.dumps(payload).encode("utf-8"),
            headers={"Nats-Msg-Id": str(delivery.id)},
        )
        delivery.queue_stream = acknowledgement.stream
        delivery.queue_sequence = acknowledgement.seq
        session.add(delivery)
        session.commit()

        try:
            message = await subscription.next_msg(
                timeout=gateway_settings.acknowledgement_timeout_seconds
            )
            result = json.loads(message.data.decode("utf-8"))
            delivery.status = str(result.get("status") or "failed")
            delivery.error = result.get("error")
            delivery.provider_result = result
            delivery.completed_at = utcnow()
            activity(
                session,
                account,
                "test_message_completed",
                f"Test message {delivery.status} for {data.recipient_name}",
                level="error" if delivery.status == "failed" else "info",
                details={"delivery_id": str(delivery.id), "status": delivery.status},
            )
        except NatsTimeoutError:
            delivery.status = "timed_out"
            delivery.error = "Gateway acknowledgement timed out"
            delivery.completed_at = utcnow()
            activity(
                session,
                account,
                "test_message_timeout",
                f"Test message acknowledgement timed out for {data.recipient_name}",
                level="warning",
                details={"delivery_id": str(delivery.id)},
            )
        session.add(delivery)
        session.commit()
    except Exception as exc:
        delivery.status = "failed"
        delivery.error = str(exc)
        delivery.completed_at = utcnow()
        activity(
            session,
            account,
            "test_message_failed",
            f"Could not queue test message: {exc}",
            level="error",
            details={"delivery_id": str(delivery.id)},
        )
        session.add(delivery)
        session.commit()
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    finally:
        if client is not None:
            await client.close()
    return delivery_dict(delivery)

@router.get("/deliveries")
def deliveries(
    status_filter: str = Query(default="", alias="status"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=10, ge=1, le=100),
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    ensure_defaults(session)
    filters = [WhatsAppDelivery.status == status_filter] if status_filter else []
    total = session.scalar(
        select(func.count()).select_from(WhatsAppDelivery).where(*filters)
    ) or 0
    records = session.scalars(
        select(WhatsAppDelivery)
        .where(*filters)
        .order_by(WhatsAppDelivery.queued_at.desc())
        .limit(page_size)
        .offset((page - 1) * page_size)
    ).all()
    return {
        "items": [delivery_dict(item) for item in records],
        "total": total,
        "page": page,
        "page_size": page_size,
    }

@router.get("/recipients")
def recipients(
    search: str = "",
    recipient_type: Literal["all", "officer", "school_head"] = "all",
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=10, ge=1, le=100),
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    records: list[dict[str, Any]] = []
    if recipient_type in {"all", "officer"}:
        for officer in session.scalars(
            select(Officer).where(Officer.active.is_(True), Officer.mobile != "")
        ):
            wing = session.get(Wing, officer.wing_id)
            records.append(
                {
                    "id": str(officer.id),
                    "type": "officer",
                    "role": officer.role.upper(),
                    "name": officer.name,
                    "mobile": officer.mobile,
                    "wing": wing.name if wing else "",
                }
            )
    if recipient_type in {"all", "school_head"}:
        head_rows = session.execute(
            select(SchoolHead, School, Wing)
            .join(School, School.id == SchoolHead.school_id)
            .join(Wing, Wing.id == School.wing_id)
            .where(
                SchoolHead.active.is_(True),
                School.active.is_(True),
                SchoolHead.mobile != "",
            )
        ).all()
        for head, school, wing in head_rows:
            records.append(
                {
                    "id": str(head.id),
                    "type": "school_head",
                    "role": "School head",
                    "name": head.name,
                    "mobile": head.mobile,
                    "wing": wing.name,
                    "school": school.name,
                }
            )
    if search:
        needle = search.casefold()
        records = [
            item
            for item in records
            if needle in " ".join(str(value) for value in item.values()).casefold()
        ]
    records.sort(key=lambda item: (item["name"].casefold(), item["mobile"]))
    total = len(records)
    start = (page - 1) * page_size
    return {
        "items": records[start : start + page_size],
        "total": total,
        "page": page,
        "page_size": page_size,
    }
