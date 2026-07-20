from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import Response
from sqlmodel import Session

from automation_core.database import get_session
from crm_domain.knowledge import ComplaintKnowledgeArchive, KnowledgeFilters


router = APIRouter(prefix="/api/v1/crm/knowledge", tags=["crm-knowledge"])


def _filters(
    category: str = Query(default=""),
    sub_category: str = Query(default=""),
    source_system: str = Query(default=""),
    search: str = Query(default="", max_length=500),
    reply_scope: str = Query(default="approved", pattern="^(approved|with_reply|awaiting|all)$"),
    ai_eligible_only: bool = Query(default=False),
    date_from: date | None = Query(default=None),
    date_to: date | None = Query(default=None),
) -> KnowledgeFilters:
    return KnowledgeFilters(
        category=category.strip(),
        sub_category=sub_category.strip(),
        source_system=source_system.strip(),
        search=search.strip(),
        reply_scope=reply_scope,
        ai_eligible_only=ai_eligible_only,
        date_from=date_from,
        date_to=date_to,
    )


@router.get("/facets")
def knowledge_facets(session: Session = Depends(get_session)) -> dict[str, object]:
    return ComplaintKnowledgeArchive(session).facets()


@router.get("/statistics")
def knowledge_statistics(
    filters: KnowledgeFilters = Depends(_filters),
    session: Session = Depends(get_session),
) -> dict[str, object]:
    return ComplaintKnowledgeArchive(session).statistics(filters)


@router.get("/records")
def knowledge_records(
    filters: KnowledgeFilters = Depends(_filters),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=200),
    session: Session = Depends(get_session),
) -> dict[str, object]:
    records = ComplaintKnowledgeArchive(session).records(filters)
    start = (page - 1) * page_size
    items = records[start : start + page_size]
    pages = max(1, (len(records) + page_size - 1) // page_size)
    return {
        "items": [record.as_dict() for record in items],
        "total": len(records),
        "page": page,
        "page_size": page_size,
        "pages": pages,
        "filters": filters.public_dict(),
    }


@router.get("/exports/{format_name}")
def knowledge_export(
    format_name: str,
    filters: KnowledgeFilters = Depends(_filters),
    session: Session = Depends(get_session),
) -> Response:
    try:
        content, media_type, filename = ComplaintKnowledgeArchive(session).render(format_name, filters)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return Response(
        content=content,
        media_type=media_type,
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "X-Content-Type-Options": "nosniff",
        },
    )
