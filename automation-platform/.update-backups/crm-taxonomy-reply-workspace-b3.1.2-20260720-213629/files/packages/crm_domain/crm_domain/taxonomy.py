from __future__ import annotations

import uuid
from collections import Counter
from dataclasses import dataclass
from typing import Any, Iterable

from sqlalchemy import func
from sqlalchemy.exc import IntegrityError
from sqlmodel import Session, select

from automation_core.config import Settings
from automation_core.time import utcnow
from crm_domain.models import (
    ComplaintAuditEvent,
    ComplaintCase,
    ComplaintCategory,
    ComplaintSubcategory,
)
from crm_integrations.frappe_helpdesk.client import FrappeHelpdeskClient, FrappeHelpdeskError
from crm_integrations.frappe_helpdesk.service import build_client


class TaxonomyError(ValueError):
    pass


def clean_name(value: str) -> str:
    return " ".join(str(value or "").split()).strip()


def normalized_name(value: str) -> str:
    return clean_name(value).casefold()


@dataclass(frozen=True)
class TaxonomyMutation:
    entity: dict[str, Any]
    affected_case_ids: tuple[uuid.UUID, ...] = ()
    remote_sync: dict[str, Any] | None = None


class ComplaintTaxonomyService:
    """PostgreSQL-authoritative complaint taxonomy with additive Frappe mirroring."""

    def __init__(
        self,
        session: Session,
        settings: Settings,
        client: FrappeHelpdeskClient | None = None,
    ) -> None:
        self.session = session
        self.settings = settings
        self.client = client or build_client(settings)

    @staticmethod
    def _category_dict(category: ComplaintCategory, complaint_count: int = 0) -> dict[str, Any]:
        return {
            "id": str(category.id),
            "name": category.name,
            "description": category.description or "",
            "display_order": category.display_order,
            "active": category.active,
            "default_ai_eligible": category.default_ai_eligible,
            "reply_guidance": category.reply_guidance or "",
            "policy_notes": category.policy_notes or "",
            "frappe_ticket_type_name": category.frappe_ticket_type_name or category.name,
            "frappe_sync_status": category.frappe_sync_status,
            "frappe_last_synced_at": category.frappe_last_synced_at,
            "frappe_last_error": category.frappe_last_error,
            "merged_into_id": str(category.merged_into_id) if category.merged_into_id else None,
            "version": category.version,
            "complaint_count": complaint_count,
        }

    @staticmethod
    def _subcategory_dict(subcategory: ComplaintSubcategory, complaint_count: int = 0) -> dict[str, Any]:
        return {
            "id": str(subcategory.id),
            "category_id": str(subcategory.category_id),
            "name": subcategory.name,
            "description": subcategory.description or "",
            "display_order": subcategory.display_order,
            "active": subcategory.active,
            "reply_guidance": subcategory.reply_guidance or "",
            "policy_notes": subcategory.policy_notes or "",
            "merged_into_id": str(subcategory.merged_into_id) if subcategory.merged_into_id else None,
            "version": subcategory.version,
            "complaint_count": complaint_count,
        }

    def _audit(
        self,
        *,
        entity_type: str,
        entity_id: str,
        event_type: str,
        actor: str,
        before: dict[str, Any] | None = None,
        after: dict[str, Any] | None = None,
        details: dict[str, Any] | None = None,
        state: str = "succeeded",
        error: str | None = None,
    ) -> None:
        self.session.add(
            ComplaintAuditEvent(
                entity_type=entity_type,
                entity_id=entity_id,
                event_type=event_type,
                actor=actor,
                state=state,
                before_json=before or {},
                after_json=after or {},
                details_json=details or {},
                error=error,
            )
        )

    def tree(self, *, include_inactive: bool = True) -> dict[str, Any]:
        categories_query = select(ComplaintCategory)
        subcategories_query = select(ComplaintSubcategory)
        if not include_inactive:
            categories_query = categories_query.where(ComplaintCategory.active == True)  # noqa: E712
            subcategories_query = subcategories_query.where(ComplaintSubcategory.active == True)  # noqa: E712
        categories = list(
            self.session.exec(
                categories_query.order_by(
                    ComplaintCategory.display_order,
                    ComplaintCategory.name,
                )
            ).all()
        )
        subcategories = list(
            self.session.exec(
                subcategories_query.order_by(
                    ComplaintSubcategory.category_id,
                    ComplaintSubcategory.display_order,
                    ComplaintSubcategory.name,
                )
            ).all()
        )
        category_counts = dict(
            self.session.exec(
                select(ComplaintCase.category_id, func.count(ComplaintCase.id))
                .where(ComplaintCase.category_id.is_not(None))
                .group_by(ComplaintCase.category_id)
            ).all()
        )
        subcategory_counts = dict(
            self.session.exec(
                select(ComplaintCase.sub_category_id, func.count(ComplaintCase.id))
                .where(ComplaintCase.sub_category_id.is_not(None))
                .group_by(ComplaintCase.sub_category_id)
            ).all()
        )
        by_category: dict[uuid.UUID, list[dict[str, Any]]] = {}
        for subcategory in subcategories:
            by_category.setdefault(subcategory.category_id, []).append(
                self._subcategory_dict(
                    subcategory,
                    int(subcategory_counts.get(subcategory.id, 0)),
                )
            )
        return {
            "categories": [
                {
                    **self._category_dict(category, int(category_counts.get(category.id, 0))),
                    "subcategories": by_category.get(category.id, []),
                }
                for category in categories
            ],
            "statistics": self.statistics(),
        }

    def statistics(self) -> dict[str, Any]:
        categories = list(self.session.exec(select(ComplaintCategory)).all())
        subcategories = list(self.session.exec(select(ComplaintSubcategory)).all())
        cases = list(self.session.exec(select(ComplaintCase)).all())
        return {
            "categories": len(categories),
            "active_categories": sum(row.active for row in categories),
            "subcategories": len(subcategories),
            "active_subcategories": sum(row.active for row in subcategories),
            "classified_cases": sum(row.category_id is not None for row in cases),
            "unclassified_cases": sum(row.category_id is None for row in cases),
            "sync_statuses": dict(Counter(row.frappe_sync_status for row in categories)),
            "classification_sync_statuses": dict(
                Counter(row.classification_sync_status for row in cases)
            ),
        }

    def _ensure_unique_category_name(self, name: str, *, exclude_id: uuid.UUID | None = None) -> None:
        query = select(ComplaintCategory).where(
            ComplaintCategory.normalized_name == normalized_name(name)
        )
        if exclude_id is not None:
            query = query.where(ComplaintCategory.id != exclude_id)
        if self.session.exec(query).first() is not None:
            raise TaxonomyError(f"Category already exists: {clean_name(name)}")

    def _ensure_unique_subcategory_name(
        self,
        category_id: uuid.UUID,
        name: str,
        *,
        exclude_id: uuid.UUID | None = None,
    ) -> None:
        query = select(ComplaintSubcategory).where(
            ComplaintSubcategory.category_id == category_id,
            ComplaintSubcategory.normalized_name == normalized_name(name),
        )
        if exclude_id is not None:
            query = query.where(ComplaintSubcategory.id != exclude_id)
        if self.session.exec(query).first() is not None:
            raise TaxonomyError(f"Subcategory already exists in this category: {clean_name(name)}")

    def _require_category(self, category_id: uuid.UUID) -> ComplaintCategory:
        category = self.session.get(ComplaintCategory, category_id)
        if category is None:
            raise TaxonomyError("Complaint category was not found")
        return category

    def _require_subcategory(self, subcategory_id: uuid.UUID) -> ComplaintSubcategory:
        subcategory = self.session.get(ComplaintSubcategory, subcategory_id)
        if subcategory is None:
            raise TaxonomyError("Complaint subcategory was not found")
        return subcategory

    def sync_category(self, category_id: uuid.UUID, *, previous_name: str | None = None) -> dict[str, Any]:
        category = self._require_category(category_id)
        if not self.settings.frappe_helpdesk_enabled:
            category.frappe_sync_status = "not_required"
            category.frappe_last_error = None
            category.frappe_last_synced_at = utcnow()
            self.session.add(category)
            self.session.commit()
            return {"status": "not_required", "category_id": str(category.id)}
        try:
            ticket_type_name = category.name
            try:
                existing = self.client.get_resource("HD Ticket Type", ticket_type_name)
            except FrappeHelpdeskError as exc:
                if exc.status_code != 404:
                    raise
                existing = None
            values = {
                "name": ticket_type_name,
                "description": category.description
                or "Complaint category synchronized from Deomee CRM.",
                "disabled": 0 if category.active else 1,
                "is_system": 0,
            }
            if existing is None:
                self.client.create_resource("HD Ticket Type", values)
                action = "created"
            else:
                changed = {
                    key: value for key, value in values.items() if existing.get(key) != value
                }
                if changed:
                    self.client.update_resource("HD Ticket Type", ticket_type_name, changed)
                    action = "updated"
                else:
                    action = "unchanged"
            if previous_name and normalized_name(previous_name) != category.normalized_name:
                try:
                    old = self.client.get_resource("HD Ticket Type", previous_name)
                except FrappeHelpdeskError as exc:
                    if exc.status_code != 404:
                        raise
                    old = None
                if old is not None and not int(old.get("is_system") or 0):
                    self.client.update_resource("HD Ticket Type", previous_name, {"disabled": 1})
            verified = self.client.get_resource("HD Ticket Type", ticket_type_name)
            if clean_name(str(verified.get("name") or ticket_type_name)) != ticket_type_name:
                raise TaxonomyError("Frappe ticket type read-back did not match the category")
            category.frappe_ticket_type_name = ticket_type_name
            category.frappe_sync_status = "synchronized"
            category.frappe_last_synced_at = utcnow()
            category.frappe_last_error = None
            self.session.add(category)
            self.session.commit()
            return {
                "status": "synchronized",
                "action": action,
                "category_id": str(category.id),
                "ticket_type": ticket_type_name,
            }
        except Exception as exc:
            category.frappe_sync_status = "failed"
            category.frappe_last_error = str(exc)
            category.updated_at = utcnow()
            self.session.add(category)
            self.session.commit()
            return {
                "status": "failed",
                "category_id": str(category.id),
                "error": str(exc),
            }

    def create_category(
        self,
        *,
        name: str,
        description: str = "",
        display_order: int = 100,
        active: bool = True,
        default_ai_eligible: bool = False,
        reply_guidance: str = "",
        policy_notes: str = "",
        actor: str = "web-operator",
    ) -> TaxonomyMutation:
        cleaned = clean_name(name)
        if not cleaned:
            raise TaxonomyError("Category name is required")
        self._ensure_unique_category_name(cleaned)
        category = ComplaintCategory(
            name=cleaned,
            normalized_name=normalized_name(cleaned),
            description=description.strip() or None,
            display_order=display_order,
            active=active,
            default_ai_eligible=default_ai_eligible,
            reply_guidance=reply_guidance.strip() or None,
            policy_notes=policy_notes.strip() or None,
            frappe_ticket_type_name=cleaned,
            frappe_sync_status="pending",
        )
        self.session.add(category)
        try:
            self.session.flush()
        except IntegrityError as exc:
            self.session.rollback()
            raise TaxonomyError(f"Category already exists: {cleaned}") from exc
        after = self._category_dict(category)
        self._audit(
            entity_type="complaint_category",
            entity_id=str(category.id),
            event_type="category_created",
            actor=actor,
            after=after,
        )
        self.session.commit()
        remote = self.sync_category(category.id)
        return TaxonomyMutation(self._category_dict(category), remote_sync=remote)

    def update_category(
        self,
        category_id: uuid.UUID,
        *,
        name: str,
        description: str = "",
        display_order: int = 100,
        active: bool = True,
        default_ai_eligible: bool = False,
        reply_guidance: str = "",
        policy_notes: str = "",
        actor: str = "web-operator",
    ) -> TaxonomyMutation:
        category = self._require_category(category_id)
        if category.merged_into_id:
            raise TaxonomyError("Merged categories cannot be edited")
        cleaned = clean_name(name)
        if not cleaned:
            raise TaxonomyError("Category name is required")
        self._ensure_unique_category_name(cleaned, exclude_id=category.id)
        before = self._category_dict(category)
        previous_name = category.name
        category.name = cleaned
        category.normalized_name = normalized_name(cleaned)
        category.description = description.strip() or None
        category.display_order = display_order
        category.active = active
        category.default_ai_eligible = default_ai_eligible
        category.reply_guidance = reply_guidance.strip() or None
        category.policy_notes = policy_notes.strip() or None
        category.frappe_ticket_type_name = cleaned
        category.frappe_sync_status = "pending"
        category.frappe_last_error = None
        category.version += 1
        category.updated_at = utcnow()
        cases = list(
            self.session.exec(
                select(ComplaintCase).where(ComplaintCase.category_id == category.id)
            ).all()
        )
        for case in cases:
            case.category = category.name
            case.classification_sync_status = "pending"
            case.classification_last_error = None
            case.version += 1
            case.updated_at = utcnow()
            self.session.add(case)
        self.session.add(category)
        after = self._category_dict(category, len(cases))
        self._audit(
            entity_type="complaint_category",
            entity_id=str(category.id),
            event_type="category_updated",
            actor=actor,
            before=before,
            after=after,
            details={"affected_cases": len(cases), "previous_name": previous_name},
        )
        self.session.commit()
        remote = self.sync_category(category.id, previous_name=previous_name)
        return TaxonomyMutation(after, tuple(case.id for case in cases), remote)

    def set_category_active(
        self,
        category_id: uuid.UUID,
        *,
        active: bool,
        actor: str = "web-operator",
    ) -> TaxonomyMutation:
        category = self._require_category(category_id)
        if category.merged_into_id and active:
            raise TaxonomyError("A merged category cannot be reactivated")
        return self.update_category(
            category.id,
            name=category.name,
            description=category.description or "",
            display_order=category.display_order,
            active=active,
            default_ai_eligible=category.default_ai_eligible,
            reply_guidance=category.reply_guidance or "",
            policy_notes=category.policy_notes or "",
            actor=actor,
        )

    def create_subcategory(
        self,
        *,
        category_id: uuid.UUID,
        name: str,
        description: str = "",
        display_order: int = 100,
        active: bool = True,
        reply_guidance: str = "",
        policy_notes: str = "",
        actor: str = "web-operator",
    ) -> TaxonomyMutation:
        category = self._require_category(category_id)
        if not category.active or category.merged_into_id:
            raise TaxonomyError("Select an active category")
        cleaned = clean_name(name)
        if not cleaned:
            raise TaxonomyError("Subcategory name is required")
        self._ensure_unique_subcategory_name(category.id, cleaned)
        subcategory = ComplaintSubcategory(
            category_id=category.id,
            name=cleaned,
            normalized_name=normalized_name(cleaned),
            description=description.strip() or None,
            display_order=display_order,
            active=active,
            reply_guidance=reply_guidance.strip() or None,
            policy_notes=policy_notes.strip() or None,
        )
        self.session.add(subcategory)
        try:
            self.session.flush()
        except IntegrityError as exc:
            self.session.rollback()
            raise TaxonomyError(f"Subcategory already exists: {cleaned}") from exc
        after = self._subcategory_dict(subcategory)
        self._audit(
            entity_type="complaint_subcategory",
            entity_id=str(subcategory.id),
            event_type="subcategory_created",
            actor=actor,
            after=after,
        )
        self.session.commit()
        return TaxonomyMutation(after)

    def update_subcategory(
        self,
        subcategory_id: uuid.UUID,
        *,
        category_id: uuid.UUID,
        name: str,
        description: str = "",
        display_order: int = 100,
        active: bool = True,
        reply_guidance: str = "",
        policy_notes: str = "",
        actor: str = "web-operator",
    ) -> TaxonomyMutation:
        subcategory = self._require_subcategory(subcategory_id)
        if subcategory.merged_into_id:
            raise TaxonomyError("Merged subcategories cannot be edited")
        category = self._require_category(category_id)
        if not category.active or category.merged_into_id:
            raise TaxonomyError("Select an active category")
        cleaned = clean_name(name)
        if not cleaned:
            raise TaxonomyError("Subcategory name is required")
        self._ensure_unique_subcategory_name(
            category.id, cleaned, exclude_id=subcategory.id
        )
        before = self._subcategory_dict(subcategory)
        subcategory.category_id = category.id
        subcategory.name = cleaned
        subcategory.normalized_name = normalized_name(cleaned)
        subcategory.description = description.strip() or None
        subcategory.display_order = display_order
        subcategory.active = active
        subcategory.reply_guidance = reply_guidance.strip() or None
        subcategory.policy_notes = policy_notes.strip() or None
        subcategory.version += 1
        subcategory.updated_at = utcnow()
        cases = list(
            self.session.exec(
                select(ComplaintCase).where(
                    ComplaintCase.sub_category_id == subcategory.id
                )
            ).all()
        )
        for case in cases:
            case.category_id = category.id
            case.category = category.name
            case.sub_category = subcategory.name
            case.classification_sync_status = "pending"
            case.classification_last_error = None
            case.version += 1
            case.updated_at = utcnow()
            self.session.add(case)
        self.session.add(subcategory)
        after = self._subcategory_dict(subcategory, len(cases))
        self._audit(
            entity_type="complaint_subcategory",
            entity_id=str(subcategory.id),
            event_type="subcategory_updated",
            actor=actor,
            before=before,
            after=after,
            details={"affected_cases": len(cases)},
        )
        self.session.commit()
        return TaxonomyMutation(after, tuple(case.id for case in cases))

    def set_subcategory_active(
        self,
        subcategory_id: uuid.UUID,
        *,
        active: bool,
        actor: str = "web-operator",
    ) -> TaxonomyMutation:
        subcategory = self._require_subcategory(subcategory_id)
        if subcategory.merged_into_id and active:
            raise TaxonomyError("A merged subcategory cannot be reactivated")
        return self.update_subcategory(
            subcategory.id,
            category_id=subcategory.category_id,
            name=subcategory.name,
            description=subcategory.description or "",
            display_order=subcategory.display_order,
            active=active,
            reply_guidance=subcategory.reply_guidance or "",
            policy_notes=subcategory.policy_notes or "",
            actor=actor,
        )

    def merge_subcategory(
        self,
        source_id: uuid.UUID,
        target_id: uuid.UUID,
        *,
        actor: str = "web-operator",
    ) -> TaxonomyMutation:
        if source_id == target_id:
            raise TaxonomyError("Source and target subcategories must be different")
        source = self._require_subcategory(source_id)
        target = self._require_subcategory(target_id)
        if source.merged_into_id:
            raise TaxonomyError("Source subcategory has already been merged")
        if not target.active or target.merged_into_id:
            raise TaxonomyError("Target subcategory must be active")
        before = self._subcategory_dict(source)
        target_category = self._require_category(target.category_id)
        cases = list(
            self.session.exec(
                select(ComplaintCase).where(ComplaintCase.sub_category_id == source.id)
            ).all()
        )
        for case in cases:
            case.category_id = target.category_id
            case.category = target_category.name
            case.sub_category_id = target.id
            case.sub_category = target.name
            case.classification_sync_status = "pending"
            case.classification_last_error = None
            case.version += 1
            case.updated_at = utcnow()
            self.session.add(case)
        source.active = False
        source.merged_into_id = target.id
        source.version += 1
        source.updated_at = utcnow()
        self.session.add(source)
        after = self._subcategory_dict(source, 0)
        self._audit(
            entity_type="complaint_subcategory",
            entity_id=str(source.id),
            event_type="subcategory_merged",
            actor=actor,
            before=before,
            after=after,
            details={"target_id": str(target.id), "affected_cases": len(cases)},
        )
        self.session.commit()
        return TaxonomyMutation(after, tuple(case.id for case in cases))

    def merge_category(
        self,
        source_id: uuid.UUID,
        target_id: uuid.UUID,
        *,
        actor: str = "web-operator",
    ) -> TaxonomyMutation:
        if source_id == target_id:
            raise TaxonomyError("Source and target categories must be different")
        source = self._require_category(source_id)
        target = self._require_category(target_id)
        if source.merged_into_id:
            raise TaxonomyError("Source category has already been merged")
        if not target.active or target.merged_into_id:
            raise TaxonomyError("Target category must be active")
        before = self._category_dict(source)
        target_subcategories = {
            row.normalized_name: row
            for row in self.session.exec(
                select(ComplaintSubcategory).where(
                    ComplaintSubcategory.category_id == target.id,
                    ComplaintSubcategory.merged_into_id.is_(None),
                )
            ).all()
        }
        source_subcategories = list(
            self.session.exec(
                select(ComplaintSubcategory).where(
                    ComplaintSubcategory.category_id == source.id,
                    ComplaintSubcategory.merged_into_id.is_(None),
                )
            ).all()
        )
        replacement: dict[uuid.UUID, ComplaintSubcategory] = {}
        for subcategory in source_subcategories:
            duplicate = target_subcategories.get(subcategory.normalized_name)
            if duplicate is not None:
                replacement[subcategory.id] = duplicate
                subcategory.active = False
                subcategory.merged_into_id = duplicate.id
            else:
                subcategory.category_id = target.id
                replacement[subcategory.id] = subcategory
                target_subcategories[subcategory.normalized_name] = subcategory
            subcategory.version += 1
            subcategory.updated_at = utcnow()
            self.session.add(subcategory)
        cases = list(
            self.session.exec(
                select(ComplaintCase).where(ComplaintCase.category_id == source.id)
            ).all()
        )
        for case in cases:
            case.category_id = target.id
            case.category = target.name
            if case.sub_category_id and case.sub_category_id in replacement:
                target_subcategory = replacement[case.sub_category_id]
                case.sub_category_id = target_subcategory.id
                case.sub_category = target_subcategory.name
            case.classification_sync_status = "pending"
            case.classification_last_error = None
            case.version += 1
            case.updated_at = utcnow()
            self.session.add(case)
        previous_name = source.name
        source.active = False
        source.merged_into_id = target.id
        source.frappe_sync_status = "pending"
        source.version += 1
        source.updated_at = utcnow()
        self.session.add(source)
        after = self._category_dict(source, 0)
        self._audit(
            entity_type="complaint_category",
            entity_id=str(source.id),
            event_type="category_merged",
            actor=actor,
            before=before,
            after=after,
            details={"target_id": str(target.id), "affected_cases": len(cases)},
        )
        self.session.commit()
        source_sync = self.sync_category(source.id, previous_name=previous_name)
        target_sync = self.sync_category(target.id)
        return TaxonomyMutation(
            after,
            tuple(case.id for case in cases),
            {"source": source_sync, "target": target_sync},
        )

    def audit(self, *, limit: int = 100) -> dict[str, Any]:
        rows = list(
            self.session.exec(
                select(ComplaintAuditEvent)
                .where(
                    ComplaintAuditEvent.entity_type.in_(
                        ["complaint_category", "complaint_subcategory"]
                    )
                )
                .order_by(ComplaintAuditEvent.created_at.desc())
                .limit(max(1, min(limit, 500)))
            ).all()
        )
        return {
            "items": [row.model_dump(mode="json") for row in rows],
            "count": len(rows),
        }
