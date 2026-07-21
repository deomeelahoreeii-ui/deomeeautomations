from __future__ import annotations

import hashlib
import json
import re
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

from sqlalchemy import func, or_, select
from sqlmodel import Session

from automation_core.config import Settings
from automation_core.time import utcnow
from crm_domain.models import (
    ComplaintCase,
    ComplaintDocument,
    ComplaintDocumentCaseLink,
    CrmComplaintTag,
    CrmComplaintTagLink,
    ComplaintReplyRevision,
    CrmDispatchArtifact,
    CrmDispatchBatch,
    CrmDispatchItem,
    CrmDispatchRule,
    CrmDispatchTarget,
    CrmOfficialLetter,
    CrmOfficialLetterArtifact,
)
from crm_domain.official_letters import OfficialLetterService
from whatsapp_gateway.configuration.defaults import ensure_defaults
from whatsapp_gateway.models import (
    WhatsAppAccount,
    WhatsAppApplication,
    WhatsAppAudience,
    WhatsAppAudienceMember,
    WhatsAppDelivery,
    WhatsAppDirectoryContact,
    WhatsAppDirectoryGroup,
    WhatsAppDispatchApproval,
    WhatsAppDispatchPreview,
    WhatsAppDispatchPreviewArtifact,
    WhatsAppDispatchPreviewDelivery,
    WhatsAppDispatchProfile,
    WhatsAppRecipientScope,
    WhatsAppReportType,
    WhatsAppTemplate,
)
from whatsapp_gateway.previews.artifact_storage import freeze_artifact, sha256_file
from whatsapp_gateway.previews.state import apply_preview_state, summarize_preview_state


CRM_APPLICATION_KEY = "crm"
CRM_REPORT_TYPE_KEY = "complete_complaint_packet"
CRM_TEMPLATE_KEY = "crm_complete_complaint_packet_v1"
DOWNWARD_MESSAGE = """Respected Sir/Madam,

Complaint No. {{complaint_number}} is forwarded for {{dispatch_purpose_label}}. The complaint record is attached as a PDF.

Please provide the required report, reply and supporting evidence by {{response_due_date}} in accordance with applicable rules and policy."""
UPWARD_MESSAGE = """Respected Sir/Madam,

Compliance in Complaint No. {{complaint_number}} is submitted in accordance with official letter No. {{letter_number}} dated {{letter_date}}.

The complete compliance record is attached as a PDF for kind consideration and further necessary action."""
DEFAULT_MESSAGE = """Respected Sir/Madam,

Complaint No. {{complaint_number}} is forwarded for {{dispatch_purpose_label}}.

The relevant complaint/compliance record is attached as a PDF. Please process the matter in accordance with applicable rules and policy."""


class CrmDispatchError(RuntimeError):
    pass


class CrmDispatchNotFound(CrmDispatchError):
    pass


class CrmDispatchValidationError(CrmDispatchError):
    pass


def _slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.casefold()).strip("_")[:90] or "crm_route"


def _hash_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _json_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, ensure_ascii=False, default=str).encode("utf-8")
    ).hexdigest()


def _page_count(path: Path) -> int:
    try:
        content = path.read_bytes()
    except OSError:
        return 0
    return max(1, len(re.findall(rb"/Type\s*/Page(?!s)\b", content)))


def _render_message(template: str, values: dict[str, Any]) -> str:
    result = template
    for key, value in values.items():
        result = result.replace("{{" + key + "}}", str(value or ""))
    unresolved = sorted(set(re.findall(r"{{\s*([a-zA-Z0-9_]+)\s*}}", result)))
    if unresolved:
        raise CrmDispatchValidationError(
            "Message template contains unsupported placeholders: " + ", ".join(unresolved)
        )
    return result.strip()


def _target_identity(
    session: Session, member: WhatsAppAudienceMember
) -> tuple[str, str, str, bool]:
    if member.target_type == "group":
        target = session.get(WhatsAppDirectoryGroup, member.directory_group_id)
        if target is None:
            return "group", "Deleted group", "", False
        return "group", target.name or target.jid, target.jid, bool(target.available)
    target = session.get(WhatsAppDirectoryContact, member.directory_contact_id)
    if target is None:
        return "contact", "Deleted contact", "", False
    jid = target.phone_jid or target.primary_lid_jid or ""
    return "contact", target.display_name or jid or "Unnamed contact", jid, bool(target.active and jid)


class CrmDispatchService:
    def __init__(self, session: Session, settings: Settings | None = None):
        self.session = session
        self.settings = settings or Settings()
        self.packet_root = self.settings.artifact_root.expanduser().resolve() / "crm-dispatch-packets"
        self.packet_root.mkdir(parents=True, exist_ok=True)

    def ensure_defaults(self) -> dict[str, Any]:
        account, _settings = ensure_defaults(self.session)
        application = self.session.scalar(
            select(WhatsAppApplication).where(WhatsAppApplication.key == CRM_APPLICATION_KEY)
        )
        if application is None:
            application = WhatsAppApplication(
                key=CRM_APPLICATION_KEY,
                name="CRM",
                description="Complaint reply, official-letter and dispatch workflows.",
            )
            self.session.add(application)
            self.session.flush()
        report_type = self.session.scalar(
            select(WhatsAppReportType).where(
                WhatsAppReportType.application_id == application.id,
                WhatsAppReportType.key == CRM_REPORT_TYPE_KEY,
            )
        )
        if report_type is None:
            report_type = WhatsAppReportType(
                application_id=application.id,
                key=CRM_REPORT_TYPE_KEY,
                name="Complete Complaint Packet",
                description="Final official letter followed by original complaint and accepted attachments.",
                artifact_kind="pdf",
            )
            self.session.add(report_type)
            self.session.flush()
        scopes: dict[str, WhatsAppRecipientScope] = {}
        for channel, key, name in (
            ("individual", "crm_official_contact", "CRM official contact"),
            ("group", "crm_official_group", "CRM official group"),
        ):
            scope = self.session.scalar(
                select(WhatsAppRecipientScope).where(
                    WhatsAppRecipientScope.application_id == application.id,
                    WhatsAppRecipientScope.channel == channel,
                    WhatsAppRecipientScope.key == key,
                )
            )
            if scope is None:
                scope = WhatsAppRecipientScope(
                    application_id=application.id,
                    channel=channel,
                    key=key,
                    name=name,
                    description="Controlled CRM complaint dispatch destination.",
                )
                self.session.add(scope)
                self.session.flush()
            scopes[channel] = scope
        template = self.session.scalar(
            select(WhatsAppTemplate).where(WhatsAppTemplate.key == CRM_TEMPLATE_KEY)
        )
        if template is None:
            template = WhatsAppTemplate(
                application_id=application.id,
                report_type_id=report_type.id,
                recipient_scope_id=scopes["individual"].id,
                recipient_channel="any",
                key=CRM_TEMPLATE_KEY,
                name="CRM complete complaint packet",
                category="complaint_dispatch",
                body=DEFAULT_MESSAGE,
            )
            self.session.add(template)
        self.session.commit()
        return {
            "application": application,
            "report_type": report_type,
            "scopes": scopes,
            "template": template,
            "account": account,
        }

    # ------------------------------------------------------------------
    # Destination profiles
    # ------------------------------------------------------------------
    def profiles(self, *, search: str = "", include_inactive: bool = False) -> list[dict[str, Any]]:
        defaults = self.ensure_defaults()
        filters: list[Any] = [WhatsAppDispatchProfile.application_id == defaults["application"].id]
        if not include_inactive:
            filters.append(WhatsAppDispatchProfile.enabled.is_(True))
        if search.strip():
            pattern = f"%{search.strip()}%"
            filters.append(or_(WhatsAppDispatchProfile.name.ilike(pattern), WhatsAppDispatchProfile.key.ilike(pattern)))
        records = self.session.scalars(
            select(WhatsAppDispatchProfile)
            .where(*filters)
            .order_by(WhatsAppDispatchProfile.enabled.desc(), WhatsAppDispatchProfile.name)
        ).all()
        return [self._profile_dict(record) for record in records]

    def _profile_dict(self, profile: WhatsAppDispatchProfile) -> dict[str, Any]:
        audience = self.session.get(WhatsAppAudience, profile.audience_id)
        template = self.session.get(WhatsAppTemplate, profile.template_id) if profile.template_id else None
        account = self.session.get(WhatsAppAccount, profile.account_id)
        members = list(self.session.scalars(
            select(WhatsAppAudienceMember)
            .where(WhatsAppAudienceMember.audience_id == profile.audience_id)
            .order_by(WhatsAppAudienceMember.created_at)
        ).all())
        targets = []
        for member in members:
            target_type, name, jid, available = _target_identity(self.session, member)
            targets.append({
                "member_id": str(member.id),
                "target_type": target_type,
                "target_id": str(member.directory_group_id or member.directory_contact_id),
                "name": name,
                "jid": jid,
                "available": available,
                "enabled": member.enabled,
            })
        policy = dict(profile.presentation_policy or {})
        return {
            "id": str(profile.id),
            "key": profile.key,
            "name": profile.name,
            "description": profile.notes,
            "account_id": str(profile.account_id),
            "account_name": account.name if account else "Unavailable",
            "audience_id": str(profile.audience_id),
            "audience_name": audience.name if audience else "Unavailable",
            "template_id": str(profile.template_id) if profile.template_id else None,
            "template_name": template.name if template else "No template",
            "template_body": template.body if template else "",
            "recipient_channel": profile.recipient_channel,
            "packet_policy": policy.get("packet_policy", "complete_pdf"),
            "privacy_policy": policy.get("privacy_policy", "full"),
            "office_level": policy.get("office_level", "both"),
            "allowed_directions": policy.get("allowed_directions", ["downward", "upward"]),
            "max_packet_bytes": int(policy.get("max_packet_bytes") or 15 * 1024 * 1024),
            "require_approval": profile.require_approval,
            "messages_per_minute": profile.messages_per_minute,
            "max_retries": profile.max_retries,
            "enabled": profile.enabled,
            "version": profile.version,
            "targets": targets,
            "updated_at": profile.updated_at,
        }

    def save_profile(
        self,
        *,
        profile_id: uuid.UUID | None,
        name: str,
        target_type: str,
        target_ids: list[uuid.UUID],
        template_body: str,
        packet_policy: str,
        privacy_policy: str,
        office_level: str = "both",
        allowed_directions: list[str] | None = None,
        max_packet_bytes: int = 15 * 1024 * 1024,
        require_approval: bool,
        messages_per_minute: int,
        max_retries: int,
        enabled: bool,
        actor: str,
    ) -> dict[str, Any]:
        defaults = self.ensure_defaults()
        if target_type not in {"contact", "group"}:
            raise CrmDispatchValidationError("Destination type must be contact or group")
        if not target_ids:
            raise CrmDispatchValidationError("Select at least one synchronized WhatsApp destination")
        if packet_policy != "complete_pdf":
            raise CrmDispatchValidationError("CRM complaint dispatch requires the complete complaint PDF")
        if privacy_policy not in {"full", "restricted"}:
            raise CrmDispatchValidationError("Unsupported privacy policy")
        if office_level not in {"lower_office", "higher_office", "both"}:
            raise CrmDispatchValidationError("Office level must be lower office, higher office or both")
        directions = sorted(set(allowed_directions or (["downward"] if office_level == "lower_office" else ["upward"] if office_level == "higher_office" else ["downward", "upward"])))
        if not directions or any(value not in {"downward", "upward"} for value in directions):
            raise CrmDispatchValidationError("Choose at least one supported dispatch direction")
        profile = self.session.get(WhatsAppDispatchProfile, profile_id) if profile_id else None
        if profile_id and profile is None:
            raise CrmDispatchNotFound("Destination profile not found")
        creating = profile is None
        if creating:
            base = _slug(name)
            key = base
            suffix = 2
            while self.session.scalar(select(WhatsAppDispatchProfile.id).where(
                WhatsAppDispatchProfile.application_id == defaults["application"].id,
                WhatsAppDispatchProfile.key == key,
            )):
                key = f"{base[:84]}_{suffix}"
                suffix += 1
            audience = WhatsAppAudience(
                application_id=defaults["application"].id,
                key=f"{key}_audience",
                name=f"{name} recipients",
                description="CRM-owned destination audience.",
            )
            self.session.add(audience)
            self.session.flush()
            template = WhatsAppTemplate(
                application_id=defaults["application"].id,
                report_type_id=defaults["report_type"].id,
                recipient_scope_id=defaults["scopes"]["individual" if target_type == "contact" else "group"].id,
                recipient_channel="individual" if target_type == "contact" else "group",
                key=f"{key}_message",
                name=f"{name} message",
                category="complaint_dispatch",
                body=template_body.strip() or DEFAULT_MESSAGE,
            )
            self.session.add(template)
            self.session.flush()
            profile = WhatsAppDispatchProfile(
                application_id=defaults["application"].id,
                key=key,
                name=name.strip(),
                report_type_id=defaults["report_type"].id,
                audience_id=audience.id,
                account_id=defaults["account"].id,
                template_id=template.id,
                recipient_scope_id=defaults["scopes"]["individual" if target_type == "contact" else "group"].id,
                recipient_channel="individual" if target_type == "contact" else "group",
                delivery_mode="individuals" if target_type == "contact" else "groups",
                delivery_granularity="recipient",
                guided_setup=True,
                owns_audience=True,
                owns_template=True,
            )
        else:
            audience = self.session.get(WhatsAppAudience, profile.audience_id)
            template = self.session.get(WhatsAppTemplate, profile.template_id) if profile.template_id else None
            if audience is None or template is None:
                raise CrmDispatchValidationError("Destination profile is missing its owned audience or template")
            profile.version += 1
            profile.recipient_channel = "individual" if target_type == "contact" else "group"
            profile.delivery_mode = "individuals" if target_type == "contact" else "groups"
            profile.recipient_scope_id = defaults["scopes"][profile.recipient_channel].id
            template.recipient_channel = profile.recipient_channel
            template.recipient_scope_id = profile.recipient_scope_id
            template.body = template_body.strip() or DEFAULT_MESSAGE
            template.updated_at = utcnow()
            self.session.add(template)
            existing = list(self.session.scalars(
                select(WhatsAppAudienceMember).where(WhatsAppAudienceMember.audience_id == audience.id)
            ).all())
            for member in existing:
                self.session.delete(member)
        profile.name = name.strip()
        profile.require_approval = require_approval
        profile.messages_per_minute = max(1, min(messages_per_minute, 120))
        profile.max_retries = max(0, min(max_retries, 20))
        profile.enabled = enabled
        profile.presentation_policy = {
            "packet_policy": packet_policy,
            "privacy_policy": privacy_policy,
            "office_level": office_level,
            "allowed_directions": directions,
            "max_packet_bytes": max(1024 * 1024, min(max_packet_bytes, 100 * 1024 * 1024)),
            "message_style": "official",
            "attachment_mode": "document",
        }
        profile.notes = f"CRM destination profile maintained by {actor}."
        profile.updated_at = utcnow()
        self.session.add(profile)
        self.session.flush()
        account_id = profile.account_id
        for target_id in target_ids:
            if target_type == "group":
                target = self.session.get(WhatsAppDirectoryGroup, target_id)
                if target is None or target.account_id != account_id or not target.available:
                    raise CrmDispatchValidationError("Select only available groups from the synchronized directory")
                member = WhatsAppAudienceMember(
                    audience_id=profile.audience_id,
                    target_type="group",
                    target_key=f"group:{target.id}",
                    directory_group_id=target.id,
                    route_scope_key="other",
                    route_scope_value="crm",
                    route_scope_label="CRM complaint dispatch",
                )
            else:
                target = self.session.get(WhatsAppDirectoryContact, target_id)
                if target is None or target.account_id != account_id or not target.active:
                    raise CrmDispatchValidationError("Select only active contacts from the synchronized directory")
                if not (target.phone_jid or target.primary_lid_jid):
                    raise CrmDispatchValidationError(f"{target.display_name or 'Contact'} has no usable WhatsApp identity")
                member = WhatsAppAudienceMember(
                    audience_id=profile.audience_id,
                    target_type="contact",
                    target_key=f"contact:{target.id}",
                    directory_contact_id=target.id,
                )
            self.session.add(member)
        self.session.commit()
        return self._profile_dict(profile)

    def set_profile_enabled(self, profile_id: uuid.UUID, enabled: bool) -> dict[str, Any]:
        profile = self.session.get(WhatsAppDispatchProfile, profile_id)
        defaults = self.ensure_defaults()
        if profile is None or profile.application_id != defaults["application"].id:
            raise CrmDispatchNotFound("Destination profile not found")
        profile.enabled = enabled
        profile.version += 1
        profile.updated_at = utcnow()
        self.session.add(profile)
        self.session.commit()
        return self._profile_dict(profile)

    # ------------------------------------------------------------------
    # Routing rules
    # ------------------------------------------------------------------
    def rules(self, *, include_inactive: bool = True) -> list[dict[str, Any]]:
        filters = [] if include_inactive else [CrmDispatchRule.enabled.is_(True)]
        rows = self.session.scalars(
            select(CrmDispatchRule)
            .where(*filters)
            .order_by(CrmDispatchRule.priority.desc(), CrmDispatchRule.name)
        ).all()
        return [self._rule_dict(row) for row in rows]

    def _rule_dict(self, rule: CrmDispatchRule) -> dict[str, Any]:
        profiles = []
        for value in rule.dispatch_profile_ids_json or []:
            try:
                profile = self.session.get(WhatsAppDispatchProfile, uuid.UUID(value))
            except (ValueError, TypeError):
                profile = None
            profiles.append({"id": value, "name": profile.name if profile else "Missing profile", "enabled": bool(profile and profile.enabled)})
        return {
            "id": str(rule.id),
            "name": rule.name,
            "description": rule.description,
            "priority": rule.priority,
            "selection_mode": rule.selection_mode,
            "conditions": rule.conditions_json,
            "profiles": profiles,
            "profile_ids": list(rule.dispatch_profile_ids_json or []),
            "stop_after_match": rule.stop_after_match,
            "enabled": rule.enabled,
            "version": rule.version,
            "updated_at": rule.updated_at,
        }

    def save_rule(
        self,
        *,
        rule_id: uuid.UUID | None,
        name: str,
        description: str | None,
        priority: int,
        selection_mode: str,
        conditions: dict[str, Any],
        profile_ids: list[uuid.UUID],
        stop_after_match: bool,
        enabled: bool,
        actor: str,
    ) -> dict[str, Any]:
        if selection_mode not in {"suggested", "manual_only", "fallback"}:
            raise CrmDispatchValidationError("Unsupported routing selection mode")
        if not profile_ids:
            raise CrmDispatchValidationError("Select at least one destination profile")
        valid_profiles = {item["id"] for item in self.profiles(include_inactive=False)}
        missing = [str(value) for value in profile_ids if str(value) not in valid_profiles]
        if missing:
            raise CrmDispatchValidationError("Routing rule contains missing or inactive destination profiles")
        rule = self.session.get(CrmDispatchRule, rule_id) if rule_id else None
        if rule_id and rule is None:
            raise CrmDispatchNotFound("Routing rule not found")
        if rule is None:
            duplicate = self.session.scalar(select(CrmDispatchRule.id).where(func.lower(CrmDispatchRule.name) == name.strip().casefold()))
            if duplicate:
                raise CrmDispatchValidationError("A routing rule with this name already exists")
            rule = CrmDispatchRule(name=name.strip(), created_by=actor)
        else:
            rule.version += 1
        rule.name = name.strip()
        rule.description = description or None
        rule.priority = priority
        rule.selection_mode = selection_mode
        rule.conditions_json = self._normalize_conditions(conditions)
        rule.dispatch_profile_ids_json = [str(value) for value in profile_ids]
        rule.stop_after_match = stop_after_match
        rule.enabled = enabled
        rule.updated_by = actor
        rule.updated_at = utcnow()
        self.session.add(rule)
        self.session.commit()
        return self._rule_dict(rule)

    def _normalize_conditions(self, value: dict[str, Any]) -> dict[str, Any]:
        allowed = {
            "categories", "subcategories", "tags_any", "tags_all", "exclude_tags",
            "tehsils", "districts", "sources", "institution_types", "confidentiality",
            "directions", "purposes",
        }
        result: dict[str, Any] = {}
        for key in allowed:
            raw = value.get(key)
            if raw in (None, "", []):
                continue
            if key == "confidentiality":
                result[key] = bool(raw)
            else:
                result[key] = sorted({str(item).strip().casefold() for item in list(raw) if str(item).strip()})
        return result

    def delete_rule(self, rule_id: uuid.UUID) -> None:
        rule = self.session.get(CrmDispatchRule, rule_id)
        if rule is None:
            raise CrmDispatchNotFound("Routing rule not found")
        rule.enabled = False
        rule.version += 1
        rule.updated_at = utcnow()
        self.session.add(rule)
        self.session.commit()

    def _case_context(self, case: ComplaintCase, *, direction: str = "downward", purpose: str = "") -> dict[str, Any]:
        tags = list(self.session.execute(
            select(CrmComplaintTag.name, CrmComplaintTag.group_name)
            .join(CrmComplaintTagLink, CrmComplaintTagLink.tag_id == CrmComplaintTag.id)
            .where(CrmComplaintTagLink.complaint_case_id == case.id, CrmComplaintTag.active.is_(True))
        ).all())
        tag_names = {str(name).casefold() for name, _group in tags}
        groups: dict[str, set[str]] = {}
        for name, group in tags:
            groups.setdefault(str(group).casefold(), set()).add(str(name).casefold())
        return {
            "category": (case.category or "").casefold(),
            "subcategory": (case.sub_category or "").casefold(),
            "tehsil": (case.tehsil or "").casefold(),
            "district": (case.district or "").casefold(),
            "source": (case.source_system or "").casefold(),
            "tags": tag_names,
            "tag_groups": groups,
            "confidentiality": "confidentiality-requested" in tag_names,
            "direction": direction.casefold(),
            "purpose": purpose.casefold(),
        }

    def _rule_matches(self, rule: CrmDispatchRule, context: dict[str, Any]) -> bool:
        c = dict(rule.conditions_json or {})
        checks = (
            ("categories", context["category"]),
            ("subcategories", context["subcategory"]),
            ("tehsils", context["tehsil"]),
            ("districts", context["district"]),
            ("sources", context["source"]),
        )
        for key, actual in checks:
            expected = set(c.get(key) or [])
            if expected and actual not in expected:
                return False
        tags = set(context["tags"])
        if c.get("tags_any") and not tags.intersection(c["tags_any"]):
            return False
        if c.get("tags_all") and not set(c["tags_all"]).issubset(tags):
            return False
        if c.get("exclude_tags") and tags.intersection(c["exclude_tags"]):
            return False
        if c.get("institution_types") and not set(c["institution_types"]).intersection(context["tag_groups"].get("institution", set())):
            return False
        if c.get("confidentiality") is True and not context["confidentiality"]:
            return False
        if c.get("directions") and context.get("direction") not in set(c["directions"]):
            return False
        if c.get("purposes") and context.get("purpose") not in set(c["purposes"]):
            return False
        return True

    def test_routing(self, case_id: uuid.UUID, *, direction: str = "downward", purpose: str = "compliance_request") -> dict[str, Any]:
        case = self.session.get(ComplaintCase, case_id)
        if case is None:
            raise CrmDispatchNotFound("Complaint not found")
        if direction not in {"downward", "upward"}:
            raise CrmDispatchValidationError("Dispatch direction must be downward or upward")
        context = self._case_context(case, direction=direction, purpose=purpose)
        matches = [rule for rule in self.session.scalars(
            select(CrmDispatchRule).where(CrmDispatchRule.enabled.is_(True)).order_by(CrmDispatchRule.priority.desc())
        ).all() if self._rule_matches(rule, context)]
        resolution = self._resolve_rule_set(matches)
        selected_profiles = []
        for value in resolution.get("profile_ids", []):
            try:
                profile = self.session.get(WhatsAppDispatchProfile, uuid.UUID(value))
            except (TypeError, ValueError):
                profile = None
            if profile is not None:
                selected_profiles.append({"id": str(profile.id), "name": profile.name, "enabled": profile.enabled})
        return {
            "complaint_number": case.complaint_number,
            "context": {**context, "tags": sorted(context["tags"]), "tag_groups": {k: sorted(v) for k, v in context["tag_groups"].items()}},
            "matches": [self._rule_dict(item) for item in matches],
            "resolution": resolution,
            "selected_profiles": selected_profiles,
            "conflict": resolution["status"] == "conflict",
            "explanation": resolution["reason"],
        }

    def _resolve_rule_set(self, matches: list[CrmDispatchRule]) -> dict[str, Any]:
        suggested = [item for item in matches if item.selection_mode == "suggested"]
        fallback = [item for item in matches if item.selection_mode == "fallback"]
        selected = suggested or fallback
        if not selected:
            return {"status": "needs_review", "profile_ids": [], "rule_ids": [], "reason": "No enabled routing rule matched"}
        top_priority = max(item.priority for item in selected)
        top = [item for item in selected if item.priority == top_priority]
        profile_sets = {tuple(sorted(item.dispatch_profile_ids_json or [])) for item in top}
        if len(profile_sets) > 1:
            return {
                "status": "conflict",
                "profile_ids": [],
                "rule_ids": [str(item.id) for item in top],
                "reason": "Equal-priority routing rules resolve to different destination profiles",
            }
        profiles = sorted({value for item in top for value in item.dispatch_profile_ids_json or []})
        return {
            "status": "ready",
            "profile_ids": profiles,
            "rule_ids": [str(item.id) for item in top],
            "selection_source": "fallback" if all(item.selection_mode == "fallback" for item in top) else "rule",
            "reason": f"Matched {len(top)} routing rule(s) at priority {top_priority}",
        }

    # ------------------------------------------------------------------
    # Dispatch batches and previews
    # ------------------------------------------------------------------
    def eligible_sources(
        self, *, direction: str, search: str = "", page: int = 1, page_size: int = 200
    ) -> dict[str, Any]:
        if direction not in {"downward", "upward"}:
            raise CrmDispatchValidationError("Dispatch direction must be downward or upward")
        pattern = f"%{search.strip()}%"
        if direction == "upward":
            filters: list[Any] = [CrmOfficialLetter.status == "finalized"]
            if search.strip():
                filters.append(or_(CrmOfficialLetter.letter_number.ilike(pattern), CrmOfficialLetter.complaint_number_snapshot.ilike(pattern)))
            total = int(self.session.scalar(select(func.count()).select_from(CrmOfficialLetter).where(*filters)) or 0)
            rows = list(self.session.scalars(
                select(CrmOfficialLetter).where(*filters).order_by(CrmOfficialLetter.created_at.desc())
                .offset((page - 1) * page_size).limit(page_size)
            ).all())
            items = []
            for letter in rows:
                artifact = self.session.scalar(select(CrmOfficialLetterArtifact).where(
                    CrmOfficialLetterArtifact.official_letter_id == letter.id,
                    CrmOfficialLetterArtifact.kind == "complete_pdf",
                ))
                items.append({
                    "id": str(letter.id), "source_kind": "official_letter",
                    "case_id": str(letter.complaint_case_id),
                    "complaint_number": letter.complaint_number_snapshot,
                    "letter_number": letter.letter_number, "letter_date": letter.letter_date,
                    "ready": bool(artifact and Path(artifact.path).is_file()),
                    "packet_size_bytes": artifact.size_bytes if artifact else 0,
                    "packet_label": "Complete compliance PDF" if artifact else "Complete PDF missing",
                })
            return {"items": items, "total": total, "page": page, "page_size": page_size}

        current_approved = select(ComplaintReplyRevision.complaint_case_id).where(
            ComplaintReplyRevision.is_current.is_(True),
            ComplaintReplyRevision.approval_status.in_(["Approved", "Issued"]),
        )
        filters = [
            ComplaintCase.state.in_(["fresh", "published", "review_required"]),
            ComplaintCase.id.not_in(current_approved),
        ]
        if search.strip():
            filters.append(or_(ComplaintCase.complaint_number.ilike(pattern), ComplaintCase.remarks.ilike(pattern)))
        total = int(self.session.scalar(select(func.count()).select_from(ComplaintCase).where(*filters)) or 0)
        rows = list(self.session.scalars(
            select(ComplaintCase).where(*filters).order_by(ComplaintCase.created_at.desc())
            .offset((page - 1) * page_size).limit(page_size)
        ).all())
        items = []
        for case in rows:
            document_count = int(self.session.scalar(
                select(func.count()).select_from(ComplaintDocumentCaseLink)
                .where(ComplaintDocumentCaseLink.complaint_case_id == case.id)
            ) or 0)
            items.append({
                "id": str(case.id), "source_kind": "complaint_case", "case_id": str(case.id),
                "complaint_number": case.complaint_number or str(case.id),
                "category": case.category or "", "subcategory": case.sub_category or "",
                "state": case.state, "tehsil": case.tehsil, "district": case.district,
                "ready": True, "document_count": document_count,
                "packet_label": f"Complaint record · {document_count} linked file(s)",
            })
        return {"items": items, "total": total, "page": page, "page_size": page_size}

    def _store_dispatch_artifact(
        self, *, batch: CrmDispatchBatch, item: CrmDispatchItem, kind: str,
        name: str, content: bytes | None = None, source_path: Path | None = None,
        source_snapshot: dict[str, Any] | None = None,
    ) -> CrmDispatchArtifact:
        if content is None and source_path is None:
            raise CrmDispatchValidationError("Dispatch packet content is unavailable")
        if source_path is not None:
            if not source_path.is_file():
                raise CrmDispatchValidationError("Dispatch packet file is unavailable")
            content = source_path.read_bytes()
        assert content is not None
        digest = hashlib.sha256(content).hexdigest()
        destination = self.packet_root / str(batch.id) / f"{item.id}-{_slug(kind)}.pdf"
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(content)
        artifact = CrmDispatchArtifact(
            batch_id=batch.id, dispatch_item_id=item.id, complaint_case_id=item.complaint_case_id,
            kind=kind, name=name, path=str(destination), content_type="application/pdf",
            size_bytes=len(content), sha256=digest, page_count=_page_count(destination),
            source_snapshot_json=source_snapshot or {},
        )
        self.session.add(artifact)
        self.session.flush()
        item.packet_artifact_id = artifact.id
        item.packet_sha256 = digest
        item.packet_size_bytes = artifact.size_bytes
        item.packet_page_count = artifact.page_count
        self.session.add(item)
        return artifact

    def create_batch(
        self,
        *,
        official_letter_ids: list[uuid.UUID] | None = None,
        case_ids: list[uuid.UUID] | None = None,
        direction: str = "upward",
        actor: str,
        purpose: str | None = None,
        response_due_at: datetime | None = None,
    ) -> dict[str, Any]:
        if direction not in {"downward", "upward"}:
            raise CrmDispatchValidationError("Dispatch direction must be downward or upward")
        downward_purposes = {"compliance_request", "report_request", "evidence_request", "clarification_request", "information_only"}
        upward_purposes = {"compliance_submission", "reply_submission", "follow_up_submission", "information_only"}
        allowed = downward_purposes if direction == "downward" else upward_purposes
        purpose = purpose or ("compliance_request" if direction == "downward" else "compliance_submission")
        if purpose not in allowed:
            raise CrmDispatchValidationError("The selected dispatch purpose is not valid for this direction")
        selected = list(dict.fromkeys(case_ids or [])) if direction == "downward" else list(dict.fromkeys(official_letter_ids or []))
        if not selected:
            raise CrmDispatchValidationError(
                "Select at least one pending complaint" if direction == "downward"
                else "Select at least one finalized official letter"
            )
        now = utcnow()
        prefix = now.strftime("CRM-DSP-%Y%m%d-%H%M%S")
        suffix = uuid.uuid4().hex[:6].upper()
        batch = CrmDispatchBatch(
            batch_number=f"{prefix}-{suffix}", status="resolving_routes",
            direction=direction, source_mode="complaint_cases" if direction == "downward" else "official_letters",
            total_items=len(selected), purpose=purpose, response_due_at=response_due_at, created_by=actor,
        )
        self.session.add(batch)
        self.session.flush()
        packet_service = OfficialLetterService(self.session, self.settings)
        if direction == "downward":
            for case_id in selected:
                case = self.session.get(ComplaintCase, case_id)
                if case is None or case.state not in {"fresh", "published", "review_required"}:
                    raise CrmDispatchValidationError("Only active fresh or published complaints can be assigned for compliance")
                approved = self.session.scalar(select(ComplaintReplyRevision.id).where(
                    ComplaintReplyRevision.complaint_case_id == case.id,
                    ComplaintReplyRevision.is_current.is_(True),
                    ComplaintReplyRevision.approval_status.in_(["Approved", "Issued"]),
                ))
                if approved:
                    raise CrmDispatchValidationError(
                        f"Complaint {case.complaint_number or case.id} already has an approved reply; submit it upward instead"
                    )
                item = CrmDispatchItem(
                    batch_id=batch.id, complaint_case_id=case.id,
                    complaint_number_snapshot=case.complaint_number or str(case.id),
                    packet_sha256="pending", compliance_status="not_requested",
                )
                self.session.add(item)
                self.session.flush()
                content, snapshot = packet_service.compose_case_packet(case.id)
                self._store_dispatch_artifact(
                    batch=batch, item=item, kind="assignment_packet",
                    name=f"{item.complaint_number_snapshot} - Complaint Assignment Packet.pdf",
                    content=content, source_snapshot=snapshot,
                )
        else:
            for letter_id in selected:
                letter = self.session.get(CrmOfficialLetter, letter_id)
                if letter is None or letter.status != "finalized":
                    raise CrmDispatchValidationError("Only finalized official letters can be submitted upward")
                artifact = self.session.scalar(select(CrmOfficialLetterArtifact).where(
                    CrmOfficialLetterArtifact.official_letter_id == letter.id,
                    CrmOfficialLetterArtifact.kind == "complete_pdf",
                ))
                if artifact is None or not Path(artifact.path).is_file():
                    raise CrmDispatchValidationError(
                        f"Build the complete PDF for complaint {letter.complaint_number_snapshot} before submission"
                    )
                path = Path(artifact.path)
                if sha256_file(path) != artifact.sha256:
                    raise CrmDispatchValidationError(
                        f"Complete PDF checksum changed for complaint {letter.complaint_number_snapshot}; rebuild it"
                    )
                item = CrmDispatchItem(
                    batch_id=batch.id, complaint_case_id=letter.complaint_case_id,
                    official_letter_id=letter.id, complete_pdf_artifact_id=artifact.id,
                    complaint_number_snapshot=letter.complaint_number_snapshot,
                    letter_number_snapshot=letter.letter_number, letter_date_snapshot=letter.letter_date,
                    packet_sha256=artifact.sha256, packet_size_bytes=artifact.size_bytes,
                    packet_page_count=_page_count(path), compliance_status="incorporated",
                )
                self.session.add(item)
                self.session.flush()
                self._store_dispatch_artifact(
                    batch=batch, item=item, kind="submission_packet",
                    name=f"{item.complaint_number_snapshot} - Complete Compliance Packet.pdf",
                    source_path=path, source_snapshot={
                        "official_letter_id": str(letter.id), "official_letter_artifact_id": str(artifact.id),
                        "letter_number": letter.letter_number, "letter_date": letter.letter_date.isoformat(),
                    },
                )
        self.session.commit()
        self.resolve_batch(batch.id)
        return self.detail(batch.id)

    def resolve_batch(self, batch_id: uuid.UUID) -> dict[str, Any]:
        batch = self.session.get(CrmDispatchBatch, batch_id)
        if batch is None:
            raise CrmDispatchNotFound("Dispatch batch not found")
        items = list(self.session.scalars(select(CrmDispatchItem).where(CrmDispatchItem.batch_id == batch.id)).all())
        enabled_rules = list(self.session.scalars(
            select(CrmDispatchRule).where(CrmDispatchRule.enabled.is_(True)).order_by(CrmDispatchRule.priority.desc())
        ).all())
        for item in items:
            if item.excluded:
                item.route_status = "excluded"
                self.session.add(item)
                continue
            for existing in self.session.scalars(select(CrmDispatchTarget).where(CrmDispatchTarget.dispatch_item_id == item.id)).all():
                if existing.selection_source != "manual":
                    self.session.delete(existing)
            case = self.session.get(ComplaintCase, item.complaint_case_id)
            if case is None:
                item.route_status = "blocked"
                item.route_summary_json = {"reason": "Complaint record is missing"}
                self.session.add(item)
                continue
            matches = [rule for rule in enabled_rules if self._rule_matches(rule, self._case_context(case, direction=batch.direction, purpose=batch.purpose))]
            resolution = self._resolve_rule_set(matches)
            if resolution["status"] == "ready":
                permitted_profile_ids: list[str] = []
                rejected_profile_names: list[str] = []
                for profile_value in resolution["profile_ids"]:
                    profile = self.session.get(WhatsAppDispatchProfile, uuid.UUID(profile_value))
                    policy = dict(profile.presentation_policy or {}) if profile else {}
                    allowed_directions = set(policy.get("allowed_directions") or ["downward", "upward"])
                    if profile and profile.enabled and batch.direction in allowed_directions:
                        permitted_profile_ids.append(profile_value)
                    else:
                        rejected_profile_names.append(profile.name if profile else profile_value)
                if not permitted_profile_ids:
                    resolution = {
                        **resolution,
                        "status": "needs_review",
                        "profile_ids": [],
                        "reason": "Matched destination profiles are not enabled for this dispatch direction",
                        "rejected_profiles": rejected_profile_names,
                    }
                else:
                    resolution = {**resolution, "profile_ids": permitted_profile_ids}
            item.route_status = resolution["status"]
            item.route_summary_json = resolution
            if resolution["status"] == "ready":
                rule_id = uuid.UUID(resolution["rule_ids"][0]) if resolution["rule_ids"] else None
                for profile_value in resolution["profile_ids"]:
                    profile_id = uuid.UUID(profile_value)
                    existing = self.session.scalar(select(CrmDispatchTarget).where(
                        CrmDispatchTarget.dispatch_item_id == item.id,
                        CrmDispatchTarget.dispatch_profile_id == profile_id,
                    ))
                    if existing is None:
                        self.session.add(CrmDispatchTarget(
                            dispatch_item_id=item.id,
                            routing_rule_id=rule_id,
                            dispatch_profile_id=profile_id,
                            selection_source=resolution.get("selection_source", "rule"),
                            response_due_at=batch.response_due_at,
                        ))
            self.session.add(item)
        self.session.flush()
        self._update_batch_counts(batch)
        batch.status = "ready" if batch.blocked_items == 0 and batch.ready_items == batch.total_items else "review_required"
        batch.updated_at = utcnow()
        self.session.add(batch)
        self.session.commit()
        return self.detail(batch.id)

    def set_manual_route(
        self,
        *,
        item_id: uuid.UUID,
        profile_ids: list[uuid.UUID],
        reason: str,
        actor: str,
    ) -> dict[str, Any]:
        item = self.session.get(CrmDispatchItem, item_id)
        if item is None:
            raise CrmDispatchNotFound("Dispatch item not found")
        batch = self.session.get(CrmDispatchBatch, item.batch_id)
        if batch is None:
            raise CrmDispatchNotFound("Dispatch batch not found")
        available = {profile["id"]: profile for profile in self.profiles(include_inactive=False)}
        if not profile_ids or any(str(value) not in available for value in profile_ids):
            raise CrmDispatchValidationError("Select one or more enabled CRM destination profiles")
        invalid_direction = [
            available[str(value)]["name"] for value in profile_ids
            if batch.direction not in set(available[str(value)].get("allowed_directions") or [])
        ]
        if invalid_direction:
            raise CrmDispatchValidationError(
                "These profiles are not enabled for this dispatch direction: " + ", ".join(invalid_direction)
            )
        if not reason.strip():
            raise CrmDispatchValidationError("Record a reason for the manual routing override")
        for target in self.session.scalars(select(CrmDispatchTarget).where(CrmDispatchTarget.dispatch_item_id == item.id)).all():
            self.session.delete(target)
        for profile_id in profile_ids:
            self.session.add(CrmDispatchTarget(
                dispatch_item_id=item.id,
                dispatch_profile_id=profile_id,
                selection_source="manual",
                manual_override_reason=reason.strip() or f"Manual routing selected by {actor}",
            ))
        item.route_status = "ready"
        item.route_summary_json = {"status": "ready", "profile_ids": [str(value) for value in profile_ids], "reason": reason or "Manual route"}
        item.updated_at = utcnow()
        self.session.add(item)
        self._update_batch_counts(batch)
        batch.status = "ready" if batch.blocked_items == 0 and batch.ready_items == batch.total_items else "review_required"
        batch.updated_at = utcnow()
        self.session.add(batch)
        self.session.commit()
        return self.detail(batch.id)

    def set_item_excluded(self, item_id: uuid.UUID, *, excluded: bool, reason: str) -> dict[str, Any]:
        item = self.session.get(CrmDispatchItem, item_id)
        if item is None:
            raise CrmDispatchNotFound("Dispatch item not found")
        item.excluded = excluded
        item.exclusion_reason = reason or None
        item.route_status = "excluded" if excluded else "needs_review"
        item.updated_at = utcnow()
        self.session.add(item)
        batch = self.session.get(CrmDispatchBatch, item.batch_id)
        assert batch is not None
        self._update_batch_counts(batch)
        batch.status = "ready" if batch.blocked_items == 0 and batch.ready_items + batch.excluded_items == batch.total_items else "review_required"
        batch.updated_at = utcnow()
        self.session.add(batch)
        self.session.commit()
        return self.detail(batch.id)

    def compile_previews(self, batch_id: uuid.UUID, *, actor: str) -> dict[str, Any]:
        defaults = self.ensure_defaults()
        batch = self.session.get(CrmDispatchBatch, batch_id)
        if batch is None:
            raise CrmDispatchNotFound("Dispatch batch not found")
        items = list(self.session.scalars(select(CrmDispatchItem).where(
            CrmDispatchItem.batch_id == batch.id,
            CrmDispatchItem.excluded.is_(False),
        )).all())
        if not items or any(item.route_status != "ready" for item in items):
            raise CrmDispatchValidationError("Resolve every non-excluded complaint route before compiling previews")
        targets = list(self.session.scalars(
            select(CrmDispatchTarget)
            .join(CrmDispatchItem, CrmDispatchItem.id == CrmDispatchTarget.dispatch_item_id)
            .where(CrmDispatchItem.batch_id == batch.id, CrmDispatchTarget.business_status.in_(["planned", "blocked"]))
        ).all())
        if not targets:
            raise CrmDispatchValidationError("This batch has no resolved destination targets")
        # Remove unapproved previews from a previous compilation. Approved history is immutable.
        old_preview_ids = {target.preview_id for target in targets if target.preview_id}
        for preview_id in old_preview_ids:
            approval = self.session.scalar(select(WhatsAppDispatchApproval).where(WhatsAppDispatchApproval.preview_id == preview_id))
            if approval:
                raise CrmDispatchValidationError("An approved preview already exists. Create a new dispatch batch for changed routing")
            preview = self.session.get(WhatsAppDispatchPreview, preview_id)
            if preview:
                for row in self.session.scalars(select(WhatsAppDispatchPreviewDelivery).where(WhatsAppDispatchPreviewDelivery.preview_id == preview.id)).all():
                    self.session.delete(row)
                for row in self.session.scalars(select(WhatsAppDispatchPreviewArtifact).where(WhatsAppDispatchPreviewArtifact.preview_id == preview.id)).all():
                    self.session.delete(row)
                self.session.delete(preview)
        self.session.flush()
        for target in targets:
            target.preview_id = None
            target.preview_delivery_ids_json = []
            target.recipient_snapshot_json = []
            target.message_snapshot = ""
            target.message_sha256 = ""
            target.business_status = "planned"
            target.error = None
            self.session.add(target)
        self.session.flush()

        by_profile: dict[uuid.UUID, list[CrmDispatchTarget]] = {}
        for target in targets:
            by_profile.setdefault(target.dispatch_profile_id, []).append(target)
        compiled = []
        for profile_id, profile_targets in by_profile.items():
            profile = self.session.get(WhatsAppDispatchProfile, profile_id)
            if profile is None or not profile.enabled:
                for target in profile_targets:
                    target.business_status = "blocked"
                    target.error = "Destination profile is missing or disabled"
                    self.session.add(target)
                continue
            audience = self.session.get(WhatsAppAudience, profile.audience_id)
            account = self.session.get(WhatsAppAccount, profile.account_id)
            template = self.session.get(WhatsAppTemplate, profile.template_id) if profile.template_id else defaults["template"]
            members = list(self.session.scalars(select(WhatsAppAudienceMember).where(
                WhatsAppAudienceMember.audience_id == profile.audience_id,
                WhatsAppAudienceMember.enabled.is_(True),
            )).all())
            profile_snapshot = {
                "profile": {"id": str(profile.id), "key": profile.key, "name": profile.name, "version": profile.version},
                "audience": {
                    "id": str(audience.id) if audience else None,
                    "name": audience.name if audience else "Missing audience",
                    "target_keys": sorted(member.target_key for member in members),
                    "target_routes": sorted(f"{member.target_key}:{member.route_scope_key}:{member.route_scope_value}" for member in members),
                },
                "account": {"id": str(account.id) if account else None, "name": account.name if account else "Missing account", "worker_key": account.worker_key if account else None},
                "template": {"id": str(template.id) if template else None, "name": template.name if template else "", "body": template.body if template else ""},
            }
            preview = WhatsAppDispatchPreview(
                preview_key=f"{batch.batch_number}-{profile.key}-{uuid.uuid4().hex[:6]}",
                application_id=defaults["application"].id,
                source_job_id=None,
                source_kind="crm_dispatch_batch",
                source_reference_id=batch.id,
                source_revision=1,
                dispatch_profile_id=profile.id,
                status="blocked",
                profile_version=profile.version,
                application_name=defaults["application"].name,
                report_type_name=defaults["report_type"].name,
                audience_name=audience.name if audience else "Missing audience",
                profile_name=profile.name,
                account_name=account.name if account else "Missing account",
                template_name=template.name if template else "",
                configuration_snapshot=profile_snapshot,
                created_by=actor,
            )
            self.session.add(preview)
            self.session.flush()
            sequence = 0
            all_issues: list[dict[str, Any]] = []
            for target in profile_targets:
                item = self.session.get(CrmDispatchItem, target.dispatch_item_id)
                assert item is not None
                artifact = self.session.get(CrmDispatchArtifact, item.packet_artifact_id) if item.packet_artifact_id else None
                letter = self.session.get(CrmOfficialLetter, item.official_letter_id) if item.official_letter_id else None
                case = self.session.get(ComplaintCase, item.complaint_case_id)
                if artifact is None or case is None:
                    target.business_status = "blocked"
                    target.error = "Dispatch packet or complaint record is missing"
                    self.session.add(target)
                    continue
                policy = dict(profile.presentation_policy or {})
                allowed_directions = set(policy.get("allowed_directions") or ["downward", "upward"])
                if batch.direction not in allowed_directions:
                    target.business_status = "blocked"
                    target.error = f"Destination profile does not allow {batch.direction} dispatch"
                    self.session.add(target)
                    continue
                privacy_policy = str(policy.get("privacy_policy") or "full")
                packet_policy = str(policy.get("packet_policy") or "complete_pdf")
                case_context = self._case_context(case)
                if packet_policy != "complete_pdf":
                    target.business_status = "blocked"
                    target.error = "CRM dispatch currently requires the complete complaint PDF"
                    self.session.add(target)
                    continue
                if case_context["confidentiality"] and privacy_policy != "restricted":
                    target.business_status = "blocked"
                    target.error = "Confidential complaints require a restricted destination profile"
                    self.session.add(target)
                    continue
                source_path = Path(artifact.path)
                if not source_path.is_file() or sha256_file(source_path) != item.packet_sha256:
                    target.business_status = "blocked"
                    target.error = "Complete PDF is missing or changed; rebuild the packet"
                    self.session.add(target)
                    continue
                max_bytes = int((profile.presentation_policy or {}).get("max_packet_bytes") or 15 * 1024 * 1024)
                if source_path.stat().st_size > max_bytes:
                    target.business_status = "blocked"
                    target.error = f"Complete PDF exceeds the profile limit of {max_bytes // (1024*1024)} MB"
                    self.session.add(target)
                    continue
                frozen_path = freeze_artifact(source_path, item.packet_sha256)
                preview_artifact = WhatsAppDispatchPreviewArtifact(
                    preview_id=preview.id,
                    report_type_id=defaults["report_type"].id,
                    role="delivery",
                    name=(
                        f"{item.complaint_number_snapshot} - Complaint Assignment Packet.pdf"
                        if batch.direction == "downward"
                        else f"{item.complaint_number_snapshot} - Complete Compliance Packet.pdf"
                    ),
                    path_snapshot=str(frozen_path),
                    mime_type="application/pdf",
                    size_bytes=frozen_path.stat().st_size,
                    checksum_sha256=item.packet_sha256,
                    status="ready",
                )
                self.session.add(preview_artifact)
                self.session.flush()
                values = {
                    "recipient_name": "Sir/Madam",
                    "complaint_number": item.complaint_number_snapshot,
                    "category": case.category or "",
                    "subcategory": case.sub_category or "",
                    "letter_number": item.letter_number_snapshot or "",
                    "letter_date": item.letter_date_snapshot.strftime("%d/%m/%Y") if item.letter_date_snapshot else "",
                    "packet_page_count": item.packet_page_count,
                    "response_due_date": batch.response_due_at.strftime("%d/%m/%Y") if batch.response_due_at else "",
                    "dispatch_purpose": batch.purpose,
                    "dispatch_purpose_label": batch.purpose.replace("_", " "),
                    "dispatch_direction": batch.direction,
                    "instructions": (
                        "Please provide the required report, reply and supporting evidence."
                        if batch.direction == "downward"
                        else "Compliance is submitted for kind consideration."
                    ),
                }
                message = _render_message(template.body if template else DEFAULT_MESSAGE, values)
                recipient_snapshot = []
                preview_delivery_ids = []
                for member in members:
                    target_type, name, jid, available = _target_identity(self.session, member)
                    sequence += 1
                    issues: list[dict[str, Any]] = []
                    status = "ready"
                    if not available or not jid:
                        status = "blocked"
                        issues.append({"code": "destination_unavailable", "severity": "blocked", "message": f"{name} is unavailable in the synchronized WhatsApp directory."})
                    idempotency_key = _json_hash({
                        "case": str(item.complaint_case_id),
                        "letter": str(item.official_letter_id) if item.official_letter_id else None,
                        "direction": batch.direction,
                        "purpose": batch.purpose,
                        "packet": item.packet_sha256,
                        "target": jid,
                        "message": _hash_text(message),
                    })
                    previous = self.session.scalar(
                        select(WhatsAppDelivery)
                        .join(WhatsAppDispatchPreviewDelivery, WhatsAppDispatchPreviewDelivery.id == WhatsAppDelivery.preview_delivery_id)
                        .where(
                            WhatsAppDispatchPreviewDelivery.idempotency_key == idempotency_key,
                            WhatsAppDelivery.status.in_(["queued", "sent_pending_confirmation", "delivered"]),
                        )
                    )
                    if previous:
                        status = "skipped"
                        issues.append({"code": "duplicate_dispatch", "severity": "warning", "message": f"This exact packet and message was already queued or delivered to {name}."})
                    delivery = WhatsAppDispatchPreviewDelivery(
                        preview_id=preview.id,
                        sequence=sequence,
                        source_route_key=str(target.id),
                        target_type=target_type,
                        target_name=name,
                        target_jid=jid,
                        directory_group_id=member.directory_group_id,
                        directory_contact_id=member.directory_contact_id,
                        route_kind="crm_complaint",
                        route_scope="complaint",
                        message=message,
                        attachment_ids=[str(preview_artifact.id)],
                        routing_snapshot={
                            "crm_dispatch_batch_id": str(batch.id),
                            "crm_dispatch_item_id": str(item.id),
                            "crm_dispatch_target_id": str(target.id),
                            "complaint_number": item.complaint_number_snapshot,
                            "official_letter_id": str(item.official_letter_id) if item.official_letter_id else None,
                            "direction": batch.direction,
                            "purpose": batch.purpose,
                            "packet_sha256": item.packet_sha256,
                            "profile_version": profile.version,
                        },
                        issues=issues,
                        status=status,
                        idempotency_key=idempotency_key,
                    )
                    self.session.add(delivery)
                    self.session.flush()
                    preview_delivery_ids.append(str(delivery.id))
                    recipient_snapshot.append({"name": name, "jid": jid, "target_type": target_type, "status": status})
                target.preview_id = preview.id
                target.preview_delivery_ids_json = preview_delivery_ids
                target.recipient_snapshot_json = recipient_snapshot
                target.message_snapshot = message
                target.message_sha256 = _hash_text(message)
                if not members:
                    target.business_status = "blocked"
                    target.error = "Destination profile has no enabled recipients"
                    all_issues.append({"code": "empty_audience", "severity": "blocked", "message": f"{profile.name} has no enabled recipients."})
                elif any(item["status"] == "ready" for item in recipient_snapshot):
                    target.business_status = "planned"
                else:
                    target.business_status = "blocked"
                    target.error = "No eligible recipient remains"
                self.session.add(target)
            deliveries = list(self.session.scalars(select(WhatsAppDispatchPreviewDelivery).where(WhatsAppDispatchPreviewDelivery.preview_id == preview.id)).all())
            artifacts = list(self.session.scalars(select(WhatsAppDispatchPreviewArtifact).where(WhatsAppDispatchPreviewArtifact.preview_id == preview.id)).all())
            preview.artifact_count = len(artifacts)
            preview.issues = all_issues
            apply_preview_state(
                preview,
                summarize_preview_state(deliveries, preview.issues, artifacts),
            )
            preview.content_sha256 = _json_hash({
                "snapshot": preview.configuration_snapshot,
                "deliveries": [
                    {"target": item.target_jid, "message": item.message, "attachments": item.attachment_ids, "status": item.status}
                    for item in deliveries
                ],
            })
            self.session.add(preview)
            compiled.append(str(preview.id))
        batch.updated_at = utcnow()
        batch.status = "ready" if compiled and all(target.business_status == "planned" for target in targets) else "review_required"
        self._update_batch_counts(batch)
        self.session.add(batch)
        self.session.commit()
        return self.detail(batch.id)

    def _update_batch_counts(self, batch: CrmDispatchBatch) -> None:
        items = list(self.session.scalars(select(CrmDispatchItem).where(CrmDispatchItem.batch_id == batch.id)).all())
        batch.total_items = len(items)
        batch.ready_items = sum(item.route_status == "ready" and not item.excluded for item in items)
        batch.excluded_items = sum(item.excluded for item in items)
        batch.blocked_items = sum(item.route_status in {"blocked", "conflict", "needs_review"} and not item.excluded for item in items)
        targets = list(self.session.scalars(
            select(CrmDispatchTarget)
            .join(CrmDispatchItem, CrmDispatchItem.id == CrmDispatchTarget.dispatch_item_id)
            .where(CrmDispatchItem.batch_id == batch.id)
        ).all())
        batch.queued_items = sum(item.business_status in {"approved", "queued", "sent_pending_confirmation"} for item in targets)
        batch.successful_items = sum(item.business_status == "delivered" for item in targets)
        batch.failed_items = sum(item.business_status in {"failed", "timed_out"} for item in targets)

    def refresh(self, batch_id: uuid.UUID) -> dict[str, Any]:
        batch = self.session.get(CrmDispatchBatch, batch_id)
        if batch is None:
            raise CrmDispatchNotFound("Dispatch batch not found")
        targets = list(self.session.scalars(
            select(CrmDispatchTarget)
            .join(CrmDispatchItem, CrmDispatchItem.id == CrmDispatchTarget.dispatch_item_id)
            .where(CrmDispatchItem.batch_id == batch.id)
        ).all())
        for target in targets:
            if not target.preview_id:
                continue
            approval = self.session.scalar(select(WhatsAppDispatchApproval).where(WhatsAppDispatchApproval.preview_id == target.preview_id))
            if approval is None:
                continue
            deliveries = list(self.session.scalars(select(WhatsAppDelivery).where(WhatsAppDelivery.approval_id == approval.id)).all())
            target.whatsapp_delivery_ids_json = [str(item.id) for item in deliveries]
            statuses = {item.status for item in deliveries}
            if not deliveries and approval.delivery_count == 0:
                target.business_status = "blocked"
                target.error = approval.error or "No new WhatsApp delivery was queued; the frozen recipient was blocked, excluded, or already sent."
            elif "failed" in statuses:
                target.business_status = "failed"
                target.error = next((item.error for item in deliveries if item.error), None)
            elif "timed_out" in statuses:
                target.business_status = "timed_out"
            elif statuses and statuses <= {"delivered"}:
                target.business_status = "delivered"
                target.completed_at = max((item.completed_at for item in deliveries if item.completed_at), default=utcnow())
                target.error = None
            elif "sent_pending_confirmation" in statuses:
                target.business_status = "sent_pending_confirmation"
                target.error = None
            elif "queued" in statuses:
                target.business_status = "queued"
                target.error = None
            elif deliveries:
                target.business_status = "approved"
                target.error = None
            self.session.add(target)
        items = list(self.session.scalars(
            select(CrmDispatchItem).where(CrmDispatchItem.batch_id == batch.id)
        ).all())
        for item in items:
            item_targets = [target for target in targets if target.dispatch_item_id == item.id]
            if batch.direction == "downward":
                received_document = self.session.scalar(
                    select(ComplaintDocument.id).where(
                        ComplaintDocument.source_dispatch_item_id == item.id,
                        ComplaintDocument.role.in_(["reply", "report", "policy", "attachment"]),
                        ComplaintDocument.review_state == "accepted",
                    )
                )
                if received_document:
                    item.compliance_status = "received"
                elif any(target.business_status in {"approved", "queued", "sent_pending_confirmation", "delivered"} for target in item_targets):
                    item.compliance_status = "requested"
            elif any(target.business_status == "delivered" for target in item_targets):
                item.compliance_status = "submitted"
            item.updated_at = utcnow()
            self.session.add(item)
        self._update_batch_counts(batch)
        target_statuses = {item.business_status for item in targets}
        if target_statuses and target_statuses <= {"delivered", "excluded"}:
            batch.status = "completed"
            batch.completed_at = utcnow()
        elif target_statuses.intersection({"failed", "timed_out"}) and target_statuses.intersection({"delivered"}):
            batch.status = "completed_with_errors"
        elif target_statuses.intersection({"queued", "sent_pending_confirmation", "approved"}):
            batch.status = "sending"
        elif target_statuses.intersection({"blocked"}) and not target_statuses.intersection({"queued", "sent_pending_confirmation", "delivered"}):
            batch.status = "review_required"
        batch.updated_at = utcnow()
        self.session.add(batch)
        self.session.commit()
        return self.detail(batch.id)

    def statistics(self) -> dict[str, int]:
        count = lambda *filters: int(self.session.scalar(select(func.count()).select_from(CrmDispatchBatch).where(*filters)) or 0)
        ready_upward = int(self.session.scalar(
            select(func.count()).select_from(CrmOfficialLetter).where(CrmOfficialLetter.status == "finalized")
        ) or 0)
        approved_cases = select(ComplaintReplyRevision.complaint_case_id).where(
            ComplaintReplyRevision.is_current.is_(True),
            ComplaintReplyRevision.approval_status.in_(["Approved", "Issued"]),
        )
        ready_downward = int(self.session.scalar(
            select(func.count()).select_from(ComplaintCase).where(
                ComplaintCase.state.in_(["fresh", "published", "review_required"]),
                ComplaintCase.id.not_in(approved_cases),
            )
        ) or 0)
        return {
            "ready_to_dispatch": ready_upward + ready_downward,
            "ready_downward": ready_downward,
            "ready_upward": ready_upward,
            "needs_routing": count(CrmDispatchBatch.status == "review_required"),
            "awaiting_approval": count(CrmDispatchBatch.status == "ready"),
            "sending": count(CrmDispatchBatch.status.in_(["approved", "queued", "sending"])),
            "completed_with_errors": count(CrmDispatchBatch.status == "completed_with_errors"),
        }

    def list_batches(self, *, search: str = "", status: str = "", direction: str = "", page: int = 1, page_size: int = 25) -> dict[str, Any]:
        filters: list[Any] = []
        if status:
            filters.append(CrmDispatchBatch.status == status)
        if direction:
            filters.append(CrmDispatchBatch.direction == direction)
        if search.strip():
            pattern = f"%{search.strip()}%"
            filters.append(or_(CrmDispatchBatch.batch_number.ilike(pattern), CrmDispatchBatch.purpose.ilike(pattern)))
        total = int(self.session.scalar(select(func.count()).select_from(CrmDispatchBatch).where(*filters)) or 0)
        rows = self.session.scalars(
            select(CrmDispatchBatch).where(*filters)
            .order_by(CrmDispatchBatch.created_at.desc())
            .offset((page - 1) * page_size).limit(page_size)
        ).all()
        return {"items": [self._batch_dict(item) for item in rows], "total": total, "page": page, "page_size": page_size}

    def list_delivery_history(
        self, *, search: str = "", status: str = "", direction: str = "",
        page: int = 1, page_size: int = 25,
    ) -> dict[str, Any]:
        filters: list[Any] = [CrmDispatchTarget.preview_id.is_not(None)]
        if status:
            filters.append(CrmDispatchTarget.business_status == status)
        if direction:
            filters.append(CrmDispatchBatch.direction == direction)
        if search.strip():
            pattern = f"%{search.strip()}%"
            filters.append(or_(
                CrmDispatchBatch.batch_number.ilike(pattern),
                CrmDispatchItem.complaint_number_snapshot.ilike(pattern),
            ))
        base = (
            select(CrmDispatchTarget)
            .join(CrmDispatchItem, CrmDispatchItem.id == CrmDispatchTarget.dispatch_item_id)
            .join(CrmDispatchBatch, CrmDispatchBatch.id == CrmDispatchItem.batch_id)
            .where(*filters)
        )
        total = int(self.session.scalar(
            select(func.count()).select_from(CrmDispatchTarget)
            .join(CrmDispatchItem, CrmDispatchItem.id == CrmDispatchTarget.dispatch_item_id)
            .join(CrmDispatchBatch, CrmDispatchBatch.id == CrmDispatchItem.batch_id)
            .where(*filters)
        ) or 0)
        targets = list(self.session.scalars(
            base.order_by(CrmDispatchTarget.updated_at.desc()).offset((page - 1) * page_size).limit(page_size)
        ).all())
        rows: list[dict[str, Any]] = []
        for target in targets:
            item = self.session.get(CrmDispatchItem, target.dispatch_item_id)
            batch = self.session.get(CrmDispatchBatch, item.batch_id) if item else None
            profile = self.session.get(WhatsAppDispatchProfile, target.dispatch_profile_id)
            approval = self.session.scalar(
                select(WhatsAppDispatchApproval).where(WhatsAppDispatchApproval.preview_id == target.preview_id)
            ) if target.preview_id else None
            deliveries = list(self.session.scalars(
                select(WhatsAppDelivery).where(WhatsAppDelivery.approval_id == approval.id)
                .order_by(WhatsAppDelivery.queued_at.desc())
            ).all()) if approval else []
            latest = deliveries[0] if deliveries else None
            rows.append({
                "id": str(target.id),
                "batch_id": str(batch.id) if batch else None,
                "batch_number": batch.batch_number if batch else "—",
                "direction": batch.direction if batch else "",
                "purpose": batch.purpose if batch else "",
                "complaint_number": item.complaint_number_snapshot if item else "—",
                "profile_name": profile.name if profile else "Deleted profile",
                "recipient_name": latest.recipient_name if latest else (target.recipient_snapshot_json[0].get("name") if target.recipient_snapshot_json else "—"),
                "target": latest.target if latest else (target.recipient_snapshot_json[0].get("jid") if target.recipient_snapshot_json else "—"),
                "status": latest.status if latest else target.business_status,
                "error": latest.error if latest else target.error,
                "queued_at": latest.queued_at if latest else None,
                "completed_at": latest.completed_at if latest else target.completed_at,
                "detail_url": f"/crm/dispatch/batches/{batch.id}/" if batch else None,
            })
        return {"items": rows, "total": total, "page": page, "page_size": page_size}

    def _batch_dict(self, batch: CrmDispatchBatch) -> dict[str, Any]:
        return {
            "id": str(batch.id),
            "batch_number": batch.batch_number,
            "status": batch.status,
            "direction": batch.direction,
            "source_mode": batch.source_mode,
            "purpose": batch.purpose,
            "total_items": batch.total_items,
            "ready_items": batch.ready_items,
            "blocked_items": batch.blocked_items,
            "excluded_items": batch.excluded_items,
            "queued_items": batch.queued_items,
            "successful_items": batch.successful_items,
            "failed_items": batch.failed_items,
            "response_due_at": batch.response_due_at,
            "created_by": batch.created_by,
            "approved_by": batch.approved_by,
            "error_summary": batch.error_summary,
            "created_at": batch.created_at,
            "updated_at": batch.updated_at,
            "completed_at": batch.completed_at,
            "detail_url": f"/crm/dispatch/batches/{batch.id}/",
        }

    def detail(self, batch_id: uuid.UUID) -> dict[str, Any]:
        batch = self.session.get(CrmDispatchBatch, batch_id)
        if batch is None:
            raise CrmDispatchNotFound("Dispatch batch not found")
        items = list(self.session.scalars(
            select(CrmDispatchItem).where(CrmDispatchItem.batch_id == batch.id).order_by(CrmDispatchItem.complaint_number_snapshot)
        ).all())
        item_payload = []
        preview_ids: set[uuid.UUID] = set()
        for item in items:
            targets = list(self.session.scalars(select(CrmDispatchTarget).where(CrmDispatchTarget.dispatch_item_id == item.id)).all())
            for target in targets:
                if target.preview_id:
                    preview_ids.add(target.preview_id)
            item_payload.append({
                "id": str(item.id),
                "complaint_case_id": str(item.complaint_case_id),
                "official_letter_id": str(item.official_letter_id) if item.official_letter_id else None,
                "complete_pdf_artifact_id": str(item.complete_pdf_artifact_id) if item.complete_pdf_artifact_id else None,
                "packet_artifact_id": str(item.packet_artifact_id) if item.packet_artifact_id else None,
                "complaint_number": item.complaint_number_snapshot,
                "letter_number": item.letter_number_snapshot,
                "letter_date": item.letter_date_snapshot,
                "compliance_status": item.compliance_status,
                "packet_sha256": item.packet_sha256,
                "packet_size_bytes": item.packet_size_bytes,
                "packet_page_count": item.packet_page_count,
                "route_status": item.route_status,
                "route_summary": item.route_summary_json,
                "excluded": item.excluded,
                "exclusion_reason": item.exclusion_reason,
                "packet_download_url": f"/api/v1/crm/dispatch/artifacts/{item.packet_artifact_id}/download" if item.packet_artifact_id else None,
                "targets": [
                    {
                        "id": str(target.id),
                        "routing_rule_id": str(target.routing_rule_id) if target.routing_rule_id else None,
                        "dispatch_profile_id": str(target.dispatch_profile_id),
                        "profile": self._profile_dict(self.session.get(WhatsAppDispatchProfile, target.dispatch_profile_id)) if self.session.get(WhatsAppDispatchProfile, target.dispatch_profile_id) else None,
                        "selection_source": target.selection_source,
                        "manual_override_reason": target.manual_override_reason,
                        "recipients": target.recipient_snapshot_json,
                        "message": target.message_snapshot,
                        "preview_id": str(target.preview_id) if target.preview_id else None,
                        "preview_delivery_ids": target.preview_delivery_ids_json,
                        "whatsapp_delivery_ids": target.whatsapp_delivery_ids_json,
                        "business_status": target.business_status,
                        "error": target.error,
                    }
                    for target in targets
                ],
            })
        previews = []
        for preview_id in sorted(preview_ids, key=str):
            preview = self.session.get(WhatsAppDispatchPreview, preview_id)
            if preview is None:
                continue
            approval = self.session.scalar(select(WhatsAppDispatchApproval).where(WhatsAppDispatchApproval.preview_id == preview.id))
            deliveries = list(self.session.scalars(select(WhatsAppDispatchPreviewDelivery).where(WhatsAppDispatchPreviewDelivery.preview_id == preview.id).order_by(WhatsAppDispatchPreviewDelivery.sequence)).all())
            previews.append({
                "id": str(preview.id),
                "preview_key": preview.preview_key,
                "profile_name": preview.profile_name,
                "status": preview.status,
                "ready_count": preview.ready_count,
                "blocked_count": preview.blocked_count,
                "skipped_count": preview.skipped_count,
                "content_sha256": preview.content_sha256,
                "approval": {
                    "id": str(approval.id), "status": approval.status, "approved_by": approval.approved_by,
                } if approval else None,
                "deliveries": [
                    {
                        "id": str(row.id), "target_name": row.target_name, "target_jid": row.target_jid,
                        "message": row.message, "status": row.status, "issues": row.issues,
                    }
                    for row in deliveries
                ],
            })
        return {"batch": self._batch_dict(batch), "items": item_payload, "previews": previews, "profiles": self.profiles(include_inactive=False)}


__all__ = [
    "CrmDispatchService",
    "CrmDispatchError",
    "CrmDispatchNotFound",
    "CrmDispatchValidationError",
]
