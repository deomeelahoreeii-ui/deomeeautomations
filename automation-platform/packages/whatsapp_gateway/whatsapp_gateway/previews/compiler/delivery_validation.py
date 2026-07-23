from __future__ import annotations

import uuid

from sqlalchemy import or_, select

from master_data.models import Officer, Wing
from whatsapp_gateway.antidengue_renderer import normalize_presentation_policy
from whatsapp_gateway.models import (
    WhatsAppContactLink, WhatsAppDirectoryContact, WhatsAppDirectoryGroup, WhatsAppGroup,
)
from whatsapp_gateway.configuration.dynamic_audiences import resolve_audience_source
from whatsapp_gateway.persistence.audience_sources import WhatsAppAudienceSource
from whatsapp_gateway.previews.compiler.artifact_snapshots import ArtifactSnapshotStore
from whatsapp_gateway.previews.compiler.attachments import _classify_attachment_wings, _classify_scoped_emis_wings
from whatsapp_gateway.previews.compiler.context import CompileContext
from whatsapp_gateway.previews.compiler.delivery_state import DeliveryState
from whatsapp_gateway.previews.compiler.errors import issue
from whatsapp_gateway.previews.compiler.messages import PLACEHOLDER_RE, _render_message


def validate_source_status(state: DeliveryState) -> None:
    if state.source_skipped:
        state.problems.append(issue(
            str(state.plan.get("skip_issue_code") or "source_route_skipped"),
            str(state.plan.get("skip_issue_severity") or "warning"),
            str(state.plan.get("cause") or "The source dry run intentionally skipped this route.")))
    elif state.source_status not in {"planned", "queued", "ready"}:
        state.problems.append(issue("source_route_not_planned", "blocked",
            str(state.plan.get("cause") or f"The source route has status: {state.source_status}")))
    else:
        state.problems.extend(item for item in state.native_issues if isinstance(item, dict))


def resolve_recipient(ctx: CompileContext, state: DeliveryState) -> None:
    session, profile, account, wing = ctx.session, ctx.profile, ctx.account, ctx.wing
    if not state.target and not state.source_skipped:
        state.problems.append(issue("missing_target", "blocked", "The planned delivery has no WhatsApp target."))
    if not state.source_skipped and state.target_type == "group":
        if profile.delivery_mode == "individuals":
            state.problems.append(issue("delivery_mode_mismatch", "blocked", "A group route cannot use an individuals-only profile."))
        state.directory_group = session.scalar(select(WhatsAppDirectoryGroup).where(
            WhatsAppDirectoryGroup.account_id == account.id, WhatsAppDirectoryGroup.jid == state.target))
        if state.directory_group is None:
            state.problems.append(issue("unknown_group", "blocked", "The target group is not present in the synchronized directory."))
        else:
            if not state.directory_group.available:
                state.problems.append(issue("unavailable_group", "blocked", "The target group is not currently available."))
            if state.directory_group.id not in ctx.audience_group_ids:
                state.problems.append(issue("outside_audience", "blocked", "The target group is not in the selected audience."))
            configured_group = session.scalar(select(WhatsAppGroup).where(
                WhatsAppGroup.account_id == account.id,
                or_(WhatsAppGroup.directory_group_id == state.directory_group.id, WhatsAppGroup.jid == state.target)))
            if configured_group is None:
                state.problems.append(issue("missing_wing_authorization", "blocked", "Assign this detected group to a wing before dispatch."))
            else:
                state.target_wing = session.get(Wing, configured_group.wing_id)
                if not configured_group.enabled:
                    state.problems.append(issue("disabled_route", "blocked", "The configured group route is disabled."))
                if configured_group.wing_id != wing.id:
                    state.problems.append(issue("cross_wing_route", "blocked",
                        f"The group belongs to {state.target_wing.name if state.target_wing else 'another wing'}, not {wing.name}."))
    elif not state.source_skipped and state.plan.get("dynamic_audience"):
        dynamic = state.plan["dynamic_audience"]
        try:
            source_id = uuid.UUID(str(dynamic.get("source_id") or ""))
            member_id = uuid.UUID(str(dynamic.get("member_id") or ""))
            officer_id = uuid.UUID(str(dynamic.get("officer_id") or ""))
        except ValueError:
            state.problems.append(issue("invalid_dynamic_recipient", "blocked", "The frozen Master Data recipient identity is invalid."))
        else:
            source = session.get(WhatsAppAudienceSource, source_id)
            officer = session.get(Officer, officer_id)
            current = (
                resolve_audience_source(
                    session, source=source, account=account,
                    granularity=profile.delivery_granularity,
                )
                if source is not None and source.enabled else []
            )
            current_member = next((item for item in current if item.id == member_id), None)
            if source is None or source.audience_id != ctx.audience.id or not source.enabled:
                state.problems.append(issue("inactive_dynamic_source", "blocked", "The Master Data audience source is no longer active."))
            elif current_member is None or officer is None or not officer.active:
                state.problems.append(issue("inactive_dynamic_recipient", "blocked", "The resolved AEO is no longer active in this audience."))
            elif current_member.source_fingerprint != dynamic.get("fingerprint"):
                state.problems.append(issue("changed_dynamic_recipient", "blocked", "The AEO jurisdiction changed while this preview was being compiled."))
            elif current_member.target_jid != state.target:
                state.problems.append(issue("changed_dynamic_mobile", "blocked", "The AEO mobile number changed while this preview was being compiled."))
            elif len({item.officer_id for item in current if item.target_jid == state.target}) > 1:
                state.problems.append(issue(
                    "duplicate_dynamic_mobile", "blocked",
                    "More than one resolved AEO route uses this personal mobile number.",
                ))
            if not state.target:
                state.problems.append(issue("missing_dynamic_mobile", "blocked", "The assigned AEO has no valid personal mobile number."))
            state.target_wing = wing
            state.directory_contact = session.scalar(select(WhatsAppDirectoryContact).where(
                WhatsAppDirectoryContact.account_id == account.id,
                or_(WhatsAppDirectoryContact.phone_jid == state.target,
                    WhatsAppDirectoryContact.canonical_key == state.target))) if state.target else None
            # Master Data authorizes dynamic recipients; directory presence only enriches identity.
            if state.directory_contact and not state.directory_contact.active:
                state.problems.append(issue(
                    "inactive_directory_contact", "info",
                    "The synchronized directory identity is inactive; this dynamic route uses the current active officer mobile from Master Data.",
                ))
            elif state.directory_contact is None and state.target:
                state.problems.append(issue(
                    "master_data_phone_target", "info",
                    "This dynamic AEO route uses the current active officer mobile from Master Data; a synchronized directory identity is not required.",
                ))
    elif not state.source_skipped:
        if profile.delivery_mode == "groups":
            state.problems.append(issue("delivery_mode_mismatch", "blocked", "An individual route cannot use a groups-only profile."))
        state.directory_contact = session.scalar(select(WhatsAppDirectoryContact).where(
            WhatsAppDirectoryContact.account_id == account.id,
            or_(WhatsAppDirectoryContact.phone_jid == state.target,
                WhatsAppDirectoryContact.primary_lid_jid == state.target,
                WhatsAppDirectoryContact.canonical_key == state.target)))
        if state.directory_contact is None:
            state.problems.append(issue("unknown_contact", "blocked", "The individual is not present in the synchronized directory."))
        else:
            if not state.directory_contact.active:
                state.problems.append(issue("inactive_contact", "blocked", "The WhatsApp contact is inactive."))
            if state.directory_contact.id not in ctx.audience_contact_ids:
                state.problems.append(issue("outside_audience", "blocked", "The individual is not in the selected audience."))
            contact_links = ctx.session.scalars(select(WhatsAppContactLink).where(
                WhatsAppContactLink.directory_contact_id == state.directory_contact.id,
                WhatsAppContactLink.active.is_(True), WhatsAppContactLink.status == "verified")).all()
            if not contact_links:
                state.problems.append(issue("missing_master_data_link", "blocked", "Link this contact to an officer or school head."))
            elif len(contact_links) > 1:
                state.problems.append(issue("ambiguous_master_data_link", "blocked",
                    "This contact has multiple verified master-data links; keep only the intended recipient link."))
            else:
                state.contact_link = contact_links[0]
                state.target_wing = session.get(Wing, state.contact_link.wing_id)
                if state.contact_link.wing_id != wing.id:
                    state.problems.append(issue("cross_wing_route", "blocked",
                        f"The linked recipient belongs to {state.target_wing.name if state.target_wing else 'another wing'}, not {wing.name}."))
    if not state.source_skipped and not account.enabled:
        state.problems.append(issue("disabled_connection", "blocked", "The selected WhatsApp connection is disabled."))
    elif not state.source_skipped and not account.connected:
        state.problems.append(issue("connection_offline", "warning", "The connection is offline; the frozen preview remains reviewable."))


def validate_attachments(ctx: CompileContext, state: DeliveryState, store: ArtifactSnapshotStore) -> None:
    state.attachment_snapshots = [store.snapshot_artifact(path, store.artifacts_by_path.get(str(path)))
        for path in state.attachment_paths]
    if not state.source_skipped:
        for attachment in state.attachment_snapshots:
            state.problems.extend(attachment.issues)
    if not state.source_skipped and state.attachment_paths:
        if state.plan.get("native_renderer"):
            wing_ids, unknown_emis, classified = _classify_scoped_emis_wings(ctx.session, state.plan.get("scoped_emis") or [])
        else:
            wing_ids, unknown_emis, classified = _classify_attachment_wings(ctx.session, state.attachment_paths)
        if unknown_emis:
            state.problems.append(issue("unknown_report_schools", "blocked",
                f"{len(unknown_emis)} school EMIS value(s) are absent from PostgreSQL master data.", emis=unknown_emis[:20]))
        if len(wing_ids) > 1:
            state.problems.append(issue("mixed_wing_report", "blocked", "The attachment contains schools from more than one wing."))
        elif wing_ids and ctx.wing.id not in wing_ids:
            source_wing = ctx.session.get(Wing, next(iter(wing_ids)))
            state.problems.append(issue("cross_wing_report", "blocked",
                f"The report contains {source_wing.name if source_wing else 'another wing'} schools, not {ctx.wing.name} schools."))
        elif not wing_ids:
            state.problems.append(issue("unclassified_report_wing", "blocked",
                "The report wing could not be verified from school EMIS values.", readable_report_found=classified))
    attachment_required = ctx.report_type.artifact_kind != "message"
    if state.plan.get("native_renderer"):
        policy = normalize_presentation_policy(ctx.profile.presentation_policy, report_key=ctx.report_type.key)
        attachment_required = policy["attachment_mode"] != "none"
    if state.plan.get("message_only"):
        attachment_required = False
    if not state.source_skipped and not state.attachment_paths and attachment_required:
        state.problems.append(issue("missing_report_attachment", "blocked",
            f"{ctx.report_type.name} requires a delivery attachment, but the source route has none."))


def render_delivery_message(ctx: CompileContext, state: DeliveryState) -> None:
    context = {
        "recipient_name": state.recipient_name, "wing": ctx.wing.name, "report_name": ctx.report_type.name,
        "scope": state.route_scope or state.route_kind or "Configured scope",
        "row_count": str(state.dispatch_route.get("row_count") or state.plan.get("row_count") or ""),
        **{str(key): str(value) for key, value in state.native_context.items()},
    }
    template_for_render = None if state.plan.get("message_template_final") else ctx.template
    if state.plan.get("native_renderer"):
        variables = set(PLACEHOLDER_RE.findall(ctx.template.body)) if ctx.template else set()
        supported_native_variables = {"message", "report_body", *state.native_context.keys()}
        if ctx.template and not variables.intersection(supported_native_variables):
            template_for_render = None
            state.problems.append(issue("static_template_ignored", "warning",
                "The selected template does not include any native report variable; add {{report_body}} or a supported report block."))
    state.message, template_issues = _render_message(template_for_render, state.legacy_message, context)
    if not state.source_skipped:
        state.problems.extend(template_issues)
    if not state.source_skipped and not state.message.strip():
        state.problems.append(issue("empty_message", "blocked", "The rendered WhatsApp message is empty."))
    elif not state.source_skipped and len(state.message) > 3500:
        state.problems.append(issue("long_message", "warning", "The rendered message exceeds 3,500 characters."))
