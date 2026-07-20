#!/usr/bin/env python3
"""Audit or safely complete consolidated AntiDengue schedule profile sets.

Dry-run is the default. Use --apply to add missing enabled, guided consolidated
profiles for the same application and wing. Use --enable separately if a paused
schedule should also be re-enabled; the script never enables it implicitly.
"""
from __future__ import annotations

import argparse
from collections import defaultdict

from sqlmodel import select

from antidengue_automation.models import AntiDengueSchedule
from antidengue_automation.scheduling import next_occurrence_after
from automation_core.database import session_scope
from automation_core.time import utcnow
from whatsapp_gateway.models import WhatsAppDispatchProfile, WhatsAppReportType


PRIORITY = {
    "wing_summary_mee_wing_group_action_digest": 0,
    "tehsil_dormant_summary_mee_tehsil_group_action_digest": 1,
    "mee_aeo_markaz_dormant_personal_action_digest": 2,
}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="write missing profile IDs")
    parser.add_argument("--enable", action="store_true", help="also enable repaired schedules")
    args = parser.parse_args()
    if args.enable and not args.apply:
        parser.error("--enable requires --apply")

    changed = 0
    with session_scope() as session:
        report_type = session.exec(
            select(WhatsAppReportType).where(
                WhatsAppReportType.key == "consolidated_action_digest"
            )
        ).first()
        if report_type is None:
            raise SystemExit("Consolidated Action Digest report type was not found")

        profiles = session.exec(
            select(WhatsAppDispatchProfile).where(
                WhatsAppDispatchProfile.report_type_id == report_type.id,
                WhatsAppDispatchProfile.enabled == True,  # noqa: E712
                WhatsAppDispatchProfile.guided_setup == True,  # noqa: E712
            )
        ).all()
        profile_by_id = {str(profile.id): profile for profile in profiles}
        groups: dict[tuple[str, str], list[WhatsAppDispatchProfile]] = defaultdict(list)
        for profile in profiles:
            if profile.key not in PRIORITY:
                continue
            groups[(str(profile.application_id), str(profile.wing_id))].append(profile)

        schedules = session.exec(
            select(AntiDengueSchedule).where(AntiDengueSchedule.archived_at.is_(None))
        ).all()
        found = 0
        for schedule in schedules:
            configured = [str(value) for value in (schedule.dispatch_profile_ids or [])]
            configured_profiles = [
                profile_by_id[value]
                for value in configured
                if value in profile_by_id and profile_by_id[value].key in PRIORITY
            ]
            if not configured_profiles:
                continue
            found += 1
            anchor = configured_profiles[0]
            desired_profiles = sorted(
                groups[(str(anchor.application_id), str(anchor.wing_id))],
                key=lambda profile: (PRIORITY.get(profile.key, 99), profile.name),
            )
            desired = [str(profile.id) for profile in desired_profiles]
            missing = [value for value in desired if value not in configured]
            print(f"{schedule.name}: enabled={schedule.enabled} profiles={len(configured)} desired={len(desired)}")
            for profile in desired_profiles:
                marker = "present" if str(profile.id) in configured else "MISSING"
                print(f"  [{marker}] {profile.name}")
            if len(desired) != 3:
                print("  WARNING: expected exactly three official consolidated profiles; no automatic write")
                continue
            if not missing and not (args.enable and not schedule.enabled):
                print("  no change required")
                continue
            if not args.apply:
                print("  dry-run only; use --apply after review")
                continue
            schedule.dispatch_profile_ids = desired
            schedule.dispatch_profile_id = desired_profiles[0].id
            if args.enable:
                schedule.enabled = True
                schedule.next_run_at = next_occurrence_after(schedule, utcnow())
            schedule.updated_at = utcnow()
            session.add(schedule)
            changed += 1
            print("  repaired")
        if args.apply:
            session.commit()
        if not found:
            print("No active/non-archived schedule using official consolidated profiles was found.")
    print(f"Changed schedules: {changed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
