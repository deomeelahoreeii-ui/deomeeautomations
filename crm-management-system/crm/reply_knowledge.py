from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import re
import ssl
import time
import xml.etree.ElementTree as ET
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv
from rapidfuzz import fuzz

from crm.text_cleaning import clean_remarks


LOGGER_NAME = "pmdu_automation"
DEFAULT_NOCODB_URL = "https://crm-replies.lab.internal"
DEFAULT_BASE_TITLE = "CRM Reply Knowledge Base"
DEFAULT_PENDING_DIR = "crm-compliances/pending"
DEFAULT_CONTEXT_DIR = "crm-reply-context-packs"
CONTEXT_OUTPUT_CENTRAL = "central"
CONTEXT_OUTPUT_COMPLAINT_FOLDER = "complaint-folder"
IGNORED_RULE_TERMS = {
    "a",
    "an",
    "and",
    "in",
    "of",
    "or",
    "pm",
    "am",
    "the",
    "to",
}

MANAGED_TABLES = (
    "Complaint Categories",
    "Complaint Issues",
    "Matching Rules",
    "Fact Extractors",
    "Policy Sources",
    "Reply Blocks",
    "Review Examples",
    "Generated Draft Audits",
)

ODT_NS = {"table": "urn:oasis:names:tc:opendocument:xmlns:table:1.0"}
ODT_CELL_TAG = f"{{{ODT_NS['table']}}}table-cell"
ODT_ROW_TAG = f"{{{ODT_NS['table']}}}table-row"
ODT_REPEATED_ATTR = f"{{{ODT_NS['table']}}}number-columns-repeated"
CRM_NUMBER_RE = re.compile(r"\b\d{3}-\d{4,}\b")


@dataclass(frozen=True)
class ExtractedReport:
    complaint_number: str
    path: Path
    complaint_details: str
    manual_remarks: str
    issue_code: str


@dataclass(frozen=True)
class IssueDefinition:
    category_code: str
    title: str
    priority: int
    description: str
    keywords: str


ISSUE_DEFINITIONS: dict[str, IssueDefinition] = {
    "insufficient_actionable_details": IssueDefinition(
        "insufficient_information",
        "Insufficient actionable details / evidence",
        70,
        "Complaint lacks exact institution/person/transaction/evidence needed for immediate administrative action.",
        "vague; lacks actionable details; no clear mention; no documentary evidence; exact location; contact number; identity missing; online classes",
    ),
    "registration_untraceable_institution": IssueDefinition(
        "registration_jurisdiction",
        "Untraceable or vague institution location",
        75,
        "Inspection team cannot trace school/academy due to vague address or insufficient location details.",
        "illegal school; academy; residential area; unable to locate; vague address; exact location; premises not traced",
    ),
    "fee_summer_vacation_challan": IssueDefinition(
        "fee_financial",
        "Summer vacation fee / extra charges",
        85,
        "Complaint concerns fee, challan, extra charges, events, summer vacation charges, or fee pressure.",
        "summer vacation; july; august; financial pressure; fee structure; extra charges",
    ),
    "fee_challan_clarification_satisfied": IssueDefinition(
        "fee_financial",
        "Fee challan misunderstanding clarified and complainant satisfied",
        80,
        "Complainant misunderstood fee challan/billing cycle; office clarified and complainant was satisfied.",
        "billing cycle; misunderstanding; clarified; complainant satisfied; july; august",
    ),
    "fee_variance_due_to_promotion": IssueDefinition(
        "fee_financial",
        "Fee variance due to promotion / higher class tier",
        82,
        "Fee increase is explained by promotion to a higher class/approved fee tier rather than illegal arbitrary increase.",
        "promoted; class 7; class 8; higher tier; approved fee structure; july august",
    ),
    "school_open_summer_vacation": IssueDefinition(
        "school_operations",
        "School open during summer vacation",
        80,
        "School allegedly remained operational during government-notified summer vacation.",
        "school open; summer vacation; government notice; remained functional; official notification",
    ),
    "school_summer_camp_academic_classes": IssueDefinition(
        "school_operations",
        "Summer camp used for academic classes / compulsory attendance",
        88,
        "Summer camp allegedly conducts regular academic syllabus/classes or makes attendance effectively compulsory.",
        "regular academic classes; compulsory attendance; syllabus; co-curricular; recreational",
    ),
    "school_summer_camp_timing_violation": IssueDefinition(
        "school_operations",
        "Summer camp timing violation",
        86,
        "Summer camp timings exceed the notified/approved hours and school is directed to align timings.",
        "timing; timings; 11:30 AM; 10:30 AM; violation; health and safety",
    ),
    "school_staff_hours_internal_matter": IssueDefinition(
        "school_operations",
        "Staff working hours after summer camp dismissal",
        72,
        "Student timing is compliant; staff retention/working hours are treated as internal private school management matter.",
        "staff; 12:00 PM; students leave at 10; internal policy; working hours; private institutional management",
    ),
    "school_public_holiday_closure": IssueDefinition(
        "school_operations",
        "School open on public holiday / closure order",
        82,
        "School allegedly remained open on a notified holiday, Friday/Saturday closure, or other public closure order.",
        "friday; saturday; public holiday; holiday closure; closure orders; schools closed; government orders",
    ),
    "curriculum_unapproved_content": IssueDefinition(
        "curriculum_academics",
        "Unapproved syllabus / curriculum content",
        82,
        "Complaint alleges that a private institution is teaching unapproved syllabus, self-designed curriculum, or inappropriate academic content.",
        "syllabus; curriculum; islamic studies; punjab board; approved curriculum; self-designed curriculum",
    ),
    "student_corporal_punishment_injury": IssueDefinition(
        "student_safety",
        "Corporal punishment / student injury",
        92,
        "Complaint alleges beating, corporal punishment, physical assault, fracture, or other student injury requiring careful escalation/manual review.",
        "beaten; beat; stick; fracture; corporal punishment; physical assault; injury; medico legal",
    ),
    "private_school_staff_employment": IssueDefinition(
        "private_employment",
        "Private school staff employment / salary dispute",
        75,
        "Private-school employee complaint about salary deduction, forced resignation, leave, employment contract, or management treatment.",
        "salary deduction; private employment; employment contract; forced resignation; maternity leave; pregnant; withheld salary",
    ),
    "qat_pef_payment_jurisdiction": IssueDefinition(
        "external_jurisdiction",
        "QAT/PEF marking payment jurisdiction",
        78,
        "Complaint relates to QAT paper checking remuneration/payment, generally requiring PEF or relevant autonomous-body jurisdiction clarification.",
        "qat; online papers; marking; remuneration; pef; punjab education foundation; payment pending",
    ),
    "vocational_navttc_jurisdiction": IssueDefinition(
        "external_jurisdiction",
        "NAVTTC / vocational course jurisdiction",
        76,
        "Complaint relates to NAVTTC, vocational course, trade change, certification, or institute issues outside ordinary school education complaint processing.",
        "navttc; vocational; trade change; certification; overseas skills; course",
    ),
    "special_education_access": IssueDefinition(
        "access_facilities",
        "Special education school access / visit handling",
        70,
        "Complaint about access, entry, visit day, or visitor handling at a special education school or facility.",
        "special education; not allowing women; entry; visit day; minar pakistan; access",
    ),
    "sti_contract_period_salary": IssueDefinition(
        "sti",
        "STI contract period salary / summer vacation",
        95,
        "STI salary or stipend dispute involving contract dates and summer vacation/payment admissibility.",
        "STI; salary; stipend; contract; 31 may; 31st may; summer vacation; payment admissibility",
    ),
    "sti_complete_salary_pending": IssueDefinition(
        "sti",
        "STI complete salary pending",
        92,
        "STI claims salary/stipend remains unpaid for worked/contract period.",
        "STI; complete salary; pending salary; unpaid; stipend; arrears",
    ),
    "sti_ag_token_bill_processing": IssueDefinition(
        "sti",
        "STI AG token / bill processing",
        90,
        "STI complaint mentions bill submission, AG office token, treasury, voucher, or payment processing.",
        "STI; ag office; token; bill; treasury; voucher; payment processing",
    ),
    "manual_review_unclassified": IssueDefinition(
        "manual_review",
        "Manual review / unclassified",
        1,
        "No confident category is available.",
        "",
    ),
}

RULE_MATCH_ALL: dict[str, str] = {
    "insufficient_actionable_details": "online classes|vague|lacks actionable details|documentary evidence",
    "registration_untraceable_institution": "illegal school|academy|unable to locate|vague address",
    "fee_summer_vacation_challan": "fee|challan|chalan|tuition|charges",
    "fee_challan_clarification_satisfied": "fee|challan|chalan",
    "fee_variance_due_to_promotion": "fee|tuition",
    "school_open_summer_vacation": "school; closed|remain closed|remains functional|violating orders|government notice",
    "school_summer_camp_academic_classes": "summer camp",
    "school_summer_camp_timing_violation": "summer camp",
    "school_staff_hours_internal_matter": "staff; summer camp",
    "school_public_holiday_closure": "friday|saturday|public holiday|closure order; school|schools",
    "curriculum_unapproved_content": "syllabus|curriculum; punjab board|approved curriculum|islamic studies",
    "student_corporal_punishment_injury": "beat|beaten|stick|fracture|corporal punishment|physical assault",
    "private_school_staff_employment": "salary|employment|maternity|resignation|leave; private school|school",
    "qat_pef_payment_jurisdiction": "qat|online papers|marking; payment|remuneration|pef",
    "vocational_navttc_jurisdiction": "navttc|vocational|trade change|certification",
    "special_education_access": "special education; entry|access|visit|allowing",
    "sti_contract_period_salary": "STI|School Teacher Intern; salary|stipend",
    "sti_complete_salary_pending": "STI|School Teacher Intern; salary|stipend",
    "sti_ag_token_bill_processing": "STI|School Teacher Intern; token|bill|treasury|voucher",
}

CATEGORY_DEFINITIONS: dict[str, tuple[str, str]] = {
    "insufficient_information": (
        "Insufficient Information / Evidence",
        "Complaints where exact school/person/location/evidence is missing.",
    ),
    "registration_jurisdiction": (
        "Registration, Jurisdiction and Traceability",
        "Unregistered/illegal school, academy, jurisdiction, inspection and traceability matters.",
    ),
    "fee_financial": (
        "Fee and Financial Disputes",
        "Fee challans, tuition, summer vacation charges, billing-cycle confusion and fee variance.",
    ),
    "school_operations": (
        "School Operations, Summer Vacation and Timings",
        "School opening, summer camps, student timing, staff timing and vacation notification matters.",
    ),
    "curriculum_academics": (
        "Curriculum and Academic Content",
        "Approved syllabus, curriculum deviation and academic-content complaints.",
    ),
    "student_safety": (
        "Student Safety and Discipline",
        "Corporal punishment, injury, harassment risk and student protection matters.",
    ),
    "private_employment": (
        "Private School Employment Matters",
        "Private-school staff salary, leave, resignation and employment disputes.",
    ),
    "external_jurisdiction": (
        "External Jurisdiction / Other Authority",
        "Complaints that belong primarily to PEF, NAVTTC or another non-DEA authority.",
    ),
    "access_facilities": (
        "Access and Facility Handling",
        "Access, visit handling and facility-entry complaints.",
    ),
    "sti": (
        "School Teacher Interns",
        "STI appointment, contract, salary, stipend, AG token and bill/payment processing complaints.",
    ),
    "manual_review": (
        "Manual Review / Unclassified",
        "Fallback for low-confidence or high-risk complaints.",
    ),
}


class NocoDBClient:
    def __init__(
        self,
        url: str,
        email: str,
        password: str,
        base_title: str = DEFAULT_BASE_TITLE,
        verify_ssl: bool = False,
    ) -> None:
        self.url = url.rstrip("/")
        self.email = email
        self.password = password
        self.base_title = base_title
        self.session = requests.Session()
        self.session.verify = verify_ssl
        if not verify_ssl:
            requests.packages.urllib3.disable_warnings()  # type: ignore[attr-defined]
        self.token = ""
        self.base_id = ""
        self.tables: dict[str, str] = {}

    def connect(self) -> None:
        response = None
        retry_statuses = {502, 503, 504}
        for attempt in range(1, 6):
            try:
                response = self.session.post(
                    f"{self.url}/api/v1/auth/user/signin",
                    json={"email": self.email, "password": self.password},
                    timeout=30,
                )
                response.raise_for_status()
                break
            except requests.RequestException as exc:
                should_retry = (
                    response is None or response.status_code in retry_statuses
                )
                if attempt >= 5 or not should_retry:
                    if response is not None and response.status_code in retry_statuses:
                        raise RuntimeError(
                            "NocoDB is reachable through the proxy, but its upstream app "
                            f"returned HTTP {response.status_code}. Start or restart the "
                            "complaints-remarks-nocodb Docker Compose stack and retry."
                        ) from exc
                    raise
                time.sleep(min(2**attempt, 10))
        if response is None:
            raise RuntimeError("NocoDB sign-in did not return a response.")
        self.token = response.json()["token"]
        self.session.headers.update({"xc-auth": self.token})
        bases = self.get("/api/v1/db/meta/projects").get("list", [])
        for base in bases:
            if base.get("title") == self.base_title:
                self.base_id = base["id"]
                break
        if not self.base_id:
            raise RuntimeError(f"NocoDB base not found: {self.base_title!r}")
        self.refresh_tables()

    def refresh_tables(self) -> None:
        tables = self.get(f"/api/v1/db/meta/projects/{self.base_id}/tables").get("list", [])
        self.tables = {table["title"]: table["id"] for table in tables}

    def get(self, path: str) -> dict[str, Any]:
        response = self.session.get(f"{self.url}{path}", timeout=30)
        response.raise_for_status()
        return response.json()

    def post(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        response = self.session.post(f"{self.url}{path}", json=payload, timeout=30)
        response.raise_for_status()
        return response.json() if response.content else {}

    def patch(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        response = self.session.patch(f"{self.url}{path}", json=payload, timeout=30)
        response.raise_for_status()
        return response.json() if response.content else {}

    def delete(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        response = self.session.delete(f"{self.url}{path}", json=payload, timeout=30)
        response.raise_for_status()
        return response.json() if response.content else {}

    def table_id(self, title: str) -> str:
        table_id = self.tables.get(title)
        if not table_id:
            raise RuntimeError(f"NocoDB table not found: {title!r}")
        return table_id

    def records(self, table_title: str) -> list[dict[str, Any]]:
        table_id = self.table_id(table_title)
        rows: list[dict[str, Any]] = []
        offset = 0
        while True:
            payload = self.get(f"/api/v2/tables/{table_id}/records?limit=100&offset={offset}")
            rows.extend(payload.get("list", []))
            if payload.get("pageInfo", {}).get("isLastPage", True):
                return rows
            offset += 100

    def insert_record(self, table_title: str, row: dict[str, Any]) -> None:
        self.post(f"/api/v2/tables/{self.table_id(table_title)}/records", row)

    def upsert_record(self, table_title: str, key_field: str, row: dict[str, Any]) -> None:
        key_value = str(row.get(key_field, "")).strip()
        if not key_value:
            self.insert_record(table_title, row)
            return
        for existing in self.records(table_title):
            if str(existing.get(key_field, "")).strip() == key_value:
                self.patch(
                    f"/api/v2/tables/{self.table_id(table_title)}/records",
                    {"Id": existing["Id"], **row},
                )
                return
        self.insert_record(table_title, row)

    def delete_all_records(self, table_title: str) -> int:
        count = 0
        for row in self.records(table_title):
            self.delete(
                f"/api/v2/tables/{self.table_id(table_title)}/records",
                {"Id": row["Id"]},
            )
            count += 1
        return count


def project_root_from_cwd() -> Path:
    return Path.cwd().resolve()


def resolve_path(project_root: Path, value: str) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = project_root / path
    return path.resolve(strict=False)


def normalize_label(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def odt_cell_text(element: ET.Element) -> str:
    return " ".join("".join(element.itertext()).split())


def expanded_odt_row(row: ET.Element) -> list[str]:
    cells: list[str] = []
    for cell in row.findall(ODT_CELL_TAG):
        repeat = max(1, int(cell.attrib.get(ODT_REPEATED_ATTR, "1")))
        cells.extend([odt_cell_text(cell)] * repeat)
    return cells


def extract_report_from_odt(path: Path) -> tuple[str, str]:
    with zipfile.ZipFile(path) as archive:
        content = archive.read("content.xml")
    root = ET.fromstring(content)
    values: dict[str, str] = {}
    for table in root.findall(".//table:table", ODT_NS):
        rows = [expanded_odt_row(row) for row in table.findall(ODT_ROW_TAG)]
        for row_index, row in enumerate(rows):
            for col_index, value in enumerate(row):
                key = normalize_label(value)
                if key not in {"complaintdetails", "remarks"}:
                    continue
                if row_index + 1 < len(rows) and col_index < len(rows[row_index + 1]):
                    values[key] = rows[row_index + 1][col_index].strip()
    return values.get("complaintdetails", ""), values.get("remarks", "")


def contains_any(text: str, needles: list[str]) -> bool:
    return any(needle in text for needle in needles)


def contains_sti(text: str) -> bool:
    return bool(re.search(r"\bsti\b|school teacher intern", text, re.IGNORECASE))


def classify_manual_report(complaint_details: str, manual_remarks: str) -> str:
    details_text = complaint_details.lower()
    text = f"{complaint_details} {manual_remarks}".lower()
    if contains_sti(text):
        if contains_any(text, ["token", "ag office", "accountant general", "treasury", "voucher"]):
            return "sti_ag_token_bill_processing"
        if contains_any(text, ["contract", "31 may", "31st may", "summer vacation salary"]):
            return "sti_contract_period_salary"
        return "sti_complete_salary_pending"
    if contains_any(
        details_text,
        [
            "illegal school",
            "javeed science academy",
            "greenwood",
            "unable to locate",
            "vague address",
            "exact locations could not be traced",
        ],
    ):
        return "registration_untraceable_institution"
    if contains_any(
        text,
        [
            "online classes",
            "paid a girl",
            "kip, institute",
            "lacks actionable details",
            "absence of actionable documentary evidence",
        ],
    ):
        return "insufficient_actionable_details"
    if contains_any(text, ["fee", "challan", "chalan", "tuition", "charges", "financial pressure"]):
        if contains_any(text, ["promoted from class", "class 7 to class 8", "higher tier fee"]):
            return "fee_variance_due_to_promotion"
        if contains_any(
            text,
            [
                "misunderstood the fee structure",
                "clarified the confusion",
                "complainant expressed complete satisfaction",
                "confusion has been effectively resolved",
            ],
        ):
            return "fee_challan_clarification_satisfied"
        return "fee_summer_vacation_challan"
    if contains_any(
        text,
        [
            "summer camp",
            "summer vacation",
            "7:00 am to 10:00 am",
            "7:30 am to 10:30 am",
            "regular academic classes",
        ],
    ):
        if "staff" in text and "12:00" in text:
            return "school_staff_hours_internal_matter"
        if contains_any(text, ["regular academic classes", "compulsory attendance", "academic syllabi"]):
            return "school_summer_camp_academic_classes"
        if contains_any(text, ["timings", "10:30", "11:30", "7:00 am to 10:00 am"]):
            return "school_summer_camp_timing_violation"
        return "school_open_summer_vacation"
    if contains_any(
        text,
        [
            "friday",
            "saturday",
            "public holidays",
            "public holiday",
            "closure orders",
            "officially notified public holidays",
        ],
    ):
        return "school_public_holiday_closure"
    if contains_any(text, ["navttc", "vocational", "trade change", "certification"]):
        return "vocational_navttc_jurisdiction"
    if contains_any(text, ["qat", "online papers", "paper checking", "marking work", "remuneration"]):
        return "qat_pef_payment_jurisdiction"
    if contains_any(text, ["corporal punishment", "physical assault", "fracture", "beat the child", "beaten"]):
        return "student_corporal_punishment_injury"
    if contains_any(
        text,
        ["syllabus", "curriculum", "islamic studies", "punjab board", "approved curriculum"],
    ):
        return "curriculum_unapproved_content"
    if contains_any(
        text,
        [
            "forced me to resign",
            "forced resignation",
            "maternity leave",
            "pregnancy",
            "pregnant teachers",
            "withheld salary",
            "salary has not been given",
            "salary has been cut",
            "private employment",
            "employment contract",
        ],
    ):
        return "private_school_staff_employment"
    if contains_any(text, ["special education", "not allowing women", "changed the day"]):
        return "special_education_access"
    if contains_any(text, ["fee", "challan", "tuition", "charges", "financial pressure"]):
        if contains_any(text, ["promoted from class", "class 7 to class 8", "higher tier fee"]):
            return "fee_variance_due_to_promotion"
        if contains_any(
            text,
            [
                "misunderstood the fee structure",
                "clarified the confusion",
                "complainant expressed complete satisfaction",
                "confusion has been effectively resolved",
            ],
        ):
            return "fee_challan_clarification_satisfied"
        return "fee_summer_vacation_challan"
    if contains_any(
        text,
        [
            "friday",
            "saturday",
            "public holidays",
            "public holiday",
            "closure orders",
            "officially notified public holidays",
        ],
    ):
        return "school_public_holiday_closure"
    if contains_any(
        details_text,
        ["school to remain closed", "schools to remain closed", "government notice", "remains functional"],
    ):
        return "school_open_summer_vacation"
    if contains_any(
        text,
        [
            "summer camp",
            "summer vacation",
            "7:00 am to 10:00 am",
            "7:30 am to 10:30 am",
            "regular academic classes",
        ],
    ):
        if "staff" in text and "12:00" in text:
            return "school_staff_hours_internal_matter"
        if contains_any(text, ["regular academic classes", "compulsory attendance", "academic syllabi"]):
            return "school_summer_camp_academic_classes"
        if contains_any(text, ["timings", "10:30", "11:30", "7:00 am to 10:00 am"]):
            return "school_summer_camp_timing_violation"
        return "school_open_summer_vacation"
    return "manual_review_unclassified"


def read_pending_reports(pending_dir: Path) -> list[ExtractedReport]:
    reports: list[ExtractedReport] = []
    for odt_path in sorted(pending_dir.glob("*/*DEO Report.odt")):
        if odt_path.parent.name == "sample":
            continue
        complaint_details, manual_remarks = extract_report_from_odt(odt_path)
        if not complaint_details and not manual_remarks:
            continue
        issue_code = classify_manual_report(complaint_details, manual_remarks)
        reports.append(
            ExtractedReport(
                complaint_number=odt_path.parent.name,
                path=odt_path,
                complaint_details=complaint_details,
                manual_remarks=manual_remarks,
                issue_code=issue_code,
            )
        )
    return reports


def split_terms(value: Any) -> list[str]:
    terms: list[str] = []
    for part in re.split(r"[;\n]+", str(value or "")):
        part = part.strip()
        if part:
            terms.append(part)
    return terms


def term_matches(cleaned_text: str, raw_text: str, term: str) -> bool:
    alternatives = [item.strip() for item in term.split("|") if item.strip()]
    if not alternatives:
        return False
    for alternative in alternatives:
        cleaned_alt = clean_remarks(alternative)
        if cleaned_alt in IGNORED_RULE_TERMS or len(cleaned_alt) < 3:
            cleaned_alt = ""
        if cleaned_alt and cleaned_alt in cleaned_text:
            return True
        if alternative.lower() in raw_text.lower():
            return True
    return False


def score_rule(complaint_text: str, rule: dict[str, Any]) -> tuple[float, list[str]]:
    cleaned_text = clean_remarks(complaint_text)
    if not cleaned_text:
        return 0.0, []
    for term in split_terms(rule.get("Match None")):
        if term_matches(cleaned_text, complaint_text, term):
            return 0.0, []

    required = split_terms(rule.get("Match All"))
    if required and not all(term_matches(cleaned_text, complaint_text, term) for term in required):
        return 0.0, []

    matched: list[str] = []
    for term in required:
        if term_matches(cleaned_text, complaint_text, term):
            matched.append(term)
    optional = split_terms(rule.get("Match Any"))
    for term in optional:
        if term_matches(cleaned_text, complaint_text, term):
            matched.append(term)

    if optional and not matched:
        return 0.0, []
    base = 0.45 if required else 0.15
    optional_score = min(0.45, 0.45 * (len(matched) / max(1, len(optional) + len(required))))
    priority_score = min(0.10, float(rule.get("Priority") or 0) / 1000)
    return round(min(1.0, base + optional_score + priority_score), 3), matched


def classify_from_nocodb(complaint_text: str, rules: list[dict[str, Any]]) -> dict[str, Any]:
    scored: list[dict[str, Any]] = []
    for rule in rules:
        if rule.get("Enabled") is False:
            continue
        score, matched_terms = score_rule(complaint_text, rule)
        if score <= 0:
            continue
        scored.append(
            {
                "issue_code": rule.get("Issue Code") or "",
                "rule_code": rule.get("Rule Code") or "",
                "rule_title": rule.get("Title") or "",
                "score": score,
                "matched_terms": matched_terms,
                "manual_review_below": float(rule.get("Manual Review Below") or 0.90),
            }
        )
    scored.sort(key=lambda item: item["score"], reverse=True)
    if not scored:
        return {
            "issue_code": "manual_review_unclassified",
            "score": 0.0,
            "matched_rule": "",
            "matched_terms": [],
            "manual_review_required": True,
            "alternatives": [],
        }
    best = scored[0]
    return {
        "issue_code": best["issue_code"] or "manual_review_unclassified",
        "score": best["score"],
        "matched_rule": best["rule_code"],
        "matched_terms": best["matched_terms"],
        "manual_review_required": best["score"] < best["manual_review_below"],
        "alternatives": scored[1:4],
    }


def find_issue(issue_code: str, issues: list[dict[str, Any]]) -> dict[str, Any]:
    for issue in issues:
        if issue.get("Issue Code") == issue_code:
            return issue
    return {}


def rank_examples(
    complaint_text: str,
    issue_code: str,
    examples: list[dict[str, Any]],
    top_k: int,
) -> list[dict[str, Any]]:
    cleaned_text = clean_remarks(complaint_text)
    ranked: list[dict[str, Any]] = []
    for example in examples:
        same_issue_bonus = 20 if example.get("Issue Code") == issue_code else 0
        example_text = str(example.get("Complaint Text") or "")
        score = fuzz.token_set_ratio(cleaned_text, clean_remarks(example_text)) + same_issue_bonus
        ranked.append(
            {
                "complaint_number": example.get("Complaint Number"),
                "issue_code": example.get("Issue Code"),
                "complaint_text": example_text,
                "correct_action": example.get("Correct Action"),
                "notes": example.get("Notes"),
                "similarity": round(min(100.0, score), 2),
            }
        )
    ranked.sort(key=lambda item: item["similarity"], reverse=True)
    return ranked[:top_k]


def matching_reply_blocks(
    issue_code: str, examples: list[dict[str, Any]], blocks: list[dict[str, Any]], top_k: int
) -> list[dict[str, Any]]:
    example_numbers = {
        str(example.get("complaint_number") or example.get("Complaint Number") or "")
        for example in examples
    }
    selected: list[dict[str, Any]] = []
    for block in blocks:
        if block.get("Issue Code") != issue_code:
            continue
        block_code = str(block.get("Block Code") or "")
        title = str(block.get("Title") or "")
        if example_numbers and not any(number and number.replace("-", "_") in block_code for number in example_numbers):
            # Keep issue-level examples close to the selected issue, but prefer the ranked complaints.
            pass
        selected.append(
            {
                "title": title,
                "block_code": block_code,
                "block_type": block.get("Block Type"),
                "requires_manual_review": bool(block.get("Requires Manual Review")),
                "reply_text": block.get("Reply Text"),
                "notes": block.get("Notes"),
            }
        )
    return selected[:top_k]


def build_prompt_text(context_pack: dict[str, Any]) -> str:
    return (
        "You are drafting an official DEO compliance report reply for a CRM complaint.\n"
        "Use only the complaint facts and relevant examples in the JSON context below.\n"
        "Do not invent policy, inquiry findings, satisfaction, inspection results, or school statements.\n"
        "If the evidence or policy is not verified, keep the wording as a draft for manual review.\n"
        "Write in the same official style as the matched manual reply examples.\n\n"
        "JSON CONTEXT:\n"
        f"{json.dumps(context_pack, ensure_ascii=False, indent=2)}\n"
    )


def context_pack_paths(
    report: ExtractedReport,
    output_dir: Path,
    output_mode: str = CONTEXT_OUTPUT_CENTRAL,
) -> tuple[Path, Path]:
    if output_mode == CONTEXT_OUTPUT_COMPLAINT_FOLDER:
        folder = report.path.parent
        return (
            folder / f"{report.complaint_number}.reply-context.json",
            folder / f"{report.complaint_number}.prompt.txt",
        )
    return (
        output_dir / f"{report.complaint_number}.json",
        output_dir / f"{report.complaint_number}.prompt.txt",
    )


def write_context_pack_files(
    context_pack: dict[str, Any],
    report: ExtractedReport,
    output_dir: Path,
    output_mode: str,
    write_prompt_text: bool = True,
) -> tuple[Path, Path | None]:
    json_path, prompt_path = context_pack_paths(report, output_dir, output_mode)
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(
        json.dumps(context_pack, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    if write_prompt_text:
        prompt_path.write_text(build_prompt_text(context_pack), encoding="utf-8")
        return json_path, prompt_path
    return json_path, None


def build_context_pack(
    client: NocoDBClient,
    report: ExtractedReport,
    top_examples: int,
    include_unverified_policy: bool,
) -> dict[str, Any]:
    issues = client.records("Complaint Issues")
    rules = client.records("Matching Rules")
    examples = client.records("Review Examples")
    blocks = client.records("Reply Blocks")
    policies = client.records("Policy Sources")

    classification = classify_from_nocodb(report.complaint_details, rules)
    issue = find_issue(classification["issue_code"], issues)
    selected_examples = rank_examples(
        report.complaint_details,
        classification["issue_code"],
        examples,
        top_examples,
    )
    selected_blocks = matching_reply_blocks(
        classification["issue_code"],
        selected_examples,
        blocks,
        top_examples,
    )
    selected_policies = []
    for policy in policies:
        if policy.get("Verified") or include_unverified_policy:
            selected_policies.append(
                {
                    "policy_code": policy.get("Policy Code"),
                    "title": policy.get("Title"),
                    "authority": policy.get("Authority"),
                    "verified": bool(policy.get("Verified")),
                    "policy_text": policy.get("Policy Text"),
                    "source_reference": policy.get("Source Reference"),
                }
            )

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": {
            "nocodb_url": client.url,
            "base_title": client.base_title,
            "compliance_report": str(report.path),
        },
        "complaint": {
            "complaint_number": report.complaint_number,
            "details": report.complaint_details,
        },
        "classification": {
            **classification,
            "issue_title": issue.get("Title") or "",
            "category_code": issue.get("Category Code") or "",
            "issue_description": issue.get("Description") or "",
        },
        "style_rules": {
            "reply_type": "DEO compliance report remarks",
            "tone": "formal official administrative reply",
            "do_not_invent_unverified_facts": True,
            "manual_review_required": True,
            "do_not_mark_answered_unless_confidence_high": True,
        },
        "matched_examples": selected_examples,
        "reply_blocks": selected_blocks,
        "policy_sources": selected_policies,
    }


def build_client_from_args(args: argparse.Namespace) -> NocoDBClient:
    load_dotenv()
    url = args.nocodb_url or os.getenv("NOCODB_URL", DEFAULT_NOCODB_URL)
    email = args.nocodb_email or os.getenv("NOCODB_EMAIL", "")
    password = args.nocodb_password or os.getenv("NOCODB_PASSWORD", "")
    base_title = args.nocodb_base or os.getenv("NOCODB_BASE_TITLE", DEFAULT_BASE_TITLE)
    verify_ssl = str(args.nocodb_verify_ssl or os.getenv("NOCODB_VERIFY_SSL", "false")).lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    if not email or not password:
        raise RuntimeError("NOCODB_EMAIL and NOCODB_PASSWORD are required.")
    client = NocoDBClient(url, email, password, base_title=base_title, verify_ssl=verify_ssl)
    client.connect()
    return client


def add_nocodb_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--nocodb-url", default="", help="NocoDB base URL.")
    parser.add_argument("--nocodb-email", default="", help="NocoDB login email.")
    parser.add_argument("--nocodb-password", default="", help="NocoDB login password.")
    parser.add_argument("--nocodb-base", default="", help="NocoDB base title.")
    parser.add_argument("--nocodb-verify-ssl", action="store_true", help="Verify NocoDB HTTPS certificate.")


def paperless_custom_field_raw(
    custom_fields: list[dict[str, Any]],
    fields_by_name: dict[str, dict[str, Any]],
    field_name: str,
) -> Any:
    field = fields_by_name.get(field_name.lower())
    if not field:
        return None
    field_id = int(field["id"])
    for item in custom_fields:
        if int(item.get("field", 0)) == field_id:
            return item.get("value")
    return None


def first_document_link_id(value: Any) -> int | None:
    if isinstance(value, list) and value:
        value = value[0]
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def compact_lines(text: str) -> list[str]:
    return [
        re.sub(r"\s+", " ", line.replace("\xa0", " ")).strip()
        for line in text.splitlines()
        if re.sub(r"\s+", " ", line.replace("\xa0", " ")).strip()
    ]


def extract_complaint_details_from_content(text: str) -> str:
    lines = compact_lines(text)
    for index, line in enumerate(lines):
        if normalize_label(line) == "complaintdetails":
            return " ".join(lines[index + 1 :]).strip()
    match = re.search(r"complaint\s+details\s*[:\-]?\s*(.+)", text, flags=re.IGNORECASE | re.DOTALL)
    if match:
        return re.sub(r"\s+", " ", match.group(1)).strip()
    return re.sub(r"\s+", " ", text).strip()


def extract_report_sections_from_content(text: str) -> tuple[str, str]:
    lines = compact_lines(text)
    for index, line in enumerate(lines):
        if normalize_label(line) != "complaintdetails":
            continue
        next_index = index + 1
        if next_index < len(lines) and normalize_label(lines[next_index]) == "remarks":
            body_start = next_index + 1
            if body_start >= len(lines):
                break
            details = lines[body_start]
            remarks = " ".join(lines[body_start + 1 :]).strip()
            return details, remarks

    remarks_index = next(
        (index for index, line in enumerate(lines) if normalize_label(line) == "remarks"),
        -1,
    )
    if remarks_index >= 0:
        details = " ".join(lines[:remarks_index]).strip()
        remarks = " ".join(lines[remarks_index + 1 :]).strip()
        return details, remarks
    return "", re.sub(r"\s+", " ", text).strip()


def complaint_number_from_text(*values: str) -> str:
    for value in values:
        match = CRM_NUMBER_RE.search(value or "")
        if match:
            return match.group(0)
    return ""


def seed_base_knowledge(
    client: NocoDBClient,
    reports: list[ExtractedReport],
    source_note: str,
) -> None:
    used_issue_codes = sorted({report.issue_code for report in reports})
    used_category_codes = sorted(
        {ISSUE_DEFINITIONS[issue_code].category_code for issue_code in used_issue_codes}
    )

    for category_code in used_category_codes:
        category_name, description = CATEGORY_DEFINITIONS[category_code]
        client.upsert_record(
            "Complaint Categories",
            "category_code",
            {
                "category_code": category_code,
                "category_name": category_name,
                "parent_category": "",
                "enabled": "yes",
                "priority": "80",
                "owner_role": "DEA / DEO reviewer",
                "default_review_policy": "manual_review_before_submission",
                "description": description,
                "notes": source_note,
            },
        )

    for issue_code in used_issue_codes:
        issue = ISSUE_DEFINITIONS[issue_code]
        client.upsert_record(
            "Complaint Issues",
            "Issue Code",
            {
                "Title": issue.title,
                "Issue Code": issue_code,
                "Category Code": issue.category_code,
                "Parent Issue Code": "",
                "Enabled": True,
                "Priority": issue.priority,
                "Risk Level": "medium",
                "Default Min Score": 0.82,
                "Manual Review Below": 0.90,
                "Default Output Action": "draft_deo_report_manual_review",
                "Description": issue.description,
                "Notes": source_note,
            },
        )
        client.upsert_record(
            "Matching Rules",
            "Rule Code",
            {
                "Title": f"Match - {issue.title}",
                "Rule Code": f"rule_{issue_code}",
                "Issue Code": issue_code,
                "Enabled": True,
                "Priority": issue.priority,
                "Match All": RULE_MATCH_ALL.get(issue_code, ""),
                "Match Any": issue.keywords,
                "Match None": "",
                "Required Facts": "",
                "Score Weight": 1.0,
                "Min Score": 0.82,
                "Manual Review Below": 0.90,
                "Applies To Fields": "complaint_details; complaint_remarks; ocr_text; paperless_content; paperless_title",
                "Notes": source_note,
            },
        )

    fact_rows = (
        ("fact_school_identified", "School/institution identified", "school; campus; academy; institute"),
        ("fact_fee_or_challan", "Fee/challan mentioned", "fee; challan; chalan; tuition; charges; voucher"),
        (
            "fact_summer_vacation",
            "Summer vacation/camp mentioned",
            "summer vacation; summer camp; holidays; vacation; 7:00 AM; 10:00 AM",
        ),
        (
            "fact_complainant_satisfied",
            "Complainant satisfied after clarification",
            "satisfied; clarified; confusion cleared; misunderstanding",
        ),
        (
            "fact_location_insufficient",
            "Location/evidence insufficient",
            "vague address; unable to locate; lacks actionable; no clear mention; documentary evidence",
        ),
        ("fact_staff_hours", "Staff working hours issue", "staff; 12:00 PM; internal policy; working hours"),
        ("fact_corporal_punishment", "Student corporal punishment/injury", "beaten; beat; stick; fracture; corporal punishment"),
        ("fact_external_jurisdiction", "Likely external jurisdiction", "PEF; QAT; NAVTTC; vocational; autonomous body"),
    )
    for fact_code, label, any_terms in fact_rows:
        client.upsert_record(
            "Fact Extractors",
            "Fact Code",
            {
                "Title": label,
                "Fact Code": fact_code,
                "Issue Code": "",
                "Enabled": True,
                "Priority": 70,
                "Fact Label": label,
                "Field Type": "boolean",
                "Match All": "",
                "Match Any": any_terms,
                "Match None": "",
                "Regex Patterns": "",
                "Confidence Weight": 0.2,
                "Manual Review If Missing": False,
                "Notes": source_note,
            },
        )


def upsert_report_examples(
    client: NocoDBClient,
    reports: list[ExtractedReport],
    source_policy_code: str,
    source_note: str,
) -> None:
    client.upsert_record(
        "Policy Sources",
        "Policy Code",
        {
            "Title": "Reviewed CRM compliance replies from Paperless",
            "Policy Code": source_policy_code,
            "Enabled": True,
            "Authority": "Paperless submitted compliance reports / needs official policy verification",
            "Effective From": None,
            "Effective To": None,
            "Policy Text": "Finalized CRM compliance report remarks already uploaded to Paperless. Use these as reviewed drafting examples, not as standalone legal policy.",
            "Source Reference": "Paperless submitted CRM complaints linked with DEO MEE Report",
            "Verified": False,
            "Verified By": "",
            "Notes": source_note,
        },
    )

    for report in reports:
        issue = ISSUE_DEFINITIONS[report.issue_code]
        block_code = f"paperless_full_reply_{report.complaint_number.replace('-', '_')}"
        client.upsert_record(
            "Reply Blocks",
            "Block Code",
            {
                "Title": f"Paperless full DEO reply - {report.complaint_number} - {issue.title}",
                "Block Code": block_code,
                "Issue Code": report.issue_code,
                "Policy Code": source_policy_code,
                "Enabled": True,
                "Block Type": "full_reviewed_template",
                "Sort Order": 100,
                "Requires Facts": "",
                "Excludes Facts": "",
                "Reply Text": report.manual_remarks,
                "Requires Manual Review": True,
                "Notes": f"{source_note}; source={report.path}",
            },
        )
        client.upsert_record(
            "Review Examples",
            "Complaint Number",
            {
                "Title": f"Paperless reviewed example - {report.complaint_number} - {issue.title}",
                "Complaint Number": report.complaint_number,
                "Issue Code": report.issue_code,
                "Complaint Text": report.complaint_details,
                "Expected Facts": "",
                "Correct Action": "use_reviewed_reply_as_template_after_manual_review",
                "Reviewer": "Paperless submitted report",
                "Reviewed On": datetime.now(timezone.utc).date().isoformat(),
                "Notes": f"{source_note}; source={report.path}",
            },
        )


async def read_paperless_submitted_reply_reports(
    status: str,
    limit: int | None,
    min_reply_chars: int,
) -> tuple[list[ExtractedReport], dict[str, int]]:
    from main import load_paperless_settings
    from paperless import PaperlessClient, custom_field_label, resolve_metadata

    project_root = project_root_from_cwd()
    settings = load_paperless_settings(project_root)
    reports: list[ExtractedReport] = []
    stats = {
        "main_candidates": 0,
        "submitted_main": 0,
        "missing_report_link": 0,
        "missing_reply_text": 0,
        "importable": 0,
    }

    async with PaperlessClient(settings) as paperless:
        metadata = await resolve_metadata(paperless, settings)
        fields_by_name = metadata["custom_fields"]
        main_docs = await paperless.paginated_results(
            f"/api/documents/?document_type__id={metadata['complaint_type_id']}&page_size=100"
        )
        stats["main_candidates"] = len(main_docs)
        for main_doc in main_docs:
            custom_fields = main_doc.get("custom_fields") or []
            role_label = custom_field_label(custom_fields, fields_by_name, "Document Role").strip()
            status_label = custom_field_label(custom_fields, fields_by_name, "Status").strip()
            source_label = custom_field_label(custom_fields, fields_by_name, "Source").strip()
            if role_label.lower() != "main complaint":
                continue
            if status_label.lower() != status.lower():
                continue
            if source_label and source_label.lower() != "crm portal":
                continue

            stats["submitted_main"] += 1
            full_main = await paperless.get_document(int(main_doc["id"]))
            full_custom_fields = full_main.get("custom_fields") or []
            complaint_number = custom_field_label(
                full_custom_fields,
                fields_by_name,
                "Complaint Number",
            ).strip()
            complaint_number = complaint_number_from_text(
                complaint_number,
                str(full_main.get("title") or ""),
                str(full_main.get("content") or ""),
            )
            report_id = first_document_link_id(
                paperless_custom_field_raw(full_custom_fields, fields_by_name, "DEO MEE Report")
            )
            if not report_id:
                stats["missing_report_link"] += 1
                continue

            report_doc = await paperless.get_document(report_id)
            report_details, report_remarks = extract_report_sections_from_content(
                str(report_doc.get("content") or "")
            )
            complaint_details = report_details or extract_complaint_details_from_content(
                str(full_main.get("content") or "")
            )
            if len(report_remarks) < min_reply_chars:
                stats["missing_reply_text"] += 1
                continue
            if not complaint_number:
                complaint_number = complaint_number_from_text(str(report_doc.get("title") or ""))
            if not complaint_number:
                stats["missing_reply_text"] += 1
                continue

            issue_code = classify_manual_report(complaint_details, report_remarks)
            reports.append(
                ExtractedReport(
                    complaint_number=complaint_number,
                    path=Path(f"paperless/main-{main_doc['id']}-report-{report_id}"),
                    complaint_details=complaint_details,
                    manual_remarks=report_remarks,
                    issue_code=issue_code,
                )
            )
            stats["importable"] += 1
            if limit and len(reports) >= limit:
                break
    return reports, stats


def sync_paperless_replies_main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Sync submitted CRM replies from Paperless into NocoDB.")
    parser.add_argument("--status", default="Submitted", help="Main complaint Paperless status to import.")
    parser.add_argument("--limit", type=int, default=0, help="Maximum submitted replies to import.")
    parser.add_argument("--min-reply-chars", type=int, default=120, help="Skip report text shorter than this.")
    parser.add_argument("--clear", action="store_true", help="Clear managed NocoDB tables before import.")
    parser.add_argument("--dry-run", action="store_true", help="Read Paperless and report what would be synced.")
    add_nocodb_args(parser)
    args = parser.parse_args(argv)

    logger = logging.getLogger(LOGGER_NAME)
    reports, stats = asyncio.run(
        read_paperless_submitted_reply_reports(
            status=args.status,
            limit=args.limit or None,
            min_reply_chars=max(1, args.min_reply_chars),
        )
    )
    logger.info("Paperless CRM reply scan stats: %s", stats)
    if not reports:
        logger.error("No importable submitted CRM reply reports found in Paperless.")
        return 1
    issue_counts: dict[str, int] = {}
    for report in reports:
        issue_counts[report.issue_code] = issue_counts.get(report.issue_code, 0) + 1
    logger.info("Classified Paperless CRM replies by issue: %s", issue_counts)

    if args.dry_run:
        for report in reports[:10]:
            logger.info(
                "DRY RUN: %s -> %s (%d chars reply)",
                report.complaint_number,
                report.issue_code,
                len(report.manual_remarks),
            )
        return 0

    client = build_client_from_args(args)
    if args.clear:
        deleted_counts = {table: client.delete_all_records(table) for table in MANAGED_TABLES}
        logger.info("Cleared previous NocoDB knowledge rows: %s", deleted_counts)

    source_note = "Derived from submitted CRM main complaints and linked DEO MEE reports in Paperless."
    seed_base_knowledge(client, reports, source_note)
    upsert_report_examples(
        client,
        reports,
        source_policy_code="policy_paperless_submitted_crm_replies",
        source_note=source_note,
    )
    logger.info("Synced %d Paperless CRM reply example(s) into NocoDB.", len(reports))
    return 0


def sync_manual_replies_main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Sync manually drafted CRM DEO replies into NocoDB.")
    parser.add_argument("--pending-dir", default=DEFAULT_PENDING_DIR, help="CRM pending compliance folder.")
    parser.add_argument("--clear", action="store_true", help="Clear managed NocoDB tables before import.")
    add_nocodb_args(parser)
    args = parser.parse_args(argv)

    logger = logging.getLogger(LOGGER_NAME)
    project_root = project_root_from_cwd()
    pending_dir = resolve_path(project_root, args.pending_dir)
    reports = read_pending_reports(pending_dir)
    if not reports:
        logger.error("No manually drafted DEO report remarks found in %s", pending_dir)
        return 1

    client = build_client_from_args(args)
    deleted_counts: dict[str, int] = {}
    if args.clear:
        for table in MANAGED_TABLES:
            deleted_counts[table] = client.delete_all_records(table)

    used_issue_codes = sorted({report.issue_code for report in reports})
    used_category_codes = sorted(
        {ISSUE_DEFINITIONS[issue_code].category_code for issue_code in used_issue_codes}
    )

    for category_code in used_category_codes:
        category_name, description = CATEGORY_DEFINITIONS[category_code]
        client.insert_record(
            "Complaint Categories",
            {
                "category_code": category_code,
                "category_name": category_name,
                "parent_category": "",
                "enabled": "yes",
                "priority": "80",
                "owner_role": "DEA / DEO reviewer",
                "default_review_policy": "manual_review_before_submission",
                "description": description,
                "notes": "Derived from manually drafted DEO report remarks in crm-compliances/pending.",
            },
        )

    for issue_code in used_issue_codes:
        issue = ISSUE_DEFINITIONS[issue_code]
        client.insert_record(
            "Complaint Issues",
            {
                "Title": issue.title,
                "Issue Code": issue_code,
                "Category Code": issue.category_code,
                "Parent Issue Code": "",
                "Enabled": True,
                "Priority": issue.priority,
                "Risk Level": "medium",
                "Default Min Score": 0.82,
                "Manual Review Below": 0.90,
                "Default Output Action": "draft_deo_report_manual_review",
                "Description": issue.description,
                "Notes": "Issue generated from manually written compliance remarks.",
            },
        )
        client.insert_record(
            "Matching Rules",
            {
                "Title": f"Match - {issue.title}",
                "Rule Code": f"rule_{issue_code}",
                "Issue Code": issue_code,
                "Enabled": True,
                "Priority": issue.priority,
                "Match All": RULE_MATCH_ALL.get(issue_code, ""),
                "Match Any": issue.keywords,
                "Match None": "",
                "Required Facts": "",
                "Score Weight": 1.0,
                "Min Score": 0.82,
                "Manual Review Below": 0.90,
                "Applies To Fields": "complaint_details; complaint_remarks; ocr_text; paperless_content; paperless_title",
                "Notes": "Generated from manually drafted pending DEO report examples.",
            },
        )

    fact_rows = (
        ("fact_school_identified", "School/institution identified", "school; campus; academy; institute"),
        ("fact_fee_or_challan", "Fee/challan mentioned", "fee; challan; tuition; charges; voucher"),
        (
            "fact_summer_vacation",
            "Summer vacation/camp mentioned",
            "summer vacation; summer camp; holidays; vacation; 7:00 AM; 10:00 AM",
        ),
        (
            "fact_complainant_satisfied",
            "Complainant satisfied after clarification",
            "satisfied; clarified; confusion cleared; misunderstanding",
        ),
        (
            "fact_location_insufficient",
            "Location/evidence insufficient",
            "vague address; unable to locate; lacks actionable; no clear mention; documentary evidence",
        ),
        ("fact_staff_hours", "Staff working hours issue", "staff; 12:00 PM; internal policy; working hours"),
    )
    for fact_code, label, any_terms in fact_rows:
        client.insert_record(
            "Fact Extractors",
            {
                "Title": label,
                "Fact Code": fact_code,
                "Issue Code": "",
                "Enabled": True,
                "Priority": 70,
                "Fact Label": label,
                "Field Type": "boolean",
                "Match All": "",
                "Match Any": any_terms,
                "Match None": "",
                "Regex Patterns": "",
                "Confidence Weight": 0.2,
                "Manual Review If Missing": False,
                "Notes": "Derived from recurring facts found in manual pending replies.",
            },
        )

    for code, title, text, notes in (
        (
            "policy_manual_deo_replies_pending",
            "Manual DEO replies from pending folder",
            "Manually drafted DEO report remarks currently present in crm-compliances/pending. These records are examples/templates, not verified law by themselves.",
            "Use as drafting patterns only.",
        ),
        (
            "policy_sed_summer_camp_timings_unverified",
            "SED summer vacation/camp timing reference - unverified",
            "Replies mention approved summer camp timings and SED guidance. Paste official notification text/reference before final automation.",
            "Marked unverified intentionally.",
        ),
        (
            "policy_private_school_fee_unverified",
            "Private school fee / tuition during summer recess reference - unverified",
            "Replies mention approved standard tuition fee during summer recess and approved fee structures. Paste official policy before final automation.",
            "Marked unverified intentionally.",
        ),
    ):
        client.insert_record(
            "Policy Sources",
            {
                "Title": title,
                "Policy Code": code,
                "Enabled": True,
                "Authority": "Manual draft / needs official verification",
                "Effective From": None,
                "Effective To": None,
                "Policy Text": text,
                "Source Reference": str(pending_dir),
                "Verified": False,
                "Verified By": "",
                "Notes": notes,
            },
        )

    for report in reports:
        issue = ISSUE_DEFINITIONS[report.issue_code]
        client.insert_record(
            "Reply Blocks",
            {
                "Title": f"Manual full DEO reply - {report.complaint_number} - {issue.title}",
                "Block Code": f"manual_full_reply_{report.complaint_number.replace('-', '_')}",
                "Issue Code": report.issue_code,
                "Policy Code": "policy_manual_deo_replies_pending",
                "Enabled": True,
                "Block Type": "full_manual_template",
                "Sort Order": 100,
                "Requires Facts": "",
                "Excludes Facts": "",
                "Reply Text": report.manual_remarks,
                "Requires Manual Review": True,
                "Notes": f"Exact manually drafted remarks extracted from {report.path}",
            },
        )
        client.insert_record(
            "Review Examples",
            {
                "Title": f"Reviewed example - {report.complaint_number} - {issue.title}",
                "Complaint Number": report.complaint_number,
                "Issue Code": report.issue_code,
                "Complaint Text": report.complaint_details,
                "Expected Facts": "",
                "Correct Action": "use_manual_reply_as_template_after_review",
                "Reviewer": "manual / LLM-assisted draft",
                "Reviewed On": datetime.now(timezone.utc).date().isoformat(),
                "Notes": f"Source ODT: {report.path}",
            },
        )

    logger.info("Imported %d manually drafted CRM replies into NocoDB.", len(reports))
    if deleted_counts:
        logger.info("Cleared previous rows: %s", deleted_counts)
    return 0


def build_context_main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build compact JSON context packs for CRM reply drafting.")
    parser.add_argument("--pending-dir", default=DEFAULT_PENDING_DIR, help="CRM pending compliance folder.")
    parser.add_argument("--output-dir", default=DEFAULT_CONTEXT_DIR, help="Output folder for JSON prompt packs.")
    parser.add_argument(
        "--output-mode",
        choices=(CONTEXT_OUTPUT_CENTRAL, CONTEXT_OUTPUT_COMPLAINT_FOLDER),
        default=CONTEXT_OUTPUT_CENTRAL,
        help="Write packs to a central folder or beside each complaint ODT.",
    )
    parser.add_argument("--complaint-number", default="", help="Only build one complaint number.")
    parser.add_argument("--top-examples", type=int, default=5, help="Relevant examples/reply blocks per pack.")
    parser.add_argument("--include-unverified-policy", action="store_true", help="Include unverified policy rows.")
    parser.add_argument("--write-prompt-text", action="store_true", help="Also write a .prompt.txt file.")
    add_nocodb_args(parser)
    args = parser.parse_args(argv)

    logger = logging.getLogger(LOGGER_NAME)
    project_root = project_root_from_cwd()
    pending_dir = resolve_path(project_root, args.pending_dir)
    output_dir = resolve_path(project_root, args.output_dir)
    if args.output_mode == CONTEXT_OUTPUT_CENTRAL:
        output_dir.mkdir(parents=True, exist_ok=True)

    reports = read_pending_reports(pending_dir)
    if args.complaint_number:
        reports = [report for report in reports if report.complaint_number == args.complaint_number]
    if not reports:
        logger.error("No matching pending reports found in %s", pending_dir)
        return 1

    client = build_client_from_args(args)
    for report in reports:
        context_pack = build_context_pack(
            client,
            report,
            top_examples=max(1, args.top_examples),
            include_unverified_policy=args.include_unverified_policy,
        )
        json_path, _prompt_path = write_context_pack_files(
            context_pack,
            report,
            output_dir,
            args.output_mode,
            write_prompt_text=args.write_prompt_text,
        )
        client.insert_record(
            "Generated Draft Audits",
            {
                "Title": f"Context pack - {report.complaint_number}",
                "Complaint Number": report.complaint_number,
                "Paperless Document ID": "",
                "Issue Code": context_pack["classification"]["issue_code"],
                "Selected Rule Code": context_pack["classification"]["matched_rule"],
                "Confidence Score": context_pack["classification"]["score"],
                "Matched Facts": json.dumps(context_pack["classification"]["matched_terms"], ensure_ascii=False),
                "Blocks Used": json.dumps(
                    [block.get("block_code") for block in context_pack["reply_blocks"]],
                    ensure_ascii=False,
                ),
                "Review Status": "context_pack_created_manual_review_required",
                "Generated Reply": "",
                "Generated At": datetime.now(timezone.utc).isoformat(),
                "Notes": str(json_path),
            },
        )
        logger.info("Wrote CRM reply context pack: %s", json_path)

    logger.info("Built %d CRM reply context pack(s).", len(reports))
    return 0
