from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCHEMAS = ROOT / "packages/master_data/master_data/schemas.py"
REPOSITORY = ROOT / "packages/master_data/master_data/postgres_repository.py"
API = ROOT / "packages/master_data/master_data/api.py"
ACTIONS = ROOT / "apps/web/src/actions/index.ts"
OFFICERS = ROOT / "apps/web/src/pages/master-data/officers.astro"
JURISDICTIONS = ROOT / "apps/web/src/pages/master-data/jurisdictions.astro"
MARKAZES = ROOT / "apps/web/src/pages/master-data/markazes.astro"
MIGRATION = ROOT / "alembic/versions/d6e9f2a4b705_officer_jurisdiction_scope_ownership.py"
REPAIR = ROOT / "scripts/repair_officer_jurisdictions.py"


def test_officer_write_supports_complete_scope_sync_and_explicit_transfer() -> None:
    schemas = SCHEMAS.read_text(encoding="utf-8")
    repository = REPOSITORY.read_text(encoding="utf-8")

    assert "jurisdiction_refs: list[str] | None = None" in schemas
    assert "replace_conflicts: bool = False" in schemas
    assert "def _sync_officer_jurisdictions(" in repository
    assert 'jurisdiction_mode: str = "replace"' in repository
    assert "Enable 'Transfer existing assignments'" in repository


def test_master_data_exposes_transactional_jurisdiction_commands() -> None:
    api = API.read_text(encoding="utf-8")
    actions = ACTIONS.read_text(encoding="utf-8")

    assert '@router.post("/jurisdictions"' in api
    assert '@router.delete("/jurisdictions/{jurisdiction_id}")' in api
    assert '@router.post("/jurisdictions/{jurisdiction_id}/make-primary")' in api
    assert "assignJurisdiction:" in actions
    assert "endJurisdiction:" in actions
    assert "makePrimaryJurisdiction:" in actions


def test_officer_and_jurisdiction_pages_manage_primary_and_additional_charges() -> None:
    officers = OFFICERS.read_text(encoding="utf-8")
    jurisdictions = JURISDICTIONS.read_text(encoding="utf-8")

    assert 'id="officer-additional" multiple' in officers
    assert 'id="officer-replace-conflicts"' in officers
    assert "jurisdiction_refs: jurisdictionRefs" in officers
    assert "Save officer & assignments" in officers
    assert 'id="jurisdiction-transfer"' in jurisdictions
    assert "Make primary" in jurisdictions
    assert "End assignment" in jurisdictions
    assert "activeJurisdictions" in jurisdictions


def test_markaz_catalog_has_direct_assign_or_transfer_action() -> None:
    source = MARKAZES.read_text(encoding="utf-8")

    assert "Assign AEO" in source
    assert "Transfer AEO" in source
    assert "/master-data/jurisdictions?role=aeo" in source


def test_database_enforces_single_active_owner_per_scope() -> None:
    migration = MIGRATION.read_text(encoding="utf-8")

    assert 'revision = "d6e9f2a4b705"' in migration
    assert 'down_revision = "c3d8e1f4a702"' in migration
    assert "uq_officer_jurisdictions_active_aeo_scope" in migration
    assert "uq_officer_jurisdictions_active_ddeo_scope" in migration
    assert "active = TRUE AND role = 'aeo'" in migration
    assert "active = TRUE AND role = 'ddeo'" in migration


def test_repair_is_dry_run_by_default_and_has_data_restore() -> None:
    source = REPAIR.read_text(encoding="utf-8")

    assert 'parser.add_argument("--apply", action="store_true"' in source
    assert 'parser.add_argument("--backup", type=Path' in source
    assert 'parser.add_argument("--restore", type=Path' in source
    assert "Dry run only." in source
    assert "replace_conflicts=True" in source
