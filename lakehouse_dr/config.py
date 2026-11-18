"""Filesystem layout for the DR drill environment."""
from __future__ import annotations

import os
from dataclasses import dataclass

PROD_MARKER = ".lakehouse-prod-marker"
RECOVERY_MARKER = ".recovery-target-marker"

CATALOG_NAME = "catalog.json"
CDC_DIR = "cdc"
CDC_LOG = "cdc-log.jsonl"
TABLES_DIR = "tables"
DATA_DIR = "data"
SNAPSHOT_DIR = "snapshots"


@dataclass(frozen=True)
class DRContext:
    """Resolved paths for one DR environment.

    ``home`` holds three independent trees so a recovery can never alias the
    production lakehouse by construction:

    * ``lakehouse/``      - the simulated production lakehouse
    * ``backup_store/``   - immutable backups, content-addressed blobs, CDC mirror
    * ``recovery_area/``  - one directory per recovery drill
    """

    home: str

    @property
    def lakehouse_root(self) -> str:
        return os.path.join(self.home, "lakehouse")

    @property
    def backup_root(self) -> str:
        return os.path.join(self.home, "backup_store")

    @property
    def recovery_root(self) -> str:
        return os.path.join(self.home, "recovery_area")

    @property
    def tmp_root(self) -> str:
        return os.path.join(self.home, "tmp")

    # --- lakehouse -------------------------------------------------------
    @property
    def lakehouse_catalog(self) -> str:
        return os.path.join(self.lakehouse_root, CATALOG_NAME)

    @property
    def lakehouse_cdc_log(self) -> str:
        return os.path.join(self.lakehouse_root, CDC_DIR, CDC_LOG)

    # --- backup store ----------------------------------------------------
    @property
    def backup_index(self) -> str:
        return os.path.join(self.backup_root, "index.json")

    @property
    def backup_blob_dir(self) -> str:
        return os.path.join(self.backup_root, "blobs")

    @property
    def backup_cdc_log(self) -> str:
        return os.path.join(self.backup_root, CDC_DIR, CDC_LOG)

    @property
    def file_versions_path(self) -> str:
        return os.path.join(self.backup_root, "file-versions.json")

    def backup_dir(self, backup_id: str) -> str:
        return os.path.join(self.backup_root, "backups", backup_id)

    def manifest_path(self, backup_id: str) -> str:
        return os.path.join(self.backup_dir(backup_id), "manifest.json")

    # --- recovery area ---------------------------------------------------
    def recovery_dir(self, recovery_id: str) -> str:
        return os.path.join(self.recovery_root, recovery_id)

    def journal_path(self, recovery_id: str) -> str:
        return os.path.join(self.recovery_dir(recovery_id), "journal.json")
