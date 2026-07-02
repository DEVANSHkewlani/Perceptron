"""
File Normalizer  (source_type: file)
=====================================
Converts filesystem events into CognitiveEvents.
Uses OS kernel inotify/fsevents via Python watchdog — not polling.

Special logic:
  - Certificate files (.pem, .crt) → auto-check expiry
  - Secret files (password, key, token in name) → always HIGH severity
  - Config files (.yaml, .conf, etc.) → track modifications
"""

from __future__ import annotations

import re
from pathlib import Path

from ..schema.event import CognitiveEvent, Severity, SourceType
from .base import BaseNormalizer


class FileNormalizer(BaseNormalizer):
    """
    Converts a filesystem event dict into a CognitiveEvent.

    Input format (from FileAdapter):
    {
        "event_kind": "created" | "modified" | "deleted" | "moved",
        "path": "/etc/nginx/conf.d/default.conf",
        "is_directory": False,
        "is_secret": False,
        "cert_days_remaining": None | int,
        "source_id": "file:nginx-config",
    }
    """

    source_type = SourceType.FILE

    # File extension → semantic category
    CERT_EXTENSIONS = {".pem", ".crt", ".cer", ".p12", ".pfx", ".der"}
    SECRET_PATTERNS = [
        re.compile(
            r"(secret|credential|password|key|token|api_key)", re.IGNORECASE
        ),
        re.compile(r"\.(env|secret|key)$", re.IGNORECASE),
    ]
    CONFIG_EXTENSIONS = {
        ".conf", ".cfg", ".yaml", ".yml", ".toml", ".ini", ".json",
    }

    def _normalize(self, raw_input: dict, source_id: str) -> CognitiveEvent:
        kind      = raw_input.get("event_kind", "modified")
        path      = raw_input.get("path", "")
        is_dir    = raw_input.get("is_directory", False)
        is_secret = raw_input.get("is_secret", False)
        cert_days = raw_input.get("cert_days_remaining")
        ext       = Path(path).suffix.lower()
        filename  = Path(path).name

        # ── Detect file category ─────────────────────────────────────
        is_cert   = ext in self.CERT_EXTENSIONS
        is_config = ext in self.CONFIG_EXTENSIONS
        is_secret = is_secret or any(
            p.search(filename) for p in self.SECRET_PATTERNS
        )

        # ── Certificate expiry check ─────────────────────────────────
        if is_cert and cert_days is not None:
            if cert_days <= 0:
                return self._build_event(
                    source_id=source_id,
                    event_type="certificate_expired",
                    severity=Severity.CRITICAL,
                    payload={"path": path, "days_remaining": cert_days},
                    entity_refs=[source_id, f"file:{filename}"],
                    confidence=0.99,
                    tags=["file", "certificate", "security"],
                )
            elif cert_days <= 7:
                return self._build_event(
                    source_id=source_id,
                    event_type="certificate_expiry_warning",
                    severity=Severity.CRITICAL,
                    payload={"path": path, "days_remaining": cert_days},
                    entity_refs=[source_id, f"file:{filename}"],
                    confidence=0.99,
                    tags=["file", "certificate", "security"],
                )
            elif cert_days <= 30:
                return self._build_event(
                    source_id=source_id,
                    event_type="certificate_expiry_warning",
                    severity=Severity.HIGH,
                    payload={"path": path, "days_remaining": cert_days},
                    entity_refs=[source_id, f"file:{filename}"],
                    confidence=0.99,
                    tags=["file", "certificate", "security"],
                )

        # ── Map kind + category → event_type ─────────────────────────
        if is_secret:
            event_type = "secret_file_modified"
            severity   = Severity.HIGH
            confidence = 0.95
        elif is_cert and kind in ("modified", "created"):
            event_type = "certificate_file_changed"
            severity   = Severity.MEDIUM
            confidence = 0.97
        elif is_config and kind == "modified":
            event_type = "config_file_modified"
            severity   = Severity.MEDIUM
            confidence = 0.97
        elif is_config and kind == "deleted":
            event_type = "config_file_deleted"
            severity   = Severity.HIGH
            confidence = 0.98
        elif kind == "created" and path.endswith(
            (".tar.gz", ".zip", ".whl", ".jar", ".war")
        ):
            event_type = "deployment_artifact_created"
            severity   = Severity.INFO
            confidence = 0.94
        elif kind == "deleted" and not is_dir:
            event_type = "static_asset_missing"
            severity   = Severity.MEDIUM
            confidence = 0.92
        else:
            event_type = "config_file_modified"
            severity   = Severity.LOW
            confidence = 0.80

        return self._build_event(
            source_id=source_id,
            event_type=event_type,
            severity=severity,
            payload={
                "path":         path,
                "filename":     filename,
                "event_kind":   kind,
                "is_directory": is_dir,
                "is_cert":      is_cert,
                "is_config":    is_config,
                "is_secret":    is_secret,
            },
            entity_refs=[source_id, f"file:{filename}"],
            confidence=confidence,
            tags=["file"] + (["security"] if is_secret else []),
        )
