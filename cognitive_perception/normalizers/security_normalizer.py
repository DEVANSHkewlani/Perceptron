"""
Security Event Normalizer  (source_type: security_event)
========================================================
Normalizes security signals from:
- Cloudflare WAF firewall events (webhook)
- Fail2ban actions (custom bridge)
- Custom rate-limit detection bridge
- Auth service security signals

IP reputation is deferred to the world model. The normalizer sets
entity_refs to security:ip-{addr} so the graph can accumulate IP history.
"""

from __future__ import annotations

from ..schema.event import CognitiveEvent, Severity, SourceType
from .base import BaseNormalizer


class SecurityEventNormalizer(BaseNormalizer):
    """
    Normalizes security signals into CognitiveEvents.
    """

    source_type = SourceType.SECURITY_EVENT

    # (rule_key → (event_type, severity, confidence))
    RULE_MAP: dict[str, tuple[str, Severity, float]] = {
        # WAF rules
        "WAF_BLOCK":              ("waf_rule_triggered",            Severity.MEDIUM,   0.85),
        "WAF_CHALLENGE":          ("waf_rule_triggered",            Severity.LOW,      0.80),
        "SQL_INJECTION":          ("sql_injection_attempt",         Severity.CRITICAL, 0.92),
        "XSS":                    ("xss_attempt",                   Severity.HIGH,     0.90),
        "PATH_TRAVERSAL":         ("path_traversal_attempt",        Severity.HIGH,     0.90),
        "COMMAND_INJECTION":      ("waf_rule_triggered",            Severity.CRITICAL, 0.88),
        "RATE_LIMIT":             ("rate_limit_abuse",              Severity.MEDIUM,   0.88),

        # Authentication attacks
        "BRUTE_FORCE":            ("brute_force_detected",          Severity.HIGH,     0.87),
        "CREDENTIAL_STUFFING":    ("credential_stuffing_detected",  Severity.CRITICAL, 0.85),
        "MFA_BYPASS":             ("mfa_bypass_attempt",            Severity.CRITICAL, 0.88),
        "ACCOUNT_LOCKOUT":        ("account_lockout_triggered",     Severity.MEDIUM,   0.93),

        # Traffic anomalies
        "DDOS":                   ("ddos_detected",                 Severity.CRITICAL, 0.90),
        "BOT_DETECTED":           ("bot_traffic_detected",          Severity.LOW,      0.78),
        "SCRAPING":               ("scraping_detected",             Severity.MEDIUM,   0.82),
        "GEO_ANOMALY":            ("geo_anomaly_detected",          Severity.MEDIUM,   0.72),

        # Privilege / access
        "PRIVILEGE_ESCALATION":   ("privilege_escalation_attempt",  Severity.CRITICAL, 0.88),
        "SUSPICIOUS_ADMIN":       ("suspicious_admin_action",       Severity.HIGH,     0.80),
        "UNUSUAL_DATA_ACCESS":    ("data_exfiltration_pattern",     Severity.HIGH,     0.75),
    }

    def _normalize(self, raw_input: dict, source_id: str) -> CognitiveEvent:
        raw_type   = raw_input.get(
            "type", raw_input.get("action", "WAF_BLOCK")
        ).upper()
        ip         = raw_input.get("ip", raw_input.get("client_ip", ""))
        country    = raw_input.get("country", "")
        uri        = raw_input.get("uri", raw_input.get("path", ""))
        ts_str     = raw_input.get("timestamp")
        req_count  = raw_input.get("request_count", 1)
        source_sys = raw_input.get("source_system", "unknown")

        if raw_type in self.RULE_MAP:
            event_type, severity, confidence = self.RULE_MAP[raw_type]
        else:
            event_type = "unusual_traffic_pattern"
            severity   = Severity.MEDIUM
            confidence = 0.70

        # High volume elevates severity
        if req_count > 1000 and severity in (Severity.MEDIUM, Severity.LOW):
            severity = Severity.HIGH

        # Entity refs
        entity_refs = [source_id]
        if ip:
            entity_refs.append(f"security:ip-{ip.replace('.', '-')}")
        if country:
            entity_refs.append(f"security:country-{country.lower()}")

        return self._build_event(
            source_id=source_id,
            event_type=event_type,
            severity=severity,
            payload={
                "raw_type":      raw_type,
                "ip":            ip,
                "country":       country,
                "uri":           uri,
                "request_count": req_count,
                "time_window_s": raw_input.get("time_window_s", 60),
                "user_agent":    raw_input.get("user_agent", ""),
                "source_system": source_sys,
                "rule":          raw_input.get("rule", ""),
            },
            entity_refs=self._dedupe_refs(entity_refs),
            confidence=confidence,
            tags=["security", source_sys, country],
            timestamp=self._parse_timestamp(ts_str),
        )
