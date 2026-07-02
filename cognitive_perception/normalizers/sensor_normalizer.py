"""
Sensor Normalizer  (source_type: sensor)
=========================================
Normalizes IoT/physical sensor readings into CognitiveEvents.

Supports: temperature, humidity, pressure, motion, GPS/geofence,
and generic numeric threshold sensors via MQTT or HTTP push.

Each sensor type has configurable thresholds via the source config.
"""

from __future__ import annotations

from ..schema.event import CognitiveEvent, Severity, SourceType
from .base import BaseNormalizer


class SensorNormalizer(BaseNormalizer):
    """
    Normalizes sensor readings into CognitiveEvents.

    raw_input format (from MQTT or HTTP push adapter):
    {
        "sensor_type": "temperature" | "humidity" | "motion" | "gps" | ...,
        "value": 42.5,
        "unit": "celsius",
        "location": "server-room-a",
        "timestamp": "...",
        "thresholds": {
            "warn": 35.0,
            "critical": 45.0,
        },
        "battery_pct": 85,       # optional
        "signal_strength": -65,  # optional, dBm
    }
    """

    source_type = SourceType.SENSOR

    def _normalize(self, raw_input: dict, source_id: str) -> CognitiveEvent:
        sensor_type = raw_input.get("sensor_type", "generic")
        value       = raw_input.get("value", 0)
        unit        = raw_input.get("unit", "")
        location    = raw_input.get("location", "")
        ts_str      = raw_input.get("timestamp")
        battery     = raw_input.get("battery_pct")
        thresholds  = raw_input.get("thresholds", {})
        warn_val    = thresholds.get("warn")
        crit_val    = thresholds.get("critical")

        # ── Battery check (always takes priority) ────────────────────
        if battery is not None and battery < 10:
            return self._build_event(
                source_id=source_id,
                event_type="sensor_battery_low",
                severity=Severity.HIGH if battery < 5 else Severity.MEDIUM,
                payload={
                    "sensor_type": sensor_type,
                    "battery_pct": battery,
                    "location":    location,
                },
                entity_refs=[source_id],
                confidence=0.99,
                tags=["sensor", sensor_type, "battery"],
                timestamp=self._parse_timestamp(ts_str),
            )

        # ── Dispatch by sensor type ──────────────────────────────────
        if sensor_type == "temperature":
            return self._normalize_threshold(
                raw_input, source_id,
                event_exceeded="temperature_threshold_exceeded",
                event_critical="temperature_critical",
                event_normal="temperature_normalized",
                value=value, warn=warn_val or 35, critical=crit_val or 45,
            )

        elif sensor_type == "humidity":
            return self._normalize_threshold(
                raw_input, source_id,
                event_exceeded="humidity_threshold_exceeded",
                event_critical="humidity_critical",
                event_normal="humidity_normalized",
                value=value, warn=warn_val or 70, critical=crit_val or 90,
            )

        elif sensor_type == "motion":
            return self._build_event(
                source_id=source_id,
                event_type="motion_detected",
                severity=Severity.MEDIUM,
                payload=self._base_payload(raw_input),
                entity_refs=[source_id],
                confidence=0.92,
                tags=["sensor", "motion", location],
                timestamp=self._parse_timestamp(ts_str),
            )

        elif sensor_type == "gps":
            event_type = raw_input.get("geofence_event", "location_anomaly")
            return self._build_event(
                source_id=source_id,
                event_type=event_type,
                severity=Severity.MEDIUM,
                payload=self._base_payload(raw_input),
                entity_refs=[source_id],
                confidence=0.88,
                tags=["sensor", "gps", location],
                timestamp=self._parse_timestamp(ts_str),
            )

        elif sensor_type == "pressure":
            return self._normalize_threshold(
                raw_input, source_id,
                event_exceeded="pressure_anomaly_detected",
                event_critical="pressure_anomaly_detected",
                event_normal="pressure_anomaly_detected",
                value=value, warn=warn_val or 1050, critical=crit_val or 1100,
            )

        else:
            # Generic threshold-based sensor
            if crit_val is not None and value >= crit_val:
                event_type = "sensor_reading_anomaly"
                severity   = Severity.CRITICAL
            elif warn_val is not None and value >= warn_val:
                event_type = "sensor_reading_anomaly"
                severity   = Severity.MEDIUM
            else:
                event_type = "sensor_reading_anomaly"
                severity   = Severity.INFO

            return self._build_event(
                source_id=source_id,
                event_type=event_type,
                severity=severity,
                payload=self._base_payload(raw_input),
                entity_refs=[source_id],
                confidence=0.85,
                tags=["sensor", sensor_type, location],
                timestamp=self._parse_timestamp(ts_str),
            )

    def _normalize_threshold(
        self,
        raw_input: dict,
        source_id: str,
        event_exceeded: str,
        event_critical: str,
        event_normal: str,
        value: float,
        warn: float,
        critical: float,
    ) -> CognitiveEvent:
        """Shared threshold logic for temperature / humidity / pressure."""
        ts_str   = raw_input.get("timestamp")
        location = raw_input.get("location", "")
        s_type   = raw_input.get("sensor_type", "generic")

        if value >= critical:
            event_type = event_critical
            severity   = Severity.CRITICAL
            confidence = 0.97
        elif value >= warn:
            event_type = event_exceeded
            severity   = Severity.MEDIUM
            confidence = 0.95
        else:
            event_type = event_normal
            severity   = Severity.INFO
            confidence = 0.93

        return self._build_event(
            source_id=source_id,
            event_type=event_type,
            severity=severity,
            payload=self._base_payload(raw_input),
            entity_refs=[source_id],
            confidence=confidence,
            tags=["sensor", s_type, location],
            timestamp=self._parse_timestamp(ts_str),
        )

    @staticmethod
    def _base_payload(raw: dict) -> dict:
        """Extract common payload fields."""
        return {
            "sensor_type":     raw.get("sensor_type", "generic"),
            "value":           raw.get("value"),
            "unit":            raw.get("unit", ""),
            "location":        raw.get("location", ""),
            "battery_pct":     raw.get("battery_pct"),
            "signal_strength": raw.get("signal_strength"),
        }
