"""
Event Type Vocabulary
=====================
Define EVERY event_type label here before writing any normalizer.
This is the system's semantic vocabulary — the language the reasoning
engine speaks when querying the world model.

Naming convention: snake_case, past-tense or noun phrase describing
what happened — not what was observed, but what it MEANS.
  BAD:  "high_cpu"          → describes a reading
  GOOD: "cpu_spike"         → describes a state change worth acting on
  BAD:  "log_line_received" → describes the adapter's action
  GOOD: "jwt_validation_failure" → describes what happened in the system

Import EVENT_TYPES anywhere you need to validate or reference event types.
Never hardcode event_type strings in normalizer code — always reference here.
"""

# ─────────────────────────────────────────────
# 1. APPLICATION LOG EVENTS  (source_type: log)
# ─────────────────────────────────────────────

LOG_EVENTS: dict[str, str] = {
    # Service lifecycle
    "service_started":             "Service process came up",
    "service_stopped":             "Service process exited cleanly",
    "service_crashed":             "Service process exited unexpectedly",
    "service_restarted":           "Service was restarted (manual or automated)",

    # Error conditions
    "unhandled_exception":         "Unhandled exception reached top-level handler",
    "http_500_error":              "Internal server error returned to client",
    "http_4xx_spike":              "Elevated rate of 4xx client errors",
    "dependency_call_failed":      "Call to downstream service or DB failed",
    "database_connection_timeout": "Connection to database timed out",
    "database_query_timeout":      "SQL query exceeded timeout threshold",
    "connection_pool_exhausted":   "All DB connections in use, new requests waiting",
    "memory_leak_warning":         "Heap growing continuously, GC not reclaiming",
    "disk_write_failed":           "Could not write to filesystem",

    # Authentication / auth
    "jwt_validation_failure":      "JWT token rejected (expired, invalid signature, etc.)",
    "session_invalid":             "Session token not found or expired",
    "permission_denied":           "Authorization check failed for requested resource",
    "authentication_success":      "User successfully authenticated",

    # Deployment
    "deployment_started":          "New version deployment begun",
    "deployment_completed":        "Deployment finished successfully",
    "deployment_failed":           "Deployment aborted due to error",
    "rollback_triggered":          "Previous version being restored",
    "config_reloaded":             "Application config reloaded without restart",

    # Performance
    "slow_request":                "Request exceeded latency SLO threshold",
    "request_timeout":             "Outgoing request timed out",
    "rate_limit_hit":              "Caller hit rate limit, request rejected",

    # Framework-specific
    "orm_n_plus_one_detected":     "N+1 query pattern detected in ORM layer",
    "cache_miss_spike":            "Cache hit rate dropped significantly",
    "worker_queue_backup":         "Task worker queue depth growing",
}

# ─────────────────────────────────────────────
# 2. SYSTEM METRIC EVENTS  (source_type: metric)
# ─────────────────────────────────────────────

METRIC_EVENTS: dict[str, str] = {
    # CPU
    "cpu_spike":                   "CPU usage crossed high threshold",
    "cpu_sustained_high":          "CPU above threshold for sustained period",
    "cpu_returned_normal":         "CPU usage returned to normal range",

    # Memory
    "memory_pressure_high":        "Available memory below safe threshold",
    "memory_exhaustion":           "System memory critically low, OOM possible",
    "swap_usage_high":             "System swapping heavily, performance impacted",

    # Disk
    "disk_usage_high":             "Disk partition usage approaching capacity",
    "disk_io_saturation":          "Disk I/O wait time elevated",
    "disk_full":                   "Disk partition has no free space",

    # Network (infrastructure level)
    "network_throughput_spike":    "Network bytes/s crossed abnormal threshold",
    "network_errors_elevated":     "Network interface error rate elevated",
    "packet_loss_detected":        "Packet loss observed on network interface",

    # Process / runtime
    "process_count_anomaly":       "Process count outside expected range",
    "thread_count_spike":          "Thread count elevated, possible leak",
    "fd_limit_approaching":        "Open file descriptor count near OS limit",
    "gc_pause_excessive":          "Garbage collection pause time too long",
    "gc_frequency_high":           "GC running too frequently",

    # Container / Kubernetes
    "container_oom_killed":        "Container killed by OOM killer",
    "container_restart":           "Container restarted (could be crash loop)",
    "container_cpu_throttled":     "Container CPU being throttled by cgroup limit",
    "pod_pending_too_long":        "Kubernetes pod stuck in Pending state",
    "node_not_ready":              "Kubernetes node entered NotReady state",
}

# ─────────────────────────────────────────────
# 3. API & SERVICE EVENTS  (source_type: api)
# ─────────────────────────────────────────────

API_EVENTS: dict[str, str] = {
    # Health state changes
    "service_health_degraded":     "Health check returned non-200 or slow response",
    "service_health_restored":     "Health check returned to passing state",
    "service_unreachable":         "Health check connection refused or timed out",
    "service_ssl_expiring":        "TLS certificate expiring within warning threshold",
    "service_ssl_expired":         "TLS certificate is expired",

    # Latency
    "api_latency_spike":           "Response time crossed p95 threshold",
    "api_latency_sustained":       "Elevated latency for sustained period",
    "api_latency_normalized":      "Response time returned to normal",

    # Error rates
    "api_error_rate_high":         "HTTP 5xx rate exceeded threshold",
    "api_4xx_rate_high":           "HTTP 4xx rate elevated (bad requests / auth failures)",

    # DNS
    "dns_resolution_failed":       "DNS lookup returned NXDOMAIN or timed out",
    "dns_latency_high":            "DNS resolution taking longer than expected",

    # External dependencies
    "external_api_degraded":       "Third-party API response degraded",
    "external_api_down":           "Third-party API not responding",
    "payment_gateway_error":       "Payment processor returned error",
    "auth_provider_degraded":      "OAuth/SSO provider response slow or failing",
}

# ─────────────────────────────────────────────
# 4. DATABASE EVENTS  (source_type: database)
# ─────────────────────────────────────────────

DATABASE_EVENTS: dict[str, str] = {
    # Performance
    "slow_query_detected":         "Query exceeded slow query threshold",
    "query_latency_spike":         "Average query latency crossed threshold",
    "deadlock_detected":           "Database deadlock detected and resolved",
    "lock_wait_timeout":           "Transaction waited too long for lock",

    # Connections
    "connection_pool_high":        "Pool usage above warning threshold",
    "connection_pool_exhausted":   "All connections in use, new requests blocked",
    "connection_leak_detected":    "Connections not being returned to pool",
    "max_connections_reached":     "DB max_connections limit reached",

    # Replication
    "replication_lag_high":        "Replica is significantly behind primary",
    "replication_broken":          "Replication slot disconnected or broken",

    # Storage
    "table_size_growth_alert":     "Table growing faster than expected",
    "index_bloat_detected":        "Index has excessive dead tuples",
    "vacuum_not_running":          "Autovacuum not keeping up with dead tuples",
    "disk_space_for_db_low":       "Database tablespace running low on space",

    # Cache
    "cache_hit_ratio_dropped":     "Redis/Memcached hit ratio below threshold",
    "cache_eviction_spike":        "Cache evicting keys faster than expected",
}

# ─────────────────────────────────────────────
# 5. QUEUE EVENTS  (source_type: queue)
# ─────────────────────────────────────────────

QUEUE_EVENTS: dict[str, str] = {
    "consumer_lag_high":           "Consumer group falling behind message production",
    "consumer_lag_critical":       "Consumer lag critical — messages very old",
    "consumer_lag_resolved":       "Consumer lag returned to acceptable level",
    "queue_depth_high":            "Queue depth above warning threshold",
    "dead_letter_queue_growing":   "Dead-letter queue accumulating — messages failing",
    "consumer_group_stopped":      "All consumers in group stopped processing",
    "message_age_exceeded":        "Oldest unprocessed message age too high",
    "retry_storm_detected":        "Messages being retried at abnormally high rate",
    "partition_leader_change":     "Kafka partition leadership changed",
    "producer_rate_drop":          "Message production rate dropped significantly",
}

# ─────────────────────────────────────────────
# 6. FILESYSTEM EVENTS  (source_type: file)
# ─────────────────────────────────────────────

FILE_EVENTS: dict[str, str] = {
    "config_file_modified":        "Configuration file was changed",
    "config_file_deleted":         "Configuration file was removed",
    "certificate_file_changed":    "TLS certificate file was replaced",
    "certificate_expiry_warning":  "Certificate file will expire within threshold",
    "certificate_expired":         "Certificate file has expired",
    "deployment_artifact_created": "New deployment artifact appeared",
    "secret_file_modified":        "Secrets file was modified (potential security issue)",
    "static_asset_missing":        "Expected static asset file not found",
    "upload_received":             "User-uploaded file arrived in upload directory",
    "log_file_rotated":            "Log file was rotated by logrotate",
    "disk_quota_exceeded":         "File write rejected — disk quota exceeded",
}

# ─────────────────────────────────────────────
# 7. USER INTERACTION EVENTS  (source_type: user_event)
# ─────────────────────────────────────────────

USER_EVENTS: dict[str, str] = {
    # Navigation
    "page_viewed":                 "User viewed a page",
    "page_exit":                   "User left a page",
    "navigation_started":          "User initiated navigation to new route",

    # Engagement / frustration signals
    "rage_click_detected":         "User clicked same element 3+ times rapidly",
    "dead_click_detected":         "User clicked element that had no response",
    "scroll_depth_reached":        "User scrolled to significant depth threshold",
    "session_started":             "New user session began",
    "session_ended":               "User session ended (timeout or explicit logout)",
    "session_duration_short":      "Session ended unusually quickly (possible friction)",

    # Conversion / funnel
    "cart_item_added":             "Item added to shopping cart",
    "cart_abandoned":              "Cart left without checkout",
    "checkout_started":            "User began checkout flow",
    "checkout_completed":          "Purchase successfully completed",
    "checkout_failed":             "Checkout attempt failed",
    "signup_started":              "User began registration flow",
    "signup_completed":            "Registration successfully completed",
    "signup_abandoned":            "Registration started but not completed",

    # Forms
    "form_submitted":              "Form submission attempted",
    "form_validation_failed":      "Form failed client-side validation",
    "form_submission_error":       "Form submission received error response",

    # Feature usage
    "feature_used":                "User interacted with a specific feature",
    "search_performed":            "User executed a search query",
    "search_zero_results":         "Search returned no results",
}

# ─────────────────────────────────────────────
# 8. BROWSER ENVIRONMENT EVENTS  (source_type: browser_event)
# ─────────────────────────────────────────────

BROWSER_EVENTS: dict[str, str] = {
    # JavaScript errors
    "js_exception":                "Unhandled JavaScript exception in browser",
    "promise_rejection":           "Unhandled promise rejection in browser",
    "js_chunk_load_failed":        "Code splitting chunk failed to load",

    # Rendering / framework
    "hydration_failure":           "SSR hydration mismatch detected (React/Next.js)",
    "render_error":                "Component render threw an error",
    "white_screen_of_death":       "Page rendered empty — likely fatal JS error",

    # Core Web Vitals (Good / Needs Improvement / Poor)
    "lcp_poor":                    "Largest Contentful Paint exceeded 4s threshold",
    "lcp_needs_improvement":       "LCP between 2.5s and 4s",
    "fid_poor":                    "First Input Delay exceeded 300ms",
    "cls_poor":                    "Cumulative Layout Shift score above 0.25",
    "inp_poor":                    "Interaction to Next Paint exceeded 500ms (replaces FID)",
    "ttfb_high":                   "Time To First Byte elevated",
    "fcp_slow":                    "First Contentful Paint slow",
    "page_load_slow":              "Total page load time exceeded threshold",

    # Network (client-side)
    "asset_load_failed":           "CSS, JS, or image asset failed to load",
    "api_request_failed_client":   "Client-side fetch/XHR returned error",
    "slow_network_detected":       "Client network connection identified as slow",

    # Compatibility
    "browser_compatibility_error": "Feature not supported in user's browser",
}

# ─────────────────────────────────────────────
# 9. SECURITY EVENTS  (source_type: security_event)
# ─────────────────────────────────────────────

SECURITY_EVENTS: dict[str, str] = {
    # Authentication attacks
    "brute_force_detected":           "Multiple failed auth attempts from single IP",
    "credential_stuffing_detected":    "Distributed login attempts across many accounts",
    "account_lockout_triggered":       "Account locked after repeated failed logins",
    "mfa_bypass_attempt":              "Attempted to bypass multi-factor authentication",

    # Traffic attacks
    "ddos_detected":                   "Distributed denial of service attack detected",
    "rate_limit_abuse":                "IP exceeding rate limits aggressively",
    "bot_traffic_detected":            "Automated bot traffic pattern identified",
    "scraping_detected":               "Systematic content scraping pattern detected",

    # WAF / firewall
    "waf_rule_triggered":              "Web Application Firewall rule matched request",
    "sql_injection_attempt":           "SQL injection pattern in request",
    "xss_attempt":                     "Cross-site scripting attempt detected",
    "path_traversal_attempt":          "Directory traversal attempt detected",

    # Anomalies
    "geo_anomaly_detected":            "Request from unexpected geographic location",
    "unusual_traffic_pattern":         "Traffic pattern deviates from baseline",
    "privilege_escalation_attempt":    "Attempt to access resources above permission level",
    "suspicious_admin_action":         "Admin action outside normal hours or pattern",

    # Data / fraud
    "data_exfiltration_pattern":       "Unusually large data download pattern",
    "fraud_signal_detected":           "Payment or account fraud indicators present",
}

# ─────────────────────────────────────────────
# 10. SENSOR EVENTS  (source_type: sensor)
# ─────────────────────────────────────────────

SENSOR_EVENTS: dict[str, str] = {
    # Temperature
    "temperature_threshold_exceeded":  "Sensor temperature above warning threshold",
    "temperature_critical":            "Sensor temperature above critical threshold",
    "temperature_normalized":          "Sensor temperature returned to normal range",

    # Humidity
    "humidity_threshold_exceeded":     "Humidity above or below safe threshold",
    "humidity_critical":               "Humidity at critically unsafe level",

    # Pressure
    "pressure_anomaly_detected":       "Pressure reading outside expected range",

    # Motion / presence
    "motion_detected":                 "Motion sensor triggered",
    "intrusion_detected":              "Unauthorized presence detected by sensor",
    "occupancy_changed":               "Room/area occupancy state changed",

    # Device health
    "sensor_offline":                  "Sensor stopped sending data",
    "sensor_battery_low":              "Sensor battery below warning threshold",
    "sensor_reading_anomaly":          "Sensor reading is statistically anomalous",
    "sensor_calibration_required":     "Sensor reading drift suggests recalibration needed",

    # GPS / location
    "geofence_entered":                "Device entered a defined geographic boundary",
    "geofence_exited":                 "Device exited a defined geographic boundary",
    "location_anomaly":                "Device location is statistically unexpected",
}

# ─────────────────────────────────────────────
# 11. AGENT EVENTS  (source_type: agent_event)
# ─────────────────────────────────────────────

AGENT_EVENTS: dict[str, str] = {
    # Decisions
    "reasoning_completed":             "Reasoning engine produced a decision",
    "plan_generated":                  "Planning system produced an execution plan",
    "action_approved":                 "Human approved a high-risk action",
    "action_rejected":                 "Human rejected a proposed action",

    # Execution
    "action_started":                  "Agent began executing a plan step",
    "action_completed":                "Plan step executed successfully",
    "action_failed":                   "Plan step execution failed",
    "rollback_executed":               "Compensating action executed after failure",

    # Task lifecycle
    "task_delegated":                  "Task assigned to specialist agent",
    "task_completed":                  "Task finished successfully",
    "task_failed":                     "Task could not be completed",
    "task_escalated":                  "Task escalated to human operator",

    # Anomalies
    "reasoning_loop_detected":         "Agent appears to be in a reasoning loop",
    "confidence_too_low":              "Situation confidence below action threshold",
    "human_input_required":            "Agent cannot proceed without human decision",
}

# ─────────────────────────────────────────────
# COMBINED VOCABULARY LOOKUP
# ─────────────────────────────────────────────

ALL_EVENT_TYPES: dict[str, str] = {
    **LOG_EVENTS,
    **METRIC_EVENTS,
    **API_EVENTS,
    **DATABASE_EVENTS,
    **QUEUE_EVENTS,
    **FILE_EVENTS,
    **USER_EVENTS,
    **BROWSER_EVENTS,
    **SECURITY_EVENTS,
    **SENSOR_EVENTS,
    **AGENT_EVENTS,
}


def get_event_description(event_type: str) -> str:
    """Look up the human-readable description for an event_type."""
    return ALL_EVENT_TYPES.get(event_type, f"Unknown event type: {event_type}")


def is_known_event_type(event_type: str) -> bool:
    """Return True if event_type is in the known vocabulary."""
    return event_type in ALL_EVENT_TYPES


def get_events_by_source(source_type: str) -> dict[str, str]:
    """Return all event types for a given source_type string."""
    mapping = {
        "log":            LOG_EVENTS,
        "metric":         METRIC_EVENTS,
        "api":            API_EVENTS,
        "database":       DATABASE_EVENTS,
        "queue":          QUEUE_EVENTS,
        "file":           FILE_EVENTS,
        "user_event":     USER_EVENTS,
        "browser_event":  BROWSER_EVENTS,
        "security_event": SECURITY_EVENTS,
        "sensor":         SENSOR_EVENTS,
        "agent_event":    AGENT_EVENTS,
    }
    return mapping.get(source_type, {})
