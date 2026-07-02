from __future__ import annotations
import asyncio, logging, time, os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Awaitable
import docker, asyncpg, httpx

logger = logging.getLogger("chaos-controller")
DOCKER_COMPOSE_PROJECT = "shopcore"

class ScenarioState(Enum):
    IDLE       = "idle"
    ACTIVE     = "active"
    RECOVERING = "recovering"
    FAILED     = "failed"

@dataclass
class ChaosScenario:
    name:                   str
    description:            str
    severity:               str     # critical | high | medium | low
    expected_event_type:    str     # maps to DCA CognitiveEvent.event_type
    expected_detection_s:   float   # detection SLO in seconds
    expected_remediation_s: float   # remediation SLO in seconds
    activate:               Callable[[dict], Awaitable[None]]
    deactivate:             Callable[[], Awaitable[None]]
    verify_injected:        Callable[[], Awaitable[bool]]
    state:        ScenarioState = ScenarioState.IDLE
    activated_at: datetime | None  = None
    detected_at:  datetime | None  = None
    remediated_at:datetime | None  = None
    params:       dict             = field(default_factory=dict)

class DockerChaos:
    """Inject failures by setting env vars or manipulating containers."""

    def __init__(self):
        self.client = docker.from_env()

    def _get_container(self, service_name: str):
        containers = self.client.containers.list(
            all=True,
            filters={"label": f"com.shopcore.service={service_name}"}
        )
        if not containers:
            containers = self.client.containers.list(
                all=True,
                filters={"name": service_name}
            )
        if not containers:
            raise RuntimeError(f"Container not found: {service_name}")
        return containers[0]

    def set_env(self, service_name: str, env_vars: dict[str, str]):
        """Update environment variables on a running container by recreating it with port mappings preserved."""
        container = self._get_container(service_name)
        image = container.image
        config = container.attrs["Config"]
        host_config = container.attrs["HostConfig"]
        
        # Merge environment variables
        current_env = dict(e.split("=", 1) for e in config.get("Env", []))
        current_env.update(env_vars)
        new_env = [f"{k}={v}" for k, v in current_env.items()]

        name = container.name
        labels = config.get("Labels", {})
        cmd = config.get("Cmd")
        entrypoint = config.get("Entrypoint")
        
        # Extract port bindings correctly
        port_bindings = host_config.get("PortBindings", {})
        ports = {}
        if port_bindings:
            for container_port, host_ports in port_bindings.items():
                if host_ports:
                    ports[container_port] = host_ports[0].get("HostPort")

        # Extract volume binds correctly
        binds = host_config.get("Binds", [])

        # Extract network configuration dynamically
        networks = container.attrs.get("NetworkSettings", {}).get("Networks", {})
        network_name = None
        for net in networks.keys():
            if net != "bridge":
                network_name = net
                break
        
        # Fallback to search by compose project label if only bridge network is found
        if not network_name:
            project_name = labels.get("com.docker.compose.project")
            if project_name:
                for net_obj in self.client.networks.list():
                    net_labels = net_obj.attrs.get("Labels") or {}
                    if net_labels.get("com.docker.compose.project") == project_name:
                        network_name = net_obj.name
                        break

        # Compute aliases
        aliases = []
        if network_name and network_name in networks:
            aliases = networks[network_name].get("Aliases") or []
            aliases = [a for a in aliases if len(a) != 64 and a != name and a != container.id]
        
        service_alias = labels.get("com.docker.compose.service")
        if service_alias and service_alias not in aliases:
            aliases.append(service_alias)
        if name and name not in aliases:
            aliases.append(name)

        container.stop(timeout=5)
        container.remove()

        # Recreate container on default bridge first
        new_container = self.client.containers.create(
            image,
            name=name,
            environment=new_env,
            ports=ports,
            volumes=binds,
            labels=labels,
            command=cmd,
            entrypoint=entrypoint,
            restart_policy={"Name": "on-failure"}
        )

        # Explicitly connect to the custom network with aliases
        if network_name and network_name != "bridge":
            try:
                net_obj = self.client.networks.get(network_name)
                net_obj.connect(new_container, aliases=aliases)
            except Exception as ex:
                logger.error(f"Failed to connect container to network {network_name}: {ex}")
                
            # Disconnect from default bridge network to keep it clean
            try:
                bridge_net = self.client.networks.get("bridge")
                bridge_net.disconnect(new_container)
            except Exception:
                pass

        new_container.start()
        logger.info(f"[Chaos] Set env on {service_name}: {env_vars}")
        return new_container

    def stop_container(self, service_name: str):
        container = self._get_container(service_name)
        container.stop(timeout=5)
        logger.info(f"[Chaos] Stopped container: {service_name}")

    def start_container(self, service_name: str):
        container = self._get_container(service_name)
        container.start()
        logger.info(f"[Chaos] Started container: {service_name}")

    def is_running(self, service_name: str) -> bool:
        try:
            c = self._get_container(service_name)
            return c.status == "running"
        except RuntimeError:
            return False

class ChaosController:
    """Owns scenario states and triggers failures."""

    def __init__(self, scenarios: dict[str, ChaosScenario]):
        self.scenarios = scenarios
        self.docker = DockerChaos()
        self.active_scenarios: list[str] = []

    async def activate(self, name: str, params: dict | None = None) -> bool:
        s = self.scenarios.get(name)
        if not s:
            logger.error(f"Unknown scenario: {name}")
            return False
        if s.state == ScenarioState.ACTIVE:
            logger.warning(f"Scenario {name} already active")
            return False

        logger.info(f"[Chaos] ACTIVATING: {name}")
        s.params = params or {}
        await s.activate(s.params)
        s.state = ScenarioState.ACTIVE
        s.activated_at = datetime.now(timezone.utc)
        s.detected_at = None
        s.remediated_at = None
        if name not in self.active_scenarios:
            self.active_scenarios.append(name)
        return True

    async def deactivate(self, name: str) -> bool:
        s = self.scenarios.get(name)
        if not s:
            return False

        logger.info(f"[Chaos] DEACTIVATING: {name}")
        s.state = ScenarioState.RECOVERING
        await s.deactivate()
        s.state = ScenarioState.IDLE
        if name in self.active_scenarios:
            self.active_scenarios.remove(name)
        return True

    async def deactivate_all(self):
        """Emergency reset — deactivate all active scenarios."""
        for name in list(self.active_scenarios):
            await self.deactivate(name)

    async def mark_detected(self, name: str) -> None:
        s = self.scenarios.get(name)
        if s:
            s.detected_at = datetime.now(timezone.utc)
            if s.activated_at:
                dt = (s.detected_at - s.activated_at).total_seconds()
                logger.info(f"[Chaos] {name} detected in {dt:.1f}s (SLO: {s.expected_detection_s}s)")

    async def mark_remediated(self, name: str) -> None:
        s = self.scenarios.get(name)
        if s:
            s.remediated_at = datetime.now(timezone.utc)
            s.state = ScenarioState.RECOVERING
            if s.activated_at:
                dt = (s.remediated_at - s.activated_at).total_seconds()
                logger.info(f"[Chaos] {name} remediated in {dt:.1f}s (SLO: {s.expected_remediation_s}s)")

    def status(self) -> dict:
        return {
            "active_scenarios": self.active_scenarios,
            "scenarios": {
                name: {
                    "state": s.state.value,
                    "activated_at": s.activated_at.isoformat() if s.activated_at else None,
                    "detected_at": s.detected_at.isoformat() if s.detected_at else None,
                    "remediated_at": s.remediated_at.isoformat() if s.remediated_at else None,
                    "severity": s.severity,
                    "expected_event_type": s.expected_event_type,
                }
                for name, s in self.scenarios.items()
            }
        }

    def get_status(self) -> list[dict]:
        return [
            {
                "id": s.name,
                "name": s.name.replace("_", " ").title(),
                "description": s.description,
                "category": self._get_category(s.name),
                "active": s.state == ScenarioState.ACTIVE,
                "params": s.params,
                "state": s.state.value,
                "severity": s.severity,
                "expected_event": s.expected_event_type,
                "activated_at": s.activated_at.isoformat() if s.activated_at else None,
                "detected_at": s.detected_at.isoformat() if s.detected_at else None,
                "remediated_at": s.remediated_at.isoformat() if s.remediated_at else None,
            }
            for s in self.scenarios.values()
        ]

    def _get_category(self, name: str) -> str:
        if "gateway" in name or "api" in name:
            return "gateway"
        elif "db" in name or "query" in name or "lock" in name or "transaction" in name or "postgres" in name:
            return "database"
        elif "redis" in name or "cache" in name:
            return "cache"
        elif "kafka" in name or "consumer" in name or "queue" in name or "notify" in name:
            return "queue"
        elif "config" in name:
            return "config"
        else:
            return "host"
