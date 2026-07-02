import asyncio, logging, docker

logger = logging.getLogger("network-chaos")

class NetworkChaos:
    """
    Uses Linux tc (traffic control) inside containers for real packet-level chaos.
    Container must run in privileged mode or have NET_ADMIN capability.
    """

    def __init__(self):
        self.client = docker.from_env()

    def _get_container(self, service_name: str):
        # Try label first, fall back to name match
        containers = self.client.containers.list(
            filters={"label": f"com.shopcore.service={service_name}"}
        )
        if not containers:
            containers = self.client.containers.list(
                filters={"name": service_name}
            )
        if not containers:
            raise RuntimeError(f"Container not found: {service_name}")
        return containers[0]

    async def add_latency(self, service: str, latency_ms: int, jitter_ms: int = 10) -> None:
        """Inject network latency via tc netem inside the container."""
        # Delete existing qdisc rule just in case, ignore errors
        await self.remove(service)
        cmd = (f"tc qdisc add dev eth0 root netem "
               f"delay {latency_ms}ms {jitter_ms}ms distribution normal")
        await self._exec(service, cmd)
        logger.info(f"[network-chaos] {service}: latency {latency_ms}ms ±{jitter_ms}ms")

    async def add_packet_loss(self, service: str, loss_pct: int) -> None:
        await self.remove(service)
        cmd = f"tc qdisc add dev eth0 root netem loss {loss_pct}%"
        await self._exec(service, cmd)
        logger.info(f"[network-chaos] {service}: packet loss {loss_pct}%")

    async def remove(self, service: str) -> None:
        """Remove all tc rules — restore clean networking."""
        await self._exec(service, "tc qdisc del dev eth0 root 2>/dev/null || true")
        logger.info(f"[network-chaos] {service}: network rules removed")

    async def _exec(self, service: str, cmd: str) -> None:
        container = self._get_container(service)
        await asyncio.to_thread(container.exec_run, ["sh", "-c", cmd], privileged=True)
