import socket
import urllib.request
import json
import sys

# Color coding for output
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
BLUE = "\033[94m"
RESET = "\033[0m"

def check_tcp(host, port, name):
    try:
        with socket.create_connection((host, port), timeout=2.0):
            print(f"[{GREEN}OK{RESET}] TCP {name} is listening on {host}:{port}")
            return True
    except (socket.timeout, ConnectionRefusedError, OSError):
        print(f"[{RED}FAIL{RESET}] TCP {name} is OFFLINE on {host}:{port}")
        return False

def check_http(url, name):
    try:
        with urllib.request.urlopen(url, timeout=2.0) as response:
            code = response.getcode()
            if code == 200:
                print(f"[{GREEN}OK{RESET}] HTTP {name} is active on {url}")
                return True
            else:
                print(f"[{YELLOW}WARN{RESET}] HTTP {name} returned status {code} on {url}")
                return False
    except Exception as e:
        print(f"[{RED}FAIL{RESET}] HTTP {name} is UNREACHABLE on {url} (Error: {e})")
        return False

def main():
    print(f"\n{BLUE}=== COGNITIVE PERCEPTION ARCHITECTURE WIRING DIAGNOSTIC ==={RESET}\n")

    # 1. Backing Databases / Queues (Docker)
    print(f"{BLUE}--- Step 1: Checking Backing Services (Docker) ---{RESET}")
    infra = [
        ("localhost", 5432, "Cognitive TimescaleDB (Postgres)"),
        ("localhost", 6379, "Cognitive Redis"),
        ("localhost", 9092, "Cognitive Redpanda (Kafka)"),
        ("localhost", 7687, "Cognitive Neo4j Bolt"),
        ("localhost", 6333, "Cognitive Qdrant HTTP"),
        ("localhost", 5433, "Target ShopCore Postgres"),
        ("localhost", 6380, "Target ShopCore Redis"),
        ("localhost", 9094, "Target ShopCore Redpanda (Kafka)"),
    ]
    infra_results = [check_tcp(h, p, n) for h, p, n in infra]

    # 2. Cognitive Microservices (Uvicorn / Python processes running on host)
    print(f"\n{BLUE}--- Step 2: Checking Cognitive Layer APIs (Host Processes) ---{RESET}")
    services = [
        ("http://localhost:8080/health", "Perception API"),
        ("http://localhost:8090/health", "Memory API"),
        ("http://localhost:8091/health", "Temporal Engine API"),
        ("http://localhost:8092/health", "World Model API"),
        ("http://localhost:8093/health", "Reasoning Engine API"),
        ("http://localhost:8094/health", "Planning System API"),
        ("http://localhost:8095/health", "Execution Layer API"),
        ("http://localhost:8096/health", "Feedback Loop API"),
        ("http://localhost:8097/health", "Agent Coordinator API"),
        ("http://localhost:8000/health", "Dashboard API"),
    ]
    service_results = [check_http(u, n) for u, n in services]

    # Summary
    print(f"\n{BLUE}=== DIAGNOSTIC SUMMARY ==={RESET}")
    success_infra = sum(1 for r in infra_results if r)
    success_services = sum(1 for r in service_results if r)
    
    print(f"Infrastructure Services Online: {success_infra}/{len(infra)}")
    print(f"Host Python Services Online: {success_services}/{len(services)}")
    
    if all(infra_results) and all(service_results):
        print(f"\n{GREEN}SUCCESS: The entire architecture is wired and connected successfully!{RESET}\n")
        sys.exit(0)
    else:
        print(f"\n{RED}FAILURE: Some wiring components are offline or disconnected. Check details above.{RESET}")
        print(f"{YELLOW}Hint: Ensure Docker Desktop is active and run 'bash start_services.sh' first.{RESET}\n")
        sys.exit(1)

if __name__ == "__main__":
    main()
