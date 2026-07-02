import docker
import json

client = docker.from_env()
for c in client.containers.list():
    if "shopcore" in c.name:
        networks = c.attrs.get("NetworkSettings", {}).get("Networks", {})
        print(f"Container: {c.name}")
        for net_name, net_info in networks.items():
            print(f"  Network: {net_name}")
            print(f"    Aliases: {net_info.get('Aliases')}")
            print(f"    IPAddress: {net_info.get('IPAddress')}")
