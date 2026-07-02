from __future__ import annotations

import asyncio
import os
from pathlib import Path

import asyncpg


MIGRATIONS = [
    "add_playbook_stats.sql",
    "add_agent_tasks.sql",
]


async def main() -> None:
    dsn = os.getenv("POSTGRES_DSN", "postgresql://postgres:postgres@localhost:5432/cognitive")
    root = Path(__file__).resolve().parent
    conn = await asyncpg.connect(dsn)
    try:
        for name in MIGRATIONS:
            sql = (root / name).read_text()
            await conn.execute(sql)
            print(f"Applied migration: {name}")
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
