import asyncio, asyncpg, logging

logger = logging.getLogger("database-chaos")

class DatabaseChaos:
    def __init__(self, dsn: str):
        self.dsn = dsn

    async def inject_lock_contention(self, table: str = "products") -> None:
        """Hold an exclusive lock to simulate lock wait timeouts."""
        conn = await asyncpg.connect(self.dsn)
        try:
            await conn.execute(f"BEGIN; LOCK TABLE {table} IN EXCLUSIVE MODE;")
            logger.info(f"[db-chaos] lock held on {table} — will release after 120s")
            await asyncio.sleep(120)
        finally:
            await conn.execute("ROLLBACK")
            await conn.close()

    async def check_pool_exhausted(self, db_name: str) -> bool:
        """Return True if active connections >= max_connections - 2."""
        conn = await asyncpg.connect(self.dsn)
        try:
            row = await conn.fetchrow(
                """SELECT count(*) AS active,
                          current_setting('max_connections')::int AS max_c
                   FROM   pg_stat_activity
                   WHERE  datname = $1""",
                db_name
            )
            return row["active"] >= row["max_c"] - 2
        finally:
            await conn.close()

    async def inject_slow_query_rule(self, delay_ms: int = 3000) -> None:
        """ALTER ROLE to apply statement_timeout as a slow-query injection."""
        conn = await asyncpg.connect(self.dsn)
        try:
            await conn.execute(f"ALTER ROLE shopcore SET statement_timeout = '{delay_ms}ms'")
            logger.info(f"[db-chaos] statement_timeout set to {delay_ms}ms for shopcore")
        finally:
            await conn.close()

    async def clear_slow_query_rule(self) -> None:
        conn = await asyncpg.connect(self.dsn)
        try:
            await conn.execute("ALTER ROLE shopcore RESET statement_timeout")
            logger.info("[db-chaos] statement_timeout cleared")
        finally:
            await conn.close()
