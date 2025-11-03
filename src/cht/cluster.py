from __future__ import annotations

import logging
import re
from time import strftime, time
from typing import Callable, Iterable, Optional

import clickhouse_connect
import pandas as pd

_logger = logging.getLogger("cht.cluster")

# Detect statements that mutate data or metadata so we can guard read-only sessions.
_MUTATING_RE = re.compile(
    r"^\s*(ALTER|ATTACH|DETACH|DROP|TRUNCATE|RENAME|"
    r"INSERT|UPDATE|DELETE|REPLACE|OPTIMIZE|SYSTEM|"
    r"CREATE|KILL)\b",
    re.IGNORECASE | re.DOTALL,
)


def is_mutating(sql: str) -> bool:
    """Return True when the statement mutates ClickHouse state."""
    return bool(_MUTATING_RE.match(sql or ""))


class Cluster:
    """
    Thin wrapper around ``clickhouse_connect`` that provides:

    * lazy connection initialisation
    * structured logging for every statement
    * a single choke point for enforcing read-only sessions
    * helper methods for common introspection routines
    """

    def __init__(
        self,
        name: str,
        host: str,
        *,
        port: int = 8123,
        user: str = "default",
        password: str = "",
        read_only: bool = False,
        secure: bool = False,
        verify: bool = False,
        log_sql_text: bool = True,
        log_sql_truncate: int = 4000,
        client_factory: Callable[
            ..., clickhouse_connect.driver.client.Client
        ] = clickhouse_connect.get_client,
    ) -> None:
        self.name = name
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.read_only = read_only
        self.secure = secure
        self.verify = verify
        self.log_sql_text = log_sql_text
        self.log_sql_truncate = (
            log_sql_truncate if log_sql_truncate and log_sql_truncate > 0 else 4000
        )
        self._client_factory = client_factory
        self._client: Optional[clickhouse_connect.driver.client.Client] = None

        if not _logger.handlers:
            logging.basicConfig(
                level=logging.INFO,
                format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            )

    # ----------------------- connection management -----------------------
    @property
    def client(self) -> clickhouse_connect.driver.client.Client:
        if self._client is None:
            settings = {"readonly": 1} if self.read_only else {}

            _logger.info(
                "Establishing connection | cluster=%s host=%s:%s user=%s secure=%s "
                "verify=%s read_only=%s",
                self.name,
                self.host,
                self.port,
                self.user,
                self.secure,
                self.verify,
                self.read_only,
            )
            if not self.read_only:
                _logger.warning("Session is NOT read-only | cluster=%s", self.name)

            self._client = self._client_factory(
                host=self.host,
                port=self.port,
                username=self.user,
                password=self.password,
                secure=self.secure,
                verify=self.verify,
                settings=settings,
            )
        return self._client

    # ---------------------------- execution ------------------------------
    def _execute_logged(self, sql: str, *, test_run: bool = False):
        trimmed = (sql or "").strip()
        mutating = is_mutating(trimmed)

        if self.log_sql_text:
            display = (
                trimmed
                if len(trimmed) <= self.log_sql_truncate
                else trimmed[: self.log_sql_truncate] + " … [truncated]"
            )
            _logger.info(
                "%s | cluster=%s | len=%d | sql=%s%s",
                "MUTATION" if mutating else "QUERY",
                self.name,
                len(trimmed),
                display,
                " [TEST-RUN]" if test_run else "",
            )
        else:
            _logger.info(
                "%s | cluster=%s | len=%d%s",
                "MUTATION" if mutating else "QUERY",
                self.name,
                len(trimmed),
                " [TEST-RUN]" if test_run else "",
            )

        if mutating and self.read_only:
            _logger.error("Mutating statement denied on read-only cluster=%s", self.name)
            raise PermissionError("Mutating statement on read-only cluster session")

        if test_run:
            return None

        start = time()
        try:
            if mutating:
                self.client.command(trimmed)
                _logger.info(
                    "MUTATION OK | cluster=%s | elapsed=%.3fs",
                    self.name,
                    time() - start,
                )
                return None
            result = self.client.query(trimmed)
            _logger.info(
                "QUERY OK | cluster=%s | rows=%d | elapsed=%.3fs",
                self.name,
                len(result.result_rows),
                time() - start,
            )
            return result
        except Exception as exc:  # pragma: no cover - logging side effect
            _logger.exception(
                "%s FAILED | cluster=%s | elapsed=%.3fs | error=%s",
                "MUTATION" if mutating else "QUERY",
                self.name,
                time() - start,
                exc,
            )
            raise

    def query(self, sql: str, *, test_run: bool = False):
        """Execute SQL and return rows (or None for mutation statements)."""
        result = self._execute_logged(sql, test_run=test_run)
        return None if result is None else result.result_rows

    def query_raw(self, sql: str, *, test_run: bool = False):
        """Execute SQL and return the ``QueryResult`` object from ``clickhouse_connect``."""
        return self._execute_logged(sql, test_run=test_run)

    def query_bulk(self, queries: Iterable[str], *, test_run: bool = False) -> None:
        """Run a reusable bulk executor with progress messages to stdout."""
        queries = list(queries)
        total = len(queries)
        _logger.info(
            "Bulk execution start | cluster=%s | total=%d | test_run=%s",
            self.name,
            total,
            test_run,
        )

        for idx, query in enumerate(queries, 1):
            timestamp = strftime("%Y-%m-%d %H:%M:%S")
            trimmed = (query or "").strip()
            print(
                f"📌 [{idx}/{total}] {timestamp} len={len(trimmed)} "
                f"{'MUTATION' if is_mutating(trimmed) else 'QUERY'} (test_run={test_run})"
            )
            try:
                self._execute_logged(trimmed, test_run=test_run)
                print(f"✅ [{idx}/{total}] Success\n")
            except Exception as exc:  # pragma: no cover - interactive feedback
                print(f"❌ [{idx}/{total}] Failed: {exc}\n🛑 Stopping execution.")
                break
        else:
            print("🎉 All queries processed successfully.\n")

    # ------------------------- introspection utils -----------------------
    def get_disk_usage(self) -> pd.DataFrame:
        sql = """
        SELECT
            name AS disk_name,
            path AS disk_path,
            type AS disk_type,
            formatReadableSize(total_space) AS total_space_readable,
            formatReadableSize(free_space) AS free_space_readable,
            formatReadableSize(total_space - free_space) AS used_space_readable,
            round((1 - (free_space / total_space)) * 100, 2) AS used_percentage
        FROM system.disks
        ORDER BY used_percentage DESC
        """
        result = self.query_raw(sql)
        return pd.DataFrame(result.result_rows, columns=result.column_names)

    def get_table_disk_distribution(self, database: str = "default") -> pd.DataFrame:
        sql = f"""
        SELECT
            p.table,
            formatReadableSize(sum(p.bytes_on_disk)) || ' (' ||
                round(100 * sum(p.bytes_on_disk) / (
                    SELECT sum(bytes_on_disk) FROM system.parts WHERE active
                ), 2) || '%)' AS total_size,
            formatReadableSize(sumIf(p.bytes_on_disk, p.disk_name = 'default')) || ' (' ||
                round(100 * sumIf(p.bytes_on_disk, p.disk_name = 'default') /
                      sum(p.bytes_on_disk), 2)
                || '%)' AS on_default,
            formatReadableSize(sumIf(p.bytes_on_disk, p.disk_name = 'hdd_1')) || ' (' ||
                round(100 * sumIf(p.bytes_on_disk, p.disk_name = 'hdd_1') / sum(p.bytes_on_disk), 2)
                || '%)' AS on_hdd_1
        FROM system.parts p
        WHERE p.active AND p.database = '{database}'
        GROUP BY p.table
        ORDER BY sum(p.bytes_on_disk) DESC
        """
        result = self.query_raw(sql)
        return pd.DataFrame(result.result_rows, columns=result.column_names)

    def get_column_disk_usage(
        self,
        table: str,
        *,
        database: str = "default",
    ) -> pd.DataFrame:
        sql = f"""
        SELECT
            column AS column_name,
            formatReadableSize(sum(column_data_compressed_bytes)) || ' (' ||
                round(100 * sum(column_data_compressed_bytes) / (
                    SELECT sum(bytes_on_disk)
                    FROM system.parts
                    WHERE active AND database = '{database}' AND table = '{table}'
                ), 2) || '%)' AS compressed_size,
            formatReadableSize(sum(column_data_uncompressed_bytes)) AS uncompressed_size,
            round(sum(column_data_uncompressed_bytes) /
                  nullIf(sum(column_data_compressed_bytes), 0), 2)
                AS compression_ratio
        FROM system.parts_columns
        WHERE active AND database = '{database}' AND table = '{table}'
        GROUP BY column
        ORDER BY sum(column_data_compressed_bytes) DESC
        """
        result = self.query_raw(sql)
        return pd.DataFrame(result.result_rows, columns=result.column_names)

    # ------------------------------ misc ---------------------------------
    def __repr__(self) -> str:  # pragma: no cover - trivial
        mode = "read-only" if self.read_only else "read-write"
        return f"<Cluster {self.user}@{self.host}:{self.port} ({mode})>"
