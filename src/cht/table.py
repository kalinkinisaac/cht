from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from .cluster import Cluster
from .sql_utils import format_identifier, remote_expression, rows_to_list

_logger = logging.getLogger("cht.table")


@dataclass
class Table:
    """
    Convenience wrapper bound to a target table on a :class:`Cluster`.

    Provides declarative helpers for:

    * structural metadata (columns, parts, time column detection)
    * backup / restore flows
    * materialised view discovery and replay support
    * basic data hygiene tasks
    
    The Table class supports a default cluster mechanism to avoid specifying
    cluster in every operation. Use Table.set_default_cluster() to set a
    global default, or pass cluster explicitly to override.
    """

    name: str
    database: str = "default"
    cluster: Optional[Cluster] = None
    
    # Class-level default cluster
    _default_cluster: Optional[Cluster] = None

    def __post_init__(self) -> None:
        if not _logger.handlers:
            logging.basicConfig(
                level=logging.INFO,
                format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            )

    # ----------------------------- properties -----------------------------
    @property
    def fqdn(self) -> str:
        return f"{self.database}.{self.name}"

    def with_cluster(self, cluster: Cluster) -> "Table":
        """Return a copy of this Table bound to a different cluster."""
        return Table(name=self.name, database=self.database, cluster=cluster)

    # ----------------------------- internals ------------------------------
    def _require_cluster(self) -> Cluster:
        """Get cluster instance, checking instance cluster first, then default cluster."""
        if self.cluster:
            return self.cluster
        if Table._default_cluster:
            return Table._default_cluster
        raise RuntimeError(
            "Table operation requires a cluster. Either set cluster=... on the Table "
            "instance or use Table.set_default_cluster() to set a global default."
        )

    # -------------------------- class methods ----------------------------
    @classmethod
    def set_default_cluster(cls, cluster: Cluster) -> None:
        """
        Set the default cluster for all Table instances.
        
        Args:
            cluster: Cluster instance to use as default
            
        Example:
            >>> cluster = Cluster("local", "localhost")
            >>> Table.set_default_cluster(cluster)
            >>> table = Table("events", "analytics")  # No cluster needed
            >>> table.exists()  # Uses default cluster
        """
        cls._default_cluster = cluster

    @classmethod
    def get_default_cluster(cls) -> Optional[Cluster]:
        """Get the current default cluster."""
        return cls._default_cluster

    @classmethod
    def clear_default_cluster(cls) -> None:
        """Clear the default cluster."""
        cls._default_cluster = None

    # ----------------------------- metadata -------------------------------
    def exists(self) -> bool:
        result = self._require_cluster().query(f"EXISTS TABLE {self.fqdn}")
        value = result[0][0] if result else 0
        exists = int(value) == 1
        _logger.info("[exists] %s -> %s", self.fqdn, exists)
        return exists

    def get_columns(self) -> List[str]:
        rows = self._require_cluster().query(f"DESCRIBE TABLE {self.fqdn}")
        columns = [row[0] for row in rows]
        _logger.info("[get_columns] %s -> %d columns", self.fqdn, len(columns))
        return columns

    def get_time_column(self) -> Optional[str]:
        for row in self._require_cluster().query(f"DESCRIBE TABLE {self.fqdn}"):
            name, dtype = row[0], row[1]
            if isinstance(dtype, str) and (
                dtype.startswith("Date") or dtype.startswith("DateTime")
            ):
                _logger.info("[get_time_column] %s -> %s", self.fqdn, name)
                return name
        _logger.info("[get_time_column] %s -> None", self.fqdn)
        return None

    def list_parts_with_size(self, *, limit: Optional[int] = None) -> List[Tuple[Any, ...]]:
        where_limit = f"LIMIT {int(limit)}" if limit else ""
        sql = f"""
        SELECT
            name,
            disk_name,
            formatReadableSize(bytes_on_disk) AS size,
            modification_time
        FROM system.parts
        WHERE active AND database = '{self.database}' AND table = '{self.name}'
        ORDER BY bytes_on_disk DESC
        {where_limit}
        """
        rows = self._require_cluster().query(sql)
        _logger.info("[list_parts_with_size] %s rows=%d", self.fqdn, len(rows))
        return rows

    # ----------------------------- data access ----------------------------
    def drop(self) -> None:
        _logger.warning("[drop] %s", self.fqdn)
        self._require_cluster().query(f"DROP TABLE IF EXISTS {self.fqdn}")

    def select(self, where: str = "", limit: Optional[int] = None):
        sql = f"SELECT * FROM {self.fqdn}"
        if where:
            sql += f" WHERE {where}"
        if limit is not None:
            sql += f" LIMIT {limit}"
        _logger.info("[select] %s", sql)
        return self._require_cluster().query(sql)

    def optimize_deduplicate(self, *, test_run: bool = False) -> None:
        sql = f"OPTIMIZE TABLE {self.fqdn} FINAL DEDUPLICATE"
        if test_run:
            _logger.info("[optimize TEST] %s", sql)
            return
        cluster = self._require_cluster()
        if cluster.read_only:
            raise PermissionError("Cannot run OPTIMIZE on a read-only cluster.")
        _logger.warning("[optimize] %s", sql)
        cluster.query(sql)

    # ----------------------------- backup flows ---------------------------
    def backup_to_suffix(self, backup_suffix: str = "_backup", recreate: bool = False) -> str:
        cluster = self._require_cluster()
        backup_name = f"{self.name}{backup_suffix}"
        fq_backup = format_identifier(self.database, backup_name)

        exists = bool(
            cluster.query(
                f"""
                SELECT 1
                FROM system.tables
                WHERE database = '{self.database}' AND name = '{backup_name}'
                LIMIT 1
                """
            )
        )
        if exists:
            if recreate:
                _logger.warning("[backup] Dropping existing backup %s", fq_backup)
                cluster.query(f"DROP TABLE {fq_backup}")
            else:
                raise RuntimeError(
                    f"Backup table {self.database}.{backup_name} already exists. "
                    "Set recreate=True to drop it automatically."
                )

        fqdn = format_identifier(self.database, self.name)
        _logger.info("[backup] Creating %s AS %s", fq_backup, fqdn)
        cluster.query(f"CREATE TABLE {fq_backup} AS {fqdn}")
        _logger.info("[backup] Copying data %s -> %s", fqdn, fq_backup)
        cluster.query(f"INSERT INTO {fq_backup} SELECT * FROM {fqdn}")
        return backup_name

    def verify_backup(
        self,
        backup_suffix: str = "_backup",
        *,
        check_rows: bool = True,
        check_columns: bool = True,
    ) -> None:
        cluster = self._require_cluster()
        backup_name = f"{self.name}{backup_suffix}"
        fq_backup = format_identifier(self.database, backup_name)

        if check_columns:
            src_cols = [row[0] for row in cluster.query(f"DESCRIBE TABLE {self.fqdn}")]
            bak_cols = [row[0] for row in cluster.query(f"DESCRIBE TABLE {fq_backup}")]
            if src_cols != bak_cols:
                raise AssertionError(
                    f"Column mismatch between {self.fqdn} and {fq_backup}: {src_cols} vs {bak_cols}"
                )

        if check_rows:
            src_rows = cluster.query(f"SELECT count() FROM {self.fqdn}")[0][0]
            bak_rows = cluster.query(f"SELECT count() FROM {fq_backup}")[0][0]
            if int(src_rows) != int(bak_rows):
                raise AssertionError(
                    f"Row count mismatch between {self.fqdn} ({src_rows}) "
                    f"and {fq_backup} ({bak_rows})"
                )
        _logger.info("[verify_backup] OK %s -> %s", self.fqdn, fq_backup)

    def truncate(self) -> None:
        cluster = self._require_cluster()
        _logger.warning("[truncate] %s", self.fqdn)
        cluster.query(f"TRUNCATE TABLE {self.fqdn}")

    def restore_from_backup(
        self, backup_suffix: str = "_backup", *, drop_backup: bool = False
    ) -> None:
        cluster = self._require_cluster()
        backup_name = f"{self.name}{backup_suffix}"
        fq_backup = format_identifier(self.database, backup_name)

        exists = bool(
            cluster.query(
                f"""
                SELECT 1
                FROM system.tables
                WHERE database = '{self.database}' AND name = '{backup_name}'
                LIMIT 1
                """
            )
        )
        if not exists:
            raise RuntimeError(f"Backup table {self.database}.{backup_name} does not exist.")

        columns = [row[0] for row in cluster.query(f"DESCRIBE TABLE {self.fqdn}")]
        column_csv = ", ".join(f"`{col}`" for col in columns)

        self.truncate()
        cluster.query(
            f"INSERT INTO {self.fqdn} ({column_csv}) SELECT {column_csv} FROM {fq_backup}"
        )
        if drop_backup:
            cluster.query(f"DROP TABLE {fq_backup}")
        _logger.info("[restore] Restored %s from %s", self.fqdn, fq_backup)

    # --------------------------- MV utilities -----------------------------
    def find_targeting_mvs(self) -> List[Tuple[str, str]]:
        cluster = self._require_cluster()
        patterns = [
            f" TO {self.database}.{self.name} ",
            f" TO `{self.database}`.`{self.name}` ",
            f" TO '{self.database}'.'{self.name}' ",
        ]
        pattern_sql = " OR ".join(
            f"positionCaseInsensitive(create_table_query, {repr(p)}) > 0" for p in patterns
        )
        sql = f"""
        SELECT database, name
        FROM system.tables
        WHERE engine = 'MaterializedView'
          AND ({pattern_sql})
        """
        rows = rows_to_list(cluster.query(sql))
        return [(row[0], row[1]) for row in rows]

    def find_mv_sources(self, mv_db: str, mv_name: str) -> List[Tuple[str, str]]:
        cluster = self._require_cluster()
        sql = f"""
        SELECT d.depends_on_database, d.depends_on_table
        FROM system.dependencies d
        ANY LEFT JOIN system.tables st
            ON st.database = d.depends_on_database AND st.name = d.depends_on_table
        WHERE d.database = '{mv_db}' AND d.table = '{mv_name}'
          AND d.depends_on_database != '' AND d.depends_on_table != ''
          AND (st.engine IS NULL OR st.engine != 'MaterializedView')
        """
        rows = rows_to_list(cluster.query(sql))
        return [(row[0], row[1]) for row in rows]

    def repopulate_through_mv(
        self,
        *,
        replay_from_db: Optional[str] = None,
        replay_from_table: Optional[str] = None,
        replay_select_sql: Optional[str] = None,
        mv_db: Optional[str] = None,
        mv_name: Optional[str] = None,
        truncate_mv_source_first: bool = False,
        where: Optional[str] = None,
    ) -> Dict[str, Any]:
        cluster = self._require_cluster()

        if mv_db and mv_name:
            mvs = [(mv_db, mv_name)]
        else:
            mvs = self.find_targeting_mvs()
            if not mvs:
                raise RuntimeError(f"No materialised view targets {self.fqdn}")
            if len(mvs) > 1:
                raise RuntimeError(
                    f"Multiple materialised views target {self.fqdn}: {mvs}. "
                    "Specify mv_db/mv_name explicitly."
                )
        mv_db, mv_name = mvs[0]

        sources = self.find_mv_sources(mv_db, mv_name)
        if not sources:
            raise RuntimeError(f"Could not determine MV source for {mv_db}.{mv_name}")
        if len(sources) > 1 and not (replay_from_db and replay_from_table):
            raise RuntimeError(
                f"MV {mv_db}.{mv_name} has multiple sources {sources}; "
                "supply replay_from_db/table."
            )

        mv_src_db, mv_src_tbl = (
            (replay_from_db, replay_from_table)
            if replay_from_db and replay_from_table
            else sources[0]
        )

        fq_mv_src = format_identifier(mv_src_db, mv_src_tbl)
        src_columns = [row[0] for row in cluster.query(f"DESCRIBE TABLE {fq_mv_src}")]
        column_csv = ", ".join(f"`{col}`" for col in src_columns)

        if replay_select_sql:
            select_sql = replay_select_sql.strip()
            if where:
                select_sql = f"{select_sql} WHERE {where}"
            insert_sql = f"INSERT INTO {fq_mv_src} {select_sql}"
            estimated_rows = None
        else:
            if not (replay_from_db and replay_from_table):
                raise ValueError("Provide replay_from_db+replay_from_table or replay_select_sql.")
            fq_source = format_identifier(replay_from_db, replay_from_table)
            where_clause = f" WHERE {where}" if where else ""
            source_columns = [row[0] for row in cluster.query(f"DESCRIBE TABLE {fq_source}")]
            missing = [col for col in src_columns if col not in source_columns]
            if missing:
                raise AssertionError(
                    f"Source table {fq_source} missing columns required by MV source "
                    f"{fq_mv_src}: {missing}"
                )
            insert_sql = (
                f"INSERT INTO {fq_mv_src} ({column_csv}) "
                f"SELECT {column_csv} FROM {fq_source}{where_clause}"
            )
            estimated_rows = cluster.query(f"SELECT count() FROM {fq_source}{where_clause}")[0][0]

        if truncate_mv_source_first:
            cluster.query(f"TRUNCATE TABLE {fq_mv_src}")

        cluster.query(insert_sql)
        return {
            "mv": (mv_db, mv_name),
            "mv_source": (mv_src_db, mv_src_tbl),
            "insert_sql": insert_sql,
            "estimated_rows_replayed": estimated_rows,
        }

    # --------------------------- quality helpers -------------------------
    def remote(self, *, port: int = 9000) -> str:
        cluster = self._require_cluster()
        return remote_expression(
            host=cluster.host,
            database=self.database,
            table=self.name,
            user=cluster.user,
            password=cluster.password,
            port=port,
        )

    # --------------------------- DataFrame methods -----------------------
    def to_df(self) -> pd.DataFrame:
        """Load the entire table as a pandas DataFrame."""
        cluster = self._require_cluster()
        return cluster.client.query_df(f"SELECT * FROM {self.fqdn}")

    @classmethod
    def from_df(
        cls,
        df: pd.DataFrame,
        cluster: Optional[Cluster] = None,
        *,
        database: str = "temp",
        name: Optional[str] = None,
        create_if_not_exists: bool = True,
        mode: str = "overwrite",
        ttl: Optional[timedelta] = timedelta(days=1),
        **kwargs: Any,
    ) -> "Table":
        """
        Create a Table instance from a pandas DataFrame with optional TTL expiration.

        This is the primary API for creating temporary tables from DataFrames. It handles
        everything: table creation, data insertion, TTL management, and cleanup.

        Args:
            df: The DataFrame to load
            cluster: ClickHouse cluster instance (uses default if None)
            database: Target database (default: "temp")
            name: Table name (auto-generated with unique suffix if None)
            create_if_not_exists: Whether to create table if it doesn't exist
            mode: "overwrite" (drop/recreate) or "append" (insert into existing)
            ttl: Time to live for automatic cleanup (None = permanent, default: 1 day)
            **kwargs: Additional arguments (engine, order_by, partition_by, etc.)

        Returns:
            Table instance pointing to the created table with TTL management

        Examples:
            >>> # Simple temporary table (expires in 1 day)
            >>> df = pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
            >>> table = Table.from_df(df, cluster)
            >>> print(table.fqdn)  # temp.tmp_a1b2c3d4

            >>> # Custom TTL and table name
            >>> table = Table.from_df(
            ...     df, cluster,
            ...     name="my_analysis",
            ...     ttl=timedelta(hours=2),  # Expires in 2 hours
            ...     engine="MergeTree",
            ...     order_by=["id"]
            ... )

            >>> # Permanent table (no expiration)
            >>> table = Table.from_df(
            ...     df, cluster,
            ...     database="analytics",
            ...     name="user_data", 
            ...     ttl=None  # Never expires
            ... )

            >>> # Using default cluster (set once, use everywhere)
            >>> Table.set_default_cluster(cluster)
            >>> table = Table.from_df(df)  # No cluster needed!
        """
        from .dataframe import (
            create_table_from_dataframe,
            generate_temp_table_name,
            insert_dataframe,
        )
        from .temp_tables import format_expires_at

        # Use provided cluster or fall back to default
        if cluster is None:
            if cls._default_cluster is None:
                raise RuntimeError(
                    "Table.from_df() requires a cluster. Either pass cluster=... "
                    "or use Table.set_default_cluster() to set a global default."
                )
            cluster = cls._default_cluster

        if name is None:
            name = generate_temp_table_name()

        # Ensure name is not None for type safety
        assert name is not None, "Table name must be provided or generated"

        table = cls(name=name, database=database, cluster=cluster)

        if mode == "overwrite":
            # Drop table if it exists
            if table.exists():
                cluster.query(f"DROP TABLE {table.fqdn}")

            # Create and populate table
            create_table_from_dataframe(
                cluster=cluster,
                df=df,
                table_name=name,
                database=database,
                if_not_exists=False,
                **kwargs,
            )
            insert_dataframe(cluster=cluster, df=df, table_name=name, database=database)

        elif mode == "append":
            # Create table if needed
            if create_if_not_exists and not table.exists():
                create_table_from_dataframe(
                    cluster=cluster,
                    df=df,
                    table_name=name,
                    database=database,
                    if_not_exists=True,
                    **kwargs,
                )

            # Insert data
            insert_dataframe(cluster=cluster, df=df, table_name=name, database=database)

        else:
            raise ValueError(f"mode must be 'overwrite' or 'append', got: {mode}")

        # Add TTL comment if specified
        if ttl is not None:
            from datetime import datetime, timezone
            expires_at = datetime.now(timezone.utc) + ttl
            comment = format_expires_at(expires_at)
            cluster.query(f"ALTER TABLE {table.fqdn} MODIFY COMMENT '{comment}'")

        return table
