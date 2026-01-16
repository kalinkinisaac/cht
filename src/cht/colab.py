"""Colab-friendly ClickHouse helpers that avoid background servers.

This module uses ``clickhouse local`` with a persisted ``--path`` so each call
boots, executes a query, and exits while still keeping data on disk.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import tempfile
import urllib.request
from dataclasses import asdict, dataclass
from typing import Any, Dict, Optional, Sequence, Union

import pandas as pd
from pandas.api.types import is_bool_dtype, is_datetime64_any_dtype

from .dataframe import resolve_column_types

_FORMAT_RE = re.compile(r"\bFORMAT\b", re.IGNORECASE)
_JSON_EACH_ROW_RE = re.compile(r"\bJSONEachRow\b", re.IGNORECASE)


def _first_token(sql: str) -> str:
    stripped = (sql or "").strip().lstrip("(").strip()
    if not stripped:
        return ""
    return stripped.split(None, 1)[0].upper()


def _quote_ident(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def _quote_table(name: str) -> str:
    if "." in name:
        database, table = name.split(".", 1)
        return f"{_quote_ident(database)}.{_quote_ident(table)}"
    return _quote_ident(name)


def _normalize_df_for_csv(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for column in out.columns:
        series = out[column]
        if is_datetime64_any_dtype(series.dtype):
            if getattr(series.dt, "tz", None) is not None:
                series = series.dt.tz_convert("UTC").dt.tz_localize(None)
            out[column] = series.dt.strftime("%Y-%m-%d %H:%M:%S.%f").str.slice(0, 23)
        elif is_bool_dtype(series.dtype):
            out[column] = series.astype("Int64", errors="ignore")
    return out


def install_clickhouse(
    dest_path: str = "./clickhouse",
    *,
    force: bool = False,
    timeout_s: int = 300,
) -> str:
    """
    Download the ClickHouse binary using the official installer script.
    """
    if os.path.isfile(dest_path) and os.access(dest_path, os.X_OK) and not force:
        return dest_path

    dest_dir = os.path.dirname(dest_path) or "."
    os.makedirs(dest_dir, exist_ok=True)
    if os.path.exists(dest_path) and force:
        os.remove(dest_path)

    temp_dir = tempfile.mkdtemp(prefix="cht-clickhouse-")
    try:
        if shutil.which("curl"):
            cmd = ["/bin/sh", "-c", "curl https://clickhouse.com/ | sh"]
            proc = subprocess.run(
                cmd,
                cwd=temp_dir,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=timeout_s,
                check=False,
            )
        elif shutil.which("wget"):
            cmd = ["/bin/sh", "-c", "wget -qO- https://clickhouse.com/ | sh"]
            proc = subprocess.run(
                cmd,
                cwd=temp_dir,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=timeout_s,
                check=False,
            )
        else:
            script_path = os.path.join(temp_dir, "install_clickhouse.sh")
            with urllib.request.urlopen("https://clickhouse.com/", timeout=timeout_s) as response:
                script = response.read().decode("utf-8")
            with open(script_path, "w", encoding="utf-8") as handle:
                handle.write(script)
            proc = subprocess.run(
                ["/bin/sh", script_path],
                cwd=temp_dir,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=timeout_s,
                check=False,
            )

        if proc.returncode != 0:
            stderr = proc.stderr.decode("utf-8", errors="replace").strip()
            stdout = proc.stdout.decode("utf-8", errors="replace").strip()
            detail = stderr or stdout or "unknown installer error"
            raise RuntimeError(f"ClickHouse install failed: {detail}")

        candidates = []
        for name in os.listdir(temp_dir):
            if name.startswith("clickhouse"):
                path = os.path.join(temp_dir, name)
                if os.path.isfile(path):
                    candidates.append(path)
        if not candidates:
            raise RuntimeError("ClickHouse install failed: binary not found after download")

        binary = max(candidates, key=os.path.getmtime)
        shutil.move(binary, dest_path)
        os.chmod(dest_path, 0o755)
        return dest_path
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


@dataclass
class LazyCluster:
    """
    Serializable single-node ClickHouse cluster handle for Colab.

    Uses ``clickhouse local`` for each call to avoid background daemons.
    """

    clickhouse_bin: str = "/content/clickhouse"
    data_path: str = "/content/chdb"
    database: str = "default"

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2)

    @classmethod
    def from_json(cls, payload: str) -> "LazyCluster":
        return cls(**json.loads(payload))

    @classmethod
    def install_clickhouse(
        cls,
        dest_path: str = "./clickhouse",
        *,
        force: bool = False,
        timeout_s: int = 300,
    ) -> str:
        return install_clickhouse(dest_path, force=force, timeout_s=timeout_s)

    def _resolve_clickhouse_bin(self) -> str:
        if os.path.isfile(self.clickhouse_bin) and os.access(self.clickhouse_bin, os.X_OK):
            return self.clickhouse_bin
        resolved = shutil.which(self.clickhouse_bin)
        if resolved:
            return resolved
        raise FileNotFoundError(
            f"ClickHouse binary not found/executable at: {self.clickhouse_bin}\n"
            "In Colab, run:\n"
            "  !curl https://clickhouse.com/ | sh\n"
            "  !chmod +x ./clickhouse\n"
            "Then set clickhouse_bin='/content/clickhouse' (or wherever it is)."
        )

    def _ensure_ready(self) -> str:
        os.makedirs(self.data_path, exist_ok=True)
        return self._resolve_clickhouse_bin()

    def _run_local(
        self,
        sql: str,
        *,
        stdin_bytes: Optional[bytes] = None,
        timeout_s: int = 300,
    ) -> subprocess.CompletedProcess:
        clickhouse_bin = self._ensure_ready()

        prefix = (
            f"CREATE DATABASE IF NOT EXISTS {_quote_ident(self.database)}; "
            f"USE {_quote_ident(self.database)}; "
        )
        query_body = sql.strip().rstrip(";")
        if stdin_bytes is None:
            query_body = f"{query_body};"
        query = prefix + query_body + "\n"

        query_path = None
        try:
            with tempfile.NamedTemporaryFile("w", delete=False) as handle:
                handle.write(query)
                query_path = handle.name

            cmd = [
                clickhouse_bin,
                "local",
                "--path",
                self.data_path,
                "--queries-file",
                query_path,
            ]

            return subprocess.run(
                cmd,
                input=stdin_bytes,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=timeout_s,
                check=False,
            )
        finally:
            if query_path and os.path.exists(query_path):
                os.remove(query_path)

    @staticmethod
    def _raise_for_proc(proc: subprocess.CompletedProcess, *, context: str) -> None:
        if proc.returncode == 0:
            return
        stderr = proc.stderr.decode("utf-8", errors="replace").strip()
        stdout = proc.stdout.decode("utf-8", errors="replace").strip()
        detail = stderr or stdout or "Unknown ClickHouse error"
        raise RuntimeError(f"{context} failed: {detail}")

    def run_sql(
        self,
        sql: str,
        *,
        as_df: Optional[bool] = None,
        timeout_s: int = 300,
    ) -> Union[pd.DataFrame, str, None]:
        """
        Boot → execute → stop (process exits). SELECT-like queries return a DataFrame.
        """
        token = _first_token(sql)
        if as_df is None:
            as_df = token in {"SELECT", "WITH", "SHOW", "DESCRIBE", "EXPLAIN"}

        if as_df:
            query = sql.strip().rstrip(";")
            has_format = bool(_FORMAT_RE.search(query))
            if has_format and not _JSON_EACH_ROW_RE.search(query):
                raise ValueError("as_df=True requires FORMAT JSONEachRow")
            if not has_format:
                query = f"{query} FORMAT JSONEachRow"

            proc = self._run_local(query, timeout_s=timeout_s)
            self._raise_for_proc(proc, context="Query")

            text = proc.stdout.decode("utf-8", errors="replace").strip()
            if not text:
                return pd.DataFrame()

            rows: list[Dict[str, Any]] = []
            for line in text.splitlines():
                line = line.strip()
                if line:
                    rows.append(json.loads(line))
            return pd.DataFrame(rows)

        proc = self._run_local(sql, timeout_s=timeout_s)
        self._raise_for_proc(proc, context="Statement")
        output = proc.stdout.decode("utf-8", errors="replace").strip()
        return output or None

    def create_table_from_df(
        self,
        table: str,
        df: pd.DataFrame,
        *,
        schema: Optional[Dict[str, str]] = None,
        column_types: Optional[Dict[str, str]] = None,
        engine: str = "MergeTree",
        order_by: Optional[Sequence[str]] = None,
        if_exists: str = "fail",  # "fail" | "replace" | "append"
        auto_nullable: bool = True,
        timeout_s: int = 300,
    ) -> None:
        """
        Create a table from a DataFrame and insert rows using CSVWithNames.
        """
        if df.empty:
            raise ValueError("DataFrame is empty; cannot create table.")
        if schema and column_types:
            raise ValueError("Use only one of schema or column_types.")

        column_types = schema or column_types
        if if_exists not in {"fail", "replace", "append"}:
            raise ValueError("if_exists must be one of: fail | replace | append")

        resolved = resolve_column_types(
            df,
            column_types=column_types,
            auto_nullable=auto_nullable,
        )
        cols_sql = ", ".join(f"{_quote_ident(column)} {ch_type}" for column, ch_type in resolved)

        engine_sql = engine
        if engine.lower() == "mergetree":
            if not order_by:
                order_by = "tuple()"
            if isinstance(order_by, str):
                order_expr = order_by
            else:
                order_expr = ", ".join(_quote_ident(col) for col in order_by)
            engine_sql = f"{engine} ORDER BY {order_expr}"

        table_sql = _quote_table(table)

        if if_exists == "replace":
            self.run_sql(f"DROP TABLE IF EXISTS {table_sql}", as_df=False, timeout_s=timeout_s)
            self.run_sql(
                f"CREATE TABLE {table_sql} ({cols_sql}) ENGINE = {engine_sql}",
                as_df=False,
                timeout_s=timeout_s,
            )
        elif if_exists == "append":
            self.run_sql(
                f"CREATE TABLE IF NOT EXISTS {table_sql} ({cols_sql}) ENGINE = {engine_sql}",
                as_df=False,
                timeout_s=timeout_s,
            )
        else:
            self.run_sql(
                f"CREATE TABLE {table_sql} ({cols_sql}) ENGINE = {engine_sql}",
                as_df=False,
                timeout_s=timeout_s,
            )

        df_to_insert = _normalize_df_for_csv(df)
        csv_bytes = df_to_insert.to_csv(index=False, na_rep="\\N").encode("utf-8")
        insert_sql = f"INSERT INTO {table_sql} FORMAT CSVWithNames"

        proc = self._run_local(insert_sql, stdin_bytes=csv_bytes, timeout_s=timeout_s)
        self._raise_for_proc(proc, context="Insert")
