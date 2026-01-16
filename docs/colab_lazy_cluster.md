# Colab LazyCluster (clickhouse-local)

This guide shows how to use the `LazyCluster` helper for a single-node ClickHouse
setup in Google Colab. Each call spawns `clickhouse local`, executes a query, and
exits while persisting data under `--path`.

## Colab install cells

```bash
!curl https://clickhouse.com/ | sh
!chmod +x ./clickhouse
!./clickhouse --version
```

```bash
!pip -q install pandas
```

## Optional: install via Python

If you prefer a pure-Python installer (no shell cell), use:

```python
from cht.colab import install_clickhouse

clickhouse_bin = install_clickhouse("/content/clickhouse")
```

You can also call `LazyCluster.install_clickhouse(...)` if you want a class method.

## Usage

```python
from cht.colab import LazyCluster, install_clickhouse
import pandas as pd

clickhouse_bin = install_clickhouse("/content/clickhouse")

c = LazyCluster(
    clickhouse_bin=clickhouse_bin,
    data_path="/content/chdb",
    database="default",
)

df = c.run_sql("SELECT 1 AS x, 'hi' AS y")
print(df)

c.run_sql(
    """
    CREATE TABLE IF NOT EXISTS demo
    (
      id Int64,
      name String
    )
    ENGINE = MergeTree
    ORDER BY id
    """,
    as_df=False,
)

c.run_sql("INSERT INTO demo VALUES (1, 'alice'), (2, 'bob')", as_df=False)

print(c.run_sql("SELECT * FROM demo ORDER BY id"))

pdf = pd.DataFrame({"id": [3, 4], "name": ["carol", "dave"]})
c.create_table_from_df("demo2", pdf, if_exists="replace")
print(c.run_sql("SELECT * FROM demo2 ORDER BY id"))
```

## Persist across runtime resets (optional)

```python
from google.colab import drive
drive.mount("/content/drive")

c = LazyCluster(
    clickhouse_bin="/content/clickhouse",
    data_path="/content/drive/MyDrive/chdb",
    database="default",
)
```
