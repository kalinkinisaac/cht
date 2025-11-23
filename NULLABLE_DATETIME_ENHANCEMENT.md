# Nullable DateTime Column Handling in CHT

## Problem

When working with pandas DataFrames that contain datetime columns with missing values (NaT - Not a Time), the CHT `Table.from_df()` method would fail with the following error:

```
DataError: Unable to create Python array for source column `bidder_end_date`. 
This is usually caused by trying to insert None values into a ClickHouse column that is not Nullable
```

This happens because:
1. Pandas represents missing datetime values as `NaT` (Not a Time)
2. ClickHouse DateTime columns are non-nullable by default
3. The `clickhouse-connect` library cannot serialize NaT values to non-nullable DateTime columns

## Solution

CHT now provides **automatic nullable column detection** for DataFrames containing null/missing values. This feature is enabled by default and can be controlled manually.

### Automatic Nullable Detection (Default Behavior)

By default, `Table.from_df()` now automatically detects columns with missing values and creates them as nullable in ClickHouse:

```python
import pandas as pd
from cht import Table, Cluster

# DataFrame with missing datetime values
df = pd.DataFrame({
    'bidder_id': [79, 67, 90],
    'start_date': pd.to_datetime(['2025-07-06', '2025-07-02', '2025-07-09']),
    'end_date': pd.to_datetime(['2025-09-17', None, '2025-09-10'])  # NaT value
})

# This now works automatically!
cluster = Cluster("local", "localhost")
table = Table.from_df(df, cluster, name='bidders', auto_nullable=True)  # Default

# Generated ClickHouse schema:
# - bidder_id: Int64
# - start_date: DateTime64(3)  
# - end_date: Nullable(DateTime64(3))  ← Automatically detected as nullable
```

### Manual Column Type Control

You can still manually specify column types and disable auto-detection:

```python
# Manual control with explicit column types
table = Table.from_df(
    df, 
    cluster,
    name='bidders_manual',
    column_types={'end_date': 'Nullable(DateTime64(3))'},
    auto_nullable=False  # Disable auto-detection
)

# Or combine manual and auto-detection
table = Table.from_df(
    df,
    cluster, 
    name='bidders_hybrid',
    column_types={'bidder_id': 'UInt32'},  # Manual override
    auto_nullable=True  # Auto-detect others
)
```

### Backwards Compatibility

All existing code continues to work without changes. The `auto_nullable=True` default only affects DataFrames with missing values - clean DataFrames work exactly as before.

## Technical Details

### Supported Column Types

Auto-nullable detection works for all pandas column types:

| Pandas Type | Missing Value | ClickHouse Type |
|-------------|---------------|-----------------|
| `int64` with `None` | `NaN` | `Nullable(Int64)` |
| `float64` with `NaN` | `NaN` | `Nullable(Float64)` |
| `datetime64` with `NaT` | `NaT` | `Nullable(DateTime64(3))` |
| `object/string` with `None` | `None` | `Nullable(String)` |
| `bool` with `None` | `None` | `Nullable(UInt8)` |

### New API Parameters

#### `Table.from_df()` 

- **`auto_nullable`** (bool, default=True): Automatically detect nullable columns
- **`column_types`** (dict, optional): Manual column type overrides

#### `create_table_from_dataframe()` and `insert_dataframe()`

- **`auto_nullable`** (bool, default=False): Enable auto-detection 
- Enhanced **`column_types`** parameter support

#### New Functions

- **`detect_nullable_columns(df)`**: Returns dictionary of nullable column overrides
- **`resolve_column_types(df, column_types, auto_nullable)`**: Enhanced type resolution

## Testing

Comprehensive test coverage has been added in `tests/test_nullable_datetime.py`:

```bash
# Run the nullable datetime tests
python -m pytest tests/test_nullable_datetime.py -v

# Run all CHT tests to verify compatibility  
python -m pytest tests/ -v
```

### Test Cases

1. ✅ DataFrame with NaT values in datetime columns
2. ✅ Auto-detection of nullable columns
3. ✅ Manual column type overrides
4. ✅ Mixed data types with various null patterns
5. ✅ CREATE TABLE SQL generation with nullable types
6. ✅ Backwards compatibility with existing code
7. ✅ `from_df()` with auto_nullable enabled/disabled

## Examples

### Real-world Bidders Table

```python
# Your original problematic code
bidders = pd.DataFrame({
    'bidder_id': [1571, 1578, 1583, 1585, 1576],
    'bidder_start_date': pd.to_datetime(['2025-11-22'] * 5),
    'bidder_end_date': pd.to_datetime([None, None, None, None, None])  # All NaT
})

# Solution 1: Automatic (recommended)
bidders_ch = Table.from_df(
    bidders, 
    cluster,
    name='bidders_from_08_01_to_11_22',
    ttl=None  # auto_nullable=True by default
)

# Solution 2: Manual  
bidders_ch = Table.from_df(
    bidders,
    cluster, 
    name='bidders_manual',
    column_types={'bidder_end_date': 'Nullable(DateTime64(3))'},
    auto_nullable=False,
    ttl=None
)
```

### Check What Would Be Auto-Detected

```python
from cht.dataframe import detect_nullable_columns

# See what columns would be made nullable
nullable_cols = detect_nullable_columns(bidders)
print(nullable_cols)
# Output: {'bidder_end_date': 'Nullable(DateTime64(3))'}
```

### Mixed Data Types

```python
df_mixed = pd.DataFrame({
    'id': [1, 2, None],           # → Nullable(Float64) 
    'name': ['Alice', None, 'Bob'], # → Nullable(String)
    'date': pd.to_datetime(['2023-01-01', None, '2023-01-03']), # → Nullable(DateTime64(3))
    'score': [85.5, 92.0, 78.3]   # → Float64 (no nulls)
})

table = Table.from_df(df_mixed, cluster)  # All nullable types auto-detected
```

## Migration Guide

### If You're Getting the Error

**Before (fails):**
```python
bidders_ch = t.from_df(bidders, name='bidders_table', ttl=None)
# ERROR: Unable to create Python array for source column `bidder_end_date`
```

**After (works):**
```python
bidders_ch = t.from_df(bidders, name='bidders_table', ttl=None)
# ✅ Works automatically with auto_nullable=True (default)
```

### For Existing Code

No changes required! Your existing code will continue to work. The new functionality only activates when:
1. Your DataFrame has missing values (NaN/NaT/None)
2. `auto_nullable=True` (the new default)

### For Advanced Users

If you want the old behavior (non-nullable by default), set `auto_nullable=False`:

```python
table = Table.from_df(df, cluster, auto_nullable=False)
```

## Performance Impact

- ✅ **Minimal overhead**: Detection only scans for null values once
- ✅ **Same performance** for DataFrames without nulls  
- ✅ **Faster than manual type specification** for complex schemas
- ✅ **Memory efficient**: No data copying, only metadata analysis

## Error Prevention

This enhancement prevents these common errors:

1. `DataError: Unable to create Python array` - nullable datetime columns
2. `struct.error: required argument is not an integer` - NaT serialization 
3. Silent data corruption from manual type mismatches
4. Time spent debugging ClickHouse type compatibility issues

The solution is transparent, automatic, and maintains full backwards compatibility while significantly improving the developer experience when working with real-world data containing missing values.