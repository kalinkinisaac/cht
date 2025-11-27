🚀 CHT v0.4.3 - Enhanced Nullable DateTime Support

✨ New Features:
- Automatic detection of nullable columns with missing values (NaN/NaT/None)
- Table.from_df() now handles nullable datetime columns automatically
- New detect_nullable_columns() function for manual inspection
- Enhanced resolve_column_types() with auto_nullable parameter

🔧 Improvements:
- Default auto_nullable=True prevents DataError on nullable datetime columns
- Backwards compatible - existing code works unchanged
- Manual column type override support maintained
- Comprehensive documentation and examples in README.md

🐛 Bug Fixes:
- Resolves 'Unable to create Python array for nullable datetime columns' error
- Fixes NaT serialization issues with ClickHouse DateTime64 columns
- Prevents struct.error with clickhouse-connect library

📋 Quality Assurance:
- ✅ All 122 tests passing (10 new nullable datetime tests)
- ✅ Comprehensive test coverage for mixed data types
- ✅ Package builds successfully (cht-0.4.3-py3-none-any.whl)
- ✅ Version consistency verified across files

🎯 Usage Examples:
# Automatic (default behavior)
df_with_nulls = pd.DataFrame({
    'start_date': pd.to_datetime(['2023-01-01', '2023-01-02']),
    'end_date': pd.to_datetime(['2023-01-10', None])  # NaT value
})
table = Table.from_df(df_with_nulls, cluster)  # Works automatically!

# Manual control
table = Table.from_df(
    df_with_nulls, 
    cluster,
    column_types={'end_date': 'Nullable(DateTime64(3))'},
    auto_nullable=False
)

# Inspect what would be auto-detected
from cht import detect_nullable_columns
nullable_cols = detect_nullable_columns(df)

📦 Installation:
pip install git+https://github.com/kalinkinisaac/cht.git

🧪 Verification:
python -c "import cht; print(f'CHT version: {cht.__version__}')"

This release resolves the original DataFrame nullable datetime issue
and provides a robust, automatic solution for handling missing values
in pandas DataFrames when creating ClickHouse tables.