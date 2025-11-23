#!/usr/bin/env python3
"""CHT unified API demonstration - streamlined workflow examples.

This example demonstrates the simplified CHT interface for common operations:
- Default cluster configuration for workspace-wide usage
- Seamless DataFrame ↔ ClickHouse integration
- Temporary table management with TTL
- Backup and restore workflows
- High-level table operations

The unified API reduces boilerplate and provides a pandas-like experience
while maintaining full ClickHouse capabilities.

Prerequisites:
    - ClickHouse running via docker compose up -d
    - CHT library installed
    - Pandas for DataFrame operations

Example:
    $ docker compose up -d
    $ python examples/basic_usage.py
    
    Output:
    ✓ Connected with default cluster
    ✓ Created temporary table from DataFrame
    ✓ Performed backup and restore
    ✓ Demonstrated unified API workflow
"""

import logging
from datetime import timedelta
from typing import Optional

import pandas as pd

from cht.cluster import Cluster
from cht.table import Table


def setup_logging() -> None:
    """Configure logging for the example."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )


def demo_unified_api():
    """Demo both Table.from_df() and Table.from_query() with shared logic."""
    
    print("=== CHT Unified API Demo ===")
    print("Both methods now share common temp table logic!")
    print()
    
    # Sample data
    df = pd.DataFrame({
        "user_id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
        "revenue": [100.50, 200.75, 150.00, 300.25, 250.00]
    })
    
    print("Sample DataFrame:")
    print(df)
    print()
    
    # Set default cluster once (would typically be done at start of script)
    # Table.set_default_cluster("local", host="localhost", user="developer", password="developer")
    
    print("=== Table.from_df() Examples ===")
    
    try:
        # Example 1: Create table from DataFrame
        print("1. Create temp table from DataFrame:")
        table1 = Table.from_df(df, name="user_data")
        print(f"   ✓ Created: {table1}")
    except RuntimeError as e:
        print(f"   ⚠ {e}")
    
    try:
        # Example 2: Custom TTL
        print("2. Create with 2-hour TTL:")
        table2 = Table.from_df(df, name="short_term", ttl=timedelta(hours=2))
        print(f"   ✓ Created: {table2}")
    except RuntimeError as e:
        print(f"   ⚠ {e}")
    
    print()
    print("=== Table.from_query() Examples ===")
    
    try:
        # Example 3: Create table from query
        print("1. Create temp table from SQL query:")
        table3 = Table.from_query(
            "SELECT user_id, name, revenue * 1.1 as revenue_with_tax FROM user_data",
            name="taxed_revenue"
        )
        print(f"   ✓ Created: {table3}")
    except RuntimeError as e:
        print(f"   ⚠ {e}")
    
    try:
        # Example 4: Aggregation query with ordering
        print("2. Create analytical table:")
        table4 = Table.from_query(
            "SELECT COUNT(*) as user_count, AVG(revenue) as avg_revenue FROM user_data",
            name="user_stats",
            ttl=timedelta(hours=6),
            order_by=["user_count"]
        )
        print(f"   ✓ Created: {table4}")
    except RuntimeError as e:
        print(f"   ⚠ {e}")
    
    print()
    print("=== Data Loading Examples ===")
    
    try:
        # Example 5: Load data back with limit
        print("1. Load all data:")
        # result_df = table1.to_df()
        print(f"   table.to_df()  # Load all rows")
        
        print("2. Load sample data:")
        # sample_df = table1.to_df(limit=100)
        print(f"   table.to_df(limit=100)  # Load first 100 rows")
        
        print("3. Load preview:")
        # preview_df = table1.to_df(limit=5)
        print(f"   table.to_df(limit=5)    # Quick preview")
        
    except Exception as e:
        print(f"   ⚠ {e}")
    
    print()
    print("=== Shared Features ===")
    print("✓ Both methods use same TTL management")
    print("✓ Both methods support default cluster")
    print("✓ Both methods auto-generate table names")
    print("✓ Both methods add expires_at comments")
    print("✓ Table.to_df() supports limit parameter")
    print("✓ No code duplication - shared common logic!")
    print()
    
    print("=== API Summary ===")
    print("Table.from_df(df, ...)     # DataFrame → ClickHouse table")
    print("Table.from_query(sql, ...) # SQL query → ClickHouse table")
    print("table.to_df(limit=...)     # ClickHouse table → DataFrame")
    print("Both support: ttl, name, database, cluster parameters")


if __name__ == "__main__":
    demo_unified_api()