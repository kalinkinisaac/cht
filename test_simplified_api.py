#!/usr/bin/env python3
"""
Demonstration of the simplified CHT API using ONLY Table.from_df()
"""

import pandas as pd
from datetime import timedelta
from cht.table import Table


def test_simplified_api():
    """Test the simplified API with TTL functionality."""
    # Set default cluster once (would typically be done at start of script)
    # Table.set_default_cluster("local", host="localhost", user="developer", password="developer")
    
    # Sample data
    df = pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
        "age": [25, 30, 35, 28, 32],
        "timestamp": pd.to_datetime(["2023-01-01", "2023-01-02", "2023-01-03", "2023-01-04", "2023-01-05"])
    })
    
    print("=== CHT Simplified API Demo ===")
    print(f"Input DataFrame:\n{df}")
    print()
    
    # ONLY API: Table.from_df() with different scenarios
    print("Creating tables with different TTL settings...")
    
    # 1. Default TTL (1 day)
    print("1. Default TTL (1 day expiration):")
    try:
        table1 = Table.from_df(df, name="demo_default_ttl")
        print(f"   ✓ Created: {table1}")
    except RuntimeError as e:
        print(f"   ⚠ {e}")
        print("   (This is expected when no default cluster is configured)")
    
    # 2. Custom TTL (2 hours)
    print("2. Custom TTL (2 hours):")
    try:
        table2 = Table.from_df(df, name="demo_custom_ttl", ttl=timedelta(hours=2))
        print(f"   ✓ Created: {table2}")
    except RuntimeError as e:
        print(f"   ⚠ {e}")
    
    # 3. Permanent table (no TTL)
    print("3. Permanent table (no expiration):")
    try:
        table3 = Table.from_df(df, name="demo_permanent", ttl=None)
        print(f"   ✓ Created: {table3}")
    except RuntimeError as e:
        print(f"   ⚠ {e}")
    
    # 4. Auto-generated name
    print("4. Auto-generated name:")
    try:
        table4 = Table.from_df(df)  # No name = auto-generated
        print(f"   ✓ Created: {table4}")
    except RuntimeError as e:
        print(f"   ⚠ {e}")
    
    print()
    print("=== API Summary ===")
    print("✓ ONLY Table.from_df() needed - no cluster methods!")
    print("✓ TTL management built-in with expires_at metadata")
    print("✓ Default cluster support (set once, use everywhere)")
    print("✓ Auto-generated names for ad-hoc analysis")
    print("✓ Flexible TTL: timedelta(hours=2), timedelta(days=1), or None")


if __name__ == "__main__":
    test_simplified_api()