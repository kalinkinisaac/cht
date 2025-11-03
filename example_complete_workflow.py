#!/usr/bin/env python3
"""
Example showing the complete CHT workflow with limit functionality
"""

import pandas as pd
from datetime import timedelta

def example_complete_workflow():
    """Example of complete CHT workflow: create → query → load with limit"""
    
    print("=== Complete CHT Workflow Example ===")
    print()
    
    # Sample input data
    df = pd.DataFrame({
        "user_id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        "name": ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry", "Ivy", "Jack"],
        "revenue": [100, 200, 150, 300, 250, 180, 220, 160, 280, 190],
        "region": ["US", "EU", "US", "EU", "US", "EU", "US", "EU", "US", "EU"]
    })
    
    print("Input DataFrame (10 rows):")
    print(df)
    print()
    
    # Would work with real cluster:
    print("# Step 1: Upload DataFrame to ClickHouse")
    print("table1 = Table.from_df(df, name='user_data', ttl=timedelta(hours=6))")
    print("# Creates temp.user_data with 6-hour TTL")
    print()
    
    print("# Step 2: Create analytical table from query")
    print("table2 = Table.from_query(")
    print("    'SELECT region, COUNT(*) as users, AVG(revenue) as avg_revenue'")
    print("    ' FROM temp.user_data GROUP BY region',")
    print("    name='region_stats',")
    print("    ttl=timedelta(days=1)")
    print(")")
    print("# Creates temp.region_stats with 1-day TTL")
    print()
    
    print("# Step 3: Load data back with different limits")
    print("all_data = table1.to_df()           # Load all 10 rows")
    print("sample = table1.to_df(limit=5)      # Load first 5 rows") 
    print("preview = table1.to_df(limit=2)     # Quick preview (2 rows)")
    print("stats = table2.to_df()              # Load aggregated stats")
    print()
    
    print("=== SQL Generated ===")
    print("CREATE TABLE temp.user_data ... AS SELECT * FROM (...)")
    print("CREATE TABLE temp.region_stats ... AS SELECT region, COUNT(*) ...")
    print("SELECT * FROM temp.user_data        -- to_df()")
    print("SELECT * FROM temp.user_data LIMIT 5  -- to_df(limit=5)")
    print("SELECT * FROM temp.user_data LIMIT 2  -- to_df(limit=2)")
    print()
    
    print("=== Benefits ===")
    print("✓ Unified API: Table.from_df() + Table.from_query() + table.to_df()")
    print("✓ Automatic TTL management with expires_at comments")
    print("✓ Memory efficient: load only what you need with limit")
    print("✓ No cluster repetition: set default once, use everywhere")
    print("✓ Perfect for data pipelines and ad-hoc analysis")

if __name__ == "__main__":
    example_complete_workflow()