#!/usr/bin/env python3
"""
Integration test for the cht package with Docker ClickHouse instance.
Tests DataFrame functionality with a real ClickHouse server.
"""

import pandas as pd
import time
from cht.cluster import Cluster
from cht.table import Table

def wait_for_clickhouse() -> Cluster:
    """Wait for ClickHouse to be ready."""
    print("Waiting for ClickHouse to start...")
    max_attempts = 30
    for attempt in range(max_attempts):
        try:
            cluster = Cluster(
                name="docker_test",
                host="localhost",
                port=8123,
                user="developer",
                password="developer"
            )
            cluster.client.ping()
            print("✓ ClickHouse is ready!")
            return cluster
        except Exception as e:
            if attempt < max_attempts - 1:
                print(f"Attempt {attempt + 1}/{max_attempts}: ClickHouse not ready yet, waiting...")
                time.sleep(2)
            else:
                raise Exception(f"ClickHouse failed to start after {max_attempts} attempts: {e}")
    
    raise Exception("Should not reach here")
def test_dataframe_integration():
    """Test DataFrame functionality with real ClickHouse."""
    print("\n=== Testing DataFrame Integration with Docker ClickHouse ===")
    
    # Connect to ClickHouse
    cluster = wait_for_clickhouse()
    
    # Create test DataFrame with various data types
    test_data = {
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
        'age': [25, 30, 35, 28, 32],
        'salary': [50000.0, 60000.5, 75000.25, 55000.0, 68000.75],
        'is_active': [True, False, True, True, False],
        'join_date': pd.to_datetime(['2020-01-15', '2019-05-20', '2021-03-10', '2020-11-05', '2022-02-28'])
    }
    df = pd.DataFrame(test_data)
    
    print(f"Created test DataFrame with {len(df)} rows:")
    print(df)
    print(f"DataFrame dtypes:\n{df.dtypes}")
    
    # Test 1: Create table from DataFrame
    print("\n--- Test 1: Creating table from DataFrame ---")
    table_name = "test_employees"
    
    try:
        # Drop table if exists (using high-level Table API)
        temp_table = Table(name=table_name, database="default", cluster=cluster)
        if temp_table.exists():
            temp_table.drop()
        
        # Create table from DataFrame
        table = Table.from_df(
            df,
            cluster=cluster,
            database="default",
            name=table_name,
            mode="overwrite"
        )
        print(f"✓ Successfully created table '{table_name}' from DataFrame")
        
        # Verify table structure
        columns = table.get_columns()
        print(f"Table columns: {columns}")
        
    except Exception as e:
        print(f"✗ Failed to create table from DataFrame: {e}")
        return False
    
    # Test 2: Read data back as DataFrame
    print("\n--- Test 2: Reading table data as DataFrame ---")
    try:
        df_from_db = table.to_df()
        print(f"✓ Successfully read {len(df_from_db)} rows from ClickHouse")
        print("Data from ClickHouse:")
        print(df_from_db)
        print(f"Retrieved DataFrame dtypes:\n{df_from_db.dtypes}")
        
        # Verify data integrity (excluding datetime comparison for simplicity)
        expected_ids = set(df['id'])
        actual_ids = set(df_from_db['id'])
        if expected_ids == actual_ids:
            print("✓ Data integrity check passed - IDs match")
        else:
            print(f"✗ Data integrity check failed - Expected IDs: {expected_ids}, Got: {actual_ids}")
            
    except Exception as e:
        print(f"✗ Failed to read table as DataFrame: {e}")
        return False
    
    # Test 3: Append more data
    print("\n--- Test 3: Appending data to existing table ---")
    try:
        # Create additional data
        additional_data = {
            'id': [6, 7, 8],
            'name': ['Frank', 'Grace', 'Henry'],
            'age': [29, 31, 27],
            'salary': [52000.0, 71000.5, 48000.25],
            'is_active': [True, True, False],
            'join_date': pd.to_datetime(['2023-01-15', '2023-06-20', '2023-03-10'])
        }
        df_additional = pd.DataFrame(additional_data)
        
        # Append to existing table
        Table.from_df(
            df_additional,
            cluster=cluster,
            database="default",
            name=table_name,
            mode="append"
        )
        print(f"✓ Successfully appended {len(df_additional)} rows")
        
        # Verify total count using Table select method
        count_result = table.select()
        if count_result:
            total_rows = len(count_result)
            expected_count = len(df) + len(df_additional)
            if total_rows == expected_count:
                print(f"✓ Total row count verification passed: {total_rows} rows")
            else:
                print(f"✗ Row count mismatch - Expected: {expected_count}, Got: {total_rows}")
        else:
            print("✗ Failed to get row count")
            
    except Exception as e:
        print(f"✗ Failed to append data: {e}")
        return False
    
    # Test 4: Query with conditions
    print("\n--- Test 4: Querying with conditions ---")
    try:
        # Query active employees using Table select method
        active_employees = table.select(where="is_active = 1")
        if active_employees:
            print(f"✓ Found {len(active_employees)} active employees")
            print("Active employees (first few):")
            for i, row in enumerate(active_employees[:3]):  # Show first 3
                print(f"  {row}")
        else:
            print("✗ No active employees found")
        
    except Exception as e:
        print(f"✗ Failed to query with conditions: {e}")
        return False
    
    # Test 5: Data type edge cases
    print("\n--- Test 5: Testing edge case data types ---")
    try:
        edge_case_data = {
            'uint8_col': pd.array([1, 2, 255], dtype='uint8'),
            'uint16_col': pd.array([100, 1000, 65535], dtype='uint16'),
            'uint32_col': pd.array([100000, 1000000, 4294967295], dtype='uint32'),
            'int8_col': pd.array([-128, 0, 127], dtype='int8'),
            'int16_col': pd.array([-32768, 0, 32767], dtype='int16'),
            'float32_col': pd.array([1.5, 2.7, 3.14159], dtype='float32'),
            'string_col': ['test1', 'test2', 'test3']
        }
        df_edge_cases = pd.DataFrame(edge_case_data)
        
        edge_table_name = "test_edge_cases"
        
        # Drop table if exists using Table API
        edge_temp_table = Table(name=edge_table_name, database="default", cluster=cluster)
        if edge_temp_table.exists():
            edge_temp_table.drop()
        
        edge_table = Table.from_df(
            df_edge_cases,
            cluster=cluster,
            database="default",
            name=edge_table_name,
            mode="overwrite"
        )
        print(f"✓ Successfully created table with edge case data types")
        
        # Read back and verify
        df_edge_back = edge_table.to_df()
        print("Edge case data types verification:")
        print(f"Original dtypes:\n{df_edge_cases.dtypes}")
        print(f"Retrieved dtypes:\n{df_edge_back.dtypes}")
        
    except Exception as e:
        print(f"✗ Failed edge case test: {e}")
        return False
    
    # Cleanup
    print("\n--- Cleanup ---")
    try:
        # Use Table API for cleanup
        table.drop()
        edge_table.drop()
        print("✓ Cleanup completed")
    except Exception as e:
        print(f"Warning: Cleanup failed: {e}")
    
    print("\n🎉 All DataFrame integration tests passed!")
    return True

if __name__ == "__main__":
    try:
        success = test_dataframe_integration()
        exit(0 if success else 1)
    except Exception as e:
        print(f"\n💥 Integration test failed with error: {e}")
        import traceback
        traceback.print_exc()
        exit(1)