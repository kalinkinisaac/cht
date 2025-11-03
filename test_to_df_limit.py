#!/usr/bin/env python3
"""
Test the Table.to_df() method with limit parameter
"""

from unittest.mock import MagicMock
import pandas as pd

def test_to_df_with_limit():
    """Test Table.to_df() with limit parameter."""
    
    from cht.table import Table
    
    # Mock cluster and client
    cluster = MagicMock()
    mock_client = MagicMock()
    cluster.client = mock_client
    
    # Mock return data
    mock_df = pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
    mock_client.query_df.return_value = mock_df
    
    # Create table instance
    table = Table("test_table", database="test", cluster=cluster)
    
    # Test without limit (default behavior)
    df1 = table.to_df()
    mock_client.query_df.assert_called_with("SELECT * FROM test.test_table")
    assert len(df1) == 3
    
    # Test with limit
    df2 = table.to_df(limit=10)
    mock_client.query_df.assert_called_with("SELECT * FROM test.test_table LIMIT 10")
    assert len(df2) == 3  # Mock returns same data
    
    # Test with limit=0
    df3 = table.to_df(limit=0)
    mock_client.query_df.assert_called_with("SELECT * FROM test.test_table LIMIT 0")
    
    print("✅ Table.to_df(limit=...) works correctly!")
    print(f"   - Without limit: SELECT * FROM test.test_table")
    print(f"   - With limit=10: SELECT * FROM test.test_table LIMIT 10")
    print(f"   - With limit=0:  SELECT * FROM test.test_table LIMIT 0")

if __name__ == "__main__":
    test_to_df_with_limit()