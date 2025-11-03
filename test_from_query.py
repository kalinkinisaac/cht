#!/usr/bin/env python3
"""
Test the new Table.from_query() functionality
"""

import pandas as pd
from datetime import timedelta
from unittest.mock import MagicMock, patch

def test_from_query():
    """Test Table.from_query() method."""
    
    # Mock cluster
    cluster = MagicMock()
    cluster.query.return_value = None
    
    # Mock the create_temp_table_sql function
    with patch('cht.temp_tables.create_temp_table_sql') as mock_create_sql:
        mock_create_sql.return_value = (
            "CREATE TABLE temp.test_table ENGINE = Memory AS SELECT * FROM users",
            "ALTER TABLE temp.test_table MODIFY COMMENT 'expires_at=2025-11-05T12:00:00Z'"
        )
        
        from cht.table import Table
        
        # Mock exists to return False
        with patch.object(Table, "exists", return_value=False):
            
            # Test basic from_query
            table = Table.from_query(
                "SELECT * FROM users WHERE active = 1",
                cluster=cluster,
                name="test_table"
            )
            
            # Verify table was created
            assert table.name == "test_table"
            assert table.database == "temp"
            assert table.cluster == cluster
            
            # Verify SQL was called
            assert cluster.query.call_count == 2  # CREATE + TTL comment
            
            # Verify create_temp_table_sql was called with correct params
            mock_create_sql.assert_called_once_with(
                query="SELECT * FROM users WHERE active = 1",
                table_name="test_table",
                database="temp",
                ttl=timedelta(days=1),
                order_by=None,
                on_cluster=None,
            )

if __name__ == "__main__":
    test_from_query()
    print("✅ Table.from_query() test passed!")