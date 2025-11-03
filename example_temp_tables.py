#!/usr/bin/env python3
"""
Example demonstrating temp table management and default cluster functionality.
"""

from datetime import timedelta

from cht import Cluster, Table

def main():
    # Set up cluster connection
    cluster = Cluster(
        name="local",
        host="localhost",
        user="default",
        password="",
        port=8123
    )
    
    # Set default cluster for all Table operations
    Table.set_default_cluster(cluster)
    print(f"✅ Set default cluster: {cluster}")
    
    # Now you can create Tables without specifying cluster every time
    table = Table("demo_table", "temp")  # No cluster= needed!
    print(f"✅ Created table reference: {table.fqdn}")
    
    # Create a temporary table with TTL
    temp_table_name = cluster.create_temp_table(
        query="SELECT 1 as id, 'test' as name",
        ttl=timedelta(hours=1),  # Expires in 1 hour
        database="temp"
    )
    print(f"✅ Created temp table: {temp_table_name}")
    
    # List all temp tables with their expiration info
    temp_tables = cluster.get_temp_tables("temp")
    print(f"✅ Found {len(temp_tables)} temp tables:")
    for _, row in temp_tables.iterrows():
        status = "🔴 EXPIRED" if row['expired'] else "🟢 ACTIVE"
        expires = row['expires_at'].strftime('%Y-%m-%d %H:%M:%S') if row['expires_at'] else "NEVER"
        print(f"   {status} {row['table']} (expires: {expires})")
    
    # Dry run cleanup to see what would be deleted
    cleanup_result = cluster.cleanup_temp_tables("temp", dry_run=True)
    print(f"✅ Cleanup dry run results:")
    print(f"   Tables checked: {cleanup_result['total_tables_checked']}")
    print(f"   Expired tables: {cleanup_result['expired_tables_found']}")
    if cleanup_result['expired_tables_found'] > 0:
        print(f"   Would delete: {cleanup_result.get('tables_to_delete', [])}")
    
    # Example with custom TTL for permanent table
    persistent_table_name = cluster.create_temp_table(
        query="SELECT 'persistent' as type, current_timestamp() as created",
        ttl=None,  # No expiration
        database="temp"
    )
    print(f"✅ Created persistent temp table: {persistent_table_name}")
    
    print("\n🎉 Demo completed! Key features:")
    print("   • Default cluster eliminates repetitive cluster= parameters")
    print("   • Temp tables with TTL-based expiration via comments")
    print("   • Automatic cleanup of expired tables")
    print("   • Both temporary and persistent table support")

if __name__ == "__main__":
    main()