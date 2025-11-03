#!/usr/bin/env python3
"""
Test script to verify cht installation and basic functionality.
"""

def test_installation():
    try:
        import cht
        print(f"✅ cht imported successfully - version: {cht.__version__}")
        
        # Test basic imports
        from cht import Cluster, Table
        print("✅ Core classes imported successfully")
        
        # Test DataFrame integration
        from cht.dataframe import pandas_dtype_to_clickhouse
        print("✅ DataFrame integration imported successfully")
        
        # Test cluster creation (without connection)
        cluster = Cluster(name="test", host="localhost")
        print(f"✅ Cluster object created: {cluster}")
        
        print("\n🎉 All tests passed! cht is ready to use.")
        return True
        
    except ImportError as e:
        print(f"❌ Import failed: {e}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False

if __name__ == "__main__":
    test_installation()