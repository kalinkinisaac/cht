#!/usr/bin/env python3
"""Build CHT as a zipapp (.pyz file) for lighter distribution.

This creates a simpler single-file distribution that:
1. Contains only CHT source code (no dependencies)
2. Creates an executable .pyz file
3. Requires dependencies to be pre-installed

Usage:
    python scripts/build-zipapp.py
    
Output:
    cht.pyz - Lightweight CHT zipapp
    
Example usage of cht.pyz:
    pip install clickhouse-connect pandas  # Install deps first
    python cht.pyz --version
    python -c "import sys; sys.path.insert(0, 'cht.pyz'); from cht import Cluster"
"""

import shutil
import subprocess
import sys
import zipapp
from pathlib import Path


def build_zipapp() -> None:
    """Build the CHT zipapp."""
    print("📦 Building CHT Zipapp (.pyz)")
    print("=" * 40)
    
    project_root = Path(__file__).parent.parent
    temp_dir = project_root / "build" / "zipapp"
    
    # Clean and create temp directory
    if temp_dir.exists():
        shutil.rmtree(temp_dir)
    temp_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"📁 Working directory: {temp_dir}")
    
    # Copy CHT source
    print("📋 Copying CHT source...")
    shutil.copytree(
        project_root / "src" / "cht",
        temp_dir / "cht",
    )
    
    # Create __main__.py for zipapp entry point
    main_content = '''#!/usr/bin/env python3
"""CHT Zipapp entry point."""

import sys
from pathlib import Path

# Ensure we can import CHT
try:
    from cht import __version__ as cht_version
    from cht import Cluster, Table, DependencyGraph
    
    def main():
        """Main zipapp entry point."""
        if len(sys.argv) > 1:
            if sys.argv[1] in ('--version', '-v'):
                print(f"CHT (ClickHouse Operations Toolkit) {cht_version}")
                print("Zipapp distribution")
                return
            elif sys.argv[1] in ('--help', '-h'):
                print("CHT - ClickHouse Operations Toolkit")
                print("Zipapp version (dependencies must be pre-installed)")
                print()
                print("Required dependencies:")
                print("  pip install clickhouse-connect>=0.6.8 pandas>=1.5")
                print()
                print("Usage:")
                print("  python cht.pyz --version")
                print("  python -c \\"import sys; sys.path.insert(0, 'cht.pyz'); from cht import Cluster\\"")
                return
        
        print(f"🎯 CHT {cht_version} loaded successfully!")
        print("📖 Available: Cluster, Table, DependencyGraph, ...")
        print()
        print("Example usage:")
        print("  import sys; sys.path.insert(0, 'cht.pyz')")
        print("  from cht import Cluster, Table")
        print("  cluster = Cluster('local', 'localhost')")
        print()
    
    if __name__ == '__main__':
        main()

except ImportError as e:
    print(f"❌ Missing dependencies: {e}")
    print("Install with: pip install clickhouse-connect>=0.6.8 pandas>=1.5")
    sys.exit(1)
'''
    
    with open(temp_dir / "__main__.py", "w") as f:
        f.write(main_content)
    
    # Build zipapp
    output_path = project_root / "cht.pyz"
    print(f"🏗️  Building zipapp: {output_path}")
    
    zipapp.create_archive(
        source=temp_dir,
        target=output_path,
        interpreter="/usr/bin/env python3",
        # main is automatically detected from __main__.py
    )
    
    # Clean up temp directory
    shutil.rmtree(temp_dir)
    
    print(f"✅ CHT Zipapp built successfully!")
    print(f"📁 Output: {output_path}")
    print(f"📏 Size: {output_path.stat().st_size:,} bytes")
    print()
    print("🧪 Testing zipapp...")
    
    # Test the zipapp
    try:
        result = subprocess.run([
            sys.executable, str(output_path), '--version'
        ], capture_output=True, text=True, timeout=10)
        
        if result.returncode == 0:
            print("✅ Zipapp test passed!")
            print(f"Output: {result.stdout.strip()}")
        else:
            print(f"❌ Zipapp test failed: {result.stderr}")
    except subprocess.TimeoutExpired:
        print("⚠️  Zipapp test timed out")
    except Exception as e:
        print(f"⚠️  Could not test zipapp: {e}")
    
    print()
    print("🎉 Usage (after installing dependencies):")
    print("   pip install clickhouse-connect>=0.6.8 pandas>=1.5")
    print(f"   python {output_path.name}")
    print(f"   python {output_path.name} --version")


if __name__ == '__main__':
    build_zipapp()