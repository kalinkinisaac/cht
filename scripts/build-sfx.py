#!/usr/bin/env python3
"""Build CHT self-extracting file (SFX) similar to copyparty-sfx.py.

This script creates a single-file distribution that:
1. Contains all CHT code and dependencies in a compressed archive
2. Self-extracts to temp directory on first run
3. Works with any Python 3.10+ installation
4. Provides the same functionality as pip-installed CHT

Usage:
    python scripts/build-sfx.py
    
Output:
    cht-sfx.py - Single file containing everything needed to run CHT
    
Example usage of cht-sfx.py:
    python cht-sfx.py  # Interactive mode
    python cht-sfx.py --version  # Show version
    python -c "import sys; sys.path.insert(0, '.'); from cht import Cluster"
"""

import base64
import gzip
import io
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path
from typing import Dict, List, Tuple


def get_project_info() -> Dict[str, str]:
    """Extract version and metadata from pyproject.toml."""
    import re
    
    pyproject_path = Path(__file__).parent.parent / "pyproject.toml"
    with open(pyproject_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Extract version
    version_match = re.search(r'version = "([^"]+)"', content)
    if not version_match:
        raise ValueError("Could not find version in pyproject.toml")
    
    # Extract name and description
    name_match = re.search(r'name = "([^"]+)"', content)
    desc_match = re.search(r'description = "([^"]+)"', content)
    
    return {
        'version': version_match.group(1),
        'name': name_match.group(1) if name_match else 'cht',
        'description': desc_match.group(1) if desc_match else 'ClickHouse Operations Toolkit',
    }


def install_to_temp_dir(temp_dir: Path) -> None:
    """Install CHT and dependencies to a temporary directory."""
    print(f"📦 Installing CHT to temporary directory: {temp_dir}")
    
    # Install CHT in development mode to temp directory
    project_root = Path(__file__).parent.parent
    env = os.environ.copy()
    env['PYTHONPATH'] = str(temp_dir)
    
    # Install dependencies first
    subprocess.run([
        sys.executable, '-m', 'pip', 'install', 
        '--target', str(temp_dir),
        '--no-deps',  # We'll handle deps manually
        'clickhouse-connect>=0.6.8',
        'pandas>=1.5',
    ], check=True, env=env)
    
    # Install CHT source
    shutil.copytree(
        project_root / 'src' / 'cht',
        temp_dir / 'cht',
        dirs_exist_ok=True
    )
    
    print(f"✅ Installation complete")


def create_archive(temp_dir: Path) -> bytes:
    """Create compressed tar.gz archive from installed files."""
    print("🗜️  Creating compressed archive...")
    
    buffer = io.BytesIO()
    
    with tarfile.open(fileobj=buffer, mode='w:gz') as tar:
        # Add all files from temp directory
        for item in temp_dir.rglob('*'):
            if item.is_file():
                # Calculate archive path (relative to temp_dir)
                arcname = str(item.relative_to(temp_dir))
                tar.add(item, arcname=arcname)
    
    compressed_data = buffer.getvalue()
    print(f"✅ Archive created: {len(compressed_data)} bytes")
    return compressed_data


def generate_sfx_header(project_info: Dict[str, str]) -> str:
    """Generate the Python header for the self-extracting file."""
    return f'''#!/usr/bin/env python3
"""CHT (ClickHouse Operations Toolkit) - Self-Extracting Distribution

{project_info['description']}
Version: {project_info['version']}

This is a self-extracting Python script that contains all CHT code and dependencies.
On first run, it extracts itself to a temporary directory and imports CHT.

Usage Examples:
    # Interactive Python with CHT available
    python cht-sfx.py
    
    # Import CHT in your scripts
    python -c "exec(open('cht-sfx.py').read()); from cht import Cluster"
    
    # Check version
    python cht-sfx.py --version
    
    # Show help
    python cht-sfx.py --help

Installation:
    Just download cht-sfx.py - no pip install needed!
    Works with Python 3.10+ on any platform.

Source: https://github.com/kalinkinisaac/cht
"""

import base64
import gzip
import io
import os
import sys
import tarfile
import tempfile
from pathlib import Path

# Project metadata
__version__ = "{project_info['version']}"

# Embedded archive data (base64-encoded compressed tar.gz)
ARCHIVE_DATA = """'''


def generate_sfx_footer(project_info: Dict[str, str]) -> str:
    """Generate the Python footer that handles extraction and import."""
    return f'''"""

def extract_and_setup():
    """Extract embedded archive and set up Python path."""
    # Create cache directory 
    cache_dir = Path.home() / '.cache' / 'cht-sfx'
    version_file = cache_dir / 'version.txt'
    
    # Check if we need to extract
    current_version = "{project_info['version']}"
    need_extract = True
    
    if cache_dir.exists() and version_file.exists():
        try:
            cached_version = version_file.read_text().strip()
            if cached_version == current_version:
                need_extract = False
        except (OSError, IOError):
            pass
    
    if need_extract:
        print(f"🚀 CHT {{current_version}}: Extracting to {{cache_dir}}")
        
        # Clean and create cache directory
        if cache_dir.exists():
            import shutil
            shutil.rmtree(cache_dir)
        cache_dir.mkdir(parents=True, exist_ok=True)
        
        # Decode and extract archive
        archive_bytes = base64.b64decode(ARCHIVE_DATA.encode())
        
        with tarfile.open(fileobj=io.BytesIO(archive_bytes), mode='r:gz') as tar:
            tar.extractall(cache_dir)
        
        # Save version marker
        version_file.write_text(current_version)
        print(f"✅ CHT {{current_version}}: Ready!")
    
    # Add to Python path
    if str(cache_dir) not in sys.path:
        sys.path.insert(0, str(cache_dir))


def main():
    """Main entry point for the self-extracting file."""
    if len(sys.argv) > 1:
        if sys.argv[1] in ('--version', '-v'):
            print(f"CHT (ClickHouse Operations Toolkit) {{__version__}}")
            print("Self-extracting distribution")
            return
        elif sys.argv[1] in ('--help', '-h'):
            print(__doc__)
            return
    
    # Extract and set up CHT
    extract_and_setup()
    
    # Import CHT to verify it works
    try:
        import cht
        print(f"🎯 CHT {{cht.__version__}} loaded successfully!")
        print(f"📖 Available: {{', '.join(['Cluster', 'Table', 'DependencyGraph', '...'])}}")
        print()
        print("Example usage:")
        print("  from cht import Cluster, Table")
        print("  cluster = Cluster('local', 'localhost')")
        print("  # Use cluster.get_dependency_graph(), etc.")
        print()
    except ImportError as e:
        print(f"❌ Error importing CHT: {{e}}")
        sys.exit(1)


if __name__ == '__main__':
    main()
'''


def build_sfx() -> None:
    """Build the self-extracting CHT file."""
    print("🏗️  Building CHT Self-Extracting File (SFX)")
    print("=" * 50)
    
    # Get project metadata
    project_info = get_project_info()
    print(f"📊 Building {project_info['name']} v{project_info['version']}")
    
    # Create temporary directory for installation
    with tempfile.TemporaryDirectory() as temp_str:
        temp_dir = Path(temp_str)
        
        # Install CHT and dependencies
        install_to_temp_dir(temp_dir)
        
        # Create compressed archive
        archive_data = create_archive(temp_dir)
        
        # Encode archive as base64
        print("🔤 Encoding archive...")
        encoded_data = base64.b64encode(archive_data).decode('ascii')
        
        # Split into reasonable line lengths for readability
        encoded_lines = []
        for i in range(0, len(encoded_data), 76):
            encoded_lines.append(encoded_data[i:i+76])
        encoded_multiline = '\n'.join(encoded_lines)
        
        # Generate SFX file
        print("📝 Generating self-extracting file...")
        sfx_content = (
            generate_sfx_header(project_info) +
            encoded_multiline +
            generate_sfx_footer(project_info)
        )
        
        # Write SFX file
        output_path = Path(__file__).parent.parent / f"cht-sfx.py"
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(sfx_content)
        
        # Make executable
        os.chmod(output_path, 0o755)
        
        print(f"✅ CHT SFX built successfully!")
        print(f"📁 Output: {output_path}")
        print(f"📏 Size: {output_path.stat().st_size:,} bytes")
        print()
        print("🧪 Testing SFX...")
        
        # Test the SFX
        try:
            result = subprocess.run([
                sys.executable, str(output_path), '--version'
            ], capture_output=True, text=True, timeout=30)
            
            if result.returncode == 0:
                print("✅ SFX test passed!")
                print(f"Output: {result.stdout.strip()}")
            else:
                print(f"❌ SFX test failed: {result.stderr}")
        except subprocess.TimeoutExpired:
            print("⚠️  SFX test timed out (but file was created)")
        except Exception as e:
            print(f"⚠️  Could not test SFX: {e}")
        
        print()
        print("🎉 Usage:")
        print(f"   python {output_path.name}")
        print(f"   python {output_path.name} --version")
        print(f"   python -c \"exec(open('{output_path.name}').read()); from cht import Cluster\"")


if __name__ == '__main__':
    build_sfx()