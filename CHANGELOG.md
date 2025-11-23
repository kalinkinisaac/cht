# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **Single-File Distribution**: Two distribution options inspired by copyparty
  - `cht-sfx.py` - Self-extracting file with all dependencies (~27MB, no pip needed)
  - `cht.pyz` - Lightweight zipapp (~250KB, requires pre-installed dependencies)
  - Automatic extraction and caching for SFX version
  - Build scripts: `build-sfx.py`, `build-zipapp.py`, `build-all.sh`
  - Complete functionality equivalent to pip-installed CHT
- **Dependency Graph Mapping**: Complete table and materialized view dependency discovery
  - `Cluster.get_dependency_graph()` method for automatic pipeline discovery
  - `DependencyGraph`, `GraphNode`, and `GraphEdge` classes for structured representation
  - Support for multiple export formats (JSON, NetworkX, GraphML, DOT)
  - Advanced analysis features (cycle detection, impact analysis, influence scoring)
  - Visualization support with matplotlib and interactive HTML
  - Integration with existing `Table` class for seamless workflow
- **Table Extensions**: New dependency discovery methods
  - `Table.get_dependent_views()` - Find materialized views depending on this table
  - `Table.get_source_tables()` - Find source tables this table depends on
- **Examples Directory**: Reorganized demos with Google Python style
  - `examples/dependency_graph_basic.py` - Live ClickHouse demo
  - `examples/dependency_graph_mock.py` - Mock data demonstration
  - `examples/basic_usage.py` - Core library usage patterns
- **Documentation**: Comprehensive graph mapping guide (`GRAPH_MAPPING.md`)

### Changed
- **Project Structure**: Reorganized demos into `examples/` directory
- **Code Style**: Updated to follow Google Python style guide
- **Documentation**: Enhanced docstrings with Google style format

### Fixed
- **Test Coverage**: Added 24 comprehensive tests for graph functionality
- **Type Safety**: Improved type annotations throughout graph module

## [0.4.2] - 2024-XX-XX

### Added
- **Table.from_query()**: Create ClickHouse tables directly from SQL queries
- **Table.to_df(limit=N)**: Memory-efficient DataFrame loading with row limits
- **Enhanced TTL Management**: Improved temporary table lifecycle

### Changed
- **Code Organization**: Refactored shared utility methods
- **Error Handling**: Better exception messages and logging

### Fixed
- **Boolean Comparisons**: Resolved numpy boolean comparison issues
- **CI/CD**: Fixed linting violations for GitHub Actions compatibility

## [0.4.1] - 2024-XX-XX

### Added
- **DataFrame Integration**: Seamless pandas ↔ ClickHouse workflows
  - `Table.from_df()` with automatic type mapping
  - Support for temporary tables with TTL expiration
  - Configurable table engines and partitioning
- **Default Cluster**: Global cluster configuration for simplified usage
  - `Table.set_default_cluster()` for workspace-wide defaults
  - Automatic fallback when no cluster specified

### Changed
- **API Consistency**: Standardized method signatures across Table class
- **Logging**: Enhanced structured logging throughout operations

### Fixed
- **Memory Usage**: Optimized DataFrame operations for large datasets
- **Connection Management**: Improved cluster connection lifecycle

## [0.4.0] - 2024-XX-XX

### Added
- **Cluster Class**: High-level ClickHouse connection management
  - Lazy connection initialization
  - Structured logging for all operations
  - Read-only mode enforcement
  - Bulk query execution with progress tracking
- **Table Class**: Declarative table operations
  - Metadata introspection (columns, parts, time detection)
  - Backup and restore workflows
  - Materialized view discovery and replay
  - Data quality utilities
- **Operations Module**: High-level automation helpers
  - `rebuild_table_via_mv()` for safe table reconstruction
  - Row synchronization utilities
  - Duplicate analysis and cleanup
- **Kafka Integration**: Consumer group management and table diffing
- **SQL Utilities**: Common helper functions
  - Identifier formatting and quoting
  - Remote table expressions
  - Hash-based row comparison
- **Docker Environment**: Local development and testing setup
- **Comprehensive Test Suite**: 88 tests covering core functionality

### Infrastructure
- **Build System**: Modern Python packaging with `pyproject.toml`
- **CI/CD**: GitHub Actions for testing and releases
- **Documentation**: Extensive README and inline documentation
- **Type Safety**: Full type annotations throughout codebase

## [0.3.x] - Earlier Releases

See git history for details of earlier development.

---

## Release Guidelines

### Version Numbering
- **Major (X.0.0)**: Breaking changes, major new features
- **Minor (0.X.0)**: New features, backwards compatible
- **Patch (0.0.X)**: Bug fixes, small improvements

### Change Categories
- **Added**: New features and capabilities
- **Changed**: Changes in existing functionality
- **Deprecated**: Soon-to-be removed features
- **Removed**: Removed features
- **Fixed**: Bug fixes
- **Security**: Security vulnerability fixes

### Breaking Changes
When introducing breaking changes, we:
1. Document migration path in changelog
2. Provide deprecation warnings when possible
3. Include examples of updated usage
4. Bump major version number

### Release Process
1. Update version in `pyproject.toml` and `src/cht/__init__.py`
2. Update CHANGELOG.md with release notes
3. Create GitHub release with tag
4. Automated CI/CD builds and publishes artifacts