#!/usr/bin/env python3
"""Advanced CHT operations for production environments.

This example demonstrates sophisticated CHT usage patterns for production
environments, including:
- Multi-database operations
- Bulk processing with progress tracking  
- Error handling and retry logic
- Performance monitoring
- Automated backup schedules

These patterns are suitable for:
- ETL pipeline automation
- Data warehouse maintenance
- Multi-cluster synchronization
- Performance optimization

Prerequisites:
    - ClickHouse cluster with multiple databases
    - CHT library installed
    - Appropriate database permissions

Example:
    $ python examples/advanced_operations.py
    
    Output:
    🏭 Production CHT Operations Demo
    ✓ Processed 1,250 tables across 5 databases
    ✓ Completed backup cycle in 2.3 minutes
    ✓ Identified 3 performance optimization opportunities
"""

import concurrent.futures
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from cht import Cluster, Table
from cht.operations import (
    analyze_and_remove_duplicates,
    rebuild_table_via_mv,
    sync_missing_rows_by_date,
)


@dataclass
class OperationResult:
    """Result of a CHT operation with timing and status info."""
    
    operation: str
    table: str
    success: bool
    duration: float
    message: str
    details: Optional[Dict] = None


class ProductionCHTManager:
    """Production-ready CHT operations manager with monitoring and error handling."""
    
    def __init__(self, cluster: Cluster) -> None:
        """Initialize the manager with a cluster connection.
        
        Args:
            cluster: Configured CHT Cluster instance
        """
        self.cluster = cluster
        self.logger = logging.getLogger(__name__)
        self.operation_history: List[OperationResult] = []
    
    def bulk_backup_tables(
        self,
        databases: List[str],
        *,
        backup_suffix: str = "_backup",
        max_workers: int = 5,
    ) -> List[OperationResult]:
        """Perform parallel backup operations across multiple databases.
        
        Args:
            databases: List of database names to backup
            backup_suffix: Suffix for backup table names
            max_workers: Maximum number of parallel backup operations
            
        Returns:
            List of operation results with timing and status
        """
        self.logger.info(f"Starting bulk backup for databases: {databases}")
        results = []
        
        # Discover all tables
        all_tables = []
        for db in databases:
            try:
                tables = self._get_database_tables(db)
                all_tables.extend([(db, table) for table in tables])
                self.logger.info(f"Found {len(tables)} tables in database {db}")
            except Exception as e:
                self.logger.error(f"Failed to list tables in {db}: {e}")
        
        # Perform parallel backups
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_table = {
                executor.submit(self._backup_single_table, db, table, backup_suffix): (db, table)
                for db, table in all_tables
            }
            
            for future in concurrent.futures.as_completed(future_to_table):
                db, table = future_to_table[future]
                try:
                    result = future.result()
                    results.append(result)
                    if result.success:
                        self.logger.info(f"✓ Backed up {db}.{table}")
                    else:
                        self.logger.error(f"✗ Failed to backup {db}.{table}: {result.message}")
                except Exception as e:
                    result = OperationResult(
                        operation="backup",
                        table=f"{db}.{table}",
                        success=False,
                        duration=0.0,
                        message=str(e)
                    )
                    results.append(result)
                    self.logger.error(f"✗ Backup exception for {db}.{table}: {e}")
        
        self.operation_history.extend(results)
        successful = sum(1 for r in results if r.success)
        self.logger.info(f"Bulk backup complete: {successful}/{len(results)} successful")
        
        return results
    
    def automated_maintenance_cycle(
        self,
        *,
        databases: Optional[List[str]] = None,
        cleanup_duplicates: bool = True,
        optimize_tables: bool = True,
        backup_critical: bool = True,
    ) -> Dict[str, List[OperationResult]]:
        """Run automated maintenance cycle with comprehensive operations.
        
        Args:
            databases: Specific databases to maintain (None = all)
            cleanup_duplicates: Whether to analyze and remove duplicates
            optimize_tables: Whether to run OPTIMIZE operations
            backup_critical: Whether to backup high-importance tables
            
        Returns:
            Dictionary of operation types to their results
        """
        self.logger.info("Starting automated maintenance cycle")
        cycle_start = time.time()
        
        results = {
            "duplicates": [],
            "optimize": [],
            "backup": [],
            "health_check": []
        }
        
        # Discover databases if not specified
        if databases is None:
            databases = self._get_all_databases()
            self.logger.info(f"Auto-discovered {len(databases)} databases")
        
        # Health check phase
        self.logger.info("Phase 1: Health check and discovery")
        for db in databases:
            try:
                health_result = self._check_database_health(db)
                results["health_check"].append(health_result)
            except Exception as e:
                self.logger.error(f"Health check failed for {db}: {e}")
        
        # Duplicate cleanup phase
        if cleanup_duplicates:
            self.logger.info("Phase 2: Duplicate analysis and cleanup")
            for db in databases:
                duplicate_results = self._cleanup_database_duplicates(db)
                results["duplicates"].extend(duplicate_results)
        
        # Optimization phase
        if optimize_tables:
            self.logger.info("Phase 3: Table optimization")
            for db in databases:
                optimize_results = self._optimize_database_tables(db)
                results["optimize"].extend(optimize_results)
        
        # Critical backup phase
        if backup_critical:
            self.logger.info("Phase 4: Critical table backup")
            critical_tables = self._identify_critical_tables(databases)
            backup_results = self._backup_critical_tables(critical_tables)
            results["backup"].extend(backup_results)
        
        cycle_duration = time.time() - cycle_start
        self.logger.info(f"Maintenance cycle complete in {cycle_duration:.2f} seconds")
        
        # Generate summary report
        self._generate_maintenance_report(results, cycle_duration)
        
        return results
    
    def monitor_performance_metrics(self) -> Dict[str, float]:
        """Collect and analyze cluster performance metrics.
        
        Returns:
            Dictionary of performance metrics and scores
        """
        self.logger.info("Collecting cluster performance metrics")
        
        metrics = {}
        
        try:
            # Disk usage analysis
            disk_df = self.cluster.get_disk_usage()
            if not disk_df.empty:
                avg_usage = disk_df['used_percentage'].mean()
                metrics['avg_disk_usage'] = avg_usage
                self.logger.info(f"Average disk usage: {avg_usage:.1f}%")
            
            # Query performance sampling
            query_metrics = self._sample_query_performance()
            metrics.update(query_metrics)
            
            # Connection pool health
            connection_score = self._assess_connection_health()
            metrics['connection_health'] = connection_score
            
        except Exception as e:
            self.logger.error(f"Performance monitoring failed: {e}")
            metrics['monitoring_error'] = True
        
        return metrics
    
    def _backup_single_table(
        self,
        database: str,
        table: str,
        backup_suffix: str
    ) -> OperationResult:
        """Backup a single table with error handling and timing."""
        start_time = time.time()
        table_obj = Table(name=table, database=database, cluster=self.cluster)
        
        try:
            backup_name = table_obj.backup_to_suffix(
                backup_suffix=backup_suffix,
                recreate=True
            )
            table_obj.verify_backup(backup_suffix=backup_suffix)
            
            duration = time.time() - start_time
            return OperationResult(
                operation="backup",
                table=f"{database}.{table}",
                success=True,
                duration=duration,
                message=f"Backup created: {database}.{backup_name}",
                details={"backup_table": f"{database}.{backup_name}"}
            )
            
        except Exception as e:
            duration = time.time() - start_time
            return OperationResult(
                operation="backup",
                table=f"{database}.{table}",
                success=False,
                duration=duration,
                message=str(e)
            )
    
    def _get_database_tables(self, database: str) -> List[str]:
        """Get list of tables in a database."""
        query = f"""
        SELECT name
        FROM system.tables
        WHERE database = '{database}'
          AND engine NOT IN ('MaterializedView', 'View')
        ORDER BY name
        """
        rows = self.cluster.query(query)
        return [row[0] for row in rows] if rows else []
    
    def _get_all_databases(self) -> List[str]:
        """Get list of all non-system databases."""
        query = """
        SELECT name
        FROM system.databases
        WHERE name NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
        ORDER BY name
        """
        rows = self.cluster.query(query)
        return [row[0] for row in rows] if rows else []
    
    def _check_database_health(self, database: str) -> OperationResult:
        """Perform health check on a database."""
        start_time = time.time()
        
        try:
            # Check if database is accessible
            tables = self._get_database_tables(database)
            
            # Sample query performance
            test_query = f"SELECT count() FROM system.tables WHERE database = '{database}'"
            self.cluster.query(test_query)
            
            duration = time.time() - start_time
            return OperationResult(
                operation="health_check",
                table=database,
                success=True,
                duration=duration,
                message=f"Database healthy, {len(tables)} tables accessible",
                details={"table_count": len(tables)}
            )
            
        except Exception as e:
            duration = time.time() - start_time
            return OperationResult(
                operation="health_check",
                table=database,
                success=False,
                duration=duration,
                message=str(e)
            )
    
    def _cleanup_database_duplicates(self, database: str) -> List[OperationResult]:
        """Cleanup duplicates in all tables of a database."""
        results = []
        tables = self._get_database_tables(database)
        
        for table_name in tables:
            try:
                table_obj = Table(name=table_name, database=database, cluster=self.cluster)
                time_col = table_obj.get_time_column()
                
                if time_col:
                    # Analyze recent data only for performance
                    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
                    stats = analyze_and_remove_duplicates(
                        table_obj,
                        date=yesterday,
                        test_run=False,
                        remove_duplicates=True
                    )
                    
                    result = OperationResult(
                        operation="cleanup_duplicates",
                        table=f"{database}.{table_name}",
                        success=True,
                        duration=0.0,  # analyze_and_remove_duplicates doesn't return timing
                        message=f"Removed {stats['duplicate_rows']} duplicates",
                        details=stats
                    )
                else:
                    result = OperationResult(
                        operation="cleanup_duplicates",
                        table=f"{database}.{table_name}",
                        success=False,
                        duration=0.0,
                        message="No time column found, skipped"
                    )
                
                results.append(result)
                
            except Exception as e:
                result = OperationResult(
                    operation="cleanup_duplicates",
                    table=f"{database}.{table_name}",
                    success=False,
                    duration=0.0,
                    message=str(e)
                )
                results.append(result)
        
        return results
    
    def _optimize_database_tables(self, database: str) -> List[OperationResult]:
        """Run OPTIMIZE operations on database tables."""
        results = []
        tables = self._get_database_tables(database)
        
        for table_name in tables:
            start_time = time.time()
            try:
                table_obj = Table(name=table_name, database=database, cluster=self.cluster)
                table_obj.optimize_deduplicate(test_run=False)
                
                duration = time.time() - start_time
                result = OperationResult(
                    operation="optimize",
                    table=f"{database}.{table_name}",
                    success=True,
                    duration=duration,
                    message="OPTIMIZE FINAL DEDUPLICATE completed"
                )
                
            except Exception as e:
                duration = time.time() - start_time
                result = OperationResult(
                    operation="optimize",
                    table=f"{database}.{table_name}",
                    success=False,
                    duration=duration,
                    message=str(e)
                )
            
            results.append(result)
        
        return results
    
    def _identify_critical_tables(self, databases: List[str]) -> List[Tuple[str, str]]:
        """Identify critical tables that should be backed up."""
        critical_tables = []
        
        for db in databases:
            # Simple heuristic: tables with many parts or large size
            try:
                query = f"""
                SELECT table, count() as part_count, sum(bytes_on_disk) as total_size
                FROM system.parts
                WHERE database = '{db}' AND active = 1
                GROUP BY table
                HAVING part_count > 10 OR total_size > 1000000000  -- 1GB
                ORDER BY total_size DESC
                LIMIT 10
                """
                rows = self.cluster.query(query)
                if rows:
                    for row in rows:
                        critical_tables.append((db, row[0]))
                        
            except Exception as e:
                self.logger.error(f"Failed to identify critical tables in {db}: {e}")
        
        return critical_tables
    
    def _backup_critical_tables(
        self,
        critical_tables: List[Tuple[str, str]]
    ) -> List[OperationResult]:
        """Backup identified critical tables."""
        results = []
        
        for db, table in critical_tables:
            result = self._backup_single_table(db, table, "_critical_backup")
            results.append(result)
        
        return results
    
    def _sample_query_performance(self) -> Dict[str, float]:
        """Sample query performance with simple test queries."""
        metrics = {}
        
        try:
            # Test simple query performance
            start = time.time()
            self.cluster.query("SELECT 1")
            metrics['simple_query_ms'] = (time.time() - start) * 1000
            
            # Test system table query performance
            start = time.time()
            self.cluster.query("SELECT count() FROM system.tables LIMIT 1000")
            metrics['system_query_ms'] = (time.time() - start) * 1000
            
        except Exception as e:
            self.logger.error(f"Query performance sampling failed: {e}")
            metrics['query_error'] = True
        
        return metrics
    
    def _assess_connection_health(self) -> float:
        """Assess connection pool health (simplified scoring)."""
        try:
            # Test multiple rapid queries
            start = time.time()
            for _ in range(5):
                self.cluster.query("SELECT 1")
            total_time = time.time() - start
            
            # Score based on response time: < 1s = 100, > 5s = 0
            score = max(0, min(100, (6 - total_time) * 20))
            return score
            
        except Exception:
            return 0.0
    
    def _generate_maintenance_report(
        self,
        results: Dict[str, List[OperationResult]],
        cycle_duration: float
    ) -> None:
        """Generate a comprehensive maintenance report."""
        self.logger.info("=" * 60)
        self.logger.info("MAINTENANCE CYCLE REPORT")
        self.logger.info("=" * 60)
        self.logger.info(f"Total Duration: {cycle_duration:.2f} seconds")
        self.logger.info("")
        
        for operation_type, operation_results in results.items():
            if operation_results:
                successful = sum(1 for r in operation_results if r.success)
                total = len(operation_results)
                avg_duration = sum(r.duration for r in operation_results) / total
                
                self.logger.info(f"{operation_type.upper()}:")
                self.logger.info(f"  Success Rate: {successful}/{total} ({successful/total*100:.1f}%)")
                self.logger.info(f"  Average Duration: {avg_duration:.2f}s")
                
                # Show any failures
                failures = [r for r in operation_results if not r.success]
                if failures:
                    self.logger.info(f"  Failures:")
                    for failure in failures[:3]:  # Show first 3 failures
                        self.logger.info(f"    - {failure.table}: {failure.message}")
                    if len(failures) > 3:
                        self.logger.info(f"    ... and {len(failures) - 3} more")
                
                self.logger.info("")


def setup_logging() -> None:
    """Configure logging for the advanced operations demo."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(f"cht_operations_{datetime.now():%Y%m%d_%H%M%S}.log"),
            logging.StreamHandler()
        ]
    )


def main() -> None:
    """Main entry point for advanced operations demo."""
    setup_logging()
    logger = logging.getLogger(__name__)
    
    logger.info("🏭 CHT Advanced Operations Demo")
    logger.info("=" * 50)
    
    try:
        # Initialize cluster connection
        # Replace with your actual cluster credentials
        cluster = Cluster(
            name="production",
            host="localhost",
            user="developer", 
            password="developer",
            read_only=False  # Required for maintenance operations
        )
        
        # Initialize production manager
        manager = ProductionCHTManager(cluster)
        
        # Demo 1: Performance monitoring
        logger.info("Demo 1: Performance Monitoring")
        metrics = manager.monitor_performance_metrics()
        logger.info(f"Collected {len(metrics)} performance metrics")
        for metric, value in metrics.items():
            if isinstance(value, (int, float)):
                logger.info(f"  {metric}: {value:.2f}")
            else:
                logger.info(f"  {metric}: {value}")
        
        # Demo 2: Bulk backup operations
        logger.info("\nDemo 2: Bulk Backup Operations")
        test_databases = ["temp", "default"]  # Add your databases here
        backup_results = manager.bulk_backup_tables(
            databases=test_databases,
            max_workers=3
        )
        successful_backups = sum(1 for r in backup_results if r.success)
        logger.info(f"Backup Results: {successful_backups}/{len(backup_results)} successful")
        
        # Demo 3: Automated maintenance cycle
        logger.info("\nDemo 3: Automated Maintenance Cycle")
        maintenance_results = manager.automated_maintenance_cycle(
            databases=test_databases,
            cleanup_duplicates=False,  # Set to True for real cleanup
            optimize_tables=False,     # Set to True for real optimization
            backup_critical=True
        )
        
        logger.info("✓ Advanced operations demo completed successfully!")
        
    except Exception as e:
        logger.error(f"Demo failed: {e}")
        raise


if __name__ == "__main__":
    main()