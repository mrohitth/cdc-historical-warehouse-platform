"""
Pipeline metadata management for the CDC platform.

Tracks pipeline execution history, performance metrics, and status
in the warehouse database for monitoring and debugging.
"""

import os
import sys
import json
from datetime import datetime, timezone
from typing import Optional, Dict, Any
from pathlib import Path

import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent.parent))
from src.utils.logging_config import setup_logging

load_dotenv()

logger = setup_logging(__name__, log_level=os.getenv('LOG_LEVEL', 'INFO'))

class PipelineMetadataManager:
    """
    Manages pipeline metadata in the warehouse database.
    
    Tracks:
    - Last run time
    - Records processed
    - Pipeline status
    - Performance metrics
    - Error details
    """
    
    def __init__(self):
        """Initialize database connection and create metadata table."""
        self.connection = None
        self._connect()
        self._create_metadata_table()
    
    def _connect(self) -> None:
        """Connect to warehouse database."""
        try:
            self.connection = psycopg2.connect(
                host=os.getenv('WAREHOUSE_DB_HOST', 'localhost'),
                port=os.getenv('WAREHOUSE_DB_PORT', '5433'),
                database=os.getenv('WAREHOUSE_DB_NAME', 'warehouse_db'),
                user=os.getenv('WAREHOUSE_DB_USER', 'postgres'),
                password=os.getenv('WAREHOUSE_DB_PASSWORD', 'postgres')
            )
            self.connection.autocommit = False
            logger.info("Successfully connected to warehouse_db for metadata management")
        except psycopg2.OperationalError as e:
            logger.error(f"Failed to connect to warehouse for metadata: {e}")
            raise
    
    def _create_metadata_table(self) -> None:
        """Create the pipeline_metadata table if it doesn't exist."""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql.SQL("""
                    CREATE TABLE IF NOT EXISTS pipeline_metadata (
                        id BIGSERIAL PRIMARY KEY,
                        pipeline_name VARCHAR(100) NOT NULL,
                        run_id VARCHAR(100) NOT NULL,
                        start_time TIMESTAMP NOT NULL,
                        end_time TIMESTAMP,
                        status VARCHAR(20) NOT NULL DEFAULT 'running',
                        records_processed INTEGER DEFAULT 0,
                        records_successful INTEGER DEFAULT 0,
                        records_failed INTEGER DEFAULT 0,
                        error_message TEXT,
                        performance_metrics JSONB,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        
                        -- Indexes for performance
                        CONSTRAINT pipeline_metadata_status_check 
                            CHECK (status IN ('running', 'completed', 'failed', 'cancelled'))
                    );
                    
                    -- Create indexes
                    CREATE INDEX IF NOT EXISTS idx_pipeline_metadata_name 
                        ON pipeline_metadata(pipeline_name);
                    CREATE INDEX IF NOT EXISTS idx_pipeline_metadata_status 
                        ON pipeline_metadata(status);
                    CREATE INDEX IF NOT EXISTS idx_pipeline_metadata_start_time 
                        ON pipeline_metadata(start_time);
                    CREATE INDEX IF NOT EXISTS idx_pipeline_metadata_run_id 
                        ON pipeline_metadata(run_id);
                    
                    -- Add comments
                    COMMENT ON TABLE pipeline_metadata IS 'Pipeline execution metadata and metrics';
                    COMMENT ON COLUMN pipeline_metadata.pipeline_name IS 'Name of the pipeline (e.g., scd2_loader, cdc_extractor)';
                    COMMENT ON COLUMN pipeline_metadata.run_id IS 'Unique identifier for this pipeline run';
                    COMMENT ON COLUMN pipeline_metadata.status IS 'Current status: running, completed, failed, cancelled';
                    COMMENT ON COLUMN pipeline_metadata.performance_metrics IS 'JSON with timing, memory, and other metrics';
                """))
                
                self.connection.commit()
                logger.info("Created/verified pipeline_metadata table")
                
        except psycopg2.Error as e:
            self.connection.rollback()
            logger.error(f"Failed to create pipeline_metadata table: {e}")
            raise
    
    def start_pipeline_run(
        self, 
        pipeline_name: str, 
        run_id: str = None,
        performance_metrics: Dict[str, Any] = None
    ) -> int:
        """
        Start a new pipeline run and record metadata.
        
        Args:
            pipeline_name: Name of the pipeline
            run_id: Optional run ID (generated if not provided)
            performance_metrics: Optional initial metrics
            
        Returns:
            Pipeline run ID (database primary key)
        """
        if run_id is None:
            run_id = f"{pipeline_name}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S_%f')[:-3]}"
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql.SQL("""
                    INSERT INTO pipeline_metadata (
                        pipeline_name, run_id, start_time, status, performance_metrics
                    ) VALUES (%s, %s, %s, %s, %s)
                    RETURNING id
                """), (
                    pipeline_name,
                    run_id,
                    datetime.now(timezone.utc),
                    'running',
                    json.dumps(performance_metrics or {}) if performance_metrics else None
                ))
                
                pipeline_id = cursor.fetchone()[0]
                self.connection.commit()
                
                logger.info(f"Started pipeline run: {pipeline_name} (ID: {pipeline_id}, Run ID: {run_id})")
                return pipeline_id
                
        except psycopg2.Error as e:
            self.connection.rollback()
            logger.error(f"Failed to start pipeline run: {e}")
            raise
    
    def update_pipeline_run(
        self,
        pipeline_id: int,
        status: str = None,
        records_processed: int = None,
        records_successful: int = None,
        records_failed: int = None,
        error_message: str = None,
        performance_metrics: Dict[str, Any] = None
    ) -> bool:
        """
        Update an existing pipeline run.
        
        Args:
            pipeline_id: Pipeline run ID
            status: New status
            records_processed: Total records processed
            records_successful: Successfully processed records
            records_failed: Failed records
            error_message: Error message if failed
            performance_metrics: Additional performance metrics
            
        Returns:
            True if successful, False otherwise
        """
        try:
            with self.connection.cursor() as cursor:
                # Build dynamic update query
                updates = []
                params = []
                param_count = 1
                
                if status is not None:
                    updates.append(f"status = ${param_count}")
                    params.append(status)
                    param_count += 1
                
                if records_processed is not None:
                    updates.append(f"records_processed = ${param_count}")
                    params.append(records_processed)
                    param_count += 1
                
                if records_successful is not None:
                    updates.append(f"records_successful = ${param_count}")
                    params.append(records_successful)
                    param_count += 1
                
                if records_failed is not None:
                    updates.append(f"records_failed = ${param_count}")
                    params.append(records_failed)
                    param_count += 1
                
                if error_message is not None:
                    updates.append(f"error_message = ${param_count}")
                    params.append(error_message)
                    param_count += 1
                
                if performance_metrics is not None:
                    updates.append(f"performance_metrics = ${param_count}")
                    params.append(performance_metrics)
                    param_count += 1
                
                # Always update the updated_at timestamp
                updates.append(f"updated_at = ${param_count}")
                params.append(datetime.now(timezone.utc))
                param_count += 1
                
                # Set end_time if status is completed/failed/cancelled
                if status in ['completed', 'failed', 'cancelled']:
                    updates.append(f"end_time = ${param_count}")
                    params.append(datetime.now(timezone.utc))
                    param_count += 1
                
                # Add pipeline_id to params
                params.append(pipeline_id)
                
                if updates:
                    query = f"""
                        UPDATE pipeline_metadata 
                        SET {', '.join(updates)}
                        WHERE id = ${param_count}
                    """
                    
                    cursor.execute(query, params)
                    self.connection.commit()
                    
                    logger.debug(f"Updated pipeline run {pipeline_id}: {', '.join(updates)}")
                    return True
                else:
                    logger.warning(f"No updates provided for pipeline run {pipeline_id}")
                    return False
                    
        except psycopg2.Error as e:
            self.connection.rollback()
            logger.error(f"Failed to update pipeline run {pipeline_id}: {e}")
            return False
    
    def get_last_run_info(self, pipeline_name: str) -> Optional[Dict[str, Any]]:
        """
        Get information about the last run of a pipeline.
        
        Args:
            pipeline_name: Name of the pipeline
            
        Returns:
            Dictionary with last run info or None if no runs found
        """
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql.SQL("""
                    SELECT id, run_id, start_time, end_time, status,
                           records_processed, records_successful, records_failed,
                           error_message, performance_metrics, created_at, updated_at
                    FROM pipeline_metadata 
                    WHERE pipeline_name = %s
                    ORDER BY start_time DESC
                    LIMIT 1
                """), (pipeline_name,))
                
                result = cursor.fetchone()
                if result:
                    columns = [desc[0] for desc in cursor.description]
                    return dict(zip(columns, result))
                else:
                    return None
                    
        except psycopg2.Error as e:
            logger.error(f"Failed to get last run info for {pipeline_name}: {e}")
            return None
    
    def get_pipeline_stats(self, pipeline_name: str, days: int = 7) -> Dict[str, Any]:
        """
        Get statistics for a pipeline over the last N days.
        
        Args:
            pipeline_name: Name of the pipeline
            days: Number of days to look back
            
        Returns:
            Dictionary with pipeline statistics
        """
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql.SQL("""
                    SELECT 
                        COUNT(*) as total_runs,
                        COUNT(CASE WHEN status = 'completed' THEN 1 END) as successful_runs,
                        COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed_runs,
                        AVG(CASE WHEN end_time IS NOT NULL THEN 
                            EXTRACT(EPOCH FROM (end_time - start_time)) END) as avg_duration_seconds,
                        SUM(records_processed) as total_records_processed,
                        SUM(records_successful) as total_records_successful,
                        SUM(records_failed) as total_records_failed,
                        MAX(start_time) as last_run_time
                    FROM pipeline_metadata 
                    WHERE pipeline_name = %s 
                        AND start_time >= CURRENT_TIMESTAMP - INTERVAL '%s days'
                """), (pipeline_name, days))
                
                result = cursor.fetchone()
                if result:
                    columns = [desc[0] for desc in cursor.description]
                    return dict(zip(columns, result))
                else:
                    return {}
                    
        except psycopg2.Error as e:
            logger.error(f"Failed to get pipeline stats for {pipeline_name}: {e}")
            return {}
    
    def close(self) -> None:
        """Close database connection."""
        if self.connection:
            self.connection.close()
            logger.info("Pipeline metadata database connection closed")
