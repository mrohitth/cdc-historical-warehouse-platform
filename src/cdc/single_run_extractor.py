#!/usr/bin/env python3
"""
Single Run CDC Extractor

This script runs the CDC extraction once and exits, rather than running in a loop.
Used for testing and validation scenarios.
"""

import sys
import os
from pathlib import Path

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from cdc.log_extractor import CDCLogExtractor
import logging

logger = logging.getLogger(__name__)

def main():
    """Run CDC extraction once and exit."""
    logger.info("Starting single CDC extraction run")
    
    try:
        extractor = CDCLogExtractor()
        
        # Get current watermark
        watermark = extractor._get_watermark()
        logger.info(f"Current watermark: {watermark}")
        
        # Detect changes
        changes = extractor._detect_changes(watermark)
        
        if changes:
            # Write change logs
            extractor._write_change_logs(changes)
            
            # Update watermark to the latest change timestamp
            latest_timestamp = max(
                change['last_updated'] for change in changes
            )
            extractor._save_watermark(latest_timestamp)
            
            logger.info(f"Processed {len(changes)} changes, updated watermark to: {latest_timestamp}")
        else:
            logger.info("No changes detected")
        
        logger.info("Single CDC extraction completed successfully")
        
    except Exception as e:
        logger.error(f"Single CDC extraction failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
