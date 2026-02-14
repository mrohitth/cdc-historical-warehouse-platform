"""
Centralized logging configuration for the CDC platform.

Provides consistent structured logging across all components with:
- Standardized format: %(asctime)s - %(name)s - %(levelname)s - %(message)s
- Multiple handlers (console + file)
- Log rotation and cleanup
- Component-specific loggers
"""

import logging
import logging.handlers
import os
from pathlib import Path
from datetime import datetime

def setup_logging(
    logger_name: str,
    log_level: str = "INFO",
    log_file: str = None,
    max_bytes: int = 10 * 1024 * 1024,  # 10MB
    backup_count: int = 5
) -> logging.Logger:
    """
    Set up structured logging for a component.
    
    Args:
        logger_name: Name of the logger (usually __name__)
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional log file name. If None, uses logger_name.log
        max_bytes: Maximum bytes per log file before rotation
        backup_count: Number of backup log files to keep
        
    Returns:
        Configured logger instance
    """
    # Create logger
    logger = logging.getLogger(logger_name)
    
    # Clear existing handlers to avoid duplicates
    logger.handlers.clear()
    
    # Set log level
    level = getattr(logging, log_level.upper(), logging.INFO)
    logger.setLevel(level)
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler with rotation
    if log_file is None:
        # Sanitize logger name for file
        safe_name = logger_name.replace('.', '_').replace('/', '_')
        log_file = f"logs/{safe_name}.log"
    
    # Ensure logs directory exists
    log_path = Path(log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    
    file_handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding='utf-8'
    )
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # Prevent propagation to root logger to avoid duplicate logs
    logger.propagate = False
    
    return logger

def get_logger(logger_name: str) -> logging.Logger:
    """
    Get an existing logger or create a new one with default settings.
    
    Args:
        logger_name: Name of the logger
        
    Returns:
        Logger instance
    """
    logger = logging.getLogger(logger_name)
    
    # If logger has no handlers, set up with defaults
    if not logger.handlers:
        return setup_logging(logger_name)
    
    return logger
