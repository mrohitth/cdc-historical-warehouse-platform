"""
Graceful shutdown handler for CDC platform components.

Provides signal handling for SIGTERM and SIGINT to ensure:
- Clean database connection closure
- Proper resource cleanup
- Completion of in-progress operations
- Consistent state across shutdowns
"""

import signal
import sys
import logging
from typing import Callable, Optional

class GracefulShutdownHandler:
    """
    Handles graceful shutdown signals for long-running processes.
    
    Usage:
        shutdown_handler = GracefulShutdownHandler()
        
        def cleanup():
            # Clean up resources
            pass
            
        shutdown_handler.register_cleanup(cleanup)
        shutdown_handler.start_listening()
        
        # Main loop
        while not shutdown_handler.should_shutdown:
            # Do work
            pass
            
        shutdown_handler.cleanup()
    """
    
    def __init__(self, logger_name: str = None):
        """Initialize the shutdown handler."""
        self.should_shutdown = False
        self.cleanup_functions = []
        self.logger = logger_name or __name__
        
        if isinstance(self.logger, str):
            import logging
            self.logger = logging.getLogger(self.logger)
    
    def register_cleanup(self, cleanup_func: Callable[[], None]) -> None:
        """
        Register a cleanup function to be called on shutdown.
        
        Args:
            cleanup_func: Function to call during cleanup
        """
        self.cleanup_functions.append(cleanup_func)
        self.logger.debug(f"Registered cleanup function: {cleanup_func.__name__}")
    
    def _signal_handler(self, signum: int, frame) -> None:
        """
        Internal signal handler.
        
        Args:
            signum: Signal number
            frame: Current stack frame
        """
        signal_name = signal.Signals(signum).name
        self.logger.info(f"Received {signal_name} signal, initiating graceful shutdown...")
        self.should_shutdown = True
    
    def start_listening(self) -> None:
        """Start listening for shutdown signals."""
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
        self.logger.info("Graceful shutdown handler activated (SIGTERM, SIGINT)")
    
    def cleanup(self) -> None:
        """Execute all registered cleanup functions."""
        self.logger.info("Starting graceful shutdown cleanup...")
        
        for i, cleanup_func in enumerate(self.cleanup_functions, 1):
            try:
                self.logger.debug(f"Executing cleanup function {i}/{len(self.cleanup_functions)}: {cleanup_func.__name__}")
                cleanup_func()
                self.logger.debug(f"Successfully executed cleanup function: {cleanup_func.__name__}")
            except Exception as e:
                self.logger.error(f"Error in cleanup function {cleanup_func.__name__}: {e}")
        
        self.logger.info("Graceful shutdown cleanup completed")
    
    def wait_for_shutdown(self, check_interval: float = 1.0) -> None:
        """
        Wait for shutdown signal in a blocking manner.
        
        Args:
            check_interval: How often to check for shutdown (seconds)
        """
        import time
        
        self.logger.info("Waiting for shutdown signal...")
        while not self.should_shutdown:
            time.sleep(check_interval)
        
        self.cleanup()

class DatabaseConnectionManager:
    """
    Manages database connections with graceful shutdown support.
    
    Usage:
        conn_manager = DatabaseConnectionManager(shutdown_handler)
        conn = conn_manager.get_connection()
        # Use connection...
        # Connection will be automatically closed on shutdown
    """
    
    def __init__(self, shutdown_handler: GracefulShutdownHandler, logger_name: str = None):
        """Initialize the connection manager."""
        self.shutdown_handler = shutdown_handler
        self.connections = []
        self.logger = logger_name or __name__
        
        if isinstance(self.logger, str):
            import logging
            self.logger = logging.getLogger(self.logger)
        
        # Register cleanup function
        self.shutdown_handler.register_cleanup(self.close_all_connections)
    
    def add_connection(self, connection) -> None:
        """
        Add a database connection to be managed.
        
        Args:
            connection: Database connection object
        """
        self.connections.append(connection)
        self.logger.debug(f"Added database connection to manager (total: {len(self.connections)})")
    
    def close_all_connections(self) -> None:
        """Close all managed database connections."""
        self.logger.info(f"Closing {len(self.connections)} database connections...")
        
        for i, conn in enumerate(self.connections, 1):
            try:
                if hasattr(conn, 'close'):
                    conn.close()
                    self.logger.debug(f"Closed connection {i}/{len(self.connections)}")
                else:
                    self.logger.warning(f"Connection {i} does not have close() method")
            except Exception as e:
                self.logger.error(f"Error closing connection {i}: {e}")
        
        self.connections.clear()
        self.logger.info("All database connections closed")
