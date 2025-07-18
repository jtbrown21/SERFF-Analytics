"""
Centralized logging configuration for the CORE platform.

This module provides consistent logging across all components,
with proper formatting, file rotation, and environment-aware levels.
"""

import os
import logging
import sys
from pathlib import Path
from logging.handlers import RotatingFileHandler
from datetime import datetime
from typing import Optional, Dict, Any

# Ensure logs directory exists
LOGS_DIR = Path("logs")
LOGS_DIR.mkdir(exist_ok=True)

class ColoredFormatter(logging.Formatter):
    """Custom formatter with colors for console output."""
    
    COLORS = {
        'DEBUG': '\033[36m',    # Cyan
        'INFO': '\033[32m',     # Green
        'WARNING': '\033[33m',  # Yellow
        'ERROR': '\033[31m',    # Red
        'CRITICAL': '\033[35m', # Magenta
        'RESET': '\033[0m'      # Reset
    }
    
    def format(self, record):
        """Format log record with colors."""
        if hasattr(record, 'levelname'):
            color = self.COLORS.get(record.levelname, self.COLORS['RESET'])
            record.levelname = f"{color}{record.levelname}{self.COLORS['RESET']}"
        
        return super().format(record)

def setup_logging(
    name: str = "core",
    level: str = "INFO",
    log_to_file: bool = True,
    log_to_console: bool = True,
    max_bytes: int = 10 * 1024 * 1024,  # 10MB
    backup_count: int = 5
) -> logging.Logger:
    """
    Set up comprehensive logging for the application.
    
    Args:
        name: Logger name
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_to_file: Whether to log to file
        log_to_console: Whether to log to console
        max_bytes: Maximum size of log file before rotation
        backup_count: Number of backup files to keep
    
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    
    # Check if handlers already exist to prevent duplicates
    if logger.handlers:
        return logger
    
    # Create formatters
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
    )
    
    console_formatter = ColoredFormatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%H:%M:%S'
    )
    
    # File handler with rotation
    if log_to_file:
        log_file = LOGS_DIR / f"{name}.log"
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=max_bytes,
            backupCount=backup_count
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
    
    # Console handler
    if log_to_console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)
    
    return logger

def get_logger(name: str, level: Optional[str] = None) -> logging.Logger:
    """
    Get a logger instance with consistent configuration.
    
    Args:
        name: Logger name (typically __name__)
        level: Override log level for this logger
    
    Returns:
        Logger instance
    """
    # Just return the logger without setting up handlers
    # Let the application handle logging setup
    logger = logging.getLogger(name)
    
    if level:
        logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    
    return logger

def log_function_call(func_name: str, args: tuple = (), kwargs: Optional[Dict[str, Any]] = None):
    """
    Log function calls for debugging purposes.
    
    Args:
        func_name: Name of the function being called
        args: Function arguments
        kwargs: Function keyword arguments
    """
    logger = get_logger("core.debug")
    kwargs = kwargs or {}
    
    # Create a clean representation of arguments
    arg_strs = []
    if args:
        arg_strs.extend([repr(arg) for arg in args])
    if kwargs:
        arg_strs.extend([f"{k}={repr(v)}" for k, v in kwargs.items()])
    
    arg_str = ", ".join(arg_strs)
    logger.debug(f"Calling {func_name}({arg_str})")

def log_performance(operation: str, duration: float, details: Optional[Dict[str, Any]] = None):
    """
    Log performance metrics for operations.
    
    Args:
        operation: Name of the operation
        duration: Duration in seconds
        details: Additional details about the operation
    """
    logger = get_logger("core.performance")
    details = details or {}
    
    message = f"Operation '{operation}' completed in {duration:.2f}s"
    if details:
        detail_str = ", ".join([f"{k}={v}" for k, v in details.items()])
        message += f" ({detail_str})"
    
    logger.info(message)

def log_data_operation(operation: str, table: str, count: int, details: Optional[Dict[str, Any]] = None):
    """
    Log data operations (sync, insert, update, etc.).
    
    Args:
        operation: Type of operation (sync, insert, update, delete)
        table: Table or data source name
        count: Number of records affected
        details: Additional operation details
    """
    logger = get_logger("core.data")
    details = details or {}
    
    message = f"Data operation: {operation} on {table} ({count} records)"
    if details:
        detail_str = ", ".join([f"{k}={v}" for k, v in details.items()])
        message += f" - {detail_str}"
    
    logger.info(message)

def log_error_with_context(error: Exception, context: Optional[Dict[str, Any]] = None):
    """
    Log errors with additional context information.
    
    Args:
        error: The exception that occurred
        context: Additional context about when/where the error occurred
    """
    logger = get_logger("core.errors")
    context = context or {}
    
    message = f"Error: {type(error).__name__}: {str(error)}"
    if context:
        context_str = ", ".join([f"{k}={v}" for k, v in context.items()])
        message += f" (Context: {context_str})"
    
    logger.error(message, exc_info=True)

# Create default logger instance (commented out to prevent automatic handler creation)
# logger = get_logger("core")

# Export public interface
__all__ = [
    "setup_logging",
    "get_logger", 
    "log_function_call",
    "log_performance",
    "log_data_operation",
    "log_error_with_context",
    # "logger",  # Commented out since we're not auto-creating it
]
