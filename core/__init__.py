"""
CORE Insurance Analytics Platform

A comprehensive platform for insurance rate analysis, SERFF data processing,
and competitive intelligence reporting.
"""

__version__ = "2.0.0"
__author__ = "Insurance Analytics Team"

# Core modules
from .config import settings
from .models import *
# NOTE: Removed 'from .utils import logging' to prevent duplicate logging setup
# Import logging utilities directly when needed instead of through core.__init__

# Main components
from .data import DataManager
from .analytics import AnalyticsEngine
from .reporting import ReportManager
from .notifications import NotificationService
from .workflows import WorkflowEngine

__all__ = [
    "settings",
    "DataManager",
    "AnalyticsEngine", 
    "ReportManager",
    "NotificationService",
    "WorkflowEngine",
    # Removed "logging" - import core.utils.logging directly when needed
]
