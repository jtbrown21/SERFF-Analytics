"""
Consolidated data models for the CORE platform.

This module defines all the data structures used throughout the application,
providing a single source of truth for data schema and validation.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Union
from datetime import datetime, date
from enum import Enum
import json

class FilingStatus(Enum):
    """Status of a rate filing."""
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    WITHDRAWN = "withdrawn"
    ACTIVE = "active"

class InsuranceType(Enum):
    """Type of insurance coverage."""
    AUTO = "auto"
    HOME = "home"
    LIFE = "life"
    HEALTH = "health"
    COMMERCIAL = "commercial"
    OTHER = "other"

class ReportType(Enum):
    """Type of report generated."""
    AGENT_INTEL = "agent_intel"
    AGENT_ACTION = "agent_action"
    NEWSLETTER = "newsletter"
    COMPETITIVE_DASHBOARD = "competitive_dashboard"

@dataclass
class RateFiling:
    """
    Standardized rate filing data structure.
    
    This consolidates data from SERFF, Airtable, and other sources
    into a single, consistent format.
    """
    # Core identifiers
    filing_id: str
    serff_tracking_number: Optional[str] = None
    airtable_record_id: Optional[str] = None
    
    # Company and location
    company_name: str = ""
    state: str = ""
    
    # Filing details
    filing_type: InsuranceType = InsuranceType.OTHER
    status: FilingStatus = FilingStatus.PENDING
    
    # Dates
    filing_date: Optional[date] = None
    effective_date: Optional[date] = None
    last_updated: Optional[datetime] = None
    
    # Rate information
    rate_change_percent: Optional[float] = None
    rate_change_description: str = ""
    
    # Administrative
    notes: str = ""
    tags: List[str] = field(default_factory=list)
    
    # Raw data for debugging/reference
    raw_data: Dict[str, Any] = field(default_factory=dict)
    source: str = ""  # "serff", "airtable", "manual", etc.
    
    def __post_init__(self):
        """Validate and normalize data after initialization."""
        # Ensure filing_id is set
        if not self.filing_id:
            raise ValueError("filing_id is required")
        
        # Normalize company name
        self.company_name = self.company_name.strip()
        
        # Normalize state to uppercase
        if self.state:
            self.state = self.state.upper()
        
        # Set last_updated if not provided
        if not self.last_updated:
            self.last_updated = datetime.now()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "filing_id": self.filing_id,
            "serff_tracking_number": self.serff_tracking_number,
            "airtable_record_id": self.airtable_record_id,
            "company_name": self.company_name,
            "state": self.state,
            "filing_type": self.filing_type.value,
            "status": self.status.value,
            "filing_date": self.filing_date.isoformat() if self.filing_date else None,
            "effective_date": self.effective_date.isoformat() if self.effective_date else None,
            "last_updated": self.last_updated.isoformat() if self.last_updated else None,
            "rate_change_percent": self.rate_change_percent,
            "rate_change_description": self.rate_change_description,
            "notes": self.notes,
            "tags": self.tags,
            "raw_data": self.raw_data,
            "source": self.source,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RateFiling":
        """Create instance from dictionary."""
        # Handle enum conversions
        filing_type = InsuranceType(data.get("filing_type", "other"))
        status = FilingStatus(data.get("status", "pending"))
        
        # Handle date conversions
        filing_date = None
        if data.get("filing_date"):
            filing_date = datetime.fromisoformat(data["filing_date"]).date()
        
        effective_date = None
        if data.get("effective_date"):
            effective_date = datetime.fromisoformat(data["effective_date"]).date()
        
        last_updated = None
        if data.get("last_updated"):
            last_updated = datetime.fromisoformat(data["last_updated"])
        
        return cls(
            filing_id=data["filing_id"],
            serff_tracking_number=data.get("serff_tracking_number"),
            airtable_record_id=data.get("airtable_record_id"),
            company_name=data.get("company_name", ""),
            state=data.get("state", ""),
            filing_type=filing_type,
            status=status,
            filing_date=filing_date,
            effective_date=effective_date,
            last_updated=last_updated,
            rate_change_percent=data.get("rate_change_percent"),
            rate_change_description=data.get("rate_change_description", ""),
            notes=data.get("notes", ""),
            tags=data.get("tags", []),
            raw_data=data.get("raw_data", {}),
            source=data.get("source", ""),
        )

@dataclass
class AgentProfile:
    """Agent profile information for personalized reports."""
    agent_id: str
    name: str
    email: str
    state: str
    active_companies: List[str] = field(default_factory=list)
    interests: List[str] = field(default_factory=list)
    preferences: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Validate agent profile data."""
        if not self.agent_id:
            raise ValueError("agent_id is required")
        if not self.email:
            raise ValueError("email is required")
        
        # Normalize state
        self.state = self.state.upper()

@dataclass
class ReportData:
    """Data structure for generated reports."""
    report_id: str
    report_type: ReportType
    state: str
    generated_at: datetime
    
    # Report content
    title: str = ""
    summary: str = ""
    filings: List[RateFiling] = field(default_factory=list)
    
    # Metadata
    filters_applied: Dict[str, Any] = field(default_factory=dict)
    total_filings: int = 0
    
    def __post_init__(self):
        """Calculate derived fields."""
        if not self.total_filings:
            self.total_filings = len(self.filings)

@dataclass
class NotificationMessage:
    """Structure for notifications (email, etc.)."""
    message_id: str
    recipient: str
    subject: str
    content: str
    
    # Metadata
    created_at: datetime = field(default_factory=datetime.now)
    sent_at: Optional[datetime] = None
    delivery_status: str = "pending"
    
    # Associated data
    report_data: Optional[ReportData] = None
    agent_profile: Optional[AgentProfile] = None

@dataclass
class SyncStatus:
    """Status tracking for data synchronization operations."""
    operation_id: str
    source: str  # "airtable", "serff", etc.
    destination: str  # "duckdb", "local", etc.
    
    # Status
    status: str = "pending"  # pending, running, completed, failed
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    
    # Results
    records_processed: int = 0
    records_updated: int = 0
    records_added: int = 0
    errors: List[str] = field(default_factory=list)
    
    def mark_started(self):
        """Mark the operation as started."""
        self.status = "running"
        self.started_at = datetime.now()
    
    def mark_completed(self):
        """Mark the operation as completed."""
        self.status = "completed"
        self.completed_at = datetime.now()
    
    def mark_failed(self, error: str):
        """Mark the operation as failed with an error."""
        self.status = "failed"
        self.completed_at = datetime.now()
        self.errors.append(error)

# Export all models
__all__ = [
    "FilingStatus",
    "InsuranceType", 
    "ReportType",
    "RateFiling",
    "AgentProfile",
    "ReportData",
    "NotificationMessage",
    "SyncStatus",
]
