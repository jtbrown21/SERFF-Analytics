# CORE Architecture Guide

## Overview

This document describes the new architecture for the CORE insurance analytics platform, following an architecture-first approach to improve maintainability, scalability, and developer productivity.

## Architecture Principles

### 1. Clear Module Boundaries
- Each module has a single, well-defined responsibility
- Dependencies flow in one direction (no circular dependencies)
- Interfaces are explicit and minimal

### 2. Centralized Configuration
- All configuration is managed through the `core.config` module
- Environment-specific overrides are supported
- No scattered configuration files or hardcoded values

### 3. Consistent Data Models
- All data structures are defined in `core.models`
- Type safety is enforced throughout
- Validation happens at model boundaries

### 4. Unified Logging
- Structured logging with consistent formatting
- Performance and data operation tracking
- Error logging with context information

## Module Structure

```
core/
├── __init__.py              # Main package exports
├── config/                  # Configuration management
│   └── __init__.py         # Settings, environment handling
├── models/                  # Data models and schemas
│   └── __init__.py         # All data structures
├── data/                    # Data access layer
│   └── __init__.py         # DataManager, database operations
├── analytics/               # Analytics and insights
│   └── __init__.py         # AnalyticsEngine, trend analysis
├── reporting/               # Report generation
│   └── __init__.py         # ReportManager, templates
├── notifications/           # Communication layer
│   └── __init__.py         # NotificationService, email/webhooks
├── workflows/               # Business process orchestration
│   └── __init__.py         # WorkflowEngine, step management
└── utils/                   # Shared utilities
    └── __init__.py         # Logging, helpers
```

## Where Things Go

### Data Operations → `core.data`
- Database connections and queries
- Data synchronization (Airtable, SERFF)
- CRUD operations for all entities
- Data validation and transformation

### Business Logic → `core.analytics`
- Market trend calculations
- Competitive analysis
- Insight generation
- Statistical computations

### Report Generation → `core.reporting`
- Template rendering
- HTML/PDF generation
- Report data preparation
- Personalization logic

### Communications → `core.notifications`
- Email sending (Postmark integration)
- Webhook delivery
- Message queuing and retry logic
- Delivery tracking

### Process Orchestration → `core.workflows`
- Multi-step business processes
- Error handling and recovery
- Scheduling and coordination
- State management

### Configuration → `core.config`
- Environment variables
- Database connections
- API keys and secrets
- Feature flags

### Data Models → `core.models`
- Type definitions
- Validation rules
- Serialization/deserialization
- Business entities

## Import Rules

### Allowed Dependencies
```python
# Data layer can import from:
from core.models import RateFiling
from core.config import settings
from core.utils import logger

# Analytics can import from:
from core.data import DataManager
from core.models import RateFiling
from core.config import settings
from core.utils import logger

# Reporting can import from:
from core.data import DataManager
from core.analytics import AnalyticsEngine
from core.models import ReportData
from core.config import settings
from core.utils import logger

# Workflows can import from anywhere (orchestration layer)
from core.data import DataManager
from core.analytics import AnalyticsEngine
from core.reporting import ReportManager
from core.notifications import NotificationService
```

### Forbidden Dependencies
```python
# ❌ Never import from workflows into other modules
# from core.workflows import WorkflowEngine  # NO!

# ❌ Never create circular dependencies
# data → analytics → data  # NO!

# ❌ Never import implementation details
# from core.data.duckdb_impl import query  # NO!
```

## Migration Strategy

### Phase 1: Foundation (Completed)
- ✅ Create new module structure
- ✅ Implement core components (config, models, utils)
- ✅ Create stub implementations for all modules
- ✅ Establish import patterns

### Phase 2: Gradual Migration
- Move existing code into new modules
- Update imports to use new structure
- Maintain backward compatibility during transition
- Add proper error handling and logging

### Phase 3: Enhancement
- Implement full functionality in each module
- Add comprehensive testing
- Optimize performance
- Add monitoring and observability

## Usage Examples

### Basic Usage
```python
# Initialize the system
from core import settings, DataManager, ReportManager

# Configure for environment
settings.configure_for_environment("production")

# Use the components
data_manager = DataManager()
filings = data_manager.get_filings(state="CA", limit=100)

report_manager = ReportManager()
report = report_manager.generate_report(
    ReportType.AGENT_INTEL, filings, "CA"
)
```

### Workflow Usage
```python
# Run a complete workflow
from core.workflows import WorkflowEngine, ReportType

engine = WorkflowEngine()
execution = engine.monthly_report_workflow(
    state="CA", 
    report_type=ReportType.AGENT_INTEL
)

# Check status
if execution.status == WorkflowStatus.COMPLETED:
    print(f"Report generated: {execution.results}")
```

## Benefits

### For Developers
- **Faster onboarding**: Clear structure makes it easy to understand the system
- **Reduced navigation time**: Know exactly where to find/add code
- **Better code quality**: Clear boundaries prevent architectural drift
- **Easier testing**: Isolated components are easier to test

### For the Codebase
- **Reduced coupling**: Clear interfaces between modules
- **Better maintainability**: Changes are localized to specific modules
- **Improved scalability**: Components can be optimized independently
- **Consistent patterns**: Similar operations follow similar patterns

### For Features
- **Faster development**: Reusable components accelerate development
- **Better reliability**: Centralized error handling and logging
- **Easier debugging**: Clear data flow and comprehensive logging
- **Consistent behavior**: Shared utilities ensure consistency

## Next Steps

1. **Begin Phase 2 migration**: Move existing code into new structure
2. **Update documentation**: Ensure all docs reflect new architecture
3. **Add comprehensive tests**: Test each module in isolation
4. **Performance optimization**: Optimize critical paths
5. **Monitoring**: Add observability to track system health

---

*This architecture guide will evolve as the system grows. All changes should be documented and reviewed to maintain consistency.*
