"""
Workflow engine for the CORE platform.

This module orchestrates complex workflows like monthly report generation,
data synchronization, and automated notifications.
"""

from typing import Dict, List, Optional, Any, Callable
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum

from ..models import RateFiling, ReportType, AgentProfile, SyncStatus
from ..config import settings
from ..utils import get_logger
from ..data import DataManager
from ..analytics import AnalyticsEngine
from ..reporting import ReportManager
from ..notifications import NotificationService

logger = get_logger(__name__)

class WorkflowStatus(Enum):
    """Status of a workflow execution."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

@dataclass
class WorkflowStep:
    """Individual step in a workflow."""
    name: str
    function: Callable
    args: tuple = ()
    kwargs: Optional[Dict[str, Any]] = None
    retry_count: int = 0
    max_retries: int = 3
    
    def __post_init__(self):
        if self.kwargs is None:
            self.kwargs = {}

@dataclass
class WorkflowExecution:
    """Execution context for a workflow."""
    workflow_id: str
    name: str
    status: WorkflowStatus = WorkflowStatus.PENDING
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    current_step: int = 0
    total_steps: int = 0
    errors: Optional[List[str]] = None
    results: Optional[Dict[str, Any]] = None
    
    def __post_init__(self):
        if self.errors is None:
            self.errors = []
        if self.results is None:
            self.results = {}

class WorkflowEngine:
    """
    Workflow engine for orchestrating complex business processes.
    
    This class manages the execution of multi-step workflows,
    handling error recovery, logging, and state management.
    """
    
    def __init__(self):
        """Initialize the workflow engine."""
        self.data_manager = DataManager()
        self.analytics_engine = AnalyticsEngine()
        self.report_manager = ReportManager()
        self.notification_service = NotificationService()
        self.active_workflows: Dict[str, WorkflowExecution] = {}
        logger.info("WorkflowEngine initialized")
    
    def execute_workflow(
        self,
        workflow_id: str,
        name: str,
        steps: List[WorkflowStep]
    ) -> WorkflowExecution:
        """
        Execute a workflow with the given steps.
        
        Args:
            workflow_id: Unique identifier for the workflow
            name: Human-readable name for the workflow
            steps: List of steps to execute
        
        Returns:
            WorkflowExecution object with results
        """
        execution = WorkflowExecution(
            workflow_id=workflow_id,
            name=name,
            total_steps=len(steps),
            started_at=datetime.now(),
            status=WorkflowStatus.RUNNING
        )
        
        self.active_workflows[workflow_id] = execution
        
        logger.info(f"Starting workflow: {name} ({workflow_id})")
        
        try:
            for i, step in enumerate(steps):
                execution.current_step = i + 1
                logger.info(f"Executing step {i+1}/{len(steps)}: {step.name}")
                
                # Execute step with retry logic
                for attempt in range(step.max_retries + 1):
                    try:
                        result = step.function(*step.args, **(step.kwargs or {}))
                        if execution.results is not None:
                            execution.results[step.name] = result
                        break
                    except Exception as e:
                        error_msg = f"Step '{step.name}' failed (attempt {attempt + 1}): {str(e)}"
                        logger.error(error_msg)
                        if execution.errors is not None:
                            execution.errors.append(error_msg)
                        
                        if attempt == step.max_retries:
                            execution.status = WorkflowStatus.FAILED
                            execution.completed_at = datetime.now()
                            return execution
            
            # Workflow completed successfully
            execution.status = WorkflowStatus.COMPLETED
            execution.completed_at = datetime.now()
            logger.info(f"Workflow completed successfully: {name}")
            
        except Exception as e:
            error_msg = f"Workflow failed: {str(e)}"
            logger.error(error_msg)
            if execution.errors is not None:
                execution.errors.append(error_msg)
            execution.status = WorkflowStatus.FAILED
            execution.completed_at = datetime.now()
        
        return execution
    
    def monthly_report_workflow(
        self,
        state: str,
        report_type: ReportType = ReportType.AGENT_INTEL
    ) -> WorkflowExecution:
        """
        Execute the monthly report generation workflow.
        
        Args:
            state: State to generate report for
            report_type: Type of report to generate
        
        Returns:
            WorkflowExecution object with results
        """
        workflow_id = f"monthly_report_{state}_{datetime.now().strftime('%Y%m')}"
        
        steps = [
            WorkflowStep(
                name="fetch_filings",
                function=self._fetch_recent_filings,
                args=(state, 30)
            ),
            WorkflowStep(
                name="analyze_data",
                function=self._analyze_filings,
                kwargs={"state": state}
            ),
            WorkflowStep(
                name="generate_report",
                function=self._generate_monthly_report,
                kwargs={"state": state, "report_type": report_type}
            ),
            WorkflowStep(
                name="save_report",
                function=self._save_report_to_disk
            ),
            WorkflowStep(
                name="send_notifications",
                function=self._send_report_notifications,
                kwargs={"state": state}
            )
        ]
        
        return self.execute_workflow(workflow_id, f"Monthly Report - {state}", steps)
    
    def data_sync_workflow(
        self,
        source: str = "airtable",
        full_sync: bool = False
    ) -> WorkflowExecution:
        """
        Execute the data synchronization workflow.
        
        Args:
            source: Data source to sync from
            full_sync: Whether to perform a full or incremental sync
        
        Returns:
            WorkflowExecution object with results
        """
        workflow_id = f"data_sync_{source}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        steps = [
            WorkflowStep(
                name="prepare_sync",
                function=self._prepare_sync,
                args=(source, full_sync)
            ),
            WorkflowStep(
                name="fetch_data",
                function=self._fetch_source_data,
                args=(source,)
            ),
            WorkflowStep(
                name="process_data",
                function=self._process_sync_data
            ),
            WorkflowStep(
                name="save_data",
                function=self._save_sync_data
            ),
            WorkflowStep(
                name="cleanup",
                function=self._cleanup_sync
            )
        ]
        
        return self.execute_workflow(workflow_id, f"Data Sync - {source}", steps)
    
    def get_workflow_status(self, workflow_id: str) -> Optional[WorkflowExecution]:
        """
        Get the status of a workflow execution.
        
        Args:
            workflow_id: ID of the workflow to check
        
        Returns:
            WorkflowExecution object or None if not found
        """
        return self.active_workflows.get(workflow_id)
    
    def cancel_workflow(self, workflow_id: str) -> bool:
        """
        Cancel a running workflow.
        
        Args:
            workflow_id: ID of the workflow to cancel
        
        Returns:
            True if cancelled successfully, False otherwise
        """
        if workflow_id in self.active_workflows:
            execution = self.active_workflows[workflow_id]
            execution.status = WorkflowStatus.CANCELLED
            execution.completed_at = datetime.now()
            logger.info(f"Workflow cancelled: {workflow_id}")
            return True
        return False
    
    # Helper methods for workflow steps
    def _fetch_recent_filings(self, state: str, days: int) -> List[RateFiling]:
        """Fetch recent filings for a state."""
        return self.data_manager.get_filings(state=state, limit=100)
    
    def _analyze_filings(self, filings: List[RateFiling], state: str) -> Dict[str, Any]:
        """Analyze filings data."""
        return self.analytics_engine.calculate_market_trends(filings, state)
    
    def _generate_monthly_report(
        self, 
        filings: List[RateFiling], 
        state: str, 
        report_type: ReportType
    ) -> str:
        """Generate monthly report."""
        report_data = self.report_manager.generate_report(report_type, filings, state)
        return self.report_manager.render_html_report(report_data)
    
    def _save_report_to_disk(self, report_data: Any) -> str:
        """Save report to disk."""
        # TODO: Implement actual saving
        return f"reports/monthly_report_{datetime.now().strftime('%Y%m%d')}.html"
    
    def _send_report_notifications(self, report_path: str, state: str) -> int:
        """Send report notifications."""
        # TODO: Implement actual notification sending
        return 0  # Number of notifications sent
    
    def _prepare_sync(self, source: str, full_sync: bool) -> Dict[str, Any]:
        """Prepare data synchronization."""
        return {"source": source, "full_sync": full_sync, "prepared": True}
    
    def _fetch_source_data(self, source: str) -> List[Dict[str, Any]]:
        """Fetch data from source."""
        return []  # TODO: Implement actual data fetching
    
    def _process_sync_data(self, raw_data: List[Dict[str, Any]]) -> List[RateFiling]:
        """Process synchronized data."""
        return []  # TODO: Implement actual data processing
    
    def _save_sync_data(self, processed_data: List[RateFiling]) -> Dict[str, int]:
        """Save synchronized data."""
        return self.data_manager.save_filings_batch(processed_data)
    
    def _cleanup_sync(self, sync_results: Dict[str, int]) -> bool:
        """Clean up after synchronization."""
        return True

# Export the main class
__all__ = ["WorkflowEngine", "WorkflowStatus", "WorkflowStep", "WorkflowExecution"]
