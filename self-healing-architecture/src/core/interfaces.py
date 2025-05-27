from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List, Tuple

# Forward declaration for type hinting ConfigManager if needed, though not directly used in ABC args here
# class ConfigManager:
#     pass

class AbstractLogger(ABC):
    """
    Abstract base class for logging functionalities.
    """

    @abstractmethod
    def log_info(self, message: str, details: Optional[Dict[str, Any]] = None) -> None:
        """Logs an informational message."""
        pass

    @abstractmethod
    def log_warning(self, message: str, details: Optional[Dict[str, Any]] = None) -> None:
        """Logs a warning message."""
        pass

    @abstractmethod
    def log_error(self, message: str, error: Optional[Exception] = None, details: Optional[Dict[str, Any]] = None) -> None:
        """Logs an error message, optionally including an exception."""
        pass

    @abstractmethod
    def log_policy_event(self, event_data: Dict[str, Any]) -> None:
        """
        Logs a structured event to the policy execution tracking system (e.g., BigQuery table).
        'event_data' should conform to the schema of the 'policy_execution_log' table.
        """
        pass

    @abstractmethod
    def log_data_flow_event(self, flow_data: Dict[str, Any]) -> None:
        """
        Logs a structured event to the end-to-end data flow tracking system (e.g., BigQuery table).
        'flow_data' should conform to the schema of the 'data_flow_log' table.
        """
        pass


class AbstractAlertManager(ABC):
    """
    Abstract base class for sending alerts.
    """

    @abstractmethod
    def send_alert(self, subject: str, body: str, severity: str, details: Optional[Dict[str, Any]] = None) -> None:
        """
        Sends an alert.
        Severity could be 'INFO', 'WARNING', 'CRITICAL'.
        Details can include links to logs, affected resources, etc.
        """
        pass


class AbstractStorageHandler(ABC):
    """
    Abstract base class for cloud storage operations (e.g., GCS).
    """

    @abstractmethod
    def download_file(self, bucket_name: str, object_name: str, destination_path: str) -> None:
        """Downloads a file from storage."""
        pass

    @abstractmethod
    def upload_file(self, bucket_name: str, source_path: str, destination_object_name: str) -> bool:
        """Uploads a file to storage. Returns True on success."""
        pass

    @abstractmethod
    def move_to_dlq(self, source_bucket: str, source_object: str, error_details: Dict[str, Any]) -> str:
        """
        Moves a file to a Dead Letter Queue (DLQ) within cloud storage.
        Returns the GCS path of the file in the DLQ.
        'error_details' provides context about why the file is being moved.
        """
        pass

    @abstractmethod
    def list_files(self, bucket_name: str, prefix: Optional[str] = None) -> List[str]:
        """Lists files in a bucket, optionally filtered by a prefix."""
        pass


class AbstractDatabaseHandler(ABC):
    """
    Abstract base class for database operations (e.g., BigQuery).
    """

    @abstractmethod
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Tuple[Any, ...]]:
        """
        Executes a query and returns results.
        Params can be used for parameterized queries.
        """
        pass

    @abstractmethod
    def get_job_status(self, job_id: str, location: Optional[str] = None) -> Dict[str, Any]:
        """
        Retrieves the status of a database job (e.g., BigQuery job).
        Location might be required for some database services.
        """
        pass

    @abstractmethod
    def retry_job(self, job_id: str, location: Optional[str] = None) -> Dict[str, Any]:
        """
        Retries a database job. This might involve re-initiating the job
        with the original parameters.
        Returns the status of the new/retried job.
        """
        pass

# Forward declare ConfigManager for type hinting if AbstractPolicyEventHandler needs it.
# This avoids circular dependency if ConfigManager also imports interfaces.
# However, it's better if interfaces are standalone. For now, assume ConfigManager is passed as an instance.
class ConfigManager: # Minimal stub for type hinting, actual class in config_manager.py
    pass

class AbstractPolicyEventHandler(ABC):
    """
    Abstract base class for handlers that react to specific policy events/failures.
    """

    @abstractmethod
    def can_handle(self, event_type: str, event_details: Dict[str, Any]) -> bool:
        """
        Determines if this handler is appropriate for the given event type and details.
        """
        pass

    @abstractmethod
    def handle_event(self, 
                     event_type: str, 
                     event_details: Dict[str, Any], 
                     config_manager: ConfigManager, # Actual ConfigManager instance
                     logger: AbstractLogger, 
                     alerter: AbstractAlertManager, 
                     db_handler: Optional[AbstractDatabaseHandler] = None, 
                     storage_handler: Optional[AbstractStorageHandler] = None) -> None:
        """
        Handles the detected event/failure according to defined policy rules.
        This method will contain the core logic for diagnosis and initiating recovery.
        """
        pass

class AbstractCDCEventHandler(AbstractPolicyEventHandler):
    """
    Abstract base class for handlers specific to Change Data Capture (CDC) events.
    Inherits from AbstractPolicyEventHandler.
    """
    # Could define CDC-specific abstract methods here if common patterns emerge,
    # e.g., @abstractmethod
    #        def get_replication_lag_details(self, event_details: Dict[str, Any]) -> Dict[str, Any]:
    #            pass
    pass

class AbstractStreamingEventHandler(AbstractPolicyEventHandler):
    """
    Abstract base class for handlers specific to streaming ingestion events.
    Inherits from AbstractPolicyEventHandler.
    """
    # Could define streaming-specific abstract methods here,
    # e.g., @abstractmethod
    #        def process_dlq_message(self, message_payload: Any, event_details: Dict[str, Any]) -> None:
    #            pass
    pass
