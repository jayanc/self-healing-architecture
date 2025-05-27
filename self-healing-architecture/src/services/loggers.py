import sys
import json
import datetime
import uuid
from typing import Dict, Any, Optional, List

from ..core.interfaces import AbstractLogger # Use relative import
from ..core.config_manager import ConfigManager, ConfigManagerError

try:
    from google.cloud import bigquery
    from google.api_core.exceptions import GoogleAPICallError, NotFound
except ImportError:
    bigquery = None # type: ignore
    GoogleAPICallError = None # type: ignore
    NotFound = None # type: ignore
    print("WARNING: google-cloud-bigquery is not installed. BigQueryLogger will not be functional.", file=sys.stderr)


class ConsoleLogger(AbstractLogger):
    """
    Logs messages and events to the console (stdout/stderr).
    """

    def _format_message(self, level: str, message: str, details: Optional[Dict[str, Any]] = None) -> str:
        timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
        log_entry = f"[{timestamp}] [{level}] {message}"
        if details:
            try:
                details_str = json.dumps(details, sort_keys=True, default=str) # default=str for non-serializable
                log_entry += f" | Details: {details_str}"
            except TypeError:
                log_entry += f" | Details: (unserializable - {str(details)})"
        return log_entry

    def log_info(self, message: str, details: Optional[Dict[str, Any]] = None) -> None:
        """Logs an informational message to stdout."""
        print(self._format_message("INFO", message, details), file=sys.stdout)

    def log_warning(self, message: str, details: Optional[Dict[str, Any]] = None) -> None:
        """Logs a warning message to stdout."""
        print(self._format_message("WARNING", message, details), file=sys.stdout)

    def log_error(self, message: str, error: Optional[Exception] = None, details: Optional[Dict[str, Any]] = None) -> None:
        """Logs an error message to stderr."""
        if error:
            message += f" | Exception: {type(error).__name__}({str(error)})"
        print(self._format_message("ERROR", message, details), file=sys.stderr)

    def log_policy_event(self, event_data: Dict[str, Any]) -> None:
        """
        Logs a structured policy event to stdout, prefixed with "POLICY_EVENT:".
        """
        timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
        prefix = f"[{timestamp}] [POLICY_EVENT]"
        try:
            event_str = json.dumps(event_data, indent=2, sort_keys=True, default=str)
            print(f"{prefix}\n{event_str}", file=sys.stdout)
        except TypeError:
            print(f"{prefix} (unserializable event_data)\n{str(event_data)}", file=sys.stdout)


    def log_data_flow_event(self, flow_data: Dict[str, Any]) -> None:
        """
        Logs a structured data flow event to stdout, prefixed with "DATA_FLOW_EVENT:".
        """
        timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
        prefix = f"[{timestamp}] [DATA_FLOW_EVENT]"
        try:
            flow_str = json.dumps(flow_data, indent=2, sort_keys=True, default=str)
            print(f"{prefix}\n{flow_str}", file=sys.stdout)
        except TypeError:
            print(f"{prefix} (unserializable flow_data)\n{str(flow_data)}", file=sys.stdout)


class BigQueryLogger(AbstractLogger):
    """
    Logs structured events to specified BigQuery tables.
    Also logs simple info/warning/error messages as generic policy events.
    """

    def __init__(self, config_manager: ConfigManager, project_id: Optional[str] = None, fallback_logger: Optional[AbstractLogger] = None):
        """
        Initializes the BigQueryLogger.

        Args:
            config_manager: Instance of ConfigManager to fetch BQ table IDs.
            project_id: GCP project ID. If None, client tries to infer from environment.
            fallback_logger: Logger to use if BQ logging fails. Defaults to ConsoleLogger.
        """
        if bigquery is None:
            self._client = None
            print("ERROR: BigQueryLogger cannot be initialized because google-cloud-bigquery is not installed.", file=sys.stderr)
            # Use provided fallback or default to ConsoleLogger if BQ library is missing
            self._fallback_logger = fallback_logger if fallback_logger else ConsoleLogger()
            self._fallback_logger.log_error("BigQueryLogger disabled: google-cloud-bigquery not found.", None, {"initialization_error": True})
            return

        try:
            self._client = bigquery.Client(project=project_id)
            tracking_config = config_manager.get_tracking_and_logging_config()
            self._policy_log_table_id = tracking_config.get("policy_execution_log_table_id")
            self._data_flow_log_table_id = tracking_config.get("data_flow_log_table_id")
            
            if not self._policy_log_table_id:
                raise ConfigManagerError("BigQuery policy_execution_log_table_id not found in config.")
            if not self._data_flow_log_table_id:
                raise ConfigManagerError("BigQuery data_flow_log_table_id not found in config.")

            # Initialize fallback logger (defaults to ConsoleLogger if not provided)
            self._fallback_logger = fallback_logger if fallback_logger else ConsoleLogger()
            self._fallback_logger.log_info(f"BigQueryLogger initialized. Policy events to: {self._policy_log_table_id}, Data flow events to: {self._data_flow_log_table_id}")

        except ConfigManagerError as e:
            self._client = None # Ensure client is None if init fails
            self._fallback_logger = fallback_logger if fallback_logger else ConsoleLogger()
            self._fallback_logger.log_error(f"Failed to initialize BigQueryLogger due to ConfigManagerError: {e}", e)
            # Re-raise or handle as critical initialization failure
        except Exception as e: # Catch any other exception during client init e.g. auth
            self._client = None
            self._fallback_logger = fallback_logger if fallback_logger else ConsoleLogger()
            self._fallback_logger.log_error(f"Failed to initialize BigQuery client for BigQueryLogger: {e}", e)


    def _insert_rows(self, table_id: str, rows: List[Dict[str, Any]]) -> None:
        if not self._client or not table_id:
            self._fallback_logger.log_error("BigQuery client or table ID not configured. Cannot insert rows.", details={"table_id": table_id, "rows_count": len(rows)})
            # Log the actual rows to fallback logger for debugging if necessary
            self._fallback_logger.log_info("Data intended for BQ:", {"table_id": table_id, "rows": rows})
            return

        try:
            # Ensure timestamps are in the correct string format for BQ JSON API
            for row in rows:
                for key, value in row.items():
                    if isinstance(value, datetime.datetime):
                        row[key] = value.isoformat()
            
            errors = self._client.insert_rows_json(table_id, rows)
            if errors:
                self._fallback_logger.log_error(f"Errors occurred while inserting rows into BigQuery table {table_id}.", details={"bq_errors": errors, "rows": rows})
            # else:
            #     self._fallback_logger.log_info(f"Successfully inserted {len(rows)} row(s) into {table_id}.", details={"rows": rows}) # Optional: for verbose logging
        except GoogleAPICallError as e:
            self._fallback_logger.log_error(f"BigQuery API call error while inserting rows into {table_id}: {e}", e, {"rows": rows})
        except NotFound:
            self._fallback_logger.log_error(f"BigQuery table {table_id} not found.", details={"rows_count": len(rows)})
        except Exception as e:
            self._fallback_logger.log_error(f"Unexpected error inserting rows into BigQuery table {table_id}: {e}", e, {"rows": rows})

    def log_policy_event(self, event_data: Dict[str, Any]) -> None:
        """
        Logs a structured policy event to the configured BigQuery table.
        'event_data' should conform to the schema of 'policy_execution_log'.
        """
        if not self._policy_log_table_id:
            self._fallback_logger.log_error("Policy execution log table ID not configured.", details=event_data)
            return
        
        # Ensure essential fields are present, adding defaults if necessary
        event_data.setdefault('event_id', str(uuid.uuid4()))
        event_data.setdefault('event_timestamp', datetime.datetime.now(datetime.timezone.utc).isoformat())
        
        self._insert_rows(self._policy_log_table_id, [event_data])

    def log_data_flow_event(self, flow_data: Dict[str, Any]) -> None:
        """
        Logs a structured data flow event to the configured BigQuery table.
        'flow_data' should conform to the schema of 'data_flow_log'.
        """
        if not self._data_flow_log_table_id:
            self._fallback_logger.log_error("Data flow log table ID not configured.", details=flow_data)
            return
        
        flow_data.setdefault('flow_run_id', str(uuid.uuid4())) # Or expect it to be passed in
        flow_data.setdefault('last_updated_timestamp', datetime.datetime.now(datetime.timezone.utc).isoformat())

        self._insert_rows(self._data_flow_log_table_id, [flow_data])

    def _log_generic_event(self, level: str, message: str, details: Optional[Dict[str, Any]] = None) -> None:
        """Helper to log simple messages as generic policy events."""
        event_data = {
            'event_id': str(uuid.uuid4()),
            'correlation_id': details.pop('correlation_id', None) if details else None, # Extract if present
            'event_timestamp': datetime.datetime.now(datetime.timezone.utc).isoformat(),
            'policy_id': 'INTERNAL_LOG_MESSAGE',
            'target_resource': details.pop('target_resource', 'N/A') if details else 'N/A',
            'pipeline_stage': details.pop('pipeline_stage', 'FRAMEWORK') if details else 'FRAMEWORK',
            'detected_issue_type': f'INTERNAL_{level}',
            'detected_issue_details': message,
            'current_status': 'ACTION_LOGGED',
            'action_taken': f'LOGGED_{level}_MESSAGE_TO_POLICY_TABLE',
            'action_parameters': json.dumps(details) if details else None,
            'action_result': 'SUCCESS',
            'python_module_invoked': f'{self.__class__.__module__}.{self.__class__.__name__}'
        }
        self.log_policy_event(event_data)
        # Also log to fallback for immediate visibility if BQ has latency or issues
        self._fallback_logger.log_info(f"[BigQueryLogger-{level}] {message}", details=details)


    def log_info(self, message: str, details: Optional[Dict[str, Any]] = None) -> None:
        """Logs an informational message as a generic event to policy_execution_log."""
        self._log_generic_event("INFO", message, details)

    def log_warning(self, message: str, details: Optional[Dict[str, Any]] = None) -> None:
        """Logs a warning message as a generic event to policy_execution_log."""
        self._log_generic_event("WARNING", message, details)

    def log_error(self, message: str, error: Optional[Exception] = None, details: Optional[Dict[str, Any]] = None) -> None:
        """Logs an error message as a generic event to policy_execution_log."""
        if details is None:
            details = {}
        if error:
            details['exception_type'] = type(error).__name__
            details['exception_message'] = str(error)
        
        # Ensure message string is included in detected_issue_details
        full_message = message
        if error:
            full_message += f" | Exception: {type(error).__name__}({str(error)})"

        self._log_generic_event("ERROR", full_message, details)
