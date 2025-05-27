import time
import json
import uuid
from typing import Dict, Any, Optional

from ..core.interfaces import (
    AbstractPolicyEventHandler, 
    AbstractLogger, 
    AbstractAlertManager, 
    AbstractStorageHandler,
    AbstractDatabaseHandler # Though not directly used by these storage handlers
)
from ..core.config_manager import ConfigManager, ConfigManagerError

try:
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
except ImportError:
    print("WARNING: tenacity library not found. Retry logic in handlers will be basic loops.", file=sys.stderr)
    # Define dummy decorators if tenacity is not available, so the code doesn't break
    # This allows the structure to be in place, but retries won't be robust.
    def retry(*args, **kwargs):
        def decorator(func):
            return func
        return decorator
    def stop_after_attempt(max_attempts): return None
    def wait_exponential(multiplier=1, min_wait=4, max_wait=10): return None
    def retry_if_exception_type(exc_type): return None
    TENACITY_AVAILABLE = False
else:
    TENACITY_AVAILABLE = True


class GCSUploadRetryHandler(AbstractPolicyEventHandler):
    """
    Handles retrying GCS file uploads upon failure.
    """
    EVENT_TYPE_GCS_UPLOAD_FAILED = "GCS_UPLOAD_FAILED"

    def can_handle(self, event_type: str, event_details: Dict[str, Any]) -> bool:
        return event_type == self.EVENT_TYPE_GCS_UPLOAD_FAILED

    def handle_event(self, 
                     event_type: str, 
                     event_details: Dict[str, Any], 
                     config_manager: ConfigManager, 
                     logger: AbstractLogger, 
                     alerter: AbstractAlertManager, 
                     db_handler: Optional[AbstractDatabaseHandler], # Not used by this handler
                     storage_handler: Optional[AbstractStorageHandler]) -> None:

        if not storage_handler:
            logger.log_error("GCSUploadRetryHandler: Storage handler not provided.", details=event_details)
            return

        correlation_id = event_details.get("correlation_id", str(uuid.uuid4()))
        policy_event_base = {
            "correlation_id": correlation_id,
            "policy_id": "GCS_UPLOAD_RETRY_POLICY",
            "target_resource": f"gs://{event_details.get('bucket_name', 'N/A')}/{event_details.get('destination_object_name', 'N/A')}",
            "pipeline_stage": event_details.get("pipeline_stage", "GCS_UPLOAD"),
            "detected_issue_type": event_type,
            "detected_issue_details": event_details.get("error_message", "GCS Upload Failed"),
            "python_module_invoked": f"{self.__class__.__module__}.{self.__class__.__name__}"
        }

        bucket_name = event_details.get("bucket_name")
        source_path = event_details.get("source_path")
        destination_object_name = event_details.get("destination_object_name")

        if not all([bucket_name, source_path, destination_object_name]):
            logger.log_error("GCSUploadRetryHandler: Missing required event details (bucket_name, source_path, destination_object_name).", 
                             details=event_details)
            alerter.send_alert(
                subject="SelfHealing Misconfiguration: GCSUploadRetryHandler",
                body="GCSUploadRetryHandler received an event with missing critical details.",
                severity="ERROR",
                details=event_details
            )
            logger.log_policy_event({
                **policy_event_base,
                "current_status": "HANDLER_FAILED_CONFIG",
                "action_taken": "LOGGED_MISSING_DETAILS",
                "action_result": "FAILURE",
                "error_message": "Missing required event details."
            })
            return

        try:
            gcs_settings = config_manager.get_gcs_settings(event_details.get("gcs_layer", "bronze_layer")) # e.g. bronze_layer
            max_attempts = gcs_settings.get("upload_retry_attempts", 3)
            base_delay = gcs_settings.get("base_retry_delay_seconds", 5)
            max_delay = gcs_settings.get("max_retry_delay_seconds", 60)
        except ConfigManagerError as e:
            logger.log_error(f"GCSUploadRetryHandler: Failed to get GCS config: {e}", error=e, details=event_details)
            alerter.send_alert(
                subject="SelfHealing Config Error: GCSUploadRetryHandler",
                body=f"Could not retrieve GCS settings for retry logic. Error: {e}",
                severity="ERROR",
                details=event_details
            )
            logger.log_policy_event({
                **policy_event_base,
                "current_status": "HANDLER_FAILED_CONFIG",
                "action_taken": "LOGGED_CONFIG_ERROR",
                "action_result": "FAILURE",
                "error_message": f"ConfigManagerError: {e}"
            })
            return

        upload_successful = False
        last_exception = None

        if TENACITY_AVAILABLE:
            @retry(
                stop=stop_after_attempt(max_attempts),
                wait=wait_exponential(multiplier=base_delay/2, min=base_delay, max=max_delay), # multiplier is often 1s unit
                retry=retry_if_exception_type(Exception), # Retry on any GCS related exception for simplicity
                before_sleep=lambda retry_state: logger.log_info(
                    f"Retrying GCS upload for {destination_object_name}: attempt {retry_state.attempt_number}/{max_attempts} after {retry_state.seconds_since_start:.2f}s. Waiting {retry_state.next_action.sleep}s...",
                    details={"retry_state": str(retry_state)}
                )
            )
            def attempt_upload():
                nonlocal last_exception # To store the exception for logging after tenacity finishes
                try:
                    logger.log_policy_event({
                        **policy_event_base,
                        "current_status": "RECOVERY_ATTEMPTED",
                        "action_taken": "ATTEMPT_GCS_UPLOAD",
                        "action_parameters": json.dumps({"attempt": attempt_upload.retry.statistics.get('attempt_number', 1)}) # Tenacity specific
                    })
                    if storage_handler.upload_file(bucket_name, source_path, destination_object_name):
                        return True
                    else:
                        # This path might not be hit if upload_file raises an exception on failure
                        last_exception = Exception("GCSStorageHandler.upload_file returned False")
                        raise last_exception 
                except Exception as e:
                    last_exception = e
                    raise
            try:
                upload_successful = attempt_upload()
            except Exception as e: # Catch exception from tenacity after all retries
                last_exception = e # Ensure last_exception is set
                logger.log_warning(f"Tenacity GCS upload failed after {max_attempts} attempts for {destination_object_name}.", error=e, details=event_details)
        else: # Basic loop if tenacity is not available
            for attempt in range(1, max_attempts + 1):
                logger.log_info(f"Attempting GCS upload for {destination_object_name}: attempt {attempt}/{max_attempts}", details=event_details)
                logger.log_policy_event({
                    **policy_event_base,
                    "current_status": "RECOVERY_ATTEMPTED",
                    "action_taken": "ATTEMPT_GCS_UPLOAD",
                    "action_parameters": json.dumps({"attempt": attempt})
                })
                try:
                    if storage_handler.upload_file(bucket_name, source_path, destination_object_name):
                        upload_successful = True
                        break
                    else: # upload_file returned False
                        last_exception = Exception("GCSStorageHandler.upload_file returned False")
                        # Continue to next attempt
                except Exception as e:
                    last_exception = e
                    logger.log_warning(f"GCS upload attempt {attempt} for {destination_object_name} failed.", error=e, details=event_details)
                
                if attempt < max_attempts:
                    time.sleep(base_delay * (2**(attempt-1))) # Basic exponential backoff

        if upload_successful:
            logger.log_info(f"GCS upload successful for {destination_object_name} to bucket {bucket_name}.", details=event_details)
            logger.log_policy_event({
                **policy_event_base,
                "current_status": "RECOVERY_SUCCESSFUL",
                "action_taken": "GCS_UPLOAD_COMPLETED",
                "action_result": "SUCCESS"
            })
        else:
            error_msg = f"GCS upload finally failed for {destination_object_name} after {max_attempts} attempts."
            logger.log_error(error_msg, error=last_exception, details=event_details)
            alerter.send_alert(
                subject=f"SelfHealing CRITICAL: GCS Upload Failed Permanently",
                body=f"{error_msg}\nFile: {source_path}\nTarget: gs://{bucket_name}/{destination_object_name}\nError: {last_exception}",
                severity="CRITICAL",
                details={**event_details, "final_error": str(last_exception)}
            )
            logger.log_policy_event({
                **policy_event_base,
                "current_status": "RECOVERY_FAILED",
                "action_taken": "ALERT_ON_FINAL_UPLOAD_FAILURE",
                "action_result": "FAILURE",
                "error_message": str(last_exception)
            })
            # Potentially trigger a DLQ event for the source file if it's local and needs to be archived
            # This handler assumes the source_path is a local file that failed to upload.
            # If the source_path itself was a GCS object (e.g. copy operation), different logic applies.
            # For now, we assume local source file, so DLQ is not directly applicable in this handler.


class GCSDeadLetterHandler(AbstractPolicyEventHandler):
    """
    Handles moving GCS objects to a Dead Letter Queue (DLQ).
    """
    EVENT_TYPE_GCS_MOVE_TO_DLQ = "GCS_MOVE_TO_DLQ"

    def can_handle(self, event_type: str, event_details: Dict[str, Any]) -> bool:
        return event_type == self.EVENT_TYPE_GCS_MOVE_TO_DLQ

    def handle_event(self, 
                     event_type: str, 
                     event_details: Dict[str, Any], 
                     config_manager: ConfigManager, 
                     logger: AbstractLogger, 
                     alerter: AbstractAlertManager, 
                     db_handler: Optional[AbstractDatabaseHandler], # Not used
                     storage_handler: Optional[AbstractStorageHandler]) -> None:

        if not storage_handler:
            logger.log_error("GCSDeadLetterHandler: Storage handler not provided.", details=event_details)
            return

        source_bucket = event_details.get("source_bucket_name") # Renamed for clarity
        source_object = event_details.get("source_object_name") # Renamed for clarity
        original_error_details = event_details.get("original_error_details", {})
        
        correlation_id = event_details.get("correlation_id", str(uuid.uuid4()))
        policy_event_base = {
            "correlation_id": correlation_id,
            "policy_id": "GCS_DLQ_POLICY",
            "target_resource": f"gs://{source_bucket}/{source_object}",
            "pipeline_stage": event_details.get("pipeline_stage", "GCS_DLQ_PROCESSING"),
            "detected_issue_type": event_type, # This event itself is the issue type
            "detected_issue_details": f"Request to move gs://{source_bucket}/{source_object} to DLQ.",
            "python_module_invoked": f"{self.__class__.__module__}.{self.__class__.__name__}"
        }


        if not all([source_bucket, source_object]):
            logger.log_error("GCSDeadLetterHandler: Missing required event details (source_bucket_name, source_object_name).", 
                             details=event_details)
            alerter.send_alert(
                subject="SelfHealing Misconfiguration: GCSDeadLetterHandler",
                body="GCSDeadLetterHandler received event with missing source bucket/object.",
                severity="ERROR",
                details=event_details
            )
            logger.log_policy_event({
                **policy_event_base,
                "current_status": "HANDLER_FAILED_CONFIG",
                "action_taken": "LOGGED_MISSING_DETAILS",
                "action_result": "FAILURE",
                "error_message": "Missing source_bucket_name or source_object_name."
            })
            return

        try:
            dlq_path = storage_handler.move_to_dlq(source_bucket, source_object, error_details=original_error_details)
            
            success_msg = f"File gs://{source_bucket}/{source_object} successfully moved to DLQ at {dlq_path}."
            logger.log_info(success_msg, details=event_details)
            alerter.send_alert(
                subject="SelfHealing INFO: GCS File Moved to DLQ",
                body=success_msg,
                severity="INFO", # Or WARNING depending on how critical DLQ events are
                details={**event_details, "dlq_path": dlq_path}
            )
            logger.log_policy_event({
                **policy_event_base,
                "current_status": "RECOVERY_SUCCESSFUL", # Or "ACTION_COMPLETED"
                "action_taken": "MOVED_TO_GCS_DLQ",
                "action_parameters": json.dumps({"dlq_path": dlq_path, "original_error": original_error_details}),
                "action_result": "SUCCESS"
            })

        except FileNotFoundError as e:
            logger.log_warning(f"GCSDeadLetterHandler: File not found during DLQ attempt: {str(e)}", error=e, details=event_details)
            # Alerting might be optional here if the file simply wasn't there to move
            alerter.send_alert(
                subject="SelfHealing WARNING: GCS File Not Found for DLQ",
                body=f"Attempted to move gs://{source_bucket}/{source_object} to DLQ, but it was not found. Error: {e}",
                severity="WARNING",
                details=event_details
            )
            logger.log_policy_event({
                **policy_event_base,
                "current_status": "ACTION_FAILED_PRECONDITION",
                "action_taken": "ATTEMPT_MOVE_TO_GCS_DLQ",
                "action_result": "FAILURE",
                "error_message": f"FileNotFoundError: {e}"
            })
        except ConfigManagerError as e: # If DLQ bucket isn't configured in GCSStorageHandler
            logger.log_error(f"GCSDeadLetterHandler: Configuration error during DLQ: {str(e)}", error=e, details=event_details)
            alerter.send_alert(
                subject="SelfHealing ERROR: GCS DLQ Configuration Error",
                body=f"Failed to move gs://{source_bucket}/{source_object} to DLQ due to configuration error: {e}",
                severity="ERROR",
                details=event_details
            )
            logger.log_policy_event({
                **policy_event_base,
                "current_status": "HANDLER_FAILED_CONFIG",
                "action_taken": "ATTEMPT_MOVE_TO_GCS_DLQ",
                "action_result": "FAILURE",
                "error_message": f"ConfigManagerError: {e}"
            })
        except Exception as e:
            logger.log_error(f"GCSDeadLetterHandler: Failed to move gs://{source_bucket}/{source_object} to DLQ.", 
                             error=e, details=event_details)
            alerter.send_alert(
                subject="SelfHealing CRITICAL: Failed to Move GCS File to DLQ",
                body=f"Attempt to move gs://{source_bucket}/{source_object} to DLQ failed.\nError: {e}",
                severity="CRITICAL",
                details=event_details
            )
            logger.log_policy_event({
                **policy_event_base,
                "current_status": "RECOVERY_FAILED", # Or "ACTION_FAILED"
                "action_taken": "ATTEMPT_MOVE_TO_GCS_DLQ",
                "action_result": "FAILURE",
                "error_message": str(e)
            })
