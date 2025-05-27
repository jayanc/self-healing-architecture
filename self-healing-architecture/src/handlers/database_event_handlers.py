import time
import json
import uuid
from typing import Dict, Any, Optional

from ..core.interfaces import (
    AbstractPolicyEventHandler, 
    AbstractLogger, 
    AbstractAlertManager, 
    AbstractDatabaseHandler,
    AbstractStorageHandler 
)
from ..core.config_manager import ConfigManager, ConfigManagerError

try:
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
except ImportError:
    print("WARNING: tenacity library not found. Retry logic in handlers will be basic loops.", file=sys.stderr)
    def retry(*args, **kwargs):
        def decorator(func):
            return func
        return decorator
    def stop_after_attempt(max_attempts): return None
    def wait_exponential(multiplier=1, min_wait=4, max_wait=10): return None
    def retry_if_exception_type(exc_type): return None # Basic placeholder
    TENACITY_AVAILABLE = False
else:
    TENACITY_AVAILABLE = True

# Define common BigQuery transient error reasons (subset, can be expanded)
# These are typically found in the 'reason' field of an errorResource in a BigQuery job error.
# See: https://cloud.google.com/bigquery/docs/error-messages
TRANSIENT_BQ_ERROR_REASONS = [
    "backendError",  # General transient backend error
    "internalError", # Can sometimes be transient
    "jobBackendError", # Similar to backendError
    "rateLimitExceeded",
    "resourceUnavailable", # e.g. slots temporarily unavailable
    "timeout", # If the job itself timed out but might succeed on retry
    # "quotaExceeded" # Could be transient (e.g. concurrent queries) or persistent (e.g. daily load bytes)
                      # For quotaExceeded, specific logic might be needed to check which quota.
]


class BigQueryJobRetryHandler(AbstractPolicyEventHandler):
    """
    Handles retrying BigQuery jobs upon specific types of failures.
    """
    EVENT_TYPE_BQ_JOB_FAILED = "BQ_JOB_FAILED" # A generic job failure event

    def can_handle(self, event_type: str, event_details: Dict[str, Any]) -> bool:
        if event_type != self.EVENT_TYPE_BQ_JOB_FAILED:
            return False
        
        # Check for error details that suggest a retryable condition
        # 'error_result' structure is based on BigQuery job error object
        error_result = event_details.get("error_result") 
        if error_result and isinstance(error_result, dict):
            reason = error_result.get("reason")
            # Could also check error_result.get("message") for certain keywords
            if reason in TRANSIENT_BQ_ERROR_REASONS:
                return True
            # Example: Specific handling for quotaExceeded if it's about concurrent queries
            if reason == "quotaExceeded" and "concurrent" in error_result.get("message", "").lower():
                return True
        
        # Allow handling if explicitly marked as retryable, even if not in TRANSIENT_BQ_ERROR_REASONS
        if event_details.get("is_retryable_custom_flag", False) is True:
            return True
            
        return False

    def handle_event(self, 
                     event_type: str, 
                     event_details: Dict[str, Any], 
                     config_manager: ConfigManager, 
                     logger: AbstractLogger, 
                     alerter: AbstractAlertManager, 
                     db_handler: Optional[AbstractDatabaseHandler], 
                     storage_handler: Optional[AbstractStorageHandler]) -> None: # Not used by this handler

        if not db_handler:
            logger.log_error("BigQueryJobRetryHandler: Database handler not provided.", details=event_details)
            return

        original_job_id = event_details.get("job_id")
        bq_location = event_details.get("location") # Location of the original job
        
        correlation_id = event_details.get("correlation_id", str(uuid.uuid4()))
        policy_event_base = {
            "correlation_id": correlation_id,
            "policy_id": "BQ_JOB_RETRY_POLICY",
            "target_resource": f"bq_job:{original_job_id}",
            "pipeline_stage": event_details.get("pipeline_stage", "BIGQUERY_PROCESSING"),
            "detected_issue_type": event_type,
            "detected_issue_details": json.dumps(event_details.get("error_result", "BQ Job Failed - unknown error")),
            "python_module_invoked": f"{self.__class__.__module__}.{self.__class__.__name__}"
        }

        if not original_job_id:
            logger.log_error("BigQueryJobRetryHandler: Missing 'job_id' in event_details.", details=event_details)
            alerter.send_alert(subject="SelfHealing Misconfig: BQJobRetryHandler", body="Missing job_id.", severity="ERROR", details=event_details)
            logger.log_policy_event({**policy_event_base, "current_status": "HANDLER_FAILED_CONFIG", "action_result": "FAILURE", "error_message": "Missing job_id"})
            return

        try:
            # Determine layer for config (e.g., 'silver_layer', 'gold_layer')
            # This might need to be passed in event_details or inferred
            # For now, using a generic default from bigquery_settings.default_job_settings
            bq_default_settings = config_manager.get_bigquery_settings().get("default_job_settings", {})
            max_attempts = bq_default_settings.get("job_retry_attempts", 2) # Default to 2 if not found
            base_delay = bq_default_settings.get("base_retry_delay_seconds", 60)
            max_delay = bq_default_settings.get("max_retry_delay_seconds", 300)
        except ConfigManagerError as e:
            logger.log_error(f"BigQueryJobRetryHandler: Failed to get BQ config: {e}", error=e, details=event_details)
            alerter.send_alert(subject="SelfHealing Config Error: BQJobRetryHandler", body=f"Could not retrieve BQ settings. Error: {e}", severity="ERROR", details=event_details)
            logger.log_policy_event({**policy_event_base, "current_status": "HANDLER_FAILED_CONFIG", "action_result": "FAILURE", "error_message": f"ConfigManagerError: {e}"})
            return

        retry_successful = False
        last_exception = None
        new_job_status_info = None

        current_job_id_to_retry = original_job_id

        # Note: Tenacity is harder to apply here directly if db_handler.retry_job itself isn't idempotent
        # or if we need to check status between attempts. A manual loop is often clearer for job retries.
        for attempt in range(1, max_attempts + 1):
            logger.log_info(f"Attempting BQ job retry for {current_job_id_to_retry}: attempt {attempt}/{max_attempts}", 
                            details={"original_job_id": original_job_id, "current_retry_job_id": current_job_id_to_retry})
            logger.log_policy_event({
                **policy_event_base,
                "target_resource": f"bq_job:{current_job_id_to_retry}", # Update target for this attempt
                "current_status": "RECOVERY_ATTEMPTED",
                "action_taken": "ATTEMPT_BQ_JOB_RETRY",
                "action_parameters": json.dumps({"attempt": attempt, "original_job_id": original_job_id, "retrying_job_id": current_job_id_to_retry})
            })
            
            try:
                retry_result = db_handler.retry_job(current_job_id_to_retry, location=bq_location)
                new_job_id = retry_result["new_job_id"]
                logger.log_info(f"Retry job {new_job_id} submitted for original {current_job_id_to_retry}. Waiting for completion...",
                                details={"new_job_id": new_job_id, "attempt": attempt})

                # Wait and check status of the new job
                # This loop needs a timeout mechanism in a real scenario
                wait_time_seconds = 30 
                max_job_wait_attempts = 10 # e.g., 10 * 30s = 5 minutes
                for _ in range(max_job_wait_attempts):
                    time.sleep(wait_time_seconds)
                    new_job_status_info = db_handler.get_job_status(new_job_id, location=bq_location)
                    if new_job_status_info["state"] == "DONE":
                        if new_job_status_info.get("error_result") is None:
                            retry_successful = True
                        else:
                            last_exception = Exception(f"Retried job {new_job_id} failed: {new_job_status_info['error_result']}")
                            logger.log_warning(f"Retried BQ job {new_job_id} (attempt {attempt}) failed.", error=last_exception, details=new_job_status_info)
                            current_job_id_to_retry = new_job_id # For the next retry, use this failed new job ID
                        break # Exit wait loop
                else: # Loop finished without break (job didn't complete in time)
                    last_exception = TimeoutError(f"Retried job {new_job_id} did not complete in the allotted time.")
                    logger.log_warning(f"Retried BQ job {new_job_id} (attempt {attempt}) timed out.", error=last_exception)
                    current_job_id_to_retry = new_job_id
                
                if retry_successful:
                    break # Exit retry attempts loop

            except NotImplementedError as e: # If db_handler.retry_job doesn't support the job type
                last_exception = e
                logger.log_error(f"Cannot retry job {current_job_id_to_retry}: {e}", error=e)
                # No point in further retries if the type isn't supported
                break 
            except Exception as e:
                last_exception = e
                logger.log_error(f"Error during BQ job retry attempt {attempt} for {current_job_id_to_retry}.", error=e)
                # Consider if the new job ID should become current_job_id_to_retry if submission itself failed vs. execution
            
            if attempt < max_attempts and not retry_successful:
                sleep_duration = base_delay * (2**(attempt-1))
                sleep_duration = min(sleep_duration, max_delay) # Cap wait time
                logger.log_info(f"Waiting {sleep_duration}s before next BQ retry attempt.", details={"current_job_id_to_retry": current_job_id_to_retry})
                time.sleep(sleep_duration)

        if retry_successful and new_job_status_info:
            success_msg = f"BQ job retry successful for original job {original_job_id}. New job: {new_job_status_info['job_id']}."
            logger.log_info(success_msg, details=new_job_status_info)
            logger.log_policy_event({
                **policy_event_base,
                "target_resource": f"bq_job:{new_job_status_info['job_id']}",
                "current_status": "RECOVERY_SUCCESSFUL",
                "action_taken": "BQ_JOB_RETRY_COMPLETED",
                "action_result": "SUCCESS",
                "action_parameters": json.dumps({"new_job_id": new_job_status_info['job_id'], "original_job_id": original_job_id})
            })
        else:
            error_msg = f"BQ job retry finally failed for original job {original_job_id} after {max_attempts} attempts."
            logger.log_error(error_msg, error=last_exception, details=event_details)
            alerter.send_alert(
                subject=f"SelfHealing CRITICAL: BQ Job Retry Failed Permanently",
                body=f"{error_msg}\nOriginal Job ID: {original_job_id}\nLast Error: {last_exception}",
                severity="CRITICAL",
                details={**event_details, "final_error": str(last_exception)}
            )
            logger.log_policy_event({
                **policy_event_base,
                "current_status": "RECOVERY_FAILED",
                "action_taken": "ALERT_ON_FINAL_BQ_JOB_RETRY_FAILURE",
                "action_result": "FAILURE",
                "error_message": str(last_exception)
            })


class BigQueryDLQHandler(AbstractPolicyEventHandler):
    """
    Handles moving error files from BigQuery load jobs to a designated GCS DLQ path.
    """
    EVENT_TYPE_BQ_LOAD_JOB_ERRORS_FOR_DLQ = "BQ_LOAD_JOB_ERRORS_FOR_DLQ"

    def can_handle(self, event_type: str, event_details: Dict[str, Any]) -> bool:
        return event_type == self.EVENT_TYPE_BQ_LOAD_JOB_ERRORS_FOR_DLQ

    def handle_event(self, 
                     event_type: str, 
                     event_details: Dict[str, Any], 
                     config_manager: ConfigManager, 
                     logger: AbstractLogger, 
                     alerter: AbstractAlertManager, 
                     db_handler: Optional[AbstractDatabaseHandler], # Not used by this handler
                     storage_handler: Optional[AbstractStorageHandler]) -> None:

        if not storage_handler:
            logger.log_error("BigQueryDLQHandler: Storage handler not provided.", details=event_details)
            return

        original_job_id = event_details.get("job_id")
        # error_files_gcs_paths should be a list of GCS paths to the error files
        # generated by the BQ load job (e.g., when max_bad_records > 0)
        error_files_gcs_paths: List[str] = event_details.get("error_files_gcs_paths", [])
        
        correlation_id = event_details.get("correlation_id", str(uuid.uuid4()))
        policy_event_base = {
            "correlation_id": correlation_id,
            "policy_id": "BQ_LOAD_JOB_DLQ_POLICY",
            "target_resource": f"bq_job:{original_job_id}", # The job that produced errors
            "pipeline_stage": event_details.get("pipeline_stage", "BIGQUERY_LOAD_DLQ"),
            "detected_issue_type": event_type,
            "detected_issue_details": f"Load job {original_job_id} produced error files for DLQ.",
            "python_module_invoked": f"{self.__class__.__module__}.{self.__class__.__name__}"
        }


        if not original_job_id or not error_files_gcs_paths:
            logger.log_error("BigQueryDLQHandler: Missing 'job_id' or 'error_files_gcs_paths' in event_details.", details=event_details)
            alerter.send_alert(subject="SelfHealing Misconfig: BQDLQHandler", body="Missing job_id or error_files_gcs_paths.", severity="ERROR", details=event_details)
            logger.log_policy_event({**policy_event_base, "current_status": "HANDLER_FAILED_CONFIG", "action_result": "FAILURE", "error_message": "Missing job_id or error_files_gcs_paths."})
            return

        try:
            # Assuming DLQ path is defined per layer, e.g., silver
            # The layer might need to be part of event_details or inferred
            layer_name = event_details.get("bq_layer", "silver") # e.g., "silver_layer" or "silver"
            bq_layer_settings = config_manager.get_bigquery_settings(layer_name + "_layer") # e.g. silver_layer
            
            # The DLQ path from config is for the *rejected records* themselves, not BQ error files.
            # We might need a separate config for "BQ job error files DLQ".
            # For now, let's assume we use the same GCS path configured for general DLQ for that layer.
            dlq_gcs_path_prefix = bq_layer_settings.get("dlq_gcs_path") # e.g., "gs://my-project-silver-dlq/rejected_records/"
            
            if not dlq_gcs_path_prefix:
                raise ConfigManagerError(f"BigQuery DLQ GCS path (dlq_gcs_path) not configured for layer '{layer_name}'.")

            if not dlq_gcs_path_prefix.startswith("gs://"):
                raise ConfigManagerError(f"DLQ GCS path '{dlq_gcs_path_prefix}' must be a gs:// path.")
            
            # Extract DLQ bucket and base prefix from the configured path
            dlq_path_parts = dlq_gcs_path_prefix.replace("gs://", "").split("/", 1)
            dlq_bucket_name = dlq_path_parts[0]
            dlq_base_object_prefix = dlq_path_parts[1] if len(dlq_path_parts) > 1 else ""
            if not dlq_base_object_prefix.endswith("/"):
                dlq_base_object_prefix += "/"

        except ConfigManagerError as e:
            logger.log_error(f"BigQueryDLQHandler: Failed to get BQ DLQ config: {e}", error=e, details=event_details)
            alerter.send_alert(subject="SelfHealing Config Error: BQDLQHandler", body=f"Could not retrieve BQ DLQ GCS path. Error: {e}", severity="ERROR", details=event_details)
            logger.log_policy_event({**policy_event_base, "current_status": "HANDLER_FAILED_CONFIG", "action_result": "FAILURE", "error_message": f"ConfigManagerError: {e}"})
            return

        moved_files_count = 0
        dlq_paths_created = []
        errors_occurred = False

        for error_file_gcs_path in error_files_gcs_paths:
            try:
                if not error_file_gcs_path.startswith("gs://"):
                    logger.log_warning(f"Skipping invalid GCS path for error file: {error_file_gcs_path}", details=event_details)
                    continue

                path_parts = error_file_gcs_path.replace("gs://", "").split("/", 1)
                source_bucket = path_parts[0]
                source_object = path_parts[1]
                
                # Construct a more specific DLQ object name for BQ error files
                # e.g., <dlq_base_prefix>/<original_job_id>/<original_filename>_timestamp_uuid
                original_filename = source_object.split('/')[-1]
                timestamp_str = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d%H%M%S")
                unique_id = uuid.uuid4().hex[:8]
                dlq_object_name = f"{dlq_base_object_prefix}bq_job_error_files/{original_job_id}/{original_filename}_{timestamp_str}_{unique_id}"

                # Use GCSStorageHandler.move_to_dlq by providing its own DLQ bucket and the constructed name
                # This assumes move_to_dlq can handle a target bucket and full object name.
                # Let's refine: we need to copy from error_file_gcs_path to the DLQ bucket/path.
                # The existing move_to_dlq is designed to move *from* a source *to* a configured DLQ.
                # Here, the "source" is already an error file. We want to archive it.
                # Re-using move_to_dlq by passing the error file's bucket/object as source,
                # and the target DLQ bucket/prefix is configured in GCSStorageHandler or derived here.
                
                # Simpler: copy and delete, or just copy if they are already in a temp GCS location.
                # For now, let's assume storage_handler.move_to_dlq is smart enough or we adapt.
                # We need a way to specify the *target* DLQ bucket/path in move_to_dlq.
                # The current GCSStorageHandler.move_to_dlq gets DLQ from config.
                # Let's assume it uses a *general* DLQ bucket and constructs path.
                # We can pass the original_job_id in error_details for path construction.
                
                dlq_details_for_move = {
                    "original_job_id": original_job_id,
                    "error_file_source_path": error_file_gcs_path,
                    "pipeline_stage": policy_event_base["pipeline_stage"],
                    "detected_issue_type": "BQ_JOB_ERROR_FILE" 
                }
                
                # The GCSStorageHandler's move_to_dlq will use its configured DLQ bucket.
                # The dlq_details_for_move will help it construct a meaningful path.
                created_dlq_path = storage_handler.move_to_dlq(source_bucket, source_object, error_details=dlq_details_for_move)
                
                dlq_paths_created.append(created_dlq_path)
                moved_files_count += 1
                logger.log_info(f"BQ error file {error_file_gcs_path} moved to DLQ: {created_dlq_path}", details=event_details)

            except Exception as e:
                errors_occurred = True
                logger.log_error(f"Failed to move BQ error file {error_file_gcs_path} to DLQ.", error=e, details=event_details)
                # Alert for individual file move failure
                alerter.send_alert(
                    subject=f"SelfHealing WARNING: Failed to move BQ error file to DLQ",
                    body=f"File: {error_file_gcs_path}\nJob ID: {original_job_id}\nError: {e}",
                    severity="WARNING",
                    details={**event_details, "failed_file": error_file_gcs_path, "error": str(e)}
                )
        
        final_status = "RECOVERY_SUCCESSFUL" if moved_files_count > 0 and not errors_occurred else \
                       "RECOVERY_PARTIAL_SUCCESS" if moved_files_count > 0 and errors_occurred else \
                       "RECOVERY_FAILED"
        
        summary_message = f"BigQueryDLQHandler processed {len(error_files_gcs_paths)} error files for job {original_job_id}. Moved {moved_files_count} to DLQ."
        if errors_occurred:
            summary_message += " Some errors occurred during processing."
        
        logger.log_info(summary_message, details={"dlq_paths_created": dlq_paths_created, "errors_occurred": errors_occurred})
        logger.log_policy_event({
            **policy_event_base,
            "current_status": final_status,
            "action_taken": "PROCESSED_BQ_ERROR_FILES_FOR_DLQ",
            "action_parameters": json.dumps({"total_files": len(error_files_gcs_paths), "moved_count": moved_files_count, "dlq_paths": dlq_paths_created}),
            "action_result": "SUCCESS" if final_status != "RECOVERY_FAILED" else "FAILURE",
            "error_message": "Errors occurred during DLQ processing of some files." if errors_occurred else None
        })

        # Overall alert
        alert_severity = "INFO"
        if errors_occurred and moved_files_count == 0:
            alert_severity = "ERROR"
        elif errors_occurred:
            alert_severity = "WARNING"
            
        alerter.send_alert(
            subject=f"SelfHealing {alert_severity}: BQ Load Job DLQ Processing for {original_job_id}",
            body=summary_message,
            severity=alert_severity,
            details={**event_details, "moved_files_count": moved_files_count, "total_error_files": len(error_files_gcs_paths), "dlq_paths_created": dlq_paths_created}
        )
