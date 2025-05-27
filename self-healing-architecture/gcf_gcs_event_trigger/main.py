# --- Trigger Configuration (Comment) ---
# This Cloud Function is intended to be triggered by Google Cloud Storage events.
# Specifically, it can be configured for 'google.storage.object.finalize' events 
# (i.e., object creation, including overwrites).
#
# Example Trigger Scenarios:
# 1. Files landing in a specific GCS path designated for failed uploads that need retrying.
#    - GCS Path: gs://<your-bucket>/errors/needs_retry/*
#    - The GCF would then parse the event and trigger a 'GCS_UPLOAD_FAILED_FOR_RETRY_FROM_GCF'
#      event for the PolicyEngine.
# 2. Files landing in a path indicating an original file should be moved to DLQ.
#    - GCS Path: gs://<your-bucket>/dlq_triggers/*
#    - The GCF would parse the event (e.g., the filename might indicate the original file to DLQ)
#      and trigger a 'GCS_MOVE_TO_DLQ_FROM_GCF' event.
#
# The environment variable POLICY_CONFIG_BASE_PATH can be set in the GCF environment
# to specify the root directory of the self-healing-architecture project if the
# default relative pathing does not work in the GCF environment.
#
# Required Environment Variables for GCF (examples):
# - GCP_PROJECT: Your Google Cloud Project ID.
# - POLICY_CONFIG_PATH: (Optional) Absolute path to policy_config.yaml if not using default.
#   If not set, defaults to ../config/policy_config.yaml relative to this file's parent dir.
# -------------------------------------------

import os
import sys
import json # For formatting event_details in logs if needed
from pathlib import Path

# Adjust path to import from the main project src
# This assumes GCF's execution environment allows this structure, 
# or that a deployment script packages things appropriately.
current_file_path = Path(__file__).resolve()
project_root = current_file_path.parent.parent # self-healing-architecture directory
src_path = project_root / 'src'
sys.path.append(str(src_path))

from core.config_manager import ConfigManager, ConfigManagerError
from core.engine import PolicyEngine
from services.loggers import ConsoleLogger, BigQueryLogger
from services.alerters import EmailAlertManager
from services.storage_handlers import GCSStorageHandler
from services.database_handlers import BigQueryDatabaseHandler
from handlers.storage_event_handlers import GCSUploadRetryHandler, GCSDeadLetterHandler
from handlers.database_event_handlers import BigQueryJobRetryHandler, BigQueryDLQHandler
from handlers.cdc_event_handlers import CDCLagAlertHandler
from handlers.stream_event_handlers import StreamingDLQAlertHandler

# --- Global Initialization (managed by GCF lifecycle) ---
CONFIG_PATH_ENV_VAR = 'POLICY_CONFIG_PATH'
DEFAULT_CONFIG_PATH_RELATIVE_TO_PROJECT_ROOT = Path('config') / 'policy_config.yaml'

config_manager = None
policy_engine = None
initialization_error = None
console_logger_global = ConsoleLogger() # For logging init errors if full logger fails

try:
    # Determine config path
    env_config_path = os.getenv(CONFIG_PATH_ENV_VAR)
    if env_config_path:
        config_file_path = Path(env_config_path)
    else:
        config_file_path = project_root / DEFAULT_CONFIG_PATH_RELATIVE_TO_PROJECT_ROOT
    
    if not config_file_path.exists():
        raise ConfigManagerError(f"Policy configuration file not found at {config_file_path}. Set {CONFIG_PATH_ENV_VAR} or ensure default path is correct.")

    config_manager = ConfigManager(config_path=str(config_file_path))
    
    # Initialize Loggers
    # BigQueryLogger might need project_id from config or env
    gcp_project_id_from_config = config_manager.get_parameter("global_settings", "gcp_project_id", default=None)
    gcp_project_id = gcp_project_id_from_config or os.getenv('GCP_PROJECT') or os.getenv('GOOGLE_CLOUD_PROJECT')

    if not gcp_project_id:
        console_logger_global.log_warning("GCP Project ID not found in config (global_settings.gcp_project_id) or environment (GCP_PROJECT/GOOGLE_CLOUD_PROJECT). BigQueryLogger may be impaired.")

    bigquery_logger = BigQueryLogger(config_manager=config_manager, project_id=gcp_project_id, fallback_logger=console_logger_global)
    
    # Initialize Alerter
    alerter = EmailAlertManager(config_manager=config_manager, fallback_logger=console_logger_global)

    # Initialize Service Handlers
    gcs_storage_handler = GCSStorageHandler(config_manager=config_manager, logger=bigquery_logger, project_id=gcp_project_id)
    bq_db_handler = BigQueryDatabaseHandler(config_manager=config_manager, logger=bigquery_logger, project_id=gcp_project_id)

    # Initialize Policy Engine
    policy_engine = PolicyEngine(
        config_manager=config_manager,
        logger=bigquery_logger, 
        alerter=alerter,
        db_handler=bq_db_handler,
        storage_handler=gcs_storage_handler
    )

    # Register all implemented handlers
    policy_engine.register_handler(GCSUploadRetryHandler())
    policy_engine.register_handler(GCSDeadLetterHandler())
    policy_engine.register_handler(BigQueryJobRetryHandler())
    policy_engine.register_handler(BigQueryDLQHandler())
    policy_engine.register_handler(CDCLagAlertHandler())
    policy_engine.register_handler(StreamingDLQAlertHandler())
    
    console_logger_global.log_info("PolicyEngine GCF initialized successfully.")
    
except Exception as e:
    initialization_error = e
    console_logger_global.log_error(f"CRITICAL: PolicyEngine GCF failed to initialize: {e}", error=e)
    policy_engine = None # Ensure it's None if init fails

# --- Cloud Function Entry Point ---
def gcs_event_policy_trigger(event: Dict[str, Any], context: Any): # context type is google.cloud.functions.Context
    """
    Triggered by a change to a GCS bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    # Use the globally initialized console_logger for GCF's own operational logs
    console_logger_global.log_info(f"GCF Event ID: {context.event_id}, Event Type: {context.event_type}, Bucket: {event.get('bucket')}, File: {event.get('name')}")
    # console_logger_global.log_info(f"GCF Event Data: {json.dumps(event, default=str)}") # Can be very verbose

    if initialization_error:
        console_logger_global.log_error(f"CRITICAL: GCF cannot process event {context.event_id} due to initialization error: {initialization_error}", error=initialization_error)
        # Depending on GCF retry policy, this might be retried.
        # Raising an exception can trigger GCF's retry mechanisms.
        raise RuntimeError(f"GCF Initialization Failed: {initialization_error}")

    if not policy_engine or not config_manager: # Should not happen if init_error is not None, but as a safeguard
        console_logger_global.log_error("CRITICAL: PolicyEngine or ConfigManager not available in GCF execution.")
        raise RuntimeError("GCF critical component not initialized.")

    bucket_name = event.get('bucket')
    object_name = event.get('name')
    time_created = event.get('timeCreated')
    correlation_id = f"gcf_{context.event_id}" # Use GCF event ID for correlation

    processed_event = False
    try:
        # Example rule: Files in a path like "errors/needs_retry/original_bucket/original_object_name.txt"
        # The content of this trigger file could be metadata about the original failed file.
        # For simplicity, we'll use prefixes from config.
        
        # Path for triggering GCS Upload Retries
        # Example: gs://my-bucket/self-healing-triggers/gcs_upload_failed/source_bucket/path/to/original_file.csv
        # The GCF would detect this, extract 'source_bucket' and 'path/to/original_file.csv'
        # and the event_details would need to include 'source_path' (local path if applicable, or GCS path for copy)
        # and 'destination_object_name'. This is complex if the GCF needs to download content or read metadata.

        # Simpler example: GCS_UPLOAD_FAILED event implies a local file failed to upload,
        # and a "receipt" or "error marker" file was placed in GCS, which triggers this GCF.
        # The event_details would need to contain what the GCSUploadRetryHandler expects.
        
        # Let's assume GCS event directly refers to a file that itself is an error marker or needs action.
        # This configuration should be in policy_config.yaml
        gcs_error_prefix = config_manager.get_parameter("gcs_settings", "error_file_prefix_for_retry", default="errors/needs_retry/")
        gcs_dlq_trigger_prefix = config_manager.get_parameter("gcs_settings", "dlq_trigger_file_prefix", default="dlq_triggers/")

        if object_name and object_name.startswith(gcs_error_prefix):
            # This event implies the GCS object *is* the one that failed or represents the failure.
            # GCSUploadRetryHandler expects 'bucket_name', 'source_path' (local), 'destination_object_name'.
            # This GCF trigger is for an *existing* GCS object, so it's not a direct fit for GCSUploadRetryHandler
            # unless we interpret `object_name` as a marker whose content tells us about a local file.
            # For now, let's adapt it to mean: "this GCS object itself failed some prior validation and needs DLQ"
            # OR "this GCS object is a trigger file whose name/content points to another GCS object to retry"
            
            # Let's assume a different event type for GCF-originated GCS issues for clarity.
            event_type = 'GCS_FILE_REQUIRES_ACTION_FROM_GCF' 
            event_details = {
                "correlation_id": correlation_id,
                "bucket_name": bucket_name,
                "object_name": object_name, 
                "timeCreated": time_created,
                "source_trigger": "GCF_GCS_EVENT",
                "message": "GCS object created in monitored error path, requires policy engine evaluation.",
                # Further parsing of object_name or metadata could refine event_type & details
                # For example, if object_name is "errors/needs_retry/my_source_file.txt_FAILED_UPLOAD_ATTEMPT"
                # This could be parsed to try and find "my_source_file.txt" if it's still local.
                # This is highly dependent on the upstream process that creates these error files.
            }
            # This generic event would need a new handler or specific logic in existing ones.
            # Let's assume for now that GCSDeadLetterHandler might pick this up if no retry is possible.
            console_logger_global.log_info(f"Processing {event_type} for {object_name}")
            policy_engine.process_event(event_type, event_details)
            processed_event = True
        
        elif object_name and object_name.startswith(gcs_dlq_trigger_prefix):
            event_type = GCSDeadLetterHandler.EVENT_TYPE_GCS_MOVE_TO_DLQ
            # Assuming the object_name (after prefix) is the actual file that needs to be moved to DLQ
            # e.g., dlq_triggers/source_bucket_name/actual_object_name
            # This requires parsing `object_name` to get the true source bucket and object.
            # This example is simplified: assuming the event file *is* the one to DLQ
            # or its name directly implies the target (which is more complex).
            
            # Simplification: assume the file itself that triggered the event is the one to be moved.
            # This would require the GCSDeadLetterHandler to know the *source* bucket/object.
            # The event only gives bucket/object of the *trigger* file.
            # A more robust way: trigger file content has details of the actual file to DLQ.
            # For this example, let's assume the trigger file means "the file that just landed *is* the error file"
            # and it needs moving from its current location (the trigger path) to the actual DLQ.
            
            actual_object_to_dlq = object_name # Simplified: the trigger file itself is moved
            
            event_details = {
                "correlation_id": correlation_id,
                "source_bucket_name": bucket_name, # Bucket where the trigger file landed
                "source_object_name": actual_object_to_dlq, # The trigger file itself
                "timeCreated": time_created,
                "source_trigger": "GCF_GCS_EVENT_DLQ_TRIGGER",
                "pipeline_stage": "GCF_TRIGGERED_DLQ",
                "original_error_details": {"reason": "DLQ trigger file received in GCS", "trigger_file": object_name}
            }
            console_logger_global.log_info(f"Processing {event_type} for {actual_object_to_dlq}")
            policy_engine.process_event(event_type, event_details)
            processed_event = True
            
        else:
            console_logger_global.log_info(f"GCS Event for gs://{bucket_name}/{object_name} did not match specific GCF processing rules. No policy engine event triggered by GCF logic itself.")
            # Optionally, log this to BQ as an "unhandled GCF event" or generic GCS event for audit.
            # policy_engine.logger.log_policy_event({...})

    except Exception as e:
        console_logger_global.log_error(f"Error processing GCS event in GCF: {e}", error=e, details={"event_id": context.event_id, "event_data": event})
        # Alert on GCF processing failure
        if policy_engine and hasattr(policy_engine, 'alerter') and policy_engine.alerter:
            try:
                policy_engine.alerter.send_alert(
                    subject="SelfHealing CRITICAL: GCF Event Processing Failed",
                    body=f"Cloud Function failed to process GCS event.\nError: {str(e)}\nEventID: {context.event_id}\nFile: gs://{bucket_name}/{object_name}",
                    severity="CRITICAL",
                    details={"gcf_event_id": context.event_id, "gcs_file": f"gs://{bucket_name}/{object_name}", "error": str(e)}
                )
            except Exception as alert_e:
                 console_logger_global.log_error("Additionally, failed to send alert about GCF processing failure.", error=alert_e)
        # Re-raise to potentially trigger GCF retries for the event
        raise
    
    if processed_event:
        console_logger_global.log_info(f"GCF event {context.event_id} for gs://{bucket_name}/{object_name} processed by PolicyEngine.")
    else:
        console_logger_global.log_info(f"GCF event {context.event_id} for gs://{bucket_name}/{object_name} did not trigger a specific policy engine action via GCF rules.")

# Example (for local testing if you could simulate event and context):
# if __name__ == '__main__':
#     class MockContext:
#         event_id = 'test-event-id'
#         event_type = 'google.storage.object.finalize'
#     mock_event_data = {
#         'bucket': 'my-test-bucket',
#         'name': 'errors/needs_retry/some_failed_file.txt',
#         'timeCreated': '2024-03-18T10:00:00Z'
#     }
#     gcs_event_policy_trigger(mock_event_data, MockContext())

#     mock_event_data_dlq = {
#         'bucket': 'my-test-bucket',
#         'name': 'dlq_triggers/sourceA/original_file.csv', # Example, implies sourceA/original_file.csv is the target for DLQ
#         'timeCreated': '2024-03-18T10:05:00Z'
#     }
#     gcs_event_policy_trigger(mock_event_data_dlq, MockContext())
#     print("Local GCF test run finished.")
