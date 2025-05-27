import uuid
import json
from typing import Dict, Any, Optional

from ..core.interfaces import (
    AbstractStreamingEventHandler, 
    AbstractLogger, 
    AbstractAlertManager,
    AbstractDatabaseHandler, # Included for interface consistency
    AbstractStorageHandler   # Included for interface consistency
)
from ..core.config_manager import ConfigManager # Included for interface consistency

class StreamingDLQAlertHandler(AbstractStreamingEventHandler):
    """
    Handles alerting for streaming Dead Letter Queue (DLQ) thresholds being reached.
    """
    EVENT_TYPE_STREAM_DLQ_THRESHOLD_REACHED = "STREAM_DLQ_THRESHOLD_REACHED"

    def can_handle(self, event_type: str, event_details: Dict[str, Any]) -> bool:
        return event_type == self.EVENT_TYPE_STREAM_DLQ_THRESHOLD_REACHED

    def handle_event(self, 
                     event_type: str, 
                     event_details: Dict[str, Any], 
                     config_manager: ConfigManager, 
                     logger: AbstractLogger, 
                     alerter: AbstractAlertManager, 
                     db_handler: Optional[AbstractDatabaseHandler],
                     storage_handler: Optional[AbstractStorageHandler]) -> None:
        
        correlation_id = event_details.get("correlation_id", str(uuid.uuid4()))
        stream_name = event_details.get("stream_name", "Unknown Stream")
        dlq_path = event_details.get("dlq_path", "N/A")
        message_count = event_details.get("message_count", "N/A")
        dlq_threshold = event_details.get("dlq_threshold", "N/A")
        
        policy_event_details = {
            "correlation_id": correlation_id,
            "policy_id": "STREAM_DLQ_ALERT_POLICY",
            "target_resource": f"stream:{stream_name}/dlq:{dlq_path}",
            "pipeline_stage": event_details.get("pipeline_stage", "STREAMING_INGESTION_DLQ_MONITORING"),
            "detected_issue_type": event_type,
            "detected_issue_details": f"Streaming DLQ for {stream_name} at {dlq_path} reached threshold. Current messages: {message_count}, Threshold: {dlq_threshold}.",
            "current_status": "ALERT_SENT", # This handler's action is to alert
            "action_taken": "SENT_STREAM_DLQ_THRESHOLD_ALERT",
            "action_parameters": json.dumps(event_details), # Log all incoming details
            "action_result": "SUCCESS", # Assuming alert sending itself is successful
            "python_module_invoked": f"{self.__class__.__module__}.{self.__class__.__name__}"
        }
        logger.log_policy_event(policy_event_details)

        alert_subject = f"SelfHealing ALERT: Streaming DLQ Threshold Reached for {stream_name}"
        alert_body = (
            f"The Dead Letter Queue (DLQ) for streaming source: {stream_name} has reached its threshold.\n"
            f"DLQ Path/Topic: {dlq_path}\n"
            f"Current Message Count: {message_count}\n"
            f"Configured Threshold: {dlq_threshold}\n\n"
            f"Please investigate the messages in the DLQ and the health of the streaming consumers.\n"
            f"Correlation ID: {correlation_id}"
        )
        
        alert_details_for_payload = {
            "stream_name": stream_name,
            "dlq_path": dlq_path,
            "current_message_count": message_count,
            "configured_threshold": dlq_threshold,
            "correlation_id": correlation_id,
            **event_details # Include original event details
        }

        try:
            # Determine severity based on config, or default
            alerting_config = config_manager.get_alerting_config().get("thresholds", {})
            # Example: stream_dlq_threshold_severity: "WARNING" or "CRITICAL"
            severity = alerting_config.get("stream_dlq_threshold_severity", "WARNING") 

            alerter.send_alert(
                subject=alert_subject,
                body=alert_body,
                severity=severity,
                details=alert_details_for_payload
            )
            logger.log_info(f"Streaming DLQ threshold alert sent for stream {stream_name}.", details=alert_details_for_payload)
        except Exception as e:
            logger.log_error(f"Failed to send Streaming DLQ threshold alert for stream {stream_name}.", error=e, details=alert_details_for_payload)
            # Update policy event if alert sending failed
            policy_event_details.update({
                "current_status": "ALERT_FAILED",
                "action_result": "FAILURE",
                "error_message": f"Failed to send alert: {str(e)}"
            })
            # Re-log the event with failure status
            logger.log_policy_event(policy_event_details)
