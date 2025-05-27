import uuid
import json
from typing import Dict, Any, Optional

from ..core.interfaces import (
    AbstractCDCEventHandler, 
    AbstractLogger, 
    AbstractAlertManager,
    AbstractDatabaseHandler, # Included for interface consistency, though not used by this specific handler
    AbstractStorageHandler   # Included for interface consistency
)
from ..core.config_manager import ConfigManager # Included for interface consistency

class CDCLagAlertHandler(AbstractCDCEventHandler):
    """
    Handles alerting for high CDC replication lag.
    """
    EVENT_TYPE_CDC_LAG_HIGH = "CDC_REPLICATION_LAG_HIGH"

    def can_handle(self, event_type: str, event_details: Dict[str, Any]) -> bool:
        return event_type == self.EVENT_TYPE_CDC_LAG_HIGH

    def handle_event(self, 
                     event_type: str, 
                     event_details: Dict[str, Any], 
                     config_manager: ConfigManager, 
                     logger: AbstractLogger, 
                     alerter: AbstractAlertManager, 
                     db_handler: Optional[AbstractDatabaseHandler],
                     storage_handler: Optional[AbstractStorageHandler]) -> None:
        
        correlation_id = event_details.get("correlation_id", str(uuid.uuid4()))
        source_name = event_details.get("source_name", "Unknown CDC Source")
        replication_lag_minutes = event_details.get("replication_lag_minutes", "N/A")
        lag_threshold_minutes = event_details.get("lag_threshold_minutes", "N/A")
        
        policy_event_details = {
            "correlation_id": correlation_id,
            "policy_id": "CDC_LAG_ALERT_POLICY",
            "target_resource": f"cdc_source:{source_name}",
            "pipeline_stage": event_details.get("pipeline_stage", "CDC_INGESTION_MONITORING"),
            "detected_issue_type": event_type,
            "detected_issue_details": f"CDC replication lag of {replication_lag_minutes} minutes detected for {source_name}, exceeding threshold of {lag_threshold_minutes} minutes.",
            "current_status": "ALERT_SENT", # This handler's action is to alert
            "action_taken": "SENT_CDC_LAG_ALERT",
            "action_parameters": json.dumps(event_details), # Log all incoming details
            "action_result": "SUCCESS", # Assuming alert sending itself is successful
            "python_module_invoked": f"{self.__class__.__module__}.{self.__class__.__name__}"
        }
        logger.log_policy_event(policy_event_details)

        alert_subject = f"SelfHealing ALERT: High CDC Replication Lag for {source_name}"
        alert_body = (
            f"High CDC replication lag detected for source: {source_name}.\n"
            f"Current Lag: {replication_lag_minutes} minutes.\n"
            f"Configured Threshold: {lag_threshold_minutes} minutes.\n\n"
            f"Please investigate the CDC process for this source.\n"
            f"Correlation ID: {correlation_id}"
        )
        
        alert_details_for_payload = {
            "source_name": source_name,
            "current_lag_minutes": replication_lag_minutes,
            "configured_threshold_minutes": lag_threshold_minutes,
            "correlation_id": correlation_id,
            **event_details # Include original event details
        }

        try:
            # Determine severity based on config, or default
            alerting_config = config_manager.get_alerting_config().get("thresholds", {})
            # Example: cdc_lag_high_severity: "WARNING" or "CRITICAL"
            severity = alerting_config.get("cdc_lag_high_severity", "WARNING") 

            alerter.send_alert(
                subject=alert_subject,
                body=alert_body,
                severity=severity,
                details=alert_details_for_payload
            )
            logger.log_info(f"CDC lag alert sent for source {source_name}.", details=alert_details_for_payload)
        except Exception as e:
            logger.log_error(f"Failed to send CDC lag alert for source {source_name}.", error=e, details=alert_details_for_payload)
            # Update policy event if alert sending failed
            policy_event_details.update({
                "current_status": "ALERT_FAILED",
                "action_result": "FAILURE",
                "error_message": f"Failed to send alert: {str(e)}"
            })
            # Re-log the event with failure status
            logger.log_policy_event(policy_event_details)
