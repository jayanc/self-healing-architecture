import pytest
from unittest.mock import MagicMock, call, ANY # ANY is useful for some BQ client calls
import json

from core.config_manager import ConfigManager
from core.interfaces import AbstractLogger, AbstractAlertManager
from handlers.cdc_event_handlers import CDCLagAlertHandler
from handlers.stream_event_handlers import StreamingDLQAlertHandler

# Fixture from conftest.py: config_manager_instance

@pytest.fixture
def mock_logger(mocker: MagicMock) -> MagicMock:
    return mocker.MagicMock(spec=AbstractLogger)

@pytest.fixture
def mock_alerter(mocker: MagicMock) -> MagicMock:
    return mocker.MagicMock(spec=AbstractAlertManager)

# --- CDCLagAlertHandler Tests ---

@pytest.fixture
def cdc_lag_event_details() -> dict:
    return {
        "source_name": "postgres_source_main_db",
        "replication_lag_minutes": 150,
        "lag_threshold_minutes": 120,
        "pipeline_stage": "CDC_MONITOR",
        "correlation_id": "corr_cdc_lag_001"
    }

def test_cdc_lag_alert_handler_can_handle(cdc_lag_event_details: dict):
    handler = CDCLagAlertHandler()
    assert handler.can_handle(CDCLagAlertHandler.EVENT_TYPE_CDC_LAG_HIGH, cdc_lag_event_details)
    assert not handler.can_handle("SOME_OTHER_EVENT", cdc_lag_event_details)

def test_cdc_lag_alert_handler_sends_alert_and_logs_policy_event(
    config_manager_instance: ConfigManager, # Using the actual config instance from conftest
    mock_logger: MagicMock,
    mock_alerter: MagicMock,
    cdc_lag_event_details: dict
):
    handler = CDCLagAlertHandler()
    handler.handle_event(
        CDCLagAlertHandler.EVENT_TYPE_CDC_LAG_HIGH,
        cdc_lag_event_details,
        config_manager_instance,
        mock_logger,
        mock_alerter,
        db_handler=None, # Not used
        storage_handler=None # Not used
    )

    # Verify policy event logging
    mock_logger.log_policy_event.assert_called_once()
    policy_event_arg = mock_logger.log_policy_event.call_args[0][0]
    assert policy_event_arg["policy_id"] == "CDC_LAG_ALERT_POLICY"
    assert policy_event_arg["target_resource"] == f"cdc_source:{cdc_lag_event_details['source_name']}"
    assert policy_event_arg["current_status"] == "ALERT_SENT"
    assert f"exceeding threshold of {cdc_lag_event_details['lag_threshold_minutes']}" in policy_event_arg["detected_issue_details"]

    # Verify alert sending
    mock_alerter.send_alert.assert_called_once()
    alert_args, alert_kwargs = mock_alerter.send_alert.call_args
    assert alert_kwargs["subject"] == f"SelfHealing ALERT: High CDC Replication Lag for {cdc_lag_event_details['source_name']}"
    assert str(cdc_lag_event_details["replication_lag_minutes"]) in alert_kwargs["body"]
    assert str(cdc_lag_event_details["lag_threshold_minutes"]) in alert_kwargs["body"]
    
    # Check severity (default is WARNING if not configured otherwise)
    # To make this more robust, mock get_alerting_config or ensure the test config has the key
    expected_severity = config_manager_instance.get_alerting_config().get("thresholds", {}).get("cdc_lag_high_severity", "WARNING")
    assert alert_kwargs["severity"] == expected_severity
    assert alert_kwargs["details"]["source_name"] == cdc_lag_event_details["source_name"]


def test_cdc_lag_alert_handler_alert_failure(
    config_manager_instance: ConfigManager,
    mock_logger: MagicMock,
    mock_alerter: MagicMock,
    cdc_lag_event_details: dict
):
    mock_alerter.send_alert.side_effect = Exception("SMTP server down")
    
    handler = CDCLagAlertHandler()
    handler.handle_event(
        CDCLagAlertHandler.EVENT_TYPE_CDC_LAG_HIGH,
        cdc_lag_event_details,
        config_manager_instance,
        mock_logger,
        mock_alerter,
        db_handler=None,
        storage_handler=None
    )

    # Initial policy event log + updated one after failure
    assert mock_logger.log_policy_event.call_count == 2
    final_policy_event_call = mock_logger.log_policy_event.call_args_list[-1][0][0]
    assert final_policy_event_call["current_status"] == "ALERT_FAILED"
    assert "SMTP server down" in final_policy_event_call["error_message"]
    
    mock_logger.log_error.assert_called_once() # From the handler catching the alerter error
    assert "Failed to send CDC lag alert" in mock_logger.log_error.call_args[0][0]


# --- StreamingDLQAlertHandler Tests ---

@pytest.fixture
def stream_dlq_event_details() -> dict:
    return {
        "stream_name": "order_events_stream",
        "dlq_path": "projects/my-proj/topics/order_events_dlq",
        "message_count": 150,
        "dlq_threshold": 100,
        "pipeline_stage": "STREAMING_INGEST_ORDERS",
        "correlation_id": "corr_stream_dlq_002"
    }

def test_streaming_dlq_alert_handler_can_handle(stream_dlq_event_details: dict):
    handler = StreamingDLQAlertHandler()
    assert handler.can_handle(StreamingDLQAlertHandler.EVENT_TYPE_STREAM_DLQ_THRESHOLD_REACHED, stream_dlq_event_details)
    assert not handler.can_handle("SOME_OTHER_EVENT", stream_dlq_event_details)

def test_streaming_dlq_alert_handler_sends_alert_and_logs_policy_event(
    config_manager_instance: ConfigManager,
    mock_logger: MagicMock,
    mock_alerter: MagicMock,
    stream_dlq_event_details: dict
):
    handler = StreamingDLQAlertHandler()
    handler.handle_event(
        StreamingDLQAlertHandler.EVENT_TYPE_STREAM_DLQ_THRESHOLD_REACHED,
        stream_dlq_event_details,
        config_manager_instance,
        mock_logger,
        mock_alerter,
        db_handler=None, 
        storage_handler=None 
    )

    mock_logger.log_policy_event.assert_called_once()
    policy_event_arg = mock_logger.log_policy_event.call_args[0][0]
    assert policy_event_arg["policy_id"] == "STREAM_DLQ_ALERT_POLICY"
    assert policy_event_arg["target_resource"] == f"stream:{stream_dlq_event_details['stream_name']}/dlq:{stream_dlq_event_details['dlq_path']}"
    assert policy_event_arg["current_status"] == "ALERT_SENT"
    assert f"Current messages: {stream_dlq_event_details['message_count']}" in policy_event_arg["detected_issue_details"]

    mock_alerter.send_alert.assert_called_once()
    alert_args, alert_kwargs = mock_alerter.send_alert.call_args
    assert alert_kwargs["subject"] == f"SelfHealing ALERT: Streaming DLQ Threshold Reached for {stream_dlq_event_details['stream_name']}"
    assert str(stream_dlq_event_details["message_count"]) in alert_kwargs["body"]
    assert str(stream_dlq_event_details["dlq_threshold"]) in alert_kwargs["body"]
    
    expected_severity = config_manager_instance.get_alerting_config().get("thresholds", {}).get("stream_dlq_threshold_severity", "WARNING")
    assert alert_kwargs["severity"] == expected_severity
    assert alert_kwargs["details"]["stream_name"] == stream_dlq_event_details["stream_name"]

def test_streaming_dlq_alert_handler_alert_failure(
    config_manager_instance: ConfigManager,
    mock_logger: MagicMock,
    mock_alerter: MagicMock,
    stream_dlq_event_details: dict
):
    mock_alerter.send_alert.side_effect = Exception("Network issue sending alert")

    handler = StreamingDLQAlertHandler()
    handler.handle_event(
        StreamingDLQAlertHandler.EVENT_TYPE_STREAM_DLQ_THRESHOLD_REACHED,
        stream_dlq_event_details,
        config_manager_instance,
        mock_logger,
        mock_alerter,
        db_handler=None,
        storage_handler=None
    )

    assert mock_logger.log_policy_event.call_count == 2
    final_policy_event_call = mock_logger.log_policy_event.call_args_list[-1][0][0]
    assert final_policy_event_call["current_status"] == "ALERT_FAILED"
    assert "Network issue sending alert" in final_policy_event_call["error_message"]

    mock_logger.log_error.assert_called_once()
    assert "Failed to send Streaming DLQ threshold alert" in mock_logger.log_error.call_args[0][0]
