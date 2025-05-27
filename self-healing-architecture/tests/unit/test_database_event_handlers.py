import pytest
from unittest.mock import MagicMock, call, patch, ANY
import time 
import json

from core.config_manager import ConfigManager
from core.interfaces import AbstractLogger, AbstractAlertManager, AbstractDatabaseHandler, AbstractStorageHandler
from handlers.database_event_handlers import BigQueryJobRetryHandler, BigQueryDLQHandler, TRANSIENT_BQ_ERROR_REASONS
# from services.database_handlers import BigQueryDatabaseHandler # For type hinting if needed

# Fixture from conftest.py: config_manager_instance
# We'll create a more specific one for these tests if needed, or use the global one.

@pytest.fixture
def mock_config_manager_for_bq_handlers(config_manager_instance: ConfigManager, mocker: MagicMock) -> MagicMock:
    """
    Mocks ConfigManager for BQ handler tests, ensuring relevant BQ settings are available.
    """
    mock_cm = mocker.MagicMock(spec=ConfigManager)
    
    # Fetch real BQ settings and provide them through the mock
    try:
        bq_settings_real = config_manager_instance.get_section("bigquery_settings")
        default_job_settings = bq_settings_real.get("default_job_settings", {
            "job_retry_attempts": 2, 
            "base_retry_delay_seconds": 1, 
            "max_retry_delay_seconds": 5
        })
        silver_settings = bq_settings_real.get("silver_layer", {
            "dlq_gcs_path": "gs://test-silver-dlq/rejected_records/" 
        })
        # To allow direct access like config_manager.get_bigquery_settings().get("default_job_settings")
        mock_cm.get_bigquery_settings.return_value = {
            "default_job_settings": default_job_settings,
            "silver_layer": silver_settings,
            # Add other layers if your main config has them and they are needed
            "gold_layer": bq_settings_real.get("gold_layer", {}) 
        }
        
        # If handlers use get_parameter for specific BQ items:
        def get_param_side_effect(section, param, default=None):
            if section == "bigquery_settings":
                if param == "default_job_settings": return default_job_settings
                if param == "silver_layer": return silver_settings
            return config_manager_instance.get_parameter(section, param, default) # Fallback for others
        
        mock_cm.get_parameter.side_effect = get_param_side_effect
        mock_cm.get_section.side_effect = lambda name: bq_settings_real if name == "bigquery_settings" else config_manager_instance.get_section(name)

    except Exception as e:
        # Minimal fallback if the main config is problematic
        default_job_settings = {"job_retry_attempts": 2, "base_retry_delay_seconds": 1, "max_retry_delay_seconds": 5}
        silver_settings = {"dlq_gcs_path": "gs://test-silver-dlq/rejected_records/"}
        mock_cm.get_bigquery_settings.return_value = {
            "default_job_settings": default_job_settings,
            "silver_layer": silver_settings
        }
        print(f"Warning: Using fallback BQ settings for BQ handler tests due to config issue: {e}")
    return mock_cm


@pytest.fixture
def bq_job_failed_event_details_retryable() -> dict:
    return {
        "job_id": "job_retry_123",
        "location": "US",
        "error_result": {"reason": TRANSIENT_BQ_ERROR_REASONS[0], "message": "A transient backend error occurred."},
        "pipeline_stage": "BQ_SILVER_TRANSFORM",
        "correlation_id": "corr_bq_retry_789"
    }

@pytest.fixture
def bq_job_failed_event_details_non_retryable() -> dict:
    return {
        "job_id": "job_nonretry_456",
        "location": "US",
        "error_result": {"reason": "invalidQuery", "message": "Syntax error in query."},
        "pipeline_stage": "BQ_GOLD_AGGREGATION"
    }

@pytest.fixture
def bq_dlq_event_details() -> dict:
    return {
        "job_id": "job_load_errors_789",
        "error_files_gcs_paths": ["gs://temp-error-bucket/job_789_error_file_1.csv"],
        "bq_layer": "silver", # Used to get DLQ path config from bigquery_settings.silver_layer.dlq_gcs_path
        "correlation_id": "corr_bq_dlq_101"
    }

# --- BigQueryJobRetryHandler Tests ---

def test_bq_job_retry_handler_can_handle_retryable(bq_job_failed_event_details_retryable: dict):
    handler = BigQueryJobRetryHandler()
    assert handler.can_handle(BigQueryJobRetryHandler.EVENT_TYPE_BQ_JOB_FAILED, bq_job_failed_event_details_retryable)

def test_bq_job_retry_handler_can_handle_custom_flag(bq_job_failed_event_details_non_retryable: dict):
    handler = BigQueryJobRetryHandler()
    event_custom_retryable = {**bq_job_failed_event_details_non_retryable, "is_retryable_custom_flag": True}
    assert handler.can_handle(BigQueryJobRetryHandler.EVENT_TYPE_BQ_JOB_FAILED, event_custom_retryable)


def test_bq_job_retry_handler_cannot_handle_non_retryable(bq_job_failed_event_details_non_retryable: dict):
    handler = BigQueryJobRetryHandler()
    assert not handler.can_handle(BigQueryJobRetryHandler.EVENT_TYPE_BQ_JOB_FAILED, bq_job_failed_event_details_non_retryable)
    assert not handler.can_handle("SOME_OTHER_EVENT", bq_job_failed_event_details_retryable)

@patch('handlers.database_event_handlers.TENACITY_AVAILABLE', False) # Test basic loop
@patch('time.sleep') # Mock time.sleep
def test_bq_job_retry_handler_success_after_retry(
    mock_sleep: MagicMock,
    mock_config_manager_for_bq_handlers: MagicMock, 
    mocker: MagicMock, 
    bq_job_failed_event_details_retryable: dict
):
    mock_logger = mocker.MagicMock(spec=AbstractLogger)
    mock_alerter = mocker.MagicMock(spec=AbstractAlertManager)
    mock_db_handler = mocker.MagicMock(spec=AbstractDatabaseHandler)

    new_job_id = "new_job_id_for_job_retry_123"
    mock_db_handler.retry_job.return_value = {"new_job_id": new_job_id, "state": "PENDING"}
    # Simulate job completion check loop
    mock_db_handler.get_job_status.return_value = {"job_id": new_job_id, "state": "DONE", "error_result": None}

    handler = BigQueryJobRetryHandler()
    handler.handle_event(
        BigQueryJobRetryHandler.EVENT_TYPE_BQ_JOB_FAILED,
        bq_job_failed_event_details_retryable,
        mock_config_manager_for_bq_handlers,
        mock_logger,
        mock_alerter,
        db_handler=mock_db_handler,
        storage_handler=None # Not used
    )

    mock_db_handler.retry_job.assert_called_once_with(bq_job_failed_event_details_retryable["job_id"], location="US")
    mock_db_handler.get_job_status.assert_called_once_with(new_job_id, location="US")
    
    final_policy_event_call = mock_logger.log_policy_event.call_args_list[-1][0][0]
    assert final_policy_event_call["current_status"] == "RECOVERY_SUCCESSFUL"
    action_params = json.loads(final_policy_event_call["action_parameters"])
    assert action_params["new_job_id"] == new_job_id
    mock_alerter.send_alert.assert_not_called()


@patch('handlers.database_event_handlers.TENACITY_AVAILABLE', False)
@patch('time.sleep')
def test_bq_job_retry_handler_final_failure(
    mock_sleep: MagicMock,
    mock_config_manager_for_bq_handlers: MagicMock, 
    mocker: MagicMock, 
    bq_job_failed_event_details_retryable: dict
):
    mock_logger = mocker.MagicMock(spec=AbstractLogger)
    mock_alerter = mocker.MagicMock(spec=AbstractAlertManager)
    mock_db_handler = mocker.MagicMock(spec=AbstractDatabaseHandler)

    retried_job_ids = ["retry1_job_retry_123", "retry2_job_retry_123"]
    mock_db_handler.retry_job.side_effect = [
        {"new_job_id": retried_job_ids[0], "state": "PENDING"},
        {"new_job_id": retried_job_ids[1], "state": "PENDING"}
    ]
    mock_db_handler.get_job_status.side_effect = [
        {"job_id": retried_job_ids[0], "state": "DONE", "error_result": {"reason": "backendError", "message": "Failed again"}},
        {"job_id": retried_job_ids[1], "state": "DONE", "error_result": {"reason": "internalError", "message": "Failed terminally"}}
    ]
    
    handler = BigQueryJobRetryHandler()
    handler.handle_event(
        BigQueryJobRetryHandler.EVENT_TYPE_BQ_JOB_FAILED,
        bq_job_failed_event_details_retryable,
        mock_config_manager_for_bq_handlers,
        mock_logger,
        mock_alerter,
        db_handler=mock_db_handler,
        storage_handler=None
    )
    
    # Configured for 2 attempts by mock_config_manager_for_bq_handlers
    assert mock_db_handler.retry_job.call_count == 2 
    assert mock_db_handler.get_job_status.call_count == 2 
    
    final_policy_event_call = mock_logger.log_policy_event.call_args_list[-1][0][0]
    assert final_policy_event_call["current_status"] == "RECOVERY_FAILED"
    
    mock_alerter.send_alert.assert_called_once()
    alert_args, alert_kwargs = mock_alerter.send_alert.call_args
    assert alert_kwargs["severity"] == "CRITICAL"
    assert "BQ Job Retry Failed Permanently" in alert_kwargs["subject"]

# --- BigQueryDLQHandler Tests ---

def test_bq_dlq_handler_can_handle(bq_dlq_event_details: dict):
    handler = BigQueryDLQHandler()
    assert handler.can_handle(BigQueryDLQHandler.EVENT_TYPE_BQ_LOAD_JOB_ERRORS_FOR_DLQ, bq_dlq_event_details)
    assert not handler.can_handle("SOME_OTHER_EVENT", bq_dlq_event_details)

def test_bq_dlq_handler_success(
    mock_config_manager_for_bq_handlers: MagicMock, 
    mocker: MagicMock, 
    bq_dlq_event_details: dict
):
    mock_logger = mocker.MagicMock(spec=AbstractLogger)
    mock_alerter = mocker.MagicMock(spec=AbstractAlertManager)
    mock_storage_handler = mocker.MagicMock(spec=AbstractStorageHandler)

    expected_dlq_file_path = "gs://test-silver-dlq/rejected_records/bq_job_error_files/job_load_errors_789/job_789_error_file_1.csv_timestamp_uuid" # Simplified
    mock_storage_handler.move_to_dlq.return_value = expected_dlq_file_path

    handler = BigQueryDLQHandler()
    handler.handle_event(
        BigQueryDLQHandler.EVENT_TYPE_BQ_LOAD_JOB_ERRORS_FOR_DLQ,
        bq_dlq_event_details,
        mock_config_manager_for_bq_handlers,
        mock_logger,
        mock_alerter,
        db_handler=None, 
        storage_handler=mock_storage_handler
    )

    error_file_path = bq_dlq_event_details["error_files_gcs_paths"][0]
    source_bucket, source_object = error_file_path.replace("gs://", "").split("/", 1)
    mock_storage_handler.move_to_dlq.assert_called_once_with(
        source_bucket, 
        source_object,
        error_details=ANY 
    )

    final_policy_event_call = mock_logger.log_policy_event.call_args_list[-1][0][0]
    assert final_policy_event_call["current_status"] == "RECOVERY_SUCCESSFUL"
    action_params = json.loads(final_policy_event_call["action_parameters"])
    assert action_params["moved_count"] == 1
    assert expected_dlq_file_path in action_params["dlq_paths"]
    
    mock_alerter.send_alert.assert_called_once()
    alert_args, alert_kwargs = mock_alerter.send_alert.call_args
    assert alert_kwargs["severity"] == "INFO" # Default or configured
    assert "BQ Load Job DLQ Processing" in alert_kwargs["subject"]

def test_bq_dlq_handler_storage_failure(
    mock_config_manager_for_bq_handlers: MagicMock, 
    mocker: MagicMock, 
    bq_dlq_event_details: dict
):
    mock_logger = mocker.MagicMock(spec=AbstractLogger)
    mock_alerter = mocker.MagicMock(spec=AbstractAlertManager)
    mock_storage_handler = mocker.MagicMock(spec=AbstractStorageHandler)
    
    mock_storage_handler.move_to_dlq.side_effect = Exception("Simulated GCS failure during DLQ move")

    handler = BigQueryDLQHandler()
    handler.handle_event(
        BigQueryDLQHandler.EVENT_TYPE_BQ_LOAD_JOB_ERRORS_FOR_DLQ,
        bq_dlq_event_details,
        mock_config_manager_for_bq_handlers,
        mock_logger,
        mock_alerter,
        db_handler=None,
        storage_handler=mock_storage_handler
    )

    final_policy_event_call = mock_logger.log_policy_event.call_args_list[-1][0][0]
    assert final_policy_event_call["current_status"] == "RECOVERY_FAILED"
    assert "Simulated GCS failure" in final_policy_event_call["error_message"]
    
    assert mock_alerter.send_alert.call_count == 2 # One for individual file, one summary
    last_alert_call_args, last_alert_call_kwargs = mock_alerter.send_alert.call_args_list[-1]
    assert last_alert_call_kwargs["severity"] == "ERROR"
    assert "BQ Load Job DLQ Processing" in last_alert_call_kwargs["subject"]
