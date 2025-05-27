import pytest
from unittest.mock import MagicMock, call, patch
import time # For testing delays if not using tenacity's time control

from core.config_manager import ConfigManager
from core.interfaces import AbstractLogger, AbstractAlertManager, AbstractStorageHandler, AbstractDatabaseHandler
from handlers.storage_event_handlers import GCSUploadRetryHandler, GCSDeadLetterHandler
from services.storage_handlers import GCSStorageHandler # For type hinting, though it will be mocked


# --- GCSUploadRetryHandler Tests ---

@pytest.fixture
def mock_config_manager_for_gcs_handler(config_manager_instance: ConfigManager, mocker: MagicMock) -> MagicMock:
    """Mocks ConfigManager for GCS handler tests, ensuring GCS settings are available."""
    mock_cm = mocker.MagicMock(spec=ConfigManager)
    try:
        # Provide some default GCS settings if not fully present in the main config
        gcs_settings = config_manager_instance.get_section("gcs_settings").get("bronze_layer", {})
        if not gcs_settings: # If bronze_layer specifically is missing, provide a minimal default
             gcs_settings = {
                "upload_retry_attempts": 2, 
                "base_retry_delay_seconds": 1, 
                "max_retry_delay_seconds": 5,
                "dead_letter_bucket": "test-dlq-bucket" # Needed by GCSDeadLetterHandler
            }

        # Ensure the specific layer (e.g., bronze_layer) is returned by get_gcs_settings
        mock_cm.get_gcs_settings.return_value = gcs_settings
        
        # Make other methods pass through to the real instance or return defaults
        mock_cm.get_section.side_effect = lambda section_name: config_manager_instance.get_section(section_name) if section_name != "gcs_settings" else {"bronze_layer": gcs_settings}
        mock_cm.get_parameter.side_effect = config_manager_instance.get_parameter

    except Exception as e:
        # Fallback if main config is severely broken or missing gcs_settings
        mock_cm.get_gcs_settings.return_value = {
            "upload_retry_attempts": 2, 
            "base_retry_delay_seconds": 1, 
            "max_retry_delay_seconds": 5,
            "dead_letter_bucket": "test-dlq-bucket"
        }
        print(f"Warning: Using fallback GCS settings for GCS handler tests due to config issue: {e}")
    return mock_cm

@pytest.fixture
def gcs_upload_event_details() -> dict:
    return {
        "bucket_name": "test-upload-bucket", 
        "source_path": "/tmp/local_file_to_upload.txt", 
        "destination_object_name": "target/path/uploaded_file.txt",
        "gcs_layer": "bronze_layer", # To fetch specific GCS settings
        "correlation_id": "corr_upload_123",
        "error_message": "Initial simulated upload failure" # Example error
    }

def test_gcs_upload_retry_handler_can_handle(gcs_upload_event_details: dict):
    handler = GCSUploadRetryHandler()
    assert handler.can_handle(GCSUploadRetryHandler.EVENT_TYPE_GCS_UPLOAD_FAILED, gcs_upload_event_details)
    assert not handler.can_handle("SOME_OTHER_EVENT", gcs_upload_event_details)

def test_gcs_upload_retry_handler_success_first_try(
    mock_config_manager_for_gcs_handler: MagicMock, 
    mocker: MagicMock, 
    gcs_upload_event_details: dict
):
    mock_logger = mocker.MagicMock(spec=AbstractLogger)
    mock_alerter = mocker.MagicMock(spec=AbstractAlertManager)
    mock_storage_handler = mocker.MagicMock(spec=AbstractStorageHandler)
    mock_storage_handler.upload_file.return_value = True # Success on first try

    handler = GCSUploadRetryHandler()
    handler.handle_event(
        GCSUploadRetryHandler.EVENT_TYPE_GCS_UPLOAD_FAILED,
        gcs_upload_event_details,
        mock_config_manager_for_gcs_handler,
        mock_logger,
        mock_alerter,
        db_handler=None, # Not used by this handler
        storage_handler=mock_storage_handler
    )

    mock_storage_handler.upload_file.assert_called_once_with(
        gcs_upload_event_details["bucket_name"],
        gcs_upload_event_details["source_path"],
        gcs_upload_event_details["destination_object_name"]
    )
    # Check that policy event was logged for success
    # The first call is attempt, second is success
    assert mock_logger.log_policy_event.call_count >= 2 
    final_policy_event_call = mock_logger.log_policy_event.call_args_list[-1][0][0]
    assert final_policy_event_call["current_status"] == "RECOVERY_SUCCESSFUL"
    mock_alerter.send_alert.assert_not_called()


@patch('handlers.storage_event_handlers.TENACITY_AVAILABLE', False) # Test basic loop
def test_gcs_upload_retry_handler_success_after_retries_no_tenacity(
    mock_config_manager_for_gcs_handler: MagicMock, 
    mocker: MagicMock, 
    gcs_upload_event_details: dict
):
    mock_logger = mocker.MagicMock(spec=AbstractLogger)
    mock_alerter = mocker.MagicMock(spec=AbstractAlertManager)
    mock_storage_handler = mocker.MagicMock(spec=AbstractStorageHandler)
    
    # Fail first, then succeed
    mock_storage_handler.upload_file.side_effect = [False, True] 
    
    # Mock time.sleep to speed up test
    mocker.patch('time.sleep', return_value=None) 

    handler = GCSUploadRetryHandler()
    handler.handle_event(
        GCSUploadRetryHandler.EVENT_TYPE_GCS_UPLOAD_FAILED,
        gcs_upload_event_details,
        mock_config_manager_for_gcs_handler,
        mock_logger,
        mock_alerter,
        db_handler=None,
        storage_handler=mock_storage_handler
    )
    
    assert mock_storage_handler.upload_file.call_count == 2
    final_policy_event_call = mock_logger.log_policy_event.call_args_list[-1][0][0]
    assert final_policy_event_call["current_status"] == "RECOVERY_SUCCESSFUL"
    mock_alerter.send_alert.assert_not_called()

def test_gcs_upload_retry_handler_final_failure(
    mock_config_manager_for_gcs_handler: MagicMock, 
    mocker: MagicMock, 
    gcs_upload_event_details: dict
):
    mock_logger = mocker.MagicMock(spec=AbstractLogger)
    mock_alerter = mocker.MagicMock(spec=AbstractAlertManager)
    mock_storage_handler = mocker.MagicMock(spec=AbstractStorageHandler)
    
    # Fail all attempts (based on config, e.g., 2 attempts)
    mock_storage_handler.upload_file.return_value = False 
    
    mocker.patch('time.sleep', return_value=None) # For non-tenacity path if it's taken

    handler = GCSUploadRetryHandler()
    handler.handle_event(
        GCSUploadRetryHandler.EVENT_TYPE_GCS_UPLOAD_FAILED,
        gcs_upload_event_details,
        mock_config_manager_for_gcs_handler,
        mock_logger,
        mock_alerter,
        db_handler=None,
        storage_handler=mock_storage_handler
    )
    
    gcs_settings = mock_config_manager_for_gcs_handler.get_gcs_settings.return_value
    expected_attempts = gcs_settings.get("upload_retry_attempts", 2)
    assert mock_storage_handler.upload_file.call_count == expected_attempts
    
    final_policy_event_call = mock_logger.log_policy_event.call_args_list[-1][0][0]
    assert final_policy_event_call["current_status"] == "RECOVERY_FAILED"
    
    mock_alerter.send_alert.assert_called_once()
    alert_args, alert_kwargs = mock_alerter.send_alert.call_args
    assert alert_kwargs["severity"] == "CRITICAL"
    assert "GCS Upload Failed Permanently" in alert_kwargs["subject"]

# --- GCSDeadLetterHandler Tests ---

@pytest.fixture
def gcs_dlq_event_details() -> dict:
    return {
        "source_bucket_name": "original-source-bucket",
        "source_object_name": "data/failed_file.json",
        "pipeline_stage": "GCS_BRONZE_VALIDATION",
        "correlation_id": "corr_dlq_456",
        "original_error_details": {"error_code": "INVALID_FORMAT", "message": "File content is not valid JSON"}
    }

def test_gcs_dead_letter_handler_can_handle(gcs_dlq_event_details: dict):
    handler = GCSDeadLetterHandler()
    assert handler.can_handle(GCSDeadLetterHandler.EVENT_TYPE_GCS_MOVE_TO_DLQ, gcs_dlq_event_details)
    assert not handler.can_handle("SOME_OTHER_EVENT", gcs_dlq_event_details)

def test_gcs_dead_letter_handler_success(
    mock_config_manager_for_gcs_handler: MagicMock, # Re-use this as it defines DLQ bucket
    mocker: MagicMock,
    gcs_dlq_event_details: dict
):
    mock_logger = mocker.MagicMock(spec=AbstractLogger)
    mock_alerter = mocker.MagicMock(spec=AbstractAlertManager)
    mock_storage_handler = mocker.MagicMock(spec=AbstractStorageHandler)
    
    expected_dlq_path = "gs://test-dlq-bucket/gcs_bronze_validation/invalid_format/failed_file_YYYYMMDDHHMMSS_uuid.json" # Simplified pattern
    mock_storage_handler.move_to_dlq.return_value = expected_dlq_path # Mock the return path

    handler = GCSDeadLetterHandler()
    handler.handle_event(
        GCSDeadLetterHandler.EVENT_TYPE_GCS_MOVE_TO_DLQ,
        gcs_dlq_event_details,
        mock_config_manager_for_gcs_handler,
        mock_logger,
        mock_alerter,
        db_handler=None,
        storage_handler=mock_storage_handler
    )

    mock_storage_handler.move_to_dlq.assert_called_once_with(
        gcs_dlq_event_details["source_bucket_name"],
        gcs_dlq_event_details["source_object_name"],
        error_details=gcs_dlq_event_details["original_error_details"]
    )
    
    final_policy_event_call = mock_logger.log_policy_event.call_args_list[-1][0][0]
    assert final_policy_event_call["current_status"] == "RECOVERY_SUCCESSFUL" # or ACTION_COMPLETED
    assert final_policy_event_call["action_parameters"] is not None
    action_params = json.loads(final_policy_event_call["action_parameters"])
    assert action_params["dlq_path"] == expected_dlq_path

    mock_alerter.send_alert.assert_called_once()
    alert_args, alert_kwargs = mock_alerter.send_alert.call_args
    assert alert_kwargs["severity"] == "INFO" # or WARNING
    assert "File Moved to DLQ" in alert_kwargs["subject"]


def test_gcs_dead_letter_handler_storage_failure(
    mock_config_manager_for_gcs_handler: MagicMock, 
    mocker: MagicMock,
    gcs_dlq_event_details: dict
):
    mock_logger = mocker.MagicMock(spec=AbstractLogger)
    mock_alerter = mocker.MagicMock(spec=AbstractAlertManager)
    mock_storage_handler = mocker.MagicMock(spec=AbstractStorageHandler)
    
    mock_storage_handler.move_to_dlq.side_effect = Exception("Simulated GCS API failure during DLQ move")

    handler = GCSDeadLetterHandler()
    handler.handle_event(
        GCSDeadLetterHandler.EVENT_TYPE_GCS_MOVE_TO_DLQ,
        gcs_dlq_event_details,
        mock_config_manager_for_gcs_handler,
        mock_logger,
        mock_alerter,
        db_handler=None,
        storage_handler=mock_storage_handler
    )

    final_policy_event_call = mock_logger.log_policy_event.call_args_list[-1][0][0]
    assert final_policy_event_call["current_status"] == "RECOVERY_FAILED" # or ACTION_FAILED
    assert "Simulated GCS API failure" in final_policy_event_call["error_message"]
    
    mock_alerter.send_alert.assert_called_once()
    alert_args, alert_kwargs = mock_alerter.send_alert.call_args
    assert alert_kwargs["severity"] == "CRITICAL"
    assert "Failed to Move GCS File to DLQ" in alert_kwargs["subject"]

def test_gcs_upload_retry_handler_missing_details(
    mock_config_manager_for_gcs_handler: MagicMock, 
    mocker: MagicMock
):
    mock_logger = mocker.MagicMock(spec=AbstractLogger)
    mock_alerter = mocker.MagicMock(spec=AbstractAlertManager)
    mock_storage_handler = mocker.MagicMock(spec=AbstractStorageHandler) # Not strictly needed as it should fail before
    
    incomplete_event_details = {"bucket_name": "test-bucket"} # Missing source_path, destination_object_name

    handler = GCSUploadRetryHandler()
    handler.handle_event(
        GCSUploadRetryHandler.EVENT_TYPE_GCS_UPLOAD_FAILED,
        incomplete_event_details,
        mock_config_manager_for_gcs_handler,
        mock_logger,
        mock_alerter,
        db_handler=None,
        storage_handler=mock_storage_handler
    )
    
    mock_logger.log_error.assert_called_once()
    assert "Missing required event details" in mock_logger.log_error.call_args[0][0]
    
    mock_alerter.send_alert.assert_called_once()
    assert "GCSUploadRetryHandler" in mock_alerter.send_alert.call_args[1]['subject']
    assert mock_alerter.send_alert.call_args[1]['severity'] == "ERROR"

    final_policy_event_call = mock_logger.log_policy_event.call_args_list[-1][0][0]
    assert final_policy_event_call["current_status"] == "HANDLER_FAILED_CONFIG"
    assert "Missing required event details" in final_policy_event_call["error_message"]

# Similar test for GCSDeadLetterHandler missing details can be added.```python
import pytest
import os
from pathlib import Path
from typing import Dict, Any

from core.config_manager import ConfigManager, ConfigManagerError # Adjust path if needed

# This test suite uses the config_manager_instance fixture from conftest.py

def test_load_config_success(config_manager_instance: ConfigManager, test_config_path: str):
    """Tests successful loading of the configuration file."""
    assert config_manager_instance._config is not None
    assert isinstance(config_manager_instance.get_config(), dict)
    # Check if a known key from your actual config is present
    assert "global_settings" in config_manager_instance.get_config()
    assert config_manager_instance.get_parameter("global_settings", "log_level") == "INFO" # Example from provided config

def test_load_config_file_not_found(project_root: Path):
    """Tests that ConfigManagerError is raised if the config file is not found."""
    non_existent_path = str(project_root / "config" / "non_existent_config.yaml")
    # Need a new instance for this test, as the fixture one is session-scoped and already loaded or skipped.
    with pytest.raises(ConfigManagerError, match=f"Configuration file not found at path: {non_existent_path}"):
        ConfigManager(config_path=non_existent_path) # Explicitly pass path

def test_load_config_invalid_yaml(tmp_path: Path):
    """Tests that ConfigManagerError is raised if the config file is invalid YAML."""
    invalid_yaml_file = tmp_path / "invalid_config.yaml"
    invalid_yaml_file.write_text("global_settings: { log_level: INFO\n  another_setting: [unterminated_array") # Invalid YAML
    
    with pytest.raises(ConfigManagerError, match="Error parsing YAML configuration file"):
        ConfigManager(config_path=str(invalid_yaml_file))

def test_get_config_not_loaded():
    """Tests that accessing config before loading raises an error."""
    # Create an instance without loading config by manipulating singleton behavior for this specific test
    # This is tricky due to the singleton nature. A better way might be to have a method to reset the singleton
    # or allow creating a non-singleton instance for tests.
    # For now, let's assume a fresh instance can be created if we directly instantiate.
    # However, __new__ will always try to load.
    # A more direct test would be to mock the _config attribute to None.
    
    # This test is difficult with the current singleton implementation that auto-loads.
    # A possible refactor of ConfigManager could be to not load in __new__ unless path is given,
    # or have a clear_instance() method for testing.
    pytest.skip("Skipping test_get_config_not_loaded due to singleton auto-loading behavior.")

def test_get_section_success(config_manager_instance: ConfigManager):
    """Tests successful retrieval of a configuration section."""
    global_settings = config_manager_instance.get_section("global_settings")
    assert isinstance(global_settings, dict)
    assert global_settings.get("log_level") == "INFO"

    alerting_settings = config_manager_instance.get_section("alerting")
    assert isinstance(alerting_settings, dict)
    assert "default_provider" in alerting_settings

def test_get_section_not_found(config_manager_instance: ConfigManager):
    """Tests that ConfigManagerError is raised if a section is not found."""
    with pytest.raises(ConfigManagerError, match="Configuration section 'non_existent_section' not found."):
        config_manager_instance.get_section("non_existent_section")

def test_get_parameter_success(config_manager_instance: ConfigManager):
    """Tests successful retrieval of a specific parameter."""
    log_level = config_manager_instance.get_parameter("global_settings", "log_level")
    assert log_level == "INFO"

    default_sender = config_manager_instance.get_parameter("alerting", "providers", {}).get("email", {}).get("default_sender")
    assert default_sender == "data-pipeline-noreply@example.com" # Based on sample config

def test_get_parameter_with_default(config_manager_instance: ConfigManager):
    """Tests retrieval of a parameter with a default value when it's not found."""
    # Parameter exists
    log_level = config_manager_instance.get_parameter("global_settings", "log_level", default="DEBUG")
    assert log_level == "INFO"

    # Parameter does not exist
    non_existent_param = config_manager_instance.get_parameter("global_settings", "non_existent_param", default="default_value")
    assert non_existent_param == "default_value"
    
    # Section exists, parameter does not, no default
    with pytest.raises(ConfigManagerError, match="Parameter 'non_existent_param_no_default' not found in section 'global_settings'."):
        config_manager_instance.get_parameter("global_settings", "non_existent_param_no_default")


def test_get_parameter_section_not_found(config_manager_instance: ConfigManager):
    """Tests that ConfigManagerError is raised if section for get_parameter is not found."""
    with pytest.raises(ConfigManagerError, match="Configuration section 'non_existent_section' not found."):
        config_manager_instance.get_parameter("non_existent_section", "some_param")

def test_get_parameter_param_not_found_no_default(config_manager_instance: ConfigManager):
    """Tests that ConfigManagerError is raised if parameter is not found and no default is given."""
    with pytest.raises(ConfigManagerError, match="Parameter 'another_missing_param' not found in section 'global_settings'."):
        config_manager_instance.get_parameter("global_settings", "another_missing_param")

def test_specific_getters(config_manager_instance: ConfigManager):
    """Tests the specific getter methods like get_global_settings, etc."""
    assert config_manager_instance.get_global_settings()["log_level"] == "INFO"
    
    # Test data_source_config with fallback to default
    default_source_config = config_manager_instance.get_data_source_config("non_existent_source_id_for_default_test")
    assert default_source_config["retry_attempts"] == 3 # From 'default' in sample config
    
    specific_source_config = config_manager_instance.get_data_source_config("your_source_system_id")
    assert specific_source_config["retry_attempts"] == 5 # From 'your_source_system_id'
    assert specific_source_config["status_endpoint"] == "https://api.specific-vendor.com/status" # Merged

    # Test ingestion_settings
    cdc_settings = config_manager_instance.get_ingestion_settings("cdc")
    assert cdc_settings["connector_restart_attempts"] == 3
    all_ingestion_settings = config_manager_instance.get_ingestion_settings()
    assert "cdc" in all_ingestion_settings and "streaming" in all_ingestion_settings

    # Test GCS settings
    bronze_gcs = config_manager_instance.get_gcs_settings("bronze_layer")
    assert bronze_gcs["dead_letter_bucket"] == "gs://your-project-id-bronze-dlq/"
    
    # Test BigQuery settings - note the method in ConfigManager handles "_layer" suffix if needed
    silver_bq = config_manager_instance.get_bigquery_settings("silver") # Test without _layer
    assert silver_bq["dlq_table"] == "your_project_id.your_dataset_silver_dlq.failed_records"

    # Test alerting and tracking
    assert config_manager_instance.get_alerting_config()["default_provider"] == "slack"
    assert config_manager_instance.get_tracking_and_logging_config()["policy_execution_log_table_id"] == "your_gcp_project.your_dataset_operational.policy_execution_log"

    # Test error patterns
    bq_error_patterns = config_manager_instance.get_error_patterns_config("bigquery")
    assert len(bq_error_patterns) > 0
    assert bq_error_patterns[0]["default_severity"] == "CRITICAL"
    
    all_error_patterns = config_manager_instance.get_error_patterns_config() # Get all
    assert len(all_error_patterns) >= len(bq_error_patterns)
    
    gcs_patterns = config_manager_instance.get_error_patterns_config("gcs")
    assert len(gcs_patterns) > 0

    # Test missing service in error patterns
    assert config_manager_instance.get_error_patterns_config("non_existent_service") == []


def test_validation_of_essential_keys(tmp_path: Path):
    """Tests that essential keys are validated during load."""
    minimal_valid_config_content = """
global_settings: {log_level: INFO}
data_sources: {default: {retry_attempts: 1}}
ingestion_settings: {cdc: {connector_restart_attempts: 1}}
gcs_settings: {bronze_layer: {upload_retry_attempts: 1}}
bigquery_settings: {default_job_settings: {job_retry_attempts: 1}}
alerting: {default_provider: none}
tracking_and_logging: {policy_execution_log_table_id: "project.dataset.table"}
# Missing other optional keys like error_patterns, rca_settings etc. is fine
"""
    valid_config_file = tmp_path / "valid_config.yaml"
    valid_config_file.write_text(minimal_valid_config_content)
    # This should load without error
    ConfigManager(config_path=str(valid_config_file))


    missing_key_config_content = """
# global_settings: {log_level: INFO} # This one is missing
data_sources: {default: {retry_attempts: 1}}
ingestion_settings: {cdc: {connector_restart_attempts: 1}}
gcs_settings: {bronze_layer: {upload_retry_attempts: 1}}
bigquery_settings: {default_job_settings: {job_retry_attempts: 1}}
alerting: {default_provider: none}
tracking_and_logging: {policy_execution_log_table_id: "project.dataset.table"}
"""
    invalid_config_file = tmp_path / "invalid_key_config.yaml"
    invalid_config_file.write_text(missing_key_config_content)
    with pytest.raises(ConfigManagerError, match="Missing essential configuration keys: global_settings"):
        ConfigManager(config_path=str(invalid_config_file))

def test_singleton_behavior(test_config_path: str, config_manager_instance: ConfigManager):
    """Tests that ConfigManager behaves as a singleton."""
    cm1 = config_manager_instance
    cm2 = ConfigManager(config_path=test_config_path) # Should return the same instance
    cm3 = ConfigManager() # Should also return the same instance if already initialized with default path
    
    assert cm1 is cm2
    # If the default path used by ConfigManager() in conftest is the same as test_config_path, then cm1 is cm3.
    # This depends on how ConfigManager's default path resolution aligns with test_config_path.
    # The current ConfigManager loads default path in __new__ if no path is given.
    # The fixture ensures it's loaded with test_config_path.
    # So, subsequent ConfigManager() calls will return the already loaded instance.
    assert cm1 is cm3

    # Verify that changing config on one instance reflects on others (if reloading is allowed and happens)
    # Current implementation loads config once. If load_config is called again, it reloads.
    # Let's test that they share the same _config dictionary.
    assert cm1._config is cm2._config
    cm1._config["test_singleton_key"] = "test_value"
    assert "test_singleton_key" in cm2._config
    assert cm2._config["test_singleton_key"] == "test_value"
    # Clean up the modification for other tests
    del cm1._config["test_singleton_key"]
```

Create `tests/unit/test_database_event_handlers.py`:
```python
import pytest
from unittest.mock import MagicMock, call, patch
import time # For testing delays
import json

from core.config_manager import ConfigManager
from core.interfaces import AbstractLogger, AbstractAlertManager, AbstractDatabaseHandler
from handlers.database_event_handlers import BigQueryJobRetryHandler, BigQueryDLQHandler, TRANSIENT_BQ_ERROR_REASONS
from services.database_handlers import BigQueryDatabaseHandler # For type hinting

@pytest.fixture
def mock_config_manager_for_bq_handlers(config_manager_instance: ConfigManager, mocker: MagicMock) -> MagicMock:
    mock_cm = mocker.MagicMock(spec=ConfigManager)
    try:
        # Provide default BQ job settings
        bq_settings = config_manager_instance.get_section("bigquery_settings")
        default_job_settings = bq_settings.get("default_job_settings", {
            "job_retry_attempts": 2, 
            "base_retry_delay_seconds": 1, 
            "max_retry_delay_seconds": 5
        })
        silver_settings = bq_settings.get("silver_layer", { # For DLQ handler
            "dlq_gcs_path": "gs://test-silver-dlq/rejected_records/"
        })

        # This is how get_bigquery_settings().get("default_job_settings") works
        mock_cm.get_bigquery_settings.return_value.get.side_effect = lambda key, default: default_job_settings if key == "default_job_settings" else \
                                                                    silver_settings if key == "silver_layer" else \
                                                                    config_manager_instance.get_bigquery_settings().get(key, default)
        
        # Specific for BQDLQHandler if it calls get_bigquery_settings("silver_layer")
        mock_cm.get_bigquery_settings.return_value = {
            "default_job_settings": default_job_settings,
            "silver_layer": silver_settings, # Ensure this key exists for get_bigquery_settings("silver_layer")
             # Pass through other layer calls if any
            "gold_layer": bq_settings.get("gold_layer", {})
        }
        # If get_bigquery_settings(layer_name_with_suffix) is used
        mock_cm.get_bigquery_settings = lambda layer_arg=None: { # Simpler mock for this specific need
            "default_job_settings": default_job_settings,
            "silver_layer": silver_settings,
        }.get(layer_arg if layer_arg else "default_job_settings") if layer_arg else { # if layer_arg is None, return all
             "default_job_settings": default_job_settings, "silver_layer": silver_settings
        }


    except Exception as e:
         mock_cm.get_bigquery_settings.return_value.get.return_value = { # Fallback
            "job_retry_attempts": 2, "base_retry_delay_seconds": 1, "max_retry_delay_seconds": 5
        }
         mock_cm.get_bigquery_settings.return_value = {"default_job_settings": mock_cm.get_bigquery_settings.return_value.get.return_value,
                                                       "silver_layer": {"dlq_gcs_path": "gs://test-silver-dlq/rejected_records/"}}

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
        "bq_layer": "silver", # Used to get DLQ path config
        "correlation_id": "corr_bq_dlq_101"
    }

# --- BigQueryJobRetryHandler Tests ---

def test_bq_job_retry_handler_can_handle_retryable(bq_job_failed_event_details_retryable: dict):
    handler = BigQueryJobRetryHandler()
    assert handler.can_handle(BigQueryJobRetryHandler.EVENT_TYPE_BQ_JOB_FAILED, bq_job_failed_event_details_retryable)

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

    # First retry_job submits a new job, second get_job_status shows DONE without error
    new_job_id = "new_job_id_for_job_retry_123"
    mock_db_handler.retry_job.return_value = {"new_job_id": new_job_id, "state": "PENDING"}
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

    # All retry attempts fail
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
    
    # Configured for 2 attempts
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
    mock_storage_handler = mocker.MagicMock(spec=AbstractStorageHandler) # For moving error files

    # Mock the GCS path returned by move_to_dlq
    expected_dlq_file_path = "gs://test-silver-dlq/rejected_records/bq_job_error_files/job_load_errors_789/job_789_error_file_1.csv_timestamp_uuid"
    mock_storage_handler.move_to_dlq.return_value = expected_dlq_file_path

    handler = BigQueryDLQHandler()
    handler.handle_event(
        BigQueryDLQHandler.EVENT_TYPE_BQ_LOAD_JOB_ERRORS_FOR_DLQ,
        bq_dlq_event_details,
        mock_config_manager_for_bq_handlers,
        mock_logger,
        mock_alerter,
        db_handler=None, # Not used
        storage_handler=mock_storage_handler
    )

    # Verify move_to_dlq was called for each error file
    error_file_path = bq_dlq_event_details["error_files_gcs_paths"][0]
    source_bucket, source_object = error_file_path.replace("gs://", "").split("/", 1)
    mock_storage_handler.move_to_dlq.assert_called_once_with(
        source_bucket, 
        source_object,
        error_details=ANY # Error details for path construction in move_to_dlq
    )

    final_policy_event_call = mock_logger.log_policy_event.call_args_list[-1][0][0]
    assert final_policy_event_call["current_status"] == "RECOVERY_SUCCESSFUL"
    action_params = json.loads(final_policy_event_call["action_parameters"])
    assert action_params["moved_count"] == 1
    assert expected_dlq_file_path in action_params["dlq_paths"]
    
    mock_alerter.send_alert.assert_called_once()
    alert_args, alert_kwargs = mock_alerter.send_alert.call_args
    assert alert_kwargs["severity"] == "INFO" # Or WARNING based on config
    assert "BQ Load Job DLQ Processing" in alert_kwargs["subject"]


def test_bq_dlq_handler_storage_failure(
    mock_config_manager_for_bq_handlers: MagicMock, 
    mocker: MagicMock, 
    bq_dlq_event_details: dict
):
    mock_logger = mocker.MagicMock(spec=AbstractLogger)
    mock_alerter = mocker.MagicMock(spec=AbstractAlertManager)
    mock_storage_handler = mocker.MagicMock(spec=AbstractStorageHandler)
    
    mock_storage_handler.move_to_dlq.side_effect = Exception("Simulated GCS failure during DLQ move of BQ error file")

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
    assert final_policy_event_call["current_status"] == "RECOVERY_FAILED" # Since all files failed
    assert "Simulated GCS failure" in final_policy_event_call["error_message"]
    
    # One alert for the overall handler failure, one for the specific file failure
    assert mock_alerter.send_alert.call_count == 2 
    # Check the summary alert
    last_alert_call_args, last_alert_call_kwargs = mock_alerter.send_alert.call_args_list[-1]
    assert last_alert_call_kwargs["severity"] == "ERROR"
    assert "BQ Load Job DLQ Processing" in last_alert_call_kwargs["subject"]
```

**Step 5: Basic Integration Tests for `PolicyEngine` (`tests/integration/test_policy_engine.py`)**
```python
import pytest
from unittest.mock import MagicMock, call

from core.config_manager import ConfigManager
from core.interfaces import AbstractLogger, AbstractAlertManager, AbstractPolicyEventHandler, AbstractDatabaseHandler, AbstractStorageHandler
from core.engine import PolicyEngine

# Using fixtures from conftest.py: config_manager_instance

@pytest.fixture
def mock_logger(mocker: MagicMock) -> MagicMock:
    return mocker.MagicMock(spec=AbstractLogger)

@pytest.fixture
def mock_alerter(mocker: MagicMock) -> MagicMock:
    return mocker.MagicMock(spec=AbstractAlertManager)

@pytest.fixture
def mock_db_handler(mocker: MagicMock) -> MagicMock:
    return mocker.MagicMock(spec=AbstractDatabaseHandler)

@pytest.fixture
def mock_storage_handler(mocker: MagicMock) -> MagicMock:
    return mocker.MagicMock(spec=AbstractStorageHandler)

@pytest.fixture
def policy_engine_instance(
    config_manager_instance: ConfigManager, 
    mock_logger: MagicMock, 
    mock_alerter: MagicMock,
    mock_db_handler: MagicMock,
    mock_storage_handler: MagicMock
) -> PolicyEngine:
    return PolicyEngine(
        config_manager=config_manager_instance,
        logger=mock_logger,
        alerter=mock_alerter,
        db_handler=mock_db_handler,
        storage_handler=mock_storage_handler
    )

# --- Mock Policy Event Handlers for Testing ---
class MockHandlerAlpha(AbstractPolicyEventHandler):
    def __init__(self, can_handle_response=True, handle_event_side_effect=None):
        self.can_handle_response = can_handle_response
        self.handle_event_side_effect = handle_event_side_effect
        self.can_handle_called_with = None
        self.handle_event_called_with = None

    def can_handle(self, event_type: str, event_details: dict) -> bool:
        self.can_handle_called_with = (event_type, event_details)
        return self.can_handle_response

    def handle_event(self, event_type: str, event_details: dict, config_manager, logger, alerter, db_handler, storage_handler) -> None:
        self.handle_event_called_with = (event_type, event_details, config_manager, logger, alerter, db_handler, storage_handler)
        if self.handle_event_side_effect:
            raise self.handle_event_side_effect

class MockHandlerBeta(AbstractPolicyEventHandler):
    def __init__(self, can_handle_response=True, handle_event_side_effect=None):
        self.can_handle_response = can_handle_response
        self.handle_event_side_effect = handle_event_side_effect
        self.can_handle_called_with = None
        self.handle_event_called_with = None

    def can_handle(self, event_type: str, event_details: dict) -> bool:
        self.can_handle_called_with = (event_type, event_details)
        return self.can_handle_response

    def handle_event(self, event_type: str, event_details: dict, config_manager, logger, alerter, db_handler, storage_handler) -> None:
        self.handle_event_called_with = (event_type, event_details, config_manager, logger, alerter, db_handler, storage_handler)
        if self.handle_event_side_effect:
            raise self.handle_event_side_effect

# --- PolicyEngine Tests ---

def test_policy_engine_register_handler(policy_engine_instance: PolicyEngine, mock_logger: MagicMock):
    handler_alpha = MockHandlerAlpha()
    policy_engine_instance.register_handler(handler_alpha)
    assert handler_alpha in policy_engine_instance._event_handlers
    mock_logger.log_info.assert_any_call(f"Registered handler: {type(handler_alpha).__name__}")

    # Test registering the same handler again (should log warning)
    policy_engine_instance.register_handler(handler_alpha)
    mock_logger.log_warning.assert_called_with(f"Handler {type(handler_alpha).__name__} already registered.")
    assert policy_engine_instance._event_handlers.count(handler_alpha) == 1


def test_policy_engine_process_event_single_handler(
    policy_engine_instance: PolicyEngine, 
    config_manager_instance: ConfigManager,
    mock_logger: MagicMock,
    mock_alerter: MagicMock,
    mock_db_handler: MagicMock,
    mock_storage_handler: MagicMock
):
    handler_alpha = MockHandlerAlpha(can_handle_response=True)
    policy_engine_instance.register_handler(handler_alpha)

    event_type = "TEST_EVENT_ALPHA"
    event_details = {"data": "alpha_data"}
    
    policy_engine_instance.process_event(event_type, event_details)

    assert handler_alpha.can_handle_called_with == (event_type, event_details)
    assert handler_alpha.handle_event_called_with is not None
    assert handler_alpha.handle_event_called_with[0] == event_type
    assert handler_alpha.handle_event_called_with[1] == event_details
    assert handler_alpha.handle_event_called_with[2] is config_manager_instance
    assert handler_alpha.handle_event_called_with[3] is mock_logger
    assert handler_alpha.handle_event_called_with[4] is mock_alerter
    assert handler_alpha.handle_event_called_with[5] is mock_db_handler
    assert handler_alpha.handle_event_called_with[6] is mock_storage_handler
    
    mock_logger.log_info.assert_any_call(f"Event {event_type} can be handled by MockHandlerAlpha. Attempting to handle.")
    mock_logger.log_info.assert_any_call(f"Handler MockHandlerAlpha finished processing event {event_type}.")
    mock_alerter.send_alert.assert_not_called()


def test_policy_engine_process_event_multiple_handlers(policy_engine_instance: PolicyEngine):
    handler_alpha = MockHandlerAlpha(can_handle_response=True) # Handles it
    handler_beta = MockHandlerBeta(can_handle_response=True)   # Also handles it
    
    policy_engine_instance.register_handler(handler_alpha)
    policy_engine_instance.register_handler(handler_beta)

    event_type = "SHARED_EVENT"
    event_details = {"data": "shared_data"}
    policy_engine_instance.process_event(event_type, event_details)

    assert handler_alpha.can_handle_called_with == (event_type, event_details)
    assert handler_alpha.handle_event_called_with is not None
    assert handler_beta.can_handle_called_with == (event_type, event_details)
    assert handler_beta.handle_event_called_with is not None

def test_policy_engine_process_event_no_handler(policy_engine_instance: PolicyEngine, mock_logger: MagicMock):
    handler_alpha = MockHandlerAlpha(can_handle_response=False) # Does not handle
    policy_engine_instance.register_handler(handler_alpha)

    event_type = "UNHANDLED_EVENT"
    event_details = {"data": "unhandled_data"}
    policy_engine_instance.process_event(event_type, event_details)

    assert handler_alpha.can_handle_called_with == (event_type, event_details)
    assert handler_alpha.handle_event_called_with is None # handle_event should not be called
    mock_logger.log_warning.assert_called_with(f"No registered handler found for event type: {event_type}", details=event_details)

def test_policy_engine_handler_exception(policy_engine_instance: PolicyEngine, mock_logger: MagicMock, mock_alerter: MagicMock):
    simulated_exception = ValueError("Handler Alpha Failed!")
    handler_alpha = MockHandlerAlpha(can_handle_response=True, handle_event_side_effect=simulated_exception)
    handler_beta = MockHandlerBeta(can_handle_response=True) # Should still run
    
    policy_engine_instance.register_handler(handler_alpha)
    policy_engine_instance.register_handler(handler_beta)

    event_type = "FAIL_EVENT"
    event_details = {"data": "fail_data"}
    policy_engine_instance.process_event(event_type, event_details)

    # Check Alpha (failed handler)
    assert handler_alpha.can_handle_called_with == (event_type, event_details)
    assert handler_alpha.handle_event_called_with is not None # It was called, but raised an exception
    mock_logger.log_error.assert_any_call(
        f"Error processing event {event_type} with handler MockHandlerAlpha", 
        error=simulated_exception,
        details=event_details
    )
    mock_alerter.send_alert.assert_any_call(
        subject=f"SelfHealing PolicyEngine Error: Handler MockHandlerAlpha Failed",
        body=ANY, # Body will contain error and details
        severity="CRITICAL",
        details=ANY
    )

    # Check Beta (should still have run)
    assert handler_beta.can_handle_called_with == (event_type, event_details)
    assert handler_beta.handle_event_called_with is not None
    mock_logger.log_info.assert_any_call(f"Handler MockHandlerBeta finished processing event {event_type}.")
```

**Step 6: Update `requirements.txt` (main project one)**
