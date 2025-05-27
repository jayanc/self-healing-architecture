import pytest
import sys
import json
from unittest.mock import MagicMock, patch, ANY # ANY is useful for some BQ client calls

from core.config_manager import ConfigManager
from core.interfaces import AbstractLogger # For type hinting if needed
from services.loggers import ConsoleLogger, BigQueryLogger
from services.alerters import EmailAlertManager

# --- ConsoleLogger Tests ---

def test_console_logger_log_info(capsys):
    logger = ConsoleLogger()
    logger.log_info("Test info message", details={"key": "value"})
    captured = capsys.readouterr()
    assert "[INFO] Test info message" in captured.out
    assert '"key": "value"' in captured.out
    assert captured.err == ""

def test_console_logger_log_warning(capsys):
    logger = ConsoleLogger()
    logger.log_warning("Test warning message", details={"code": 123})
    captured = capsys.readouterr()
    assert "[WARNING] Test warning message" in captured.out
    assert '"code": 123' in captured.out
    assert captured.err == ""

def test_console_logger_log_error(capsys):
    logger = ConsoleLogger()
    try:
        raise ValueError("Simulated error")
    except ValueError as e:
        logger.log_error("Test error message", error=e, details={"context": "testing"})
    
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "[ERROR] Test error message" in captured.err
    assert "ValueError(Simulated error)" in captured.err
    assert '"context": "testing"' in captured.err

def test_console_logger_log_policy_event(capsys):
    logger = ConsoleLogger()
    event_data = {"event_id": "evt_123", "policy_id": "POLICY_X", "status": "SUCCESS"}
    logger.log_policy_event(event_data)
    captured = capsys.readouterr()
    assert "[POLICY_EVENT]" in captured.out
    assert '"event_id": "evt_123"' in captured.out
    assert '"policy_id": "POLICY_X"' in captured.out
    assert captured.err == ""

def test_console_logger_log_data_flow_event(capsys):
    logger = ConsoleLogger()
    flow_data = {"flow_run_id": "flow_abc", "status": "COMPLETED_SILVER"}
    logger.log_data_flow_event(flow_data)
    captured = capsys.readouterr()
    assert "[DATA_FLOW_EVENT]" in captured.out
    assert '"flow_run_id": "flow_abc"' in captured.out
    assert '"status": "COMPLETED_SILVER"' in captured.out
    assert captured.err == ""

# --- BigQueryLogger Tests ---

@pytest.fixture
def mock_bq_client():
    """Mocks the BigQuery client."""
    with patch('services.loggers.bigquery.Client') as mock_client_constructor:
        mock_instance = MagicMock()
        mock_client_constructor.return_value = mock_instance
        yield mock_instance

@pytest.fixture
def mock_config_manager_for_bq(config_manager_instance: ConfigManager, mocker: MagicMock) -> MagicMock:
    """
    Provides a ConfigManager mock specific for BigQueryLogger tests,
    ensuring tracking_and_logging config is present.
    """
    # Use the real config_manager_instance to get most settings, but mock specific ones if needed
    # For BQLogger, we need 'tracking_and_logging'
    mock_cm = mocker.MagicMock(spec=ConfigManager)
    
    # Get real tracking config to ensure table IDs are valid as per policy_config.yaml
    try:
        tracking_config = config_manager_instance.get_tracking_and_logging_config()
        mock_cm.get_tracking_and_logging_config.return_value = tracking_config
    except Exception as e: # If the main config is missing the section
        pytest.skip(f"Skipping BigQueryLogger tests: 'tracking_and_logging' section missing or error in config: {e}")

    # Mock other methods if BigQueryLogger's constructor uses them
    mock_cm.get_parameter.side_effect = config_manager_instance.get_parameter 
    mock_cm.get_section.side_effect = config_manager_instance.get_section

    return mock_cm


def test_bigquery_logger_initialization(mock_config_manager_for_bq: MagicMock, mock_bq_client: MagicMock):
    """Tests successful initialization of BigQueryLogger."""
    logger = BigQueryLogger(config_manager=mock_config_manager_for_bq, project_id="test-project")
    assert logger._client is not None
    mock_config_manager_for_bq.get_tracking_and_logging_config.assert_called_once()
    assert logger._policy_log_table_id is not None
    assert logger._data_flow_log_table_id is not None

def test_bigquery_logger_initialization_no_bq_lib(mock_config_manager_for_bq: MagicMock, mocker: MagicMock, capsys):
    """Tests BQLogger initialization when google-cloud-bigquery is not installed."""
    mocker.patch.object(sys.modules['services.loggers'], 'bigquery', None) # Simulate not installed
    
    logger = BigQueryLogger(config_manager=mock_config_manager_for_bq)
    assert logger._client is None
    captured = capsys.readouterr()
    assert "BigQueryLogger disabled: google-cloud-bigquery not found." in captured.err # From fallback
    assert "BigQueryLogger cannot be initialized because google-cloud-bigquery is not installed." in captured.err # From BQLogger itself

def test_bigquery_logger_log_policy_event(mock_config_manager_for_bq: MagicMock, mock_bq_client: MagicMock):
    logger = BigQueryLogger(config_manager=mock_config_manager_for_bq)
    event_data = {"event_id": "evt_policy_001", "policy_id": "TEST_POLICY", "status": "TRIGGERED"}
    
    logger.log_policy_event(event_data.copy()) # Pass a copy as it might be modified
    
    # Check that insert_rows_json was called on the mocked BQ client
    # The first argument to insert_rows_json is the table_id, the second is the list of rows
    mock_bq_client.insert_rows_json.assert_called_once()
    args, kwargs = mock_bq_client.insert_rows_json.call_args
    assert args[0] == logger._policy_log_table_id
    assert len(args[1]) == 1
    # event_id and event_timestamp are added if not present, so we check for policy_id.
    assert args[1][0]['policy_id'] == "TEST_POLICY"
    assert 'event_id' in args[1][0] # Should be auto-generated if not provided
    assert 'event_timestamp' in args[1][0] # Should be auto-generated

def test_bigquery_logger_log_data_flow_event(mock_config_manager_for_bq: MagicMock, mock_bq_client: MagicMock):
    logger = BigQueryLogger(config_manager=mock_config_manager_for_bq)
    flow_data = {"flow_run_id": "flow_run_789", "data_source_name": "source_A", "status": "INGESTION_COMPLETE"}
    
    logger.log_data_flow_event(flow_data.copy())
    
    mock_bq_client.insert_rows_json.assert_called_once()
    args, kwargs = mock_bq_client.insert_rows_json.call_args
    assert args[0] == logger._data_flow_log_table_id
    assert len(args[1]) == 1
    assert args[1][0]['data_source_name'] == "source_A"
    assert 'last_updated_timestamp' in args[1][0]

def test_bigquery_logger_log_info_as_generic_event(mock_config_manager_for_bq: MagicMock, mock_bq_client: MagicMock):
    logger = BigQueryLogger(config_manager=mock_config_manager_for_bq)
    logger.log_info("BQ info message", details={"component": "test_component"})
    
    mock_bq_client.insert_rows_json.assert_called_once()
    args, kwargs = mock_bq_client.insert_rows_json.call_args
    assert args[0] == logger._policy_log_table_id # Logs to policy_execution_log
    logged_row = args[1][0]
    assert logged_row["policy_id"] == "INTERNAL_LOG_MESSAGE"
    assert logged_row["detected_issue_type"] == "INTERNAL_INFO"
    assert logged_row["detected_issue_details"] == "BQ info message"
    assert '"component": "test_component"' in logged_row["action_parameters"]

def test_bigquery_logger_insert_rows_failure_fallback(mock_config_manager_for_bq: MagicMock, mock_bq_client: MagicMock, mocker: MagicMock):
    mock_fallback_logger = mocker.MagicMock(spec=AbstractLogger)
    logger = BigQueryLogger(config_manager=mock_config_manager_for_bq, fallback_logger=mock_fallback_logger)
    
    mock_bq_client.insert_rows_json.side_effect = Exception("BigQuery Write Failed")
    
    event_data = {"event_id": "evt_fail", "policy_id": "FAIL_POLICY"}
    logger.log_policy_event(event_data)
    
    mock_fallback_logger.log_error.assert_called_once()
    args, kwargs = mock_fallback_logger.log_error.call_args
    assert "BigQuery API call error" in args[0] or "Unexpected error inserting rows" in args[0]
    assert "FAIL_POLICY" in str(kwargs.get("details", {}))


# --- EmailAlertManager Tests ---

@pytest.fixture
def mock_smtp():
    """Mocks smtplib.SMTP."""
    with patch('services.alerters.smtplib.SMTP') as mock_smtp_constructor:
        mock_instance = MagicMock()
        # Configure for `with` statement if necessary, though often not needed for basic sendmail mock
        mock_instance.__enter__.return_value = mock_instance 
        mock_client_constructor.return_value = mock_instance
        yield mock_instance

@pytest.fixture
def mock_config_manager_for_email(config_manager_instance: ConfigManager, mocker: MagicMock) -> MagicMock:
    """Provides a ConfigManager mock specific for EmailAlertManager tests."""
    mock_cm = mocker.MagicMock(spec=ConfigManager)
    try:
        # Get real alerting config to make tests more realistic
        alerting_config = config_manager_instance.get_alerting_config()
        email_provider_config = alerting_config.get("providers", {}).get("email", {})
        
        # Ensure necessary keys exist for the test, otherwise skip
        if not all(k in email_provider_config for k in ["smtp_server", "default_sender"]):
            pytest.skip("Email provider config incomplete in policy_config.yaml, skipping EmailAlertManager tests.")

        mock_cm.get_alerting_config.return_value = alerting_config
    except Exception as e:
        pytest.skip(f"Skipping EmailAlertManager tests due to config error: {e}")
    return mock_cm


def test_email_alert_manager_initialization(mock_config_manager_for_email: MagicMock):
    alerter = EmailAlertManager(config_manager=mock_config_manager_for_email)
    assert alerter._is_configured is True
    assert alerter.smtp_server is not None
    assert alerter.sender_email is not None

def test_email_alert_manager_send_alert_critical(mock_config_manager_for_email: MagicMock, mock_smtp: MagicMock):
    alerter = EmailAlertManager(config_manager=mock_config_manager_for_email)
    if not alerter._is_configured: pytest.skip("Alerter not configured, skipping send test.")

    subject = "Test Critical Alert"
    body = "This is a critical test."
    severity = "CRITICAL"
    details = {"code": "C001"}
    
    alerter.send_alert(subject, body, severity, details)
    
    mock_smtp.sendmail.assert_called_once()
    args, _ = mock_smtp.sendmail.call_args
    sender, recipients, msg_string = args
    
    assert sender == alerter.sender_email
    # Based on sample policy_config.yaml, critical_recipients should be used
    expected_critical_recipients = alerter.critical_recipients 
    assert all(recip in recipients for recip in expected_critical_recipients)
    
    assert f"Subject: [SelfHealingAlert:CRITICAL] {subject}" in msg_string
    assert f"Severity: CRITICAL" in msg_string
    assert body in msg_string
    assert '"code": "C001"' in msg_string

def test_email_alert_manager_send_alert_info(mock_config_manager_for_email: MagicMock, mock_smtp: MagicMock):
    alerter = EmailAlertManager(config_manager=mock_config_manager_for_email)
    if not alerter._is_configured: pytest.skip("Alerter not configured, skipping send test.")

    subject = "Test Info Alert"
    body = "This is an info test."
    severity = "INFO"
    
    alerter.send_alert(subject, body, severity)
    
    mock_smtp.sendmail.assert_called_once()
    args, _ = mock_smtp.sendmail.call_args
    sender, recipients, msg_string = args
    
    assert sender == alerter.sender_email
    expected_default_recipients = alerter.default_recipients
    assert all(recip in recipients for recip in expected_default_recipients)
    
    assert f"Subject: [SelfHealingAlert:INFO] {subject}" in msg_string
    assert f"Severity: INFO" in msg_string

def test_email_alert_manager_smtp_failure(mock_config_manager_for_email: MagicMock, mock_smtp: MagicMock, mocker: MagicMock):
    mock_fallback_logger = mocker.MagicMock(spec=AbstractLogger)
    alerter = EmailAlertManager(config_manager=mock_config_manager_for_email, fallback_logger=mock_fallback_logger)
    if not alerter._is_configured: pytest.skip("Alerter not configured, skipping send test.")

    mock_smtp.sendmail.side_effect = smtplib.SMTPConnectError(500, "Connection timed out")
    
    alerter.send_alert("Test SMTP Fail", "Body", "ERROR")
    
    mock_fallback_logger.log_error.assert_called_once()
    args, kwargs = mock_fallback_logger.log_error.call_args
    assert "Failed to connect to SMTP server" in args[0]

def test_email_alert_manager_not_configured(mock_config_manager_for_email: MagicMock, mocker: MagicMock):
    # Simulate missing essential email config
    mock_cm_bad = mocker.MagicMock(spec=ConfigManager)
    mock_cm_bad.get_alerting_config.return_value = {"providers": {"email": {"default_sender": "test@example.com"}}} # Missing smtp_server
    
    mock_fallback_logger = mocker.MagicMock(spec=AbstractLogger)
    alerter = EmailAlertManager(config_manager=mock_cm_bad, fallback_logger=mock_fallback_logger)
    
    assert alerter._is_configured is False
    mock_fallback_logger.log_error.assert_called_once() # Should log error during init
    
    alerter.send_alert("Test Unconfigured", "Body", "INFO")
    # Check that another error is logged when trying to send
    # The first call was during init, this is the second.
    assert mock_fallback_logger.log_error.call_count == 2
    args, kwargs = mock_fallback_logger.log_error.call_args_list[1] # Get the second call
    assert "EmailAlertManager is not configured" in args[0]
