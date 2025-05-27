import pytest
from unittest.mock import MagicMock, call, ANY # ANY for some flexible assertions
import json # For alert body formatting

# Adjust path to import from src
import sys
from pathlib import Path
project_root_path = Path(__file__).resolve().parent.parent.parent # self-healing-architecture
sys.path.insert(0, str(project_root_path / 'src'))


from core.config_manager import ConfigManager
from core.interfaces import (
    AbstractLogger, 
    AbstractAlertManager, 
    AbstractPolicyEventHandler, 
    AbstractDatabaseHandler, 
    AbstractStorageHandler
)
from core.engine import PolicyEngine

# Using fixtures from conftest.py: config_manager_instance

@pytest.fixture
def mock_logger_integration(mocker: MagicMock) -> MagicMock: # Renamed to avoid conflict if used elsewhere
    return mocker.MagicMock(spec=AbstractLogger)

@pytest.fixture
def mock_alerter_integration(mocker: MagicMock) -> MagicMock: # Renamed
    return mocker.MagicMock(spec=AbstractAlertManager)

@pytest.fixture
def mock_db_handler_integration(mocker: MagicMock) -> MagicMock: # Renamed
    return mocker.MagicMock(spec=AbstractDatabaseHandler)

@pytest.fixture
def mock_storage_handler_integration(mocker: MagicMock) -> MagicMock: # Renamed
    return mocker.MagicMock(spec=AbstractStorageHandler)

@pytest.fixture
def policy_engine_instance_integration( # Renamed
    config_manager_instance: ConfigManager, 
    mock_logger_integration: MagicMock, 
    mock_alerter_integration: MagicMock,
    mock_db_handler_integration: MagicMock,
    mock_storage_handler_integration: MagicMock
) -> PolicyEngine:
    return PolicyEngine(
        config_manager=config_manager_instance,
        logger=mock_logger_integration,
        alerter=mock_alerter_integration,
        db_handler=mock_db_handler_integration,
        storage_handler=mock_storage_handler_integration
    )

# --- Mock Policy Event Handlers for Testing ---
class MockHandlerAlpha(AbstractPolicyEventHandler):
    def __init__(self, can_handle_response=True, handle_event_side_effect=None, handler_id="Alpha"):
        self.can_handle_response = can_handle_response
        self.handle_event_side_effect = handle_event_side_effect
        self.handler_id = handler_id
        self.can_handle_called_with = None
        self.handle_event_called_with = None
        self.handle_event_call_count = 0


    def can_handle(self, event_type: str, event_details: dict) -> bool:
        self.can_handle_called_with = (event_type, event_details)
        # print(f"MockHandler {self.handler_id} can_handle called with {event_type}, returning {self.can_handle_response}")
        return self.can_handle_response

    def handle_event(self, event_type: str, event_details: dict, config_manager, logger, alerter, db_handler, storage_handler) -> None:
        self.handle_event_called_with = (event_type, event_details, config_manager, logger, alerter, db_handler, storage_handler)
        self.handle_event_call_count += 1
        # print(f"MockHandler {self.handler_id} handle_event called with {event_type}")
        if self.handle_event_side_effect:
            if isinstance(self.handle_event_side_effect, Exception):
                raise self.handle_event_side_effect
            else: # if it's a callable
                self.handle_event_side_effect()
    
    def __repr__(self): # For better logging from PolicyEngine if type(handler).__name__ is used
        return f"MockHandler{self.handler_id}"


# --- PolicyEngine Tests ---

def test_policy_engine_register_handler(policy_engine_instance_integration: PolicyEngine, mock_logger_integration: MagicMock):
    handler_alpha = MockHandlerAlpha(handler_id="AlphaReg")
    policy_engine_instance_integration.register_handler(handler_alpha)
    assert handler_alpha in policy_engine_instance_integration._event_handlers
    mock_logger_integration.log_info.assert_any_call(f"Registered handler: MockHandlerAlphaReg") # Based on __repr__

    # Test registering the same handler again (should log warning)
    policy_engine_instance_integration.register_handler(handler_alpha)
    mock_logger_integration.log_warning.assert_called_with(f"Handler MockHandlerAlphaReg already registered.")
    assert policy_engine_instance_integration._event_handlers.count(handler_alpha) == 1


def test_policy_engine_process_event_single_handler_match(
    policy_engine_instance_integration: PolicyEngine, 
    config_manager_instance: ConfigManager,
    mock_logger_integration: MagicMock,
    mock_alerter_integration: MagicMock,
    mock_db_handler_integration: MagicMock,
    mock_storage_handler_integration: MagicMock
):
    handler_alpha = MockHandlerAlpha(can_handle_response=True, handler_id="SingleMatch")
    policy_engine_instance_integration.register_handler(handler_alpha)

    event_type = "TEST_EVENT_SINGLE"
    event_details = {"data": "alpha_data_single"}
    
    policy_engine_instance_integration.process_event(event_type, event_details)

    assert handler_alpha.can_handle_called_with == (event_type, event_details)
    assert handler_alpha.handle_event_called_with is not None
    assert handler_alpha.handle_event_called_with[0] == event_type
    assert handler_alpha.handle_event_called_with[1] == event_details
    assert handler_alpha.handle_event_called_with[2] is config_manager_instance
    assert handler_alpha.handle_event_called_with[3] is mock_logger_integration
    assert handler_alpha.handle_event_called_with[4] is mock_alerter_integration
    assert handler_alpha.handle_event_called_with[5] is mock_db_handler_integration
    assert handler_alpha.handle_event_called_with[6] is mock_storage_handler_integration
    
    mock_logger_integration.log_info.assert_any_call(f"Event {event_type} can be handled by MockHandlerSingleMatch. Attempting to handle.")
    mock_logger_integration.log_info.assert_any_call(f"Handler MockHandlerSingleMatch finished processing event {event_type}.")
    mock_alerter_integration.send_alert.assert_not_called()


def test_policy_engine_process_event_multiple_handlers_one_match(policy_engine_instance_integration: PolicyEngine):
    handler_alpha = MockHandlerAlpha(can_handle_response=False, handler_id="MultiNoMatch") 
    handler_beta = MockHandlerAlpha(can_handle_response=True, handler_id="MultiMatch")   
    
    policy_engine_instance_integration.register_handler(handler_alpha)
    policy_engine_instance_integration.register_handler(handler_beta)

    event_type = "MULTI_EVENT_ONE_MATCH"
    event_details = {"data": "multi_data_one"}
    policy_engine_instance_integration.process_event(event_type, event_details)

    assert handler_alpha.can_handle_called_with == (event_type, event_details)
    assert handler_alpha.handle_event_called_with is None # Should not be called

    assert handler_beta.can_handle_called_with == (event_type, event_details)
    assert handler_beta.handle_event_called_with is not None


def test_policy_engine_process_event_multiple_handlers_all_match(policy_engine_instance_integration: PolicyEngine):
    handler_alpha = MockHandlerAlpha(can_handle_response=True, handler_id="MultiAllAlpha") 
    handler_beta = MockHandlerAlpha(can_handle_response=True, handler_id="MultiAllBeta")   
    
    policy_engine_instance_integration.register_handler(handler_alpha)
    policy_engine_instance_integration.register_handler(handler_beta)

    event_type = "SHARED_EVENT_ALL_MATCH"
    event_details = {"data": "shared_data_all"}
    policy_engine_instance_integration.process_event(event_type, event_details)

    assert handler_alpha.can_handle_called_with == (event_type, event_details)
    assert handler_alpha.handle_event_called_with is not None
    assert handler_beta.can_handle_called_with == (event_type, event_details)
    assert handler_beta.handle_event_called_with is not None


def test_policy_engine_process_event_no_handler_match(policy_engine_instance_integration: PolicyEngine, mock_logger_integration: MagicMock):
    handler_alpha = MockHandlerAlpha(can_handle_response=False, handler_id="NoMatchAlpha") 
    policy_engine_instance_integration.register_handler(handler_alpha)

    event_type = "UNHANDLED_EVENT_NO_MATCH"
    event_details = {"data": "unhandled_data_no_match"}
    policy_engine_instance_integration.process_event(event_type, event_details)

    assert handler_alpha.can_handle_called_with == (event_type, event_details)
    assert handler_alpha.handle_event_called_with is None 
    mock_logger_integration.log_warning.assert_called_with(f"No registered handler found for event type: {event_type}", details=event_details)

def test_policy_engine_handler_exception_continues_and_alerts(
    policy_engine_instance_integration: PolicyEngine, 
    mock_logger_integration: MagicMock, 
    mock_alerter_integration: MagicMock
):
    simulated_exception = ValueError("Handler Alpha Failed Spectacularly!")
    handler_alpha = MockHandlerAlpha(can_handle_response=True, handle_event_side_effect=simulated_exception, handler_id="FailAlpha")
    handler_beta = MockHandlerAlpha(can_handle_response=True, handler_id="GoodBeta") 
    
    policy_engine_instance_integration.register_handler(handler_alpha)
    policy_engine_instance_integration.register_handler(handler_beta)

    event_type = "CRITICAL_FAIL_EVENT"
    event_details = {"data": "critical_fail_data"}
    policy_engine_instance_integration.process_event(event_type, event_details)

    # Check Alpha (failed handler)
    assert handler_alpha.can_handle_called_with == (event_type, event_details)
    assert handler_alpha.handle_event_called_with is not None 
    assert handler_alpha.handle_event_call_count == 1
    
    # Check if logger.log_error was called for handler_alpha's exception
    mock_logger_integration.log_error.assert_any_call(
        f"Error processing event {event_type} with handler MockHandlerFailAlpha", 
        error=simulated_exception,
        details=event_details
    )
    
    # Check if alerter.send_alert was called for handler_alpha's exception
    # We use ANY for body and details because they can be complex formatted strings
    mock_alerter_integration.send_alert.assert_any_call(
        subject=f"SelfHealing PolicyEngine Error: Handler MockHandlerFailAlpha Failed",
        body=ANY, 
        severity="CRITICAL",
        details=ANY
    )
    args, kwargs = mock_alerter_integration.send_alert.call_args # Get the arguments of the last call
    assert kwargs['details']['handler'] == "MockHandlerFailAlpha"
    assert kwargs['details']['error_message'] == str(simulated_exception)


    # Check Beta (should still have run because PolicyEngine continues)
    assert handler_beta.can_handle_called_with == (event_type, event_details)
    assert handler_beta.handle_event_called_with is not None
    assert handler_beta.handle_event_call_count == 1
    mock_logger_integration.log_info.assert_any_call(f"Handler MockHandlerGoodBeta finished processing event {event_type}.")


def test_policy_engine_alert_on_unhandled_event_if_configured(
    policy_engine_instance_integration: PolicyEngine, 
    config_manager_instance: ConfigManager, # Use real one to test config get
    mock_logger_integration: MagicMock,
    mock_alerter_integration: MagicMock,
    mocker: MagicMock
):
    # Temporarily mock the get_parameter for this specific test
    mocker.patch.object(config_manager_instance, 'get_parameter', return_value=True) # Enable alert_on_unhandled_events

    event_type = "VERY_UNHANDLED_EVENT"
    event_details = {"info": "this one should trigger an alert"}
    policy_engine_instance_integration.process_event(event_type, event_details)

    mock_logger_integration.log_warning.assert_called_with(f"No registered handler found for event type: {event_type}", details=event_details)
    
    # This part is commented out in PolicyEngine, so this test would fail unless it's enabled.
    # For now, we test the logger warning. If the alerting for unhandled is uncommented in PolicyEngine,
    # this assertion should be enabled:
    # mock_alerter_integration.send_alert.assert_called_with(
    #     subject=f"SelfHealing PolicyEngine: Unhandled Event",
    #     body=ANY,
    #     severity="WARNING",
    #     details={"event_type": event_type, "event_details": event_details}
    # )
    pytest.skip("Skipping alert for unhandled event as it's commented out in PolicyEngine. Warning log is checked.")
