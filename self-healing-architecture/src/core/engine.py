from .config_manager import ConfigManager
from .interfaces import AbstractPolicyEventHandler, AbstractLogger, AbstractAlertManager, AbstractDatabaseHandler, AbstractStorageHandler
from typing import List, Dict, Any, Optional
import importlib # Keep for future use, even if dynamic loading is initially simple

class PolicyEngine:
    def __init__(self, 
                 config_manager: ConfigManager, 
                 logger: AbstractLogger, 
                 alerter: AbstractAlertManager,
                 db_handler: Optional[AbstractDatabaseHandler] = None, # Optional, injected if needed by handlers
                 storage_handler: Optional[AbstractStorageHandler] = None # Optional, injected if needed by handlers
                 ):
        self.config_manager = config_manager
        self.logger = logger
        self.alerter = alerter
        self.db_handler = db_handler
        self.storage_handler = storage_handler
        self._event_handlers: List[AbstractPolicyEventHandler] = []
        # self.load_dynamic_handlers() # Dynamic loading can be complex; start with manual registration
        self.logger.log_info("PolicyEngine initialized.")

    def register_handler(self, handler: AbstractPolicyEventHandler):
        if handler not in self._event_handlers:
            self._event_handlers.append(handler)
            self.logger.log_info(f"Registered handler: {type(handler).__name__}")
        else:
            self.logger.log_warning(f"Handler {type(handler).__name__} already registered.")

    # Optional: Placeholder for a more advanced dynamic loading mechanism
    # def load_dynamic_handlers(self):
    #     handler_configs = self.config_manager.get_section("policy_handlers_config") # Example config section
    #     if handler_configs:
    #         for handler_name, config_details in handler_configs.items():
    #             if config_details.get("enabled", False):
    #                 try:
    #                     module_path, class_name = config_details["class_path"].rsplit('.', 1)
    #                     module = importlib.import_module(module_path)
    #                     handler_class = getattr(module, class_name)
    #                     # Here, you might pass handler-specific config or shared resources
    #                     self.register_handler(handler_class()) 
    #                 except Exception as e:
    #                     self.logger.log_error(f"Failed to dynamically load handler {handler_name}", error=e)


    def process_event(self, event_type: str, event_details: Dict[Any, Any]):
        self.logger.log_info(f"Processing event: {event_type}", details=event_details)
        
        handled_by_at_least_one = False
        for handler in self._event_handlers:
            try:
                if handler.can_handle(event_type, event_details):
                    self.logger.log_info(f"Event {event_type} can be handled by {type(handler).__name__}. Attempting to handle.")
                    handler.handle_event(
                        event_type=event_type,
                        event_details=event_details,
                        config_manager=self.config_manager,
                        logger=self.logger,
                        alerter=self.alerter,
                        db_handler=self.db_handler,
                        storage_handler=self.storage_handler
                    )
                    handled_by_at_least_one = True
                    self.logger.log_info(f"Handler {type(handler).__name__} finished processing event {event_type}.")
                    # Current assumption: all applicable handlers process the event.
                    # If only the first match should handle, add 'break' here.
            except Exception as e:
                self.logger.log_error(
                    f"Error processing event {event_type} with handler {type(handler).__name__}", 
                    error=e,
                    details=event_details
                )
                # Send an alert about the handler failure
                try:
                    self.alerter.send_alert(
                        subject=f"SelfHealing PolicyEngine Error: Handler {type(handler).__name__} Failed",
                        body=f"Handler {type(handler).__name__} encountered an error processing event type {event_type}.\nError: {str(e)}\nDetails: {json.dumps(event_details, default=str)}",
                        severity="CRITICAL",
                        details={"event_type": event_type, "handler": type(handler).__name__, "error_message": str(e), "event_details": event_details}
                    )
                except Exception as alert_e:
                    self.logger.log_error(f"Failed to send alert about handler failure for {type(handler).__name__}", error=alert_e)


        if not handled_by_at_least_one:
            self.logger.log_warning(f"No registered handler found for event type: {event_type}", details=event_details)
            # Consider alerting for unhandled events based on configuration or severity
            # Example:
            # try:
            #     if self.config_manager.get_parameter("global_settings", "alert_on_unhandled_events", default=False):
            #         self.alerter.send_alert(
            #             subject=f"SelfHealing PolicyEngine: Unhandled Event",
            #             body=f"No registered handler could process event type: {event_type}.\nDetails: {json.dumps(event_details, default=str)}",
            #             severity="WARNING",
            #             details={"event_type": event_type, "event_details": event_details}
            #         )
            # except Exception as e:
            #     self.logger.log_error("Failed to process alert_on_unhandled_events setting or send unhandled event alert.", error=e)
import json # for formatting details in alert body

# Example usage (for testing, not part of the class itself)
# if __name__ == '__main__':
#     # This example assumes you have concrete implementations of the interfaces
#     # and a valid policy_config.yaml accessible.
#     from ..core.config_manager import ConfigManager
#     from ..services.loggers import ConsoleLogger # Assuming ConsoleLogger is in src/services/
#     from ..services.alerters import EmailAlertManager # Assuming EmailAlertManager is in src/services/
#     # from ..services.db_handlers import BigQueryHandler # Example
#     # from ..services.storage_handlers import GCSStorageHandler # Example

#     print("Running PolicyEngine example...")
#     try:
#         # Determine the base directory (self-healing-architecture)
#         # __file__ is src/core/engine.py
#         # engine_dir = os.path.dirname(os.path.abspath(__file__)) # src/core
#         # src_dir = os.path.dirname(engine_dir) # src
#         # base_dir = os.path.dirname(src_dir) # self-healing-architecture
#         # config_file_path = os.path.join(base_dir, "config", "policy_config.yaml")
        
#         # Simplified path for when running from self-healing-architecture root
#         config_file_path = "config/policy_config.yaml"
#         if not os.path.exists(config_file_path):
#             print(f"Config file not found at {config_file_path}. Please ensure it's correctly placed.")
#             exit(1)

#         config = ConfigManager(config_path=config_file_path)
#         logger = ConsoleLogger() # Using ConsoleLogger for this example
#         # Ensure your policy_config.yaml has 'email' provider configured for EmailAlertManager to work
#         alerter = EmailAlertManager(config_manager=config) # Basic alerter
        
#         # db_handler = BigQueryHandler(config_manager=config, logger=logger) # If you have it
#         # storage_handler = GCSStorageHandler(config_manager=config, logger=logger) # If you have it

#         engine = PolicyEngine(config, logger, alerter) #, db_handler, storage_handler)

#         # --- Example Handler (define a dummy one for testing) ---
#         class MyTestHandler(AbstractPolicyEventHandler):
#             def can_handle(self, event_type: str, event_details: Dict[Any, Any]) -> bool:
#                 return event_type == "TEST_EVENT" or event_type == "FAILING_EVENT"

#             def handle_event(self, event_type: str, event_details: Dict[Any, Any], 
#                              config_manager: ConfigManager, logger: AbstractLogger, 
#                              alerter: AbstractAlertManager, db_handler: Optional[AbstractDatabaseHandler], 
#                              storage_handler: Optional[AbstractStorageHandler]) -> None:
#                 logger.log_info(f"MyTestHandler handling {event_type}", details=event_details)
#                 if event_type == "FAILING_EVENT":
#                     raise ValueError("Simulated failure in MyTestHandler")
#                 # Example: Accessing config
#                 log_level = config_manager.get_parameter("global_settings", "log_level")
#                 logger.log_info(f"Global log level from config: {log_level}")

#         test_handler = MyTestHandler()
#         engine.register_handler(test_handler)

#         # --- Process a test event ---
#         engine.process_event("TEST_EVENT", {"data": "some_value", "id": 123})
#         engine.process_event("UNHANDLED_EVENT", {"data": "another_value"})
#         engine.process_event("FAILING_EVENT", {"critical_data": "this_will_fail_in_handler"})

#     except Exception as e:
#         print(f"Error in PolicyEngine example: {e}")
#         import traceback
#         traceback.print_exc()
