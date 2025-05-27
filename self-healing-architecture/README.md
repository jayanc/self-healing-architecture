# Self-Healing Framework for Data Architecture

## 1. Project Overview

This project provides a Python-based framework for implementing self-healing capabilities within a data architecture, particularly one involving GCS and BigQuery (though designed for extensibility). It aims to automatically detect, diagnose, and recover from common failures, log its actions, and provide data for monitoring and improvement. The framework is driven by a central policy configuration file (`config/policy_config.yaml`).

## 2. Directory Structure

-   `config/`: Contains configuration files.
    -   `policy_config.yaml`: The main configuration file defining policies, thresholds, and settings for the framework.
-   `src/`: Contains the core Python source code.
    -   `core/`: Core components like the PolicyEngine, ConfigManager, and abstract interfaces.
    -   `handlers/`: Concrete implementations of event handlers for specific issues (e.g., GCS errors, BQ job failures).
    -   `services/`: Concrete implementations for services like logging, alerting, storage interaction (GCS), and database interaction (BQ).
    -   `utils/`: Utility functions (if any).
-   `tests/`: Contains unit and integration tests.
    -   `unit/`: Unit tests for individual modules and classes.
    -   `integration/`: Tests for interactions between components.
    -   `conftest.py`: Pytest configuration and shared fixtures.
-   `gcf_gcs_event_trigger/`: Example Google Cloud Function for triggering the PolicyEngine from GCS events.
    -   `main.py`: GCF entry point.
    -   `requirements.txt`: GCF specific dependencies.
-   `POLICY.md`: The human-readable policy document describing the self-healing strategies.
-   `requirements.txt`: Main Python project dependencies.
-   `README.md`: This file.

## 3. Configuration (`config/policy_config.yaml`)

The `policy_config.yaml` file is central to the framework's operation. It defines:
-   Global settings (e.g., logging levels).
-   Source-specific parameters (retry attempts, timeouts).
-   GCS and BigQuery settings (DLQ paths, job retry counts).
-   Alerting configurations (channels, recipients, webhook URLs).
-   BigQuery tracking table IDs (`policy_execution_log_table_id`, `data_flow_log_table_id`).
-   Parameters for CDC, streaming, incremental loads, and deduplication handling.

Refer to the comments within `policy_config.yaml` and `POLICY.md` for detailed explanations of each parameter.

## 4. Setup and Installation

1.  **Clone the repository.**
2.  **Create and activate a Python virtual environment:**
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```
3.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```
    If you plan to run or deploy the GCF example, ensure its specific `requirements.txt` is also handled, typically during GCF deployment.

## 5. Running the System

### a. Running Tests

To ensure the framework is functioning correctly:
```bash
pytest tests/
```

### b. Triggering the Policy Engine

The PolicyEngine is designed to be triggered by events. 
-   **Event-Driven (Example: Google Cloud Function)**:
    The `gcf_gcs_event_trigger/` directory contains an example of a Google Cloud Function that can be deployed to trigger the PolicyEngine based on GCS events (e.g., a file landing in an error path). Refer to the comments in `gcf_gcs_event_trigger/main.py` for deployment and trigger configuration details.
-   **Manual/Scripted Invocation (for testing or specific use cases)**:
    You can write a Python script to initialize and invoke the `PolicyEngine` directly:
    ```python
    # main_manual_trigger.py (example)
    from src.core.config_manager import ConfigManager
    from src.core.engine import PolicyEngine
    from src.services.loggers import ConsoleLogger, BigQueryLogger # ... and other services
    from src.services.alerters import EmailAlertManager
    from src.services.storage_handlers import GCSStorageHandler
    from src.services.database_handlers import BigQueryDatabaseHandler
    from src.handlers.storage_event_handlers import GCSUploadRetryHandler # ... and other handlers

    if __name__ == "__main__":
        config_mgr = ConfigManager(config_path='config/policy_config.yaml')
        
        # Initialize services (logger, alerter, etc.)
        logger = ConsoleLogger() # Or BigQueryLogger, or a composite logger
        bq_logger = BigQueryLogger(config_mgr, project_id='your-gcp-project') # Example
        alerter = EmailAlertManager(config_mgr)
        gcs_handler = GCSStorageHandler(config_mgr, logger)
        bq_handler = BigQueryDatabaseHandler(config_mgr, logger)

        engine = PolicyEngine(config_mgr, bq_logger, alerter, bq_handler, gcs_handler)

        # Register handlers
        engine.register_handler(GCSUploadRetryHandler())
        # ... register other handlers ...

        # Example event
        event_type = "GCS_UPLOAD_FAILED" # Example
        event_details = {
            "bucket_name": "your-bucket", 
            "object_name": "path/to/failed_file.txt",
            "source_path": "/local/path/to/failed_file.txt", # For retry handler
            "error_message": "Simulated failure"
        }
        engine.process_event(event_type, event_details)
    ```

## 6. Key Components

-   **`PolicyEngine` (`src/core/engine.py`)**: Orchestrates event processing. Receives events, consults `policy_config.yaml` via `ConfigManager`, and dispatches to appropriate handlers.
-   **`ConfigManager` (`src/core/config_manager.py`)**: Loads and provides access to `policy_config.yaml`.
-   **Abstract Interfaces (`src/core/interfaces.py`)**: Define contracts for loggers, alerters, storage handlers, database handlers, and policy event handlers, enabling modularity.
-   **Service Implementations (`src/services/`)**: Concrete classes for logging (Console, BigQuery), alerting (Email), GCS operations, and BigQuery operations.
-   **Event Handlers (`src/handlers/`)**: Contain the logic for specific self-healing actions (e.g., retrying GCS uploads, handling BQ job failures).

## 7. Extensibility

The framework is designed to be extensible:

-   **Adding New Event Handlers**:
    1.  Create a new class inheriting from `AbstractPolicyEventHandler` (or a more specific abstract handler like `AbstractCDCEventHandler`).
    2.  Implement `can_handle(self, event_type, event_details)` to identify events it should process.
    3.  Implement `handle_event(self, event_type, event_details, config_manager, logger, alerter, ...)` with the healing logic.
    4.  Register an instance of your new handler with the `PolicyEngine`.
-   **Adding New Service Implementations**:
    1.  Create a new class inheriting from the relevant abstract interface (e.g., `AbstractAlertManager` for a new alert channel).
    2.  Implement all abstract methods.
    3.  Instantiate and inject your new service implementation when initializing the `PolicyEngine` or relevant components.
    4.  Update `policy_config.yaml` if your new service requires configuration.

## 8. Logging and Monitoring

-   The framework logs its actions using configured loggers.
-   If `BigQueryLogger` is configured and used:
    -   Policy execution events are logged to the table specified by `tracking_and_logging.policy_execution_log_table_id` in `policy_config.yaml`.
    -   End-to-end data flow milestones are logged to the table specified by `tracking_and_logging.data_flow_log_table_id`.
-   These BigQuery tables can be used as data sources for operational dashboards to monitor the self-healing system's activity and effectiveness.

```
