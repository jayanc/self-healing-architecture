# Data Pipeline Self-Healing Policy

## 1. Introduction

This document outlines the Self-Healing Policy for our data pipeline infrastructure, which spans from data sources through Google Cloud Storage (GCS) to BigQuery (bronze, silver, and gold layers). The primary goal of this policy is to establish a resilient and autonomous data pipeline capable of automatically detecting, diagnosing, and recovering from common failures.

This policy is intended to be a comprehensive guide for understanding the principles, strategies, and procedures governing our self-healing capabilities. It works in conjunction with a separate configuration file, `policy_config.yaml`, which stores the specific operational parameters (e.g., retry counts, timeouts, alert thresholds, DLQ paths) that drive the behavior of the self-healing framework.

By implementing the measures described herein, we aim to minimize disruptions, ensure data integrity, maintain high availability, and improve the overall operational efficiency of our data platform. This document will cover the scope, detection mechanisms, diagnosis and alerting strategies, recovery procedures, prevention and improvement measures, and Python implementation considerations for the self-healing system.

## Table of Contents

1.  [Introduction](#1-introduction)
2.  [Scope and Objectives](#2-scope-and-objectives)
3.  [Detection Mechanisms](#3-detection-mechanisms)
4.  [Diagnosis and Alerting Strategies](#4-diagnosis-and-alerting-strategies)
5.  [Recovery Procedures](#5-recovery-procedures)
6.  [Prevention and Improvement Measures](#6-prevention-and-improvement-measures)
7.  [Python Implementation Considerations](#7-python-implementation-considerations)
8.  [Conclusion](#8-conclusion)

## 2. Scope and Objectives

### 2.1. Introduction

The purpose of self-healing within our data architecture (Sources -> GCS -> BigQuery bronze/silver/gold) is to create a resilient and autonomous data pipeline. By implementing automated detection, diagnosis, and recovery mechanisms, we aim to minimize disruptions, ensure data integrity, and maintain high availability of our data assets. This policy outlines the objectives, scope, and common failure points addressed by our self-healing capabilities.

### 2.2. Objectives of Self-Healing

The primary objectives of implementing a self-healing system are:

*   **Increased System Reliability and Availability:** Proactively address and resolve issues to ensure the data pipeline is consistently operational and data is accessible when needed.
*   **Reduced Manual Intervention:** Automate the detection and resolution of common failures, freeing up engineering resources from repetitive troubleshooting and manual fixes.
*   **Faster Mean Time To Recovery (MTTR):** Minimize the time taken to recover from failures by automating diagnosis and recovery processes.
*   **Improved Data Integrity and Quality:** Implement automated checks and recovery mechanisms to prevent and correct data inconsistencies, ensuring trustworthy data.
*   **Enhanced Operational Efficiency:** Streamline data operations by reducing downtime and the manual effort required to manage the pipeline.
*   **Scalability and Resilience:** Design self-healing mechanisms that can adapt to growing data volumes and complexities, ensuring the pipeline remains robust under varying loads.

### 2.3. Scope of the Policy

This policy defines the boundaries of the self-healing mechanisms implemented within our data pipeline.

#### 2.3.1. In Scope

The following aspects are considered within the scope of this self-healing policy:

*   **Automated detection of failures** at each stage of the data pipeline: sources, GCS, and BigQuery (bronze, silver, gold layers).
*   **Automated diagnosis** to identify root causes for known and predefined failure patterns.
*   **Automated recovery procedures** for common and well-understood issues, including but not limited to:
    *   Configurable retries for transient errors.
    *   Automated reprocessing of failed data segments.
    *   Data replay capabilities from an earlier, stable stage of the pipeline.
*   **Alerting and notification mechanisms** to inform relevant stakeholders about detected failures, healing attempts, and successful recoveries or escalations.
*   **Comprehensive logging** of all self-healing actions, including detected failures, diagnostic steps, recovery attempts, and outcomes for auditability and continuous improvement.
*   **Failures related to:**
    *   Data ingestion from various sources.
    *   Google Cloud Storage (GCS) operations (e.g., uploads, transfers, access control issues).
    *   BigQuery jobs (e.g., load jobs, query execution, DML operations, DDL changes).
    *   Data transformations and processing between bronze, silver, and gold layers.
    *   Data quality checks and validation rule enforcement.

#### 2.3.2. Out of Scope

The following aspects are considered outside the scope of this self-healing policy:

*   **Self-healing of underlying infrastructure failures:** Issues such as network outages, virtual machine failures, or physical hardware malfunctions are typically managed by the cloud provider (Google Cloud Platform) and are not directly addressed by this policy.
*   **Automatic correction of novel or unknown bugs** in data processing logic or transformation scripts. Such issues require manual code changes, testing, and deployment.
*   **Resolution of external source system outages** that extend beyond configurable retry mechanisms. Prolonged unavailability of external data sources requires manual intervention and coordination with source owners.
*   **Complex data corruption issues** that require in-depth manual investigation, data patching, or sophisticated recovery techniques not amenable to automation.
*   **Security incidents or breaches:** These are handled by separate security incident response protocols and teams.

### 2.4. Common Failure Points to Address

The self-healing system will be designed to address common failure points across the data pipeline, including:

*   **Data Source Connectivity:**
    *   Source system unavailable or unreachable.
    *   Authentication or authorization errors (e.g., expired credentials, insufficient permissions).
    *   Network issues between our environment and the source system.
    *   API rate limits imposed by the source system.
*   **Data Ingestion/Extraction:**
    *   Incorrect or unexpected data format from the source.
    *   Missing critical data fields or files.
    *   Data corruption occurring during transit from source to GCS.
    *   Schema violations or unexpected changes in source data structure.
*   **GCS Operations:**
    *   Failures during data upload to GCS buckets (e.g., timeouts, network interruptions).
    *   Insufficient permissions to read from or write to GCS buckets.
    *   GCS storage capacity issues (though less common with auto-scaling).
    *   Object not found errors for expected files.
    *   Network timeouts during data transfers within GCS or to BigQuery.
*   **BigQuery Job Failures:**
    *   Query errors, including syntax errors or semantic errors in SQL.
    *   Resource exceeded errors (e.g., memory, CPU, slot contention).
    *   Quota limitations imposed by BigQuery (e.g., daily load jobs, concurrent queries).
    *   Table or dataset not found errors.
    *   Permission issues for accessing BigQuery resources.
    *   Transient errors during BigQuery operations.
*   **Data Transformation Logic:**
    *   Bugs or errors within data transformation scripts or queries (e.g., Spark jobs, dbt models, SQL procedures).
    *   Unexpected data values or types that cause processing logic to fail.
    *   Schema mismatches between input and output of transformation stages.
*   **Data Quality Issues:**
    *   Failed data validation rules (e.g., null values in required columns, incorrect data types, values outside expected ranges).
    *   Inconsistencies detected across different datasets or tables.
    *   Duplicate records where uniqueness is expected.
*   **Orchestration Failures:**
    *   Failures in workflow tasks within the orchestration tool (e.g., Airflow, Cloud Composer).
    *   Issues with task dependencies or scheduling.
    *   Problems with the orchestration engine itself.
*   **Configuration Errors:**
    *   Incorrect file paths, GCS bucket names, or BigQuery table/dataset identifiers.
    *   Invalid connection strings or credentials for data sources or services.
    *   Misconfigured parameters for pipeline stages or transformation jobs.

## 3. Detection Mechanisms

Effective self-healing begins with robust and timely detection of failures. This section outlines the general principles and specific strategies for identifying issues across the data pipeline.

### 3.1. General Principles of Detection

The following principles underpin our approach to failure detection:

*   **Comprehensive Logging:** All components and processes within the data pipeline must generate detailed logs. These logs should capture:
    *   **Status Information:** Start times, end times, completion status (success, failure, warning) of jobs and processes.
    *   **Error Details:** Specific error messages, stack traces, and relevant context when failures occur.
    *   **Key Metrics:** Operational metrics such as records processed, data volume, execution duration.
    *   **Transaction IDs/Correlation IDs:** To trace data flow and errors across multiple components.
*   **Monitoring Key Metrics:** Continuous monitoring of critical metrics helps in identifying deviations from normal behavior. Key metrics include:
    *   **Job Success/Failure Rates:** Tracking the percentage of successful and failed jobs over time.
    *   **Error Rates:** Monitoring the frequency and types of errors occurring in different pipeline stages.
    *   **Processing Times/Latency:** Measuring the duration of data ingestion, transformation, and loading processes.
    *   **Resource Utilization:** Monitoring CPU, memory, disk I/O, and network usage of pipeline components.
    *   **Data Volume and Throughput:** Tracking the amount of data being processed and moved through the pipeline.
    *   **Data Freshness/Staleness:** Ensuring data is updated within expected timeframes.
*   **Health Checks:** Regular, automated health checks are performed on critical components, services, and dependencies. These checks verify:
    *   Availability and responsiveness of services (e.g., source APIs, GCS, BigQuery).
    *   Connectivity between different parts of the pipeline.
    *   Validity of configurations and credentials.
*   **Proactive Alerting:** Alerts are configured to notify relevant teams or trigger automated responses when:
    *   Predefined thresholds for key metrics are breached (e.g., error rate exceeds X%, processing time is Y% above average).
    *   Anomalies or significant deviations from baseline performance are detected.
    *   Specific critical error events occur.

### 3.2. Detection Strategies by Pipeline Stage

Tailored detection strategies are applied at each stage of the data pipeline:

*   **Data Source Connectivity:**
    *   **Failures Detected:** Connection timeouts, authentication failures (e.g., 401/403 errors), API error responses (e.g., 5xx server errors from source), no data returned when expected, sudden changes in data schema.
    *   **Detection Tools/Techniques:**
        *   Application logs from ingestion services.
        *   Custom scripts to periodically test source connectivity and authentication.
        *   Google Cloud Monitoring for metrics from services interacting with sources.
        *   Analysis of HTTP status codes and response payloads from source APIs.
*   **Data Ingestion/Extraction:**
    *   **Failures Detected:** File not found errors, incorrect file format or encoding, checksum/hash mismatches indicating data corruption, zero-byte files, significant deviations in expected file size or record count, schema validation failures.
    *   **Detection Tools/Techniques:**
        *   Ingestion job logs (e.g., from custom scripts, Dataflow, or other ETL tools).
        *   Google Cloud Logging for GCS events (e.g., object creation, deletion).
        *   GCS event-triggered Cloud Functions for immediate validation upon file arrival.
        *   Data validation tools and libraries to check format, schema, and basic content.
        *   Monitoring file counts and sizes in GCS landing zones.
    *   **Extended Detection for Data Ingestion/Extraction:**
        *   **Change Data Capture (CDC) Specifics:**
            *   **Replication Lag:** Monitoring the delay between source system changes and their appearance in the CDC stream (e.g., via connector metrics or by comparing timestamps). Exceeding a threshold indicates a problem.
            *   **Connector Errors:** Directly monitoring CDC tool/connector logs (e.g., Debezium, Fivetran, custom connectors) for errors, exceptions, or stalled statuses.
            *   **Schema Drift (from CDC source):** Detecting changes in source schema that the CDC process might not handle automatically, potentially leading to data mapping errors downstream. This can be through CDC tool warnings or by comparing schemas.
            *   **Transaction Log Growth (Source DB):** For some CDC methods, issues can cause transaction logs on the source database to grow excessively.
        *   **Streaming Ingestion Specifics (e.g., Pub/Sub, Kafka to GCS/BigQuery):**
            *   **High Watermark Delays / Consumer Lag:** Significant delays between message production time and processing time by consumers.
            *   **Processing Errors in Streaming Jobs:** Failures or exceptions within the stream processing logic (e.g., in Dataflow, Spark Streaming, Cloud Functions) that prevent messages from being written to the sink.
            *   **Dead-Letter Queue (DLQ) Volume/Rate:** A sudden increase in messages landing in the streaming DLQ.
            *   **Throughput Anomalies:** Unexpected drops or spikes in message throughput.
            *   **End-to-End Latency:** Monitoring the total time it takes for a message to go from source to final destination.
        *   **Incremental Load Pattern Specifics:**
            *   **Incorrect Watermark/Timestamp Handling:** Failures in identifying the correct delta of data to load (e.g., processing overlapping data, missing data due to incorrect windowing). This might be detected by count discrepancies or data validation rules.
            *   **Source Data Not Ready:** The incremental batch is triggered, but the source data for the window is not yet available or complete.
            *   **Duplicate Records (if not expected by design):** Detection of duplicate primary keys or unique identifiers being loaded in an incremental batch when they shouldn't be.
        *   **Deduplication Process Issues:**
            *   **Deduplication Job/Task Failure:** The specific process responsible for removing duplicates fails.
            *   **Unexpected Number of Duplicates Remaining:** Data quality checks post-deduplication reveal more duplicates than acceptable.
            *   **Performance Degradation:** Deduplication process taking significantly longer than usual.
*   **GCS Operations:**
    *   **Failures Detected:** Upload/download failures (e.g., network errors, timeouts), access denied/permission errors (403 errors), object not found (404 errors) when expected, CRC32C mismatch indicating data integrity issues during transfer.
    *   **Detection Tools/Techniques:**
        *   Logs from tools interacting with GCS (e.g., `gsutil` logs, client library logs).
        *   Google Cloud Storage Audit Logs (for access patterns and permission issues).
        *   GCS event-triggered notifications or functions (e.g., for upload failures).
        *   Google Cloud Monitoring for GCS metrics (e.g., error rates, latency).
        *   Verifying object metadata and checksums post-transfer.
*   **BigQuery Job Failures:**
    *   **Failures Detected:** API errors during job submission or execution (e.g., `jobnotFound`, `accessDenied`, `invalidQuery`), jobs stuck in pending/running state for excessive durations, quota exceeded errors, specific error messages in job results (e.g., "Table not found," "Resources exceeded").
    *   **Detection Tools/Techniques:**
        *   BigQuery Audit Logs (specifically `google.cloud.bigquery.v2.JobService.InsertJob` and `google.cloud.bigquery.v2.JobService.GetQueryResults` events, examining `status.errorResult`).
        *   Polling BigQuery API for job status and checking `status.state` and `status.errorResult`.
        *   Google Cloud Monitoring for BigQuery metrics (e.g., slot utilization, query execution times, job error rates).
        *   Setting up alerts based on specific BigQuery error codes.
*   **Data Transformation Logic:**
    *   **Failures Detected:** Errors or exceptions in transformation scripts (e.g., Spark, Dataflow, dbt models, SQL procedures), unexpected output schema, no output produced, significant discrepancies in record counts between input and output.
    *   **Detection Tools/Techniques:**
        *   Application logs from transformation tools (e.g., Dataproc job logs, Dataflow job logs, dbt run logs).
        *   BigQuery query history for SQL-based transformations, checking for query errors.
        *   Implementing assertions or checks within transformation code to validate intermediate results.
        *   Comparing output schemas against predefined expected schemas.
*   **Data Quality Issues:**
    *   **Failures Detected:** Violated data quality rules (e.g., nulls in non-nullable columns, incorrect data types, values out of expected range or set), data anomalies (e.g., sudden spikes or drops in key metrics), unexpected duplicate records, referential integrity failures.
    *   **Detection Tools/Techniques:**
        *   Dedicated data quality tools (e.g., Great Expectations, dbt tests, Google Cloud Dataplex Data Quality).
        *   Custom SQL queries executed periodically or as part of the pipeline to validate data.
        *   Logs and reports generated by data quality validation processes.
        *   Statistical analysis and anomaly detection algorithms applied to data profiles.
*   **Orchestration Failures:**
    *   **Failures Detected:** Directed Acyclic Graph (DAG) or individual task failures, task timeouts, unmet task dependencies, sensor failures, issues with inter-task communication.
    *   **Detection Tools/Techniques:**
        *   Logs from the orchestration tool (e.g., Airflow scheduler/worker logs, Cloud Composer environment logs).
        *   Monitoring dashboards provided by the orchestration tool.
        *   Google Cloud Monitoring for metrics related to the orchestration service (e.g., Airflow/Composer task instance states, DAG run statuses).
        *   Alerts configured within the orchestration tool for task failures or delays.

### 3.3. Recommended Logging and Monitoring Strategy

A cohesive logging and monitoring strategy is crucial for effective detection.

*   **Centralized Logging:**
    *   All pipeline components (custom scripts, ETL tools, cloud services) should send logs to a centralized logging system.
    *   **Recommendation:** Google Cloud Logging is the preferred centralized logging solution, leveraging its integration with GCS, BigQuery, Dataflow, Dataproc, and other GCP services.
*   **Standardized Log Formats:**
    *   Adopt a consistent, structured logging format (e.g., JSON) across all applications and services.
    *   Include common fields such as timestamp, severity level (INFO, ERROR, WARN), application/service name, job/task ID, correlation ID, and a detailed message.
    *   This standardization facilitates easier parsing, searching, and analysis of logs.
*   **Key Metrics for Monitoring:**
    *   Identify and track key performance indicators (KPIs) and operational metrics for each pipeline stage in Google Cloud Monitoring.
    *   Examples:
        *   **Ingestion:** Source API error rates, files received count/size, ingestion latency.
        *   **GCS:** Bucket error rates, object upload/download latency.
        *   **BigQuery:** Query error rates, slot utilization, job completion times, table row counts.
        *   **Transformations:** Job error rates, processing duration, records processed in/out.
        *   **Data Quality:** Percentage of DQ rules passed/failed, number of anomalies detected.
*   **Dashboards for Visibility:**
    *   Create comprehensive dashboards for real-time visibility into pipeline health and performance.
    *   **Recommendation:** Utilize Google Cloud Monitoring dashboards to visualize metrics collected. Consider integrating with other BI tools (e.g., Looker, Tableau) for more advanced analytics and reporting on pipeline metadata and logs if needed.
    *   Dashboards should highlight error rates, processing bottlenecks, data freshness, and trends over time.

#### 3.3.1. Operational Tracking with BigQuery Tables

In addition to real-time monitoring and logging, maintaining a historical record of self-healing operations in BigQuery tables can provide invaluable insights for trend analysis, framework effectiveness assessment, and identifying areas for improvement.

##### Policy Execution Tracking Table

This table logs each significant event and action taken by the self-healing framework, providing an audit trail and data for monitoring the framework's operations.

**Purpose:**
*   Audit trail of all self-healing actions.
*   Monitoring the frequency and types of interventions.
*   Analyzing the success/failure rates of automated recovery actions.
*   Identifying areas where policies are frequently triggered.

**Proposed BigQuery Schema (`policy_execution_log`):**

| Column Name           | Data Type     | Description                                                                                                                               |
|-----------------------|---------------|-------------------------------------------------------------------------------------------------------------------------------------------|
| `event_id`            | `STRING`      | Unique identifier for this specific event/action log entry (e.g., UUID).                                                                    |
| `correlation_id`      | `STRING`      | Identifier to link multiple related events in a single self-healing sequence (e.g., detection, diagnosis, multiple recovery attempts).      |
| `event_timestamp`     | `TIMESTAMP`   | Timestamp when the event or action occurred.                                                                                              |
| `policy_id`           | `STRING`      | Identifier for the specific policy or rule that was triggered (e.g., 'GCS_UPLOAD_RETRY_POLICY'). Can map to `policy_config.yaml` keys.   |
| `target_resource`     | `STRING`      | The specific resource being acted upon (e.g., GCS path, BigQuery table ID, data source name).                                             |
| `pipeline_stage`      | `STRING`      | The stage of the data pipeline involved (e.g., 'INGESTION', 'GCS_BRONZE', 'BQ_SILVER', 'BQ_GOLD', 'DEDUPLICATION').                          |
| `detected_issue_type` | `STRING`      | Category of the issue detected (e.g., 'CONNECTION_ERROR', 'FILE_NOT_FOUND', 'BQ_JOB_FAILED', 'DQ_CHECK_FAILED', 'CDC_LAG').                |
| `detected_issue_details`| `STRING`    | More specific details or error message about the detected issue.                                                                          |
| `current_status`      | `STRING`      | Current status of this event/action (e.g., 'DETECTED', 'DIAGNOSIS_STARTED', 'DIAGNOSIS_COMPLETE', 'RECOVERY_ATTEMPTED', 'RECOVERY_SUCCESSFUL', 'RECOVERY_FAILED', 'ESCALATED_MANUAL', 'ACTION_LOGGED'). |
| `action_taken`        | `STRING`      | Description of the action performed by the self-healing system (e.g., 'RETRIED_UPLOAD', 'SENT_ALERT', 'SKIPPED_FILE', 'RESTARTED_BQ_JOB'). |
| `action_parameters`   | `STRING`      | JSON string representing parameters used for the action (e.g., `{'retry_attempt': 1, 'delay_seconds': 30}`).                             |
| `action_result`       | `STRING`      | Outcome of the action (e.g., 'SUCCESS', 'FAILURE', 'NO_ACTION_NEEDED').                                                                     |
| `action_duration_ms`  | `INTEGER`     | Duration of the action in milliseconds, if applicable.                                                                                      |
| `error_message`       | `STRING`      | Specific error message if the action failed.                                                                                              |
| `python_module_invoked`| `STRING`     | Name of the Python module/function that handled this event (for traceability).                                                            |
| `cloud_logging_link`  | `STRING`      | Direct link to related detailed logs in Google Cloud Logging for this specific event.                                                       |

##### End-to-End Data Flow Tracking Table

This table provides a consolidated view of data as it progresses through the different layers of the data architecture (Bronze, Silver, Gold). It helps in monitoring data completeness, timeliness, and identifying bottlenecks or failures at a macro level for each data flow or batch.

**Purpose:**
*   Track the lifecycle of a data batch/flow from ingestion to the Gold layer.
*   Monitor processing times and identify performance bottlenecks between layers.
*   Verify data completeness and successful transformation at each major stage.
*   Provide a high-level view for dashboards on overall pipeline health and data readiness.

**Proposed BigQuery Schema (`data_flow_log`):**

| Column Name                     | Data Type     | Description                                                                                                                                  |
|---------------------------------|---------------|----------------------------------------------------------------------------------------------------------------------------------------------|
| `flow_run_id`                   | `STRING`      | Unique identifier for a specific data flow or batch run (e.g., daily load for 'source_X', or a specific streaming micro-batch ID).             |
| `correlation_id_ingestion`      | `STRING`      | Identifier linking this flow to specific ingestion events in `policy_execution_log` (e.g., if multiple files form one logical batch).          |
| `data_source_name`              | `STRING`      | Name of the primary data source for this flow.                                                                                                 |
| `ingestion_trigger_timestamp`   | `TIMESTAMP`   | When the ingestion process for this flow was initiated or scheduled.                                                                           |
| `ingestion_completion_timestamp`| `TIMESTAMP`   | Timestamp when the initial data ingestion (e.g., to GCS landing zone) for this flow completed.                                               |
| `ingestion_status`              | `STRING`      | Status of the ingestion phase (e.g., 'SUCCESS', 'PARTIAL_SUCCESS', 'FAILED').                                                                  |
| `ingested_record_count`         | `INTEGER`     | Number of records/files ingested at the source.                                                                                                |
| `bronze_arrival_timestamp`      | `TIMESTAMP`   | Timestamp when data for this flow arrived in the GCS Bronze layer.                                                                             |
| `bronze_gcs_paths`              | `STRING`      | ARRAY of GCS paths for files related to this flow in the Bronze layer (or a primary manifest file). Consider `ARRAY<STRING>`.                  |
| `bronze_record_count`           | `INTEGER`     | Number of records/files landed in Bronze.                                                                                                      |
| `silver_processing_start_time`  | `TIMESTAMP`   | When processing from Bronze to Silver began for this flow.                                                                                     |
| `silver_completion_timestamp`   | `TIMESTAMP`   | Timestamp when data processing for this flow into the Silver BigQuery layer completed.                                                         |
| `silver_bq_table_ids`           | `STRING`      | ARRAY of BigQuery table IDs in the Silver layer relevant to this flow. Consider `ARRAY<STRING>`.                                               |
| `silver_processing_status`      | `STRING`      | Status of Silver layer processing (e.g., 'SUCCESS', 'FAILED', 'DQ_ISSUES').                                                                    |
| `silver_record_count`           | `INTEGER`     | Number of records processed into Silver.                                                                                                       |
| `gold_processing_start_time`    | `TIMESTAMP`   | When processing from Silver to Gold began for this flow.                                                                                       |
| `gold_completion_timestamp`     | `TIMESTAMP`   | Timestamp when data processing for this flow into the Gold BigQuery layer completed.                                                           |
| `gold_bq_table_ids`             | `STRING`      | ARRAY of BigQuery table IDs in the Gold layer relevant to this flow. Consider `ARRAY<STRING>`.                                                 |
| `gold_processing_status`        | `STRING`      | Status of Gold layer processing (e.g., 'SUCCESS', 'FAILED', 'DQ_ISSUES').                                                                      |
| `gold_record_count`             | `INTEGER`     | Number of records processed into Gold.                                                                                                         |
| `end_to_end_duration_seconds`   | `INTEGER`     | Total duration from ingestion trigger to Gold completion.                                                                                        |
| `overall_status`                | `STRING`      | Current high-level status of this data flow (e.g., 'IN_PROGRESS_BRONZE', 'COMPLETED_SILVER', 'FAILED_GOLD', 'SUCCESS_ALL_LAYERS').           |
| `data_quality_summary`          | `STRING`      | JSON string summarizing key DQ check results for this flow (e.g., `{'silver_dq_passed': true, 'gold_dq_alerts': 0}`).                          |
| `last_updated_timestamp`        | `TIMESTAMP`   | Timestamp when this tracking record was last updated.                                                                                          |
| `error_details_link_id`         | `STRING`      | Link (e.g., `event_id` or `correlation_id`) to relevant failure entries in `policy_execution_log` if this flow encountered errors.             |
| `retry_count_flow`              | `INTEGER`     | Number of times this entire flow (or significant parts of it) has been retried.                                                                |

*Note: For columns like `bronze_gcs_paths`, `silver_bq_table_ids`, and `gold_bq_table_ids`, using `ARRAY<STRING>` is preferable in BigQuery. The table description uses `STRING` for simplicity here, but the implementation should choose the most appropriate type.*

## 4. Diagnosis and Alerting Strategies

Once a failure is detected, the next steps are to diagnose the root cause and alert the appropriate teams or trigger automated recovery actions. This section outlines our strategies for diagnosis and alerting.

### 4.1. General Principles of Diagnosis

Automated diagnosis aims to quickly identify the reason for a failure, facilitating faster recovery.

*   **Correlation of Logs and Metrics:**
    *   The diagnostic process heavily relies on correlating log entries with observed metric anomalies. For example, a spike in error rates for a BigQuery load job should correlate with specific error messages in BigQuery audit logs and application logs.
    *   Using common correlation IDs across services is essential for tracing operations and errors.
*   **Error Message Parsing:**
    *   Automated systems will parse error messages from logs to extract key information, such as error codes, resource names, and specific failure reasons.
    *   This structured information is then used to categorize the error and match it against known patterns.
*   **Dependency Checking:**
    *   Many failures are due to issues in upstream dependencies. The diagnostic process should include checks for the health and availability of these dependencies.
    *   For instance, if a BigQuery load job from GCS fails, the system should check GCS accessibility and the existence/integrity of the source files.
*   **Known Issues Database/Playbooks:**
    *   A repository of known issues, their common symptoms, diagnostic steps, and resolution procedures (playbooks) will be maintained.
    *   Automated diagnosis will attempt to match current failure signatures against this database to suggest or initiate recovery actions.
    *   This database will be continuously updated based on new incidents and learnings.

### 4.2. Diagnosis Strategies by Pipeline Stage

Specific diagnostic approaches are tailored to the characteristics of each pipeline stage:

*   **Data Source Issues:**
    *   **Check Source Status:** Verify the availability of the external data source via health check endpoints or simple connectivity tests.
    *   **Analyze Ingestion Logs:** Examine logs from ingestion services for connection errors, authentication failures (e.g., HTTP 401/403), API rate limit messages, or error responses from the source.
    *   **Network Troubleshooting Tools:** For persistent connectivity issues, utilize tools like `ping`, `traceroute`, or `telnet` (where appropriate and secure) to diagnose network path problems.
*   **GCS Errors:**
    *   **Analyze Client Error Codes:** Error messages from GCS client libraries or `gsutil` often provide specific error codes (e.g., 403 Forbidden, 404 Not Found, 503 Service Unavailable).
    *   **Verify Permissions:** Check IAM permissions for the service account or user attempting to access the GCS bucket/objects.
    *   **Examine Cloud Storage Logs:** Cloud Storage Audit Logs (Data Access logs) can provide detailed information on failed operations, including the identity of the caller and the specific permission denied.
*   **BigQuery Job Failures:**
    *   **Examine BigQuery Error Messages:** The `status.errorResult` field in BigQuery job objects contains detailed error information, including `reason`, `location`, and `message`.
    *   **Validate Query Syntax and Semantics:** For query jobs, ensure the SQL is valid. For load jobs, check schema compatibility and data format.
    *   **Review Execution Details:** For performance-related issues (e.g., resources exceeded), analyze the job's execution details in the BigQuery console or via the API to identify bottlenecks (e.g., specific stages consuming too many resources).
    *   **Check Quotas:** Verify that project or user quotas for BigQuery (e.g., concurrent queries, daily load limits, slot availability) have not been exceeded.
*   **Transformation Logic Errors:**
    *   **Analyze Stack Traces:** For script-based transformations (e.g., Python/Spark in Dataflow/Dataproc, dbt models), stack traces are crucial for pinpointing the exact line of code causing the error.
    *   **Inspect Input Data:** The error might be data-dependent. Isolate and inspect the specific subset of data that was being processed when the error occurred. Look for unexpected values, nulls, or formatting issues.
    *   **Verify Code Versioning:** Ensure the correct version of the transformation code or dbt model was deployed and is being executed. Compare with previous successful runs.
*   **Data Quality Failures:**
    *   **Isolate Bad Records:** Identify the specific records or data segments that violated data quality rules.
    *   **Review DQ Rule Definitions:** Confirm that the data quality rules (e.g., in dbt tests, Great Expectations) are correctly defined and appropriate for the data.
    *   **Trace Data Lineage:** Understand the origin of the problematic data by tracing its lineage back through the pipeline. This can help identify the stage where the quality issue was introduced.

*   **Extended Diagnosis for Data Ingestion/Extraction:**
    *   **Change Data Capture (CDC) Failures:**
        *   **Diagnosis:**
            *   Check CDC connector logs/UI for specific error messages (e.g., connection issues to source/target, authentication problems, unsupported DDL operations).
            *   Inspect source database logs and replication status (e.g., PostgreSQL replication slots, Oracle LogMiner status).
            *   Verify network connectivity between CDC components and source/target.
            *   For schema drift, compare source table schema with the schema registered/expected by the CDC tool.
            *   If transaction logs are growing, investigate why the CDC process isn't consuming them.
        *   **Alerting:**
            *   CRITICAL alert for connector down or consistent failure.
            *   WARNING alert for replication lag exceeding threshold.
            *   INFO/WARNING for schema drift detection.
    *   **Streaming Ingestion Failures:**
        *   **Diagnosis:**
            *   Analyze logs from the streaming processing jobs (e.g., Dataflow, Spark Streaming, Cloud Functions) for exceptions.
            *   Examine message broker metrics (e.g., Pub/Sub subscription metrics, Kafka consumer group lag, DLQ size).
            *   Inspect messages in the DLQ to identify "poison pills" or common error patterns.
            *   Check resource utilization of streaming processing jobs (CPU, memory, autoscaling behavior).
        *   **Alerting:**
            *   CRITICAL alert for stream processing job consistently failing or high consumer lag.
            *   WARNING for sudden increases in DLQ size or processing latency.
            *   INFO for autoscaling events if they are frequent.
    *   **Incremental Load Pattern Failures:**
        *   **Diagnosis:**
            *   Examine logs of the incremental load process for errors in SQL queries, data type mismatches, or API call failures.
            *   Verify the watermark values being used: check how they are read, calculated, and written.
            *   Compare record counts and key checksums between source and target for the specific increment/batch.
            *   Manually inspect data around the problematic window in both source and target.
        *   **Alerting:**
            *   CRITICAL or WARNING alert for batch load failure, depending on impact.
            *   WARNING for significant count mismatches or data validation errors in the increment.
    *   **Deduplication Process Issues:**
        *   **Diagnosis:**
            *   Check logs of the deduplication job/task for errors.
            *   Analyze sample data that failed deduplication or was identified as a remaining duplicate.
            *   Review the deduplication logic and keys being used.
            *   If performance is an issue, analyze query plans or resource utilization of the deduplication step.
        *   **Alerting:**
            *   WARNING or CRITICAL for deduplication job failure.
            *   WARNING if post-deduplication DQ checks fail (i.e., duplicates remain above threshold).

### 4.3. Alerting Strategy

A well-defined alerting strategy ensures that the right information reaches the right people or systems at the right time.

*   **Severity Levels:**
    *   **CRITICAL (P1):** System down, significant data loss or corruption, major pipeline blockage affecting key business processes. Requires immediate attention.
    *   **WARNING (P2):** Partial system degradation, potential data issues, recoverable errors causing delays, risk of escalating to CRITICAL. Requires prompt attention.
    *   **INFO (P3):** Minor issues, successful completion of self-healing actions, non-critical errors, or information for monitoring purposes.
*   **Notification Channels:**
    *   **PagerDuty/Opsgenie (or similar):** For CRITICAL alerts requiring immediate on-call engineer response.
    *   **Slack:** For WARNING and INFO alerts, team notifications, and general awareness. Specific channels for different pipeline areas or severity levels.
    *   **Email:** For summarized reports, non-urgent INFO alerts, or notifications to broader stakeholder groups.
*   **Alert Content:**
    Alerts must be actionable and provide sufficient context. Key elements include:
    *   **Timestamp:** When the event occurred.
    *   **Affected Component/Service:** Specific pipeline stage, job, table, or system (e.g., "BigQuery Load Job `load_customer_data_hourly`").
    *   **Error Summary:** A concise description of the problem (e.g., "Failed with `resourcesExceeded`").
    *   **Severity Level:** CRITICAL, WARNING, INFO.
    *   **Correlation ID:** If available, to link related logs and traces.
    *   **Links to Logs/Dashboards:** Direct links to relevant logs in Cloud Logging, metrics in Cloud Monitoring dashboards, or specific job details (e.g., BigQuery job URL).
    *   **Suggested Initial Steps/Playbook:** If a known issue, link to the relevant diagnostic or recovery playbook.
*   **Escalation Paths:**
    *   Define clear escalation paths if an alert is not acknowledged or resolved within a specified timeframe.
    *   Example: PagerDuty alert -> On-call L1 -> On-call L2 -> Engineering Lead.
    *   Escalation rules will be configured in the alerting tools.
*   **Alert Fatigue Prevention:**
    *   **Consolidation:** Group related alerts to avoid multiple notifications for the same underlying issue (e.g., multiple task failures in a single DAG run).
    *   **Tuning:** Regularly review and tune alert thresholds and conditions to minimize false positives and ensure alerts are meaningful.
    *   **Deduplication:** Prevent repeated alerts for ongoing issues that are already being addressed.
    *   **Use of INFO for Self-Healing:** When self-healing successfully resolves an issue, an INFO alert should be generated, reducing noise from transient problems that are automatically handled.

### 4.4. Role of `policy_config.yaml` in Diagnosis and Alerting

The `policy_config.yaml` file will play a supportive role in operationalizing diagnosis and alerting:

*   **Storing Alert Endpoint URLs:**
    *   Configuration for webhook URLs for Slack, PagerDuty, or other notification services.
    *   This allows for easy updates to notification channels without code changes.
*   **Defining Alerting Thresholds:**
    *   Specifying thresholds for metrics that trigger alerts (e.g., `bq_job_error_rate_threshold: 0.1` for a 10% error rate).
    *   Defining debounce periods or evaluation windows for these thresholds.
*   **Mapping Error Patterns to Diagnostic Hints (Potential):**
    *   The configuration could potentially store mappings of known error message patterns (e.g., regex for specific BigQuery error messages) to:
        *   References to specific sections in diagnostic playbooks.
        *   Suggested initial troubleshooting steps.
        *   Default severity levels for certain error types.
    *   This can help the automated diagnosis engine to provide more targeted information in alerts.
    *   Example:
        ```yaml
        error_patterns:
          - pattern: ".*Quota exceeded: BigQuery API.*"
            diagnostic_hint: "Check BigQuery project quotas for API usage. Consider requesting an increase."
            severity: "WARNING"
          - pattern: ".*Table not found: .*"
            diagnostic_hint: "Verify table name and dataset. Check if table was dropped or schema migration failed."
            severity: "CRITICAL"
        ```
This structured approach to diagnosis and alerting, supported by configuration, is key to maintaining a responsive and resilient data pipeline.

## 5. Recovery Procedures

Automated recovery procedures are the core of the self-healing system, designed to restore normal pipeline operation with minimal manual intervention. This section details the principles guiding these procedures and specific strategies for different pipeline stages.

### 5.1. General Recovery Principles

The following principles ensure that recovery actions are safe, effective, and predictable:

*   **Idempotency:**
    *   Recovery actions must be idempotent, meaning that applying them multiple times should have the same effect as applying them once. This is crucial to prevent unintended consequences if a recovery action is retried or partially executed.
    *   For example, reprocessing a data file should replace or ignore already processed data, not duplicate it.
*   **Configurable Retries (with Exponential Backoff):**
    *   Many failures are transient (e.g., temporary network glitches, brief service unavailability). Automated retries are the first line of defense.
    *   Retries must be configurable in terms of the number of attempts and the delay between attempts.
    *   **Exponential backoff** (increasing the delay after each failed attempt) should be used to avoid overwhelming a struggling service and to give more time for transient issues to resolve.
    *   Parameters like `max_retry_attempts` and `base_retry_delay_seconds` would be defined in `policy_config.yaml` for various operations.
*   **Dead-Letter Queues (DLQ) / Dead-Letter Storage:**
    *   For data that consistently fails processing even after retries (e.g., malformed records, data that violates integrity constraints), a mechanism is needed to isolate this "bad" data.
    *   This prevents a small amount of problematic data from halting the entire pipeline.
    *   Failed data or messages will be moved to a designated DLQ (e.g., a separate GCS bucket or BigQuery table).
    *   The location for DLQs (e.g., `gcs_dead_letter_bucket_bronze`, `bq_silver_dlq_table`) will be specified in `policy_config.yaml`.
*   **Rollback Mechanisms:**
    *   In some cases, a failed process might leave the system in an inconsistent state. Automated or semi-automated rollback procedures are necessary to revert to a previously known good state.
    *   For data transformations, this might involve restoring a previous version of a BigQuery table or reprocessing data from an earlier, correct stage.
    *   For code deployments, this might involve rolling back to a previous version of a Dataflow template or dbt models.
*   **State Management for Recovery:**
    *   The self-healing system needs to maintain state about ongoing recovery efforts to avoid conflicting actions and to understand the history of failures and recovery attempts for a particular data item or job.
    *   This includes tracking retry counts, the current status of a recovery operation, and whether an issue has been escalated.
    *   This state might be stored in a dedicated metadata database or using features of the orchestration tool.
*   **Graceful Degradation:**
    *   If full recovery is not immediately possible, the system should aim to degrade gracefully.
    *   This might involve:
        *   Processing partial data if possible and safe.
        *   Skipping optional components or enrichment steps.
        *   Serving slightly stale data if real-time updates are failing but a recent snapshot is available.
        *   Clearly communicating the degraded state to downstream consumers.

### 5.2. Recovery Procedures by Pipeline Stage

Specific recovery actions are defined for each stage, configurable via `policy_config.yaml`.

*   **Data Ingestion from Sources:**
    *   **Common Failures:** Source unavailable, connection timeout, authentication error, API rate limit.
    *   **Automated Actions:**
        1.  **Retry Connection/Download:** Automatically retry the connection or download operation using exponential backoff.
            *   `policy_config.yaml`: `source_retry_attempts: 5`, `source_base_retry_delay_seconds: 60`, `source_max_retry_delay_seconds: 300`.
        2.  **Check Source Status (if available):** If the source provides a status API or health check endpoint, query it to determine if the source is reporting issues.
            *   `policy_config.yaml`: `source_status_endpoint: "https://api.vendor.com/status"`.
        3.  **Switch to Secondary Source (if configured):** For critical sources, if a secondary endpoint or failover system is available, attempt to switch to it.
            *   `policy_config.yaml`: `source_failover_endpoint: "https://api.backup-vendor.com/data"`.
        4.  **Alert:** If retries are exhausted or the source reports a persistent issue, escalate with a CRITICAL alert.
    *   **DLQ Strategy:** For persistent source connection issues, the "dead letter" is the failed attempt to ingest a batch; this is primarily handled by alerting and manual investigation rather than moving data.
    *   **Extended Recovery Procedures for Data Ingestion:**
        *   **Change Data Capture (CDC) Specific Recovery:**
            *   **Action**:
                *   Attempt to gracefully restart the CDC connector/task (configurable retries).
                *   If schema drift causes failure and the change is non-breaking (e.g., new nullable column), attempt to automatically update downstream schema or alert for manual schema evolution.
                *   For persistent errors or significant replication lag, escalate with detailed diagnostic information.
                *   In extreme cases (e.g., corrupted replication slot, unrecoverable connector state), alert for manual intervention, which might involve re-snapshotting or re-initializing replication (this should be a last resort and clearly documented).
            *   **`policy_config.yaml` Examples**:
                *   `cdc_connector_restart_attempts: 3`
                *   `cdc_connector_retry_delay_seconds: 60`
                *   `cdc_auto_schema_update_non_breaking: false` (boolean to enable/disable)
                *   `cdc_max_replication_lag_minutes_for_alert: 120`
                *   `cdc_critical_error_keywords_for_escalation: ["FATAL", "Unrecoverable", "CorruptLog"]`
        *   **Streaming Ingestion Specific Recovery:**
            *   **Action**:
                *   For transient errors in stream processing jobs, implement retries with backoff.
                *   If messages are failing due to "poison pills" (malformed or problematic data):
                    *   Automatically divert such messages to a streaming Dead-Letter Queue (DLQ) after a few failed processing attempts.
                    *   Alert when messages are sent to DLQ.
                *   If processing lag is high due to volume:
                    *   Attempt to autoscale processing instances (if platform supports and configured).
                    *   Alert if scaling limits are reached or lag persists.
                *   For certain failures, allow reprocessing from a specific offset/timestamp if the message broker and processing framework support it (manual trigger or carefully automated).
            *   **`policy_config.yaml` Examples**:
                *   `stream_job_retry_attempts: 5`
                *   `stream_job_retry_delay_seconds: 30`
                *   `stream_poison_pill_threshold_attempts: 3`
                *   `stream_dlq_topic_name: "projects/your-project/topics/your-stream-dlq"`
                *   `stream_enable_autoscaling_alerts: true`
                *   `stream_allow_automated_reprocessing_offset: false` (typically needs careful consideration)
        *   **Incremental Load Pattern Specific Recovery:**
            *   **Action**:
                *   Retry the failed incremental batch load (configurable retries).
                *   If a batch fails due to source data issues (e.g., temporary unavailability for that window), delay and retry.
                *   If a batch fails due to bad data within the increment:
                    *   Isolate problematic records (if possible) and load the rest of the batch.
                    *   Alternatively, skip the entire batch, alert, and add to a reprocessing queue/log.
                *   If duplicates are loaded incorrectly or data is missed:
                    *   Provide mechanisms/scripts for manual or semi-automated correction (e.g., deleting a bad batch, reloading a specific window). This is often complex to fully automate.
            *   **`policy_config.yaml` Examples**:
                *   `incremental_load_batch_retry_attempts: 2`
                *   `incremental_load_source_data_not_ready_delay_seconds: 300`
                *   `incremental_load_error_handling_strategy: "SKIP_BATCH_AND_ALERT"` # Options: "RETRY_ONLY", "SKIP_BATCH_AND_ALERT", "ISOLATE_BAD_RECORDS" (if feasible)
                *   `incremental_load_manual_correction_playbook_link: "http://link.to/incremental-load-correction-playbook"`
        *   **Deduplication Process Specific Recovery:**
            *   **Action**:
                *   Retry the deduplication job/task (configurable retries).
                *   If failures are due to resource constraints, attempt with increased resources (if configurable) or alert for manual scaling.
                *   For data that consistently fails deduplication logic (but is otherwise valid), move to a separate "manual review" queue or table.
            *   **`policy_config.yaml` Examples**:
                *   `deduplication_job_retry_attempts: 3`
                *   `deduplication_resource_increase_on_failure_enabled: false`
                *   `deduplication_manual_review_gcs_path: "gs://your-project/dedup-manual-review/"`

*   **GCS (Bronze Layer - Raw Data Landing):**
    *   **Common Failures:** Upload failures (network, timeouts), permission errors, checksum mismatches, partial file uploads.
    *   **Automated Actions:**
        1.  **Retry Upload/Transfer:** Retry the GCS operation (e.g., `gsutil cp`) using exponential backoff.
            *   `policy_config.yaml`: `gcs_upload_retry_attempts: 3`, `gcs_base_retry_delay_seconds: 30`, `gcs_max_retry_delay_seconds: 120`.
        2.  **Verify Checksums:** After a successful upload, if the source provides checksums (e.g., MD5, CRC32C), verify them against the GCS object's checksum to ensure data integrity. Re-upload if mismatched.
        3.  **Handle Partial Files:** If a file is suspected to be partial (e.g., based on size or metadata), attempt to delete the partial file and re-initiate the transfer.
        4.  **Isolate Problematic Files:** If a specific file consistently fails to upload or pass verification, move it to a designated GCS "dead letter" bucket for investigation.
            *   `policy_config.yaml`: `gcs_bronze_dead_letter_bucket: "gs://my-project-bronze-dlq/"`.
        5.  **Alert on Permission Issues:** If errors indicate IAM permission problems, generate a CRITICAL alert immediately, as this usually requires manual intervention.
*   **BigQuery (Silver Layer - Staging/Transformation):**
    *   **Common Failures:** Transient BQ API errors, query errors (syntax, resource limits), load job errors (schema mismatch, bad records), transformation logic bugs.
    *   **Automated Actions:**
        1.  **Retry Transient BigQuery Errors:** Retry operations that fail due to transient BigQuery errors (e.g., "backendError", "rateLimitExceeded" for certain APIs) with exponential backoff.
            *   `policy_config.yaml`: `bq_silver_job_retry_attempts: 3`, `bq_silver_base_retry_delay_seconds: 60`, `bq_silver_max_retry_delay_seconds: 240`.
        2.  **Log/Alert on Query Errors:** For SQL query errors in transformations:
            *   **Known Remediable Errors:** If the error is a known one that can be fixed by a configuration change (e.g., adjusting resource allocation for a query that hit limits, if possible via API), attempt remediation.
            *   **Other Query Errors:** Log detailed error information. For persistent errors, generate a WARNING or CRITICAL alert for developer investigation.
        3.  **DLQ for Bad Records in Load Jobs:** For BigQuery load jobs, if rows are rejected due to format or schema issues:
            *   Configure the load job to send these "bad" records to a GCS path or another BigQuery table (DLQ).
            *   `policy_config.yaml`: `bq_silver_dlq_gcs_path: "gs://my-project-silver-dlq/"`, `bq_silver_dlq_table: "my_dataset.silver_failed_records"`.
            *   Generate an INFO or WARNING alert with the count of bad records.
        4.  **Rollback/Reprocess Transformations:**
            *   If a transformation job (e.g., a dbt model run, a Dataflow job writing to Silver) fails and leaves data in an inconsistent state, attempt to rollback (e.g., restore table from snapshot if available, delete partial output).
            *   Trigger reprocessing of the transformation logic for the affected data segment from the Bronze layer or a previous valid Silver state.
            *   `policy_config.yaml`: `bq_silver_enable_snapshot_rollbacks: true`.
*   **BigQuery (Gold Layer - Curated/Reporting):**
    *   **Common Failures:** Similar to Silver (transient errors, query issues). Additionally, data quality validation failures based on business rules.
    *   **Automated Actions:**
        1.  **Retry Transient/Query Errors:** Similar retry mechanisms as for the Silver layer.
            *   `policy_config.yaml`: `bq_gold_job_retry_attempts: 2`, `bq_gold_base_retry_delay_seconds: 120`, `bq_gold_max_retry_delay_seconds: 300`.
        2.  **Handle Data Quality (DQ) Failures:**
            *   If DQ checks (e.g., dbt tests, custom SQL validation) fail:
                *   Generate a WARNING or CRITICAL alert detailing the failed DQ rules and affected data.
                *   `policy_config.yaml`: `gold_dq_alert_threshold_criticality: "WARNING"`.
                *   **Option 1 (Halt Propagation):** Prevent the data that failed DQ checks from being published or promoted to production views/tables.
                *   **Option 2 (Flag Data):** Publish the data but include metadata flags indicating its quality issues.
                *   **Option 3 (Automatic Reprocessing):** If the DQ failure suggests an upstream issue that might be resolved by reprocessing, trigger reprocessing from the Silver layer for the relevant data partitions.
                    *   `policy_config.yaml`: `gold_dq_failure_action: "HALT_AND_ALERT"` (options: `FLAG_DATA`, `REPROCESS_SILVER`).
        3.  **DLQ for Gold Layer Issues:** Similar to Silver, problematic data that cannot be processed into the Gold layer or fails critical DQ checks might be moved to a DLQ.
            *   `policy_config.yaml`: `bq_gold_dlq_table: "my_dataset.gold_failed_records"`.

### 5.3. Manual Escalation Points

Automated recovery is not foolproof. The system must define clear points at which human intervention is required:

*   **Exhausted Retries:** If an action fails after the maximum configured number of retries (e.g., defined by `source_retry_attempts`, `gcs_upload_retry_attempts`).
*   **Critical Errors:** Certain errors (e.g., persistent permission denied, authentication failures, critical schema mismatches not handled by DLQs) should trigger immediate alerts for manual review.
*   **Unrecognized Error Patterns:** If the diagnostic engine encounters an error it cannot categorize or map to a known recovery playbook.
*   **Prolonged System Unavailability:** If a critical component remains unavailable for an extended period despite retry attempts.
*   **DLQ Growth:** Significant or rapidly growing DLQs indicate a persistent problem that needs investigation. Alerts should be configured for DLQ size or growth rate.
    *   `policy_config.yaml`: `dlq_max_size_gb_alert_threshold: 10`, `dlq_growth_rate_records_per_hour_alert_threshold: 1000`.
*   **Self-Healing Action Fails:** If an automated recovery procedure itself fails repeatedly.
*   **Data Inconsistencies Detected Post-Recovery:** If automated checks after a recovery action detect that the system is still in an inconsistent state.

When these points are reached, the self-healing system should:
1.  Stop further automated actions on the specific failed component/data to prevent compounding issues.
2.  Generate a CRITICAL alert with all gathered diagnostic information.
3.  Clearly indicate that manual intervention is required.

### 5.4. Logging Recovery Actions

Comprehensive logging of all recovery actions is essential for:

*   **Auditability:** Maintaining a record of what actions were taken by the automated system, when, and why.
*   **Troubleshooting:** Understanding the sequence of events leading to a manual escalation.
*   **Continuous Improvement:** Analyzing the effectiveness of recovery procedures and identifying areas for enhancement.
*   **Debugging Self-Healing Logic:** Providing insights into the behavior of the self-healing system itself.

Logs should capture:
*   Timestamp of the recovery action.
*   The specific failure that triggered the recovery.
*   The recovery action taken (e.g., retry, rollback, DLQ move).
*   Parameters used for the recovery action (e.g., retry attempt number, delay).
*   Outcome of the recovery action (success, failure).
*   Any errors or warnings generated during the recovery attempt.
*   Correlation IDs to link recovery actions back to the original failure detection and diagnosis logs.

All recovery logs should be sent to the centralized logging system (Google Cloud Logging) and be clearly distinguishable for analysis.

## 6. Prevention and Improvement Measures

Beyond automated recovery, a mature self-healing strategy includes proactive measures to prevent failures and continuously improve the system's resilience. This involves learning from incidents, enhancing system design, and maintaining the self-healing capabilities themselves.

### 6.1. Post-Incident Analysis (Root Cause Analysis - RCA)

A structured approach to analyzing failures is critical for long-term improvement.

*   **Process for RCAs:**
    *   **Trigger:** RCAs are triggered for all CRITICAL (P1) incidents, recurring WARNING (P2) incidents, or any incident where the automated recovery failed or was insufficient.
    *   **Participants:** Include representatives from data engineering, operations, and potentially development or business stakeholders, depending on the incident's scope.
    *   **Methodology:** Employ a standard RCA methodology (e.g., "5 Whys," Fishbone diagram) to systematically uncover the true root cause(s), not just surface symptoms.
    *   **Focus:** The RCA should identify:
        *   The timeline of the event.
        *   The exact failure point(s).
        *   The direct cause(s) of the failure.
        *   The underlying root cause(s).
        *   The impact of the failure.
        *   The effectiveness of detection, diagnosis, and recovery (both automated and manual).
        *   Lessons learned.
        *   Action items for prevention and improvement.
*   **Documenting Findings and Tracking Actions:**
    *   **RCA Document:** All RCA findings will be documented in a standardized template stored in a central repository (e.g., Confluence, shared drive).
        *   `policy_config.yaml` could specify `rca_template_link: "http://link.to.rca.template"`.
    *   **Action Item Tracking:** Action items identified during RCAs will be tracked in a project management tool (e.g., Jira, Asana) with assigned owners and due dates.
        *   `policy_config.yaml` could specify `rca_action_item_tracking_tool: "https://our-company.jira.com/browse/DATAPLATFORM"`.
    *   **Regular Review:** Action items will be reviewed regularly in team meetings to ensure progress and accountability.

### 6.2. Proactive System Improvements

Continuously enhancing the data pipeline and its components is key to reducing failure occurrences.

*   **Capacity Planning:**
    *   Regularly review resource utilization (GCS storage, BigQuery slots, Dataflow/Dataproc worker capacity, source API quotas) to anticipate and mitigate future bottlenecks.
    *   Implement auto-scaling where appropriate and cost-effective.
*   **Data Validation Enhancements:**
    *   Continuously expand data validation rules at all stages (ingestion, transformation, loading).
    *   Incorporate business-specific validation checks beyond technical data integrity.
    *   Review and refine existing validation rules based on past failures or evolving data characteristics.
*   **Schema Management:**
    *   **Schema Validation:** Implement robust schema validation for all data entering the pipeline and between transformation stages.
        *   Alert or quarantine data that does not conform to expected schemas.
    *   **Schema Evolution Strategy:** Define a clear process for managing schema changes (e.g., adding new columns, changing data types) to minimize disruption. This includes versioning schemas and ensuring backward/forward compatibility where possible.
*   **Code Reviews and Thorough Testing:**
    *   Enforce rigorous code review practices for all pipeline code (ingestion scripts, transformation logic, dbt models, orchestration DAGs).
    *   Implement comprehensive unit, integration, and end-to-end tests, including tests for failure scenarios and edge cases.
    *   Automate testing as part of the CI/CD process.
*   **Dependency Monitoring:**
    *   Actively monitor the health and performance of external dependencies (source systems, third-party APIs, cloud services).
    *   Understand their SLAs and incorporate them into our pipeline's reliability planning.
    *   Implement circuit breaker patterns for interactions with less reliable dependencies.

### 6.3. Self-Healing System Maintenance and Improvement

The self-healing system itself requires ongoing attention to remain effective.

*   **Regular Review of `POLICY.md` and `policy_config.yaml`:**
    *   Schedule periodic reviews (e.g., quarterly) of this policy document and the associated `policy_config.yaml` to ensure they remain current with the evolving data architecture, new failure modes, and improved recovery techniques.
*   **Monitoring Self-Healing Actions:**
    *   Track the frequency of self-healing interventions, success rates of automated recovery actions, and common failure points that trigger self-healing.
    *   Dashboards should be created to visualize these metrics, helping to identify areas where the pipeline is inherently unstable or where self-healing is over-utilized.
*   **Testing Self-Healing Mechanisms:**
    *   **"Fire Drills" / Chaos Engineering Principles:** Periodically conduct controlled tests of the self-healing mechanisms.
        *   Simulate common failures (e.g., inject transient errors, temporarily make a GCS bucket inaccessible, simulate a BigQuery quota error) to verify that detection, diagnosis, and recovery actions perform as expected.
        *   This helps build confidence in the system and identify weaknesses proactively.
        *   (Advanced) `policy_config.yaml` could define parameters for such tests:
            ```yaml
            simulated_failure_tests:
              - name: "gcs_read_transient_error"
                target_component: "bronze_ingestion_job_X"
                failure_type: "GCS_READ_TIMEOUT"
                simulation_duration_minutes: 5
                expected_recovery: "RETRY_SUCCESSFUL"
            ```
*   **Updating Playbooks:**
    *   Keep diagnostic and recovery playbooks (both automated and manual) up-to-date with new learnings from incidents and system changes.
*   **Feedback Loop from Manual Interventions:**
    *   When manual intervention is required, ensure that the reasons for the failure of automated systems are analyzed.
    *   Use this analysis to identify opportunities to:
        *   Enhance detection/diagnosis logic.
        *   Add new automated recovery procedures.
        *   Improve existing recovery procedures.
        *   Update the `policy_config.yaml` with new error patterns or recovery parameters.

### 6.4. Training and Knowledge Sharing

The entire team should be familiar with the self-healing policy and procedures.

*   **Onboarding:** Include an overview of the self-healing system and this policy as part of the onboarding process for new team members.
*   **Regular Refreshers:** Conduct periodic refreshers or workshops to ensure the team understands the self-healing capabilities, their roles in incident response (especially for manual escalations), and how to use the available tools and documentation.
*   **Documentation Accessibility:** Ensure all documentation related to self-healing (this policy, playbooks, RCA templates) is easily accessible in a central location.

### 6.5. `policy_config.yaml` Considerations for Prevention and Improvement

The `policy_config.yaml` file can further support these proactive measures:

*   **Links to RCA Templates/Tools:**
    *   `rca_template_link: "url_to_rca_template.docx"`
    *   `rca_tracking_system_url: "url_to_jira_or_similar"`
*   **Configuration for Simulated Failure Tests (Advanced):**
    *   As mentioned above, parameters for "fire drill" scenarios can be defined, allowing for more structured and repeatable testing of self-healing mechanisms. This can include:
        *   `test_target_service` (e.g., a specific BigQuery dataset, a GCS bucket path pattern).
        *   `failure_injection_type` (e.g., introduce latency, return specific error codes).
        *   `expected_outcome` (e.g., successful automated recovery, specific alert triggered).
*   **Thresholds for Proactive Review:**
    *   `policy_config.yaml` could define thresholds that, when breached, trigger a review of a specific pipeline segment or self-healing rule. For example:
        *   `self_healing_trigger_frequency_alert_threshold_per_day: 10` (if a specific rule triggers more than 10 times a day, it might indicate an underlying issue).
        *   `dlq_record_age_alert_threshold_hours: 24` (if records stay in a DLQ for more than 24 hours, it warrants investigation).

By embedding these prevention and improvement measures into our operational rhythm, we aim to create a data platform that is not only capable of recovering from failures but also becomes increasingly robust and less prone to issues over time.

## 7. Python Implementation Considerations

This section outlines key considerations for implementing the self-healing framework using Python, focusing on best practices and relevant tools to achieve a robust, maintainable, and extensible system. The actual implementation will be a separate Python project that consumes and acts upon the `policy_config.yaml`.

### 7.1. Core Philosophy for Python Implementation

The Python implementation of the self-healing framework should adhere to the following core principles:

*   **Modularity:**
    *   Break down the self-healing logic into smaller, reusable modules. Each module should have a specific responsibility (e.g., GCS operations, BigQuery job handling, alerting).
    *   This improves maintainability, testability, and allows for easier updates or replacements of individual components.
*   **Configurability:**
    *   All operational parameters (retry counts, timeouts, thresholds, DLQ paths, notification endpoints, etc.) should be driven by the `policy_config.yaml` file.
    *   Avoid hardcoding values within the Python scripts. This allows for dynamic adjustments to the self-healing behavior without code changes.
*   **Extensibility:**
    *   Design the system to be easily extensible to support new types of failures, new services in the data pipeline, or new recovery strategies.
    *   Utilize design patterns (like the Strategy pattern) to allow for adding new diagnostic or recovery modules with minimal changes to the core framework.
*   **Testability:**
    *   Write code with testability in mind from the outset. This includes designing functions and classes with clear inputs and outputs, and minimizing side effects where possible.
    *   Strive for high unit test coverage and include integration tests for critical interaction points.

### 7.2. Key Python Libraries and SDKs

Leveraging established Python libraries and Google Cloud SDKs will accelerate development and ensure reliability.

*   **Google Cloud Client Libraries:**
    *   `google-cloud-storage`: For interacting with Google Cloud Storage (uploading, downloading, listing objects, managing buckets, accessing metadata and checksums).
    *   `google-cloud-bigquery`: For managing BigQuery jobs (querying, loading, copying tables), managing datasets and tables, and reading job status/error information.
    *   `google-cloud-logging`: For programmatically writing structured logs to Google Cloud Logging, which is crucial for detection, diagnosis, and auditing self-healing actions.
    *   `google-cloud-monitoring`: For programmatically creating custom metrics or checking existing ones, potentially to verify recovery actions or to drive more sophisticated detection logic.
*   **Configuration Management:**
    *   `PyYAML`: For loading, parsing, and validating the `policy_config.yaml` file, making its parameters easily accessible to the Python scripts.
*   **HTTP Requests:**
    *   `requests`: For making HTTP calls to external source APIs, health check endpoints, or notification webhooks (e.g., Slack, PagerDuty).
*   **Orchestration and Execution Environment:**
    *   **Apache Airflow / Google Cloud Composer:** If self-healing logic is embedded within orchestration DAGs (e.g., retry mechanisms for tasks, dynamic task generation based on failures).
    *   **Google Cloud Functions:** For event-driven self-healing actions. For example, a Cloud Function triggered by a GCS object finalization event can perform validation and initiate recovery if needed. Also suitable for running small, targeted diagnostic or recovery scripts.
*   **Retry Mechanisms:**
    *   `tenacity`: A powerful and flexible library for adding retry logic to Python functions with features like exponential backoff, configurable stop conditions, and callback actions on retries. This is highly recommended for implementing the retry strategies outlined in this policy.

### 7.3. Design Patterns and Approaches

Employing appropriate design patterns can significantly improve the structure and resilience of the self-healing code.

*   **Policy Engine/Runner Concept:**
    *   A central "engine" or "runner" script that loads the `policy_config.yaml`.
    *   Based on detected failures (e.g., from logs, monitoring alerts, or direct invocation), this engine selects and executes the appropriate diagnostic and recovery strategies defined in the policy.
*   **State Machines:**
    *   For complex recovery processes involving multiple steps or states (e.g., `DETECTED` -> `DIAGNOSING` -> `RECOVERING` -> `VALIDATING` -> `RESOLVED`/`FAILED_ESCALATED`).
    *   A state machine can manage the transitions and ensure that recovery proceeds in an orderly manner.
*   **Strategy Pattern:**
    *   Define a family of algorithms (e.g., different recovery strategies for different BigQuery errors), encapsulate each one, and make them interchangeable.
    *   This allows the policy engine to select the appropriate strategy at runtime based on the type of failure and the configuration in `policy_config.yaml`.
    *   Example:
        ```python
        # strategy_interface.py
        class RecoveryStrategy:
            def execute(self, context):
                raise NotImplementedError

        # bq_quota_error_strategy.py
        class BQQuotaErrorStrategy(RecoveryStrategy):
            def execute(self, context):
                # Logic for handling BQ Quota errors
                pass
        ```
*   **Circuit Breaker Pattern:**
    *   Useful when interacting with external services (e.g., data sources, APIs) that might be temporarily unavailable.
    *   After a configured number of consecutive failures, the circuit "opens," and further calls are failed immediately (or routed to a fallback) for a set period, preventing the system from repeatedly trying a failing operation.
    *   Libraries like `pybreaker` can implement this.
*   **Idempotent Operations:**
    *   Ensure that Python functions performing recovery actions are idempotent. For example, re-running a script to move a file to a DLQ should not fail or cause issues if the file was already moved.
    *   This often involves checking the current state before performing an action (e.g., "does the DLQ file already exist?").
*   **Consistent Error Handling:**
    *   Implement a consistent approach to error handling and exception management throughout the Python codebase.
    *   Define custom exceptions for specific self-healing failures where appropriate.
    *   Ensure that exceptions are caught, logged with meaningful context, and contribute to the diagnosis and alerting process.

### 7.4. Using `policy_config.yaml` in Python

Python scripts will need to load and utilize parameters from `policy_config.yaml`.

```python
import yaml

def load_policy_config(config_path="policy_config.yaml"):
    """Loads the policy configuration from a YAML file."""
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        return config
    except FileNotFoundError:
        # Log error and potentially raise a critical exception
        print(f"ERROR: Policy config file not found at {config_path}")
        raise
    except yaml.YAMLError as e:
        # Log error and potentially raise a critical exception
        print(f"ERROR: Error parsing policy config file: {e}")
        raise

# Example usage:
# policy_params = load_policy_config()
# retry_attempts = policy_params.get('recovery_settings', {}).get('bq_silver_job_retry_attempts', 3)
# dlq_path = policy_params.get('dead_letter_queues', {}).get('bq_silver_dlq_gcs_path')
```
*   The `load_policy_config` function reads the YAML file.
*   Access parameters by navigating the dictionary structure derived from the YAML.
*   Implement default values or robust error handling if expected keys are missing to prevent script failures due to incomplete configurations.
*   Consider using a dedicated configuration management class that provides typed access to parameters and handles validation.

**Interfacing with BigQuery Tracking Tables:**
The Python implementation should include modules or functions responsible for logging operational data to the BigQuery tables defined in this policy (`policy_execution_log` and `data_flow_log`). The specific BigQuery table IDs for the target environment will be read from the `policy_config.yaml` file (e.g., from `tracking_and_logging.policy_execution_log_table_id` and `tracking_and_logging.data_flow_log_table_id`). This ensures that all self-healing actions and data flow milestones are captured for auditing, monitoring, and dashboarding purposes. Ensure that the Python code correctly formats data to match the schemas of these tables.

### 7.5. Testing Self-Healing Scripts

Rigorous testing is paramount for a system designed to automatically fix issues.

*   **Unit Tests (`unittest` / `pytest`):**
    *   Test individual functions and classes in isolation.
    *   Use mocking (`unittest.mock` or `pytest-mock`) extensively to simulate:
        *   Failures from Google Cloud services (e.g., BigQuery API returning an error).
        *   Responses from external APIs.
        *   The behavior of other parts of the self-healing system.
        *   File system operations, logging calls.
    *   Example: Mocking a BigQuery client to simulate a job failure.
*   **Integration Tests:**
    *   Test interactions between different components of the self-healing system (e.g., does the diagnosis module correctly trigger the recovery module?).
    *   Test interactions with actual (sandboxed/test) Google Cloud services if feasible, but be mindful of cost and cleanup. For example, test a DLQ process by creating a dummy file, processing it, and ensuring it lands in the test DLQ bucket.
    *   These tests are more complex to set up but provide higher confidence.
*   **Scenario-Based Tests (related to "Fire Drills"):**
    *   Design tests that simulate end-to-end failure and recovery scenarios as outlined in the "Prevention and Improvement Measures" section.
    *   These can be orchestrated scripts that intentionally introduce a failure condition and then verify that the self-healing system detects, diagnoses, and recovers as expected.

### 7.6. Suggested Directory Structure for Python Code

A clear directory structure aids organization and maintainability. The self-healing Python code would reside in its own repository or a dedicated directory within a larger monorepo.

```
self-healing-framework/
├── POLICY.md
├── policy_config.yaml
├── src/
│   ├── __init__.py
│   ├── main.py                 # Main entry point or policy engine runner
│   ├── config_loader.py        # For loading and validating policy_config.yaml
│   ├── detection/
│   │   ├── __init__.py
│   │   └── bq_job_monitor.py   # Example: monitors BQ jobs
│   ├── diagnosis/
│   │   ├── __init__.py
│   │   └── error_parser.py     # Example: parses BQ error logs
│   ├── recovery/
│   │   ├── __init__.py
│   │   ├── strategies/         # Strategy pattern implementations
│   │   │   ├── __init__.py
│   │   │   ├── bq_retry_strategy.py
│   │   │   └── gcs_dlq_strategy.py
│   │   └── recovery_manager.py # Manages execution of recovery strategies
│   ├── alerting/
│   │   ├── __init__.py
│   │   └── slack_alerter.py    # Example: sends alerts to Slack
│   └── utils/
│       ├── __init__.py
│       └── gcp_clients.py      # Helper for initializing GCP clients
├── tests/
│   ├── __init__.py
│   ├── unit/
│   │   ├── __init__.py
│   │   ├── detection/
│   │   │   └── test_bq_job_monitor.py
│   │   └── recovery/
│   │       └── strategies/
│   │           └── test_bq_retry_strategy.py
│   └── integration/
│       ├── __init__.py
│       └── test_full_bq_failure_recovery.py
├── requirements.txt
└── README.md
```

This structure separates concerns (detection, diagnosis, recovery, alerting), facilitates testing, and aligns with common Python project layouts. The actual modules and their names will depend on the specific implementation choices.

## 8. Conclusion

This Self-Healing Policy document provides a comprehensive framework for building and maintaining a resilient data pipeline. By defining clear objectives, scope, and strategies for detection, diagnosis, recovery, and continuous improvement, we lay the foundation for a system that can autonomously handle common failures, thereby increasing reliability and reducing manual intervention.

The successful implementation of this policy relies on the diligent application of its principles, the robust engineering of the Python-based self-healing framework, and the careful configuration of operational parameters in the `policy_config.yaml` file.

It is crucial to recognize that self-healing is not a one-time setup but an ongoing process. Regular review of this policy, analysis of incidents, refinement of recovery procedures, and proactive system enhancements are essential to adapt to evolving data needs and new failure modes. By fostering a culture of continuous improvement and leveraging the capabilities outlined in this document, we can significantly enhance the stability and trustworthiness of our data platform, ensuring it consistently meets business requirements.
