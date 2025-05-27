import time
import uuid
from typing import Dict, Any, Optional, List, Tuple

from ..core.interfaces import AbstractDatabaseHandler, AbstractLogger
from ..core.config_manager import ConfigManager, ConfigManagerError

try:
    from google.cloud import bigquery
    from google.cloud.bigquery.job import QueryJobConfig, LoadJobConfig # For type hinting and job creation
    from google.api_core.exceptions import GoogleAPICallError, NotFound, Conflict
except ImportError:
    bigquery = None # type: ignore
    QueryJobConfig = None # type: ignore
    LoadJobConfig = None # type: ignore
    GoogleAPICallError = None # type: ignore
    NotFound = None # type: ignore
    Conflict = None # type: ignore
    print("WARNING: google-cloud-bigquery is not installed. BigQueryDatabaseHandler will not be functional.", file=sys.stderr)


class BigQueryDatabaseHandler(AbstractDatabaseHandler):
    """
    Implements database operations for Google BigQuery.
    """

    def __init__(self, config_manager: ConfigManager, logger: AbstractLogger, project_id: Optional[str] = None):
        """
        Initializes the BigQueryDatabaseHandler.

        Args:
            config_manager: Instance of ConfigManager.
            logger: Instance of AbstractLogger for logging.
            project_id: GCP project ID. If None, client tries to infer from environment.
        """
        self.config_manager = config_manager
        self.logger = logger
        self._client: Optional[bigquery.Client] = None
        self._is_configured = False
        self.default_job_location: Optional[str] = None

        if bigquery is None:
            self.logger.log_error("BigQueryDatabaseHandler disabled: google-cloud-bigquery not found.", None, {"initialization_error": True})
            return

        try:
            self._client = bigquery.Client(project=project_id)
            # Fetch default location from config
            bq_settings = self.config_manager.get_bigquery_settings()
            self.default_job_location = bq_settings.get("default_job_settings", {}).get("default_location")
            
            self._is_configured = True
            self.logger.log_info(f"BigQueryDatabaseHandler initialized successfully. Default location: {self.default_job_location or 'Not Set'}.")
        except ConfigManagerError as e:
             self.logger.log_error(f"Failed to initialize BigQueryDatabaseHandler due to ConfigManagerError: {e}", e, {"initialization_error": True})
             self._client = None
        except Exception as e:
            self.logger.log_error(f"Failed to initialize GCS client for BigQueryDatabaseHandler: {e}", e, {"initialization_error": True})
            self._client = None # Ensure client is None if init fails


    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None, job_config: Optional[QueryJobConfig] = None) -> List[Tuple[Any, ...]]:
        if not self._is_configured or not self._client:
            self.logger.log_error("BigQueryDatabaseHandler not configured. Cannot execute query.", details={"query_snippet": query[:100]})
            raise ConnectionError("BigQueryDatabaseHandler not configured.")

        query_job_config = job_config if job_config else QueryJobConfig()
        
        # Example of using named parameters if provided
        if params:
            # This is a simplified approach. Real named parameters require specific setup.
            # For now, assume params are for string formatting or specific BQ parameter types.
            # query_job_config.query_parameters = [...]
            self.logger.log_info("Named parameters provided, ensure QueryJobConfig is set up for them.", details=params)


        job_id = f"query_job_{uuid.uuid4().hex}" # Generate a unique job ID
        self.logger.log_info(f"Executing BigQuery query with job_id: {job_id}", details={"query": query, "params": params, "job_config": str(query_job_config)})
        
        try:
            query_job = self._client.query(query, job_config=query_job_config, job_id=job_id, location=self.default_job_location)
            self.logger.log_info(f"BigQuery query job {query_job.job_id} created. Waiting for results...")
            
            # Wait for the job to complete
            results = query_job.result() # This will block until the job is finished
            
            self.logger.log_info(f"BigQuery query job {query_job.job_id} completed. Status: {query_job.state}", details={"rows_returned": results.total_rows})
            return list(results) # Convert to list of tuples
            
        except GoogleAPICallError as e:
            self.logger.log_error(f"BigQuery API error executing query (job_id: {job_id}): {e}", e, {"query": query, "params": params})
            raise
        except Exception as e:
            self.logger.log_error(f"Unexpected error executing BigQuery query (job_id: {job_id}): {e}", e, {"query": query, "params": params})
            raise

    def get_job_status(self, job_id: str, location: Optional[str] = None) -> Dict[str, Any]:
        if not self._is_configured or not self._client:
            self.logger.log_error("BigQueryDatabaseHandler not configured. Cannot get job status.", details={"job_id": job_id})
            raise ConnectionError("BigQueryDatabaseHandler not configured.")
        
        job_location = location or self.default_job_location
        self.logger.log_info(f"Fetching status for BigQuery job {job_id}", details={"location": job_location})
        
        try:
            job = self._client.get_job(job_id, location=job_location)
            status_info = {
                "job_id": job.job_id,
                "job_type": job.job_type,
                "state": job.state,
                "created": job.created.isoformat() if job.created else None,
                "started": job.started.isoformat() if job.started else None,
                "ended": job.ended.isoformat() if job.ended else None,
                "error_result": job.error_result, # This will be None if no error
                "errors": job.errors, # This will be None if no errors
                "user_email": job.user_email,
            }
            if job.error_result:
                self.logger.log_warning(f"BigQuery job {job_id} has error_result.", details=status_info)
            return status_info
        except NotFound:
            self.logger.log_error(f"BigQuery job {job_id} not found.", details={"location": job_location})
            raise
        except GoogleAPICallError as e:
            self.logger.log_error(f"BigQuery API error fetching status for job {job_id}: {e}", e, {"job_id": job_id})
            raise
        except Exception as e:
            self.logger.log_error(f"Unexpected error fetching status for BigQuery job {job_id}: {e}", e, {"job_id": job_id})
            raise

    def retry_job(self, original_job_id: str, location: Optional[str] = None) -> Dict[str, Any]:
        """
        Retries a BigQuery job by fetching its configuration and creating a new job.
        This is a simplified approach. For complex jobs, more specific handling might be needed.
        Currently, this method primarily works for QueryJobs. LoadJobs, CopyJobs etc. would
        require fetching their specific configurations (e.g., source_uris, destination_table).

        Args:
            original_job_id: The ID of the job to retry.
            location: The location of the original job.

        Returns:
            A dictionary with the new job_id and its initial status.
        """
        if not self._is_configured or not self._client:
            self.logger.log_error("BigQueryDatabaseHandler not configured. Cannot retry job.", details={"original_job_id": original_job_id})
            raise ConnectionError("BigQueryDatabaseHandler not configured.")

        job_location = location or self.default_job_location
        self.logger.log_info(f"Attempting to retry BigQuery job {original_job_id}", details={"location": job_location})

        try:
            original_job = self._client.get_job(original_job_id, location=job_location)
            if not original_job: # Should be caught by NotFound, but as a safeguard
                raise NotFound(f"Original job {original_job_id} not found for retry.")

            new_job_id = f"retry_{original_job.job_type}_{uuid.uuid4().hex}"
            
            job_config_properties = {}
            if hasattr(original_job, 'destination') and original_job.destination:
                job_config_properties['destination'] = original_job.destination
            if hasattr(original_job, 'write_disposition') and original_job.write_disposition:
                job_config_properties['write_disposition'] = original_job.write_disposition
            if hasattr(original_job, 'create_disposition') and original_job.create_disposition:
                 job_config_properties['create_disposition'] = original_job.create_disposition


            if isinstance(original_job, bigquery.QueryJob):
                self.logger.log_info(f"Retrying QueryJob. Original query: {original_job.query[:100]}...", 
                                     details={"original_job_id": original_job_id, "new_job_id": new_job_id})
                
                # Create a new QueryJobConfig from the original job's properties
                # This can be tricky as not all properties are directly on QueryJobConfig
                # We'll copy common ones. For full fidelity, one might need to inspect original_job._properties
                q_config = bigquery.QueryJobConfig(
                    default_dataset=original_job.default_dataset,
                    use_legacy_sql=original_job.use_legacy_sql,
                    priority=original_job.priority,
                    **job_config_properties # Add common properties like destination
                )
                # Note: query_parameters are not directly available on the job object after creation.
                # They would need to be passed into the retry mechanism if the original query used them.

                new_job = self._client.query(original_job.query, job_config=q_config, job_id=new_job_id, location=job_location)
            
            # Placeholder for other job types - LoadJob, CopyJob, ExtractJob
            # These require different configurations and source/destination properties.
            # For example, for a LoadJob:
            # elif isinstance(original_job, bigquery.LoadJob):
            #     self.logger.log_info(f"Retrying LoadJob. Original sources: {original_job.source_uris}",
            #                          details={"original_job_id": original_job_id, "new_job_id": new_job_id})
            #     load_config = bigquery.LoadJobConfig(
            #         schema=original_job.schema,
            #         source_format=original_job.source_format,
            #         # ... other relevant LoadJobConfig properties ...
            #         **job_config_properties
            #     )
            #     new_job = self._client.load_table_from_uri(
            #         original_job.source_uris, original_job.destination, job_config=load_config, job_id=new_job_id, location=job_location
            #     )
            else:
                self.logger.log_error(f"Job type {type(original_job).__name__} not fully supported for automated retry by this handler yet.",
                                      details={"original_job_id": original_job_id})
                raise NotImplementedError(f"Automated retry for job type {type(original_job).__name__} is not fully implemented.")

            self.logger.log_info(f"Successfully submitted retry job {new_job.job_id} for original job {original_job_id}. Current state: {new_job.state}")
            return {
                "new_job_id": new_job.job_id,
                "state": new_job.state,
                "original_job_id": original_job_id
            }

        except NotFound:
            self.logger.log_error(f"Original BigQuery job {original_job_id} not found for retry.", details={"location": job_location})
            raise
        except GoogleAPICallError as e:
            self.logger.log_error(f"BigQuery API error retrying job {original_job_id}: {e}", e)
            raise
        except Exception as e:
            self.logger.log_error(f"Unexpected error retrying BigQuery job {original_job_id}: {e}", e)
            raise
