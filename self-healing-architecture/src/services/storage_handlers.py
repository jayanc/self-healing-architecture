import os
import uuid
import datetime
from typing import Dict, Any, Optional, List

from ..core.interfaces import AbstractStorageHandler, AbstractLogger
from ..core.config_manager import ConfigManager, ConfigManagerError

try:
    from google.cloud import storage
    from google.api_core.exceptions import GoogleAPICallError, NotFound
except ImportError:
    storage = None # type: ignore
    GoogleAPICallError = None # type: ignore
    NotFound = None # type: ignore
    print("WARNING: google-cloud-storage is not installed. GCSStorageHandler will not be functional.", file=sys.stderr)


class GCSStorageHandler(AbstractStorageHandler):
    """
    Implements cloud storage operations for Google Cloud Storage.
    """

    def __init__(self, config_manager: ConfigManager, logger: AbstractLogger, project_id: Optional[str] = None):
        """
        Initializes the GCSStorageHandler.

        Args:
            config_manager: Instance of ConfigManager.
            logger: Instance of AbstractLogger for logging.
            project_id: GCP project ID. If None, client tries to infer from environment.
        """
        self.config_manager = config_manager
        self.logger = logger
        self._client: Optional[storage.Client] = None
        self._is_configured = False

        if storage is None:
            self.logger.log_error("GCSStorageHandler disabled: google-cloud-storage not found.", None, {"initialization_error": True})
            return

        try:
            self._client = storage.Client(project=project_id)
            # Test connection or list a bucket to ensure auth (optional, can add overhead)
            # self._client.list_buckets(max_results=1) 
            self._is_configured = True
            self.logger.log_info("GCSStorageHandler initialized successfully.")
        except Exception as e:
            self.logger.log_error(f"Failed to initialize GCS client for GCSStorageHandler: {e}", e, {"initialization_error": True})
            self._client = None # Ensure client is None if init fails

    def download_file(self, bucket_name: str, object_name: str, destination_path: str) -> None:
        if not self._is_configured or not self._client:
            self.logger.log_error("GCSStorageHandler not configured. Cannot download file.", details={"bucket": bucket_name, "object": object_name})
            raise ConnectionError("GCSStorageHandler not configured.")
        
        try:
            bucket = self._client.bucket(bucket_name)
            blob = bucket.blob(object_name)
            
            os.makedirs(os.path.dirname(destination_path), exist_ok=True)
            blob.download_to_filename(destination_path)
            self.logger.log_info(f"File {object_name} downloaded from GCS bucket {bucket_name} to {destination_path}.")
        except NotFound:
            self.logger.log_error(f"File {object_name} not found in GCS bucket {bucket_name}.", details={"bucket": bucket_name, "object": object_name})
            raise FileNotFoundError(f"GCS object {object_name} not found in bucket {bucket_name}.")
        except GoogleAPICallError as e:
            self.logger.log_error(f"GCS API error downloading {object_name} from {bucket_name}: {e}", e, {"bucket": bucket_name, "object": object_name})
            raise
        except Exception as e:
            self.logger.log_error(f"Unexpected error downloading {object_name} from {bucket_name}: {e}", e, {"bucket": bucket_name, "object": object_name})
            raise

    def upload_file(self, bucket_name: str, source_path: str, destination_object_name: str) -> bool:
        if not self._is_configured or not self._client:
            self.logger.log_error("GCSStorageHandler not configured. Cannot upload file.", details={"bucket": bucket_name, "source": source_path})
            return False
        
        if not os.path.exists(source_path):
            self.logger.log_error(f"Source file {source_path} not found for GCS upload.", details={"bucket": bucket_name, "source": source_path})
            return False

        try:
            bucket = self._client.bucket(bucket_name)
            blob = bucket.blob(destination_object_name)
            blob.upload_from_filename(source_path)
            self.logger.log_info(f"File {source_path} uploaded to GCS object {destination_object_name} in bucket {bucket_name}.")
            return True
        except NotFound: # Should not happen for bucket on upload unless bucket name is wrong
            self.logger.log_error(f"GCS bucket {bucket_name} not found during upload.", details={"bucket": bucket_name, "source": source_path})
            return False
        except GoogleAPICallError as e:
            self.logger.log_error(f"GCS API error uploading {source_path} to {bucket_name}/{destination_object_name}: {e}", e, {"bucket": bucket_name, "source": source_path})
            return False
        except Exception as e:
            self.logger.log_error(f"Unexpected error uploading {source_path} to {bucket_name}/{destination_object_name}: {e}", e, {"bucket": bucket_name, "source": source_path})
            return False

    def move_to_dlq(self, source_bucket_name: str, source_object_name: str, error_details: Optional[Dict[str, Any]] = None) -> str:
        if not self._is_configured or not self._client:
            self.logger.log_error("GCSStorageHandler not configured. Cannot move to DLQ.", details={"source_bucket": source_bucket_name, "source_object": source_object_name})
            raise ConnectionError("GCSStorageHandler not configured.")

        try:
            # Determine DLQ bucket from config
            # Assuming structure like: gcs_settings: { bronze_layer: { dead_letter_bucket: "name" }}
            # This part might need refinement based on how DLQ bucket is specified per layer/source
            dlq_bucket_name = self.config_manager.get_parameter(
                "gcs_settings", "bronze_layer", {}).get("dead_letter_bucket") # Defaulting to bronze for now
            
            if not dlq_bucket_name:
                # Try a more generic DLQ config if layer-specific is not found
                dlq_bucket_name = self.config_manager.get_parameter(
                    "gcs_settings", "default_dead_letter_bucket", None)

            if not dlq_bucket_name:
                self.logger.log_error("GCS Dead Letter Bucket not configured. Cannot move file.", 
                                      details={"source_bucket": source_bucket_name, "source_object": source_object_name})
                raise ConfigManagerError("GCS Dead Letter Bucket not configured.")

            source_bucket = self._client.bucket(source_bucket_name)
            source_blob = source_bucket.blob(source_object_name)

            if not source_blob.exists():
                self.logger.log_warning(f"Source object {source_object_name} not found in bucket {source_bucket_name} for DLQ move.",
                                        details={"source_bucket": source_bucket_name, "source_object": source_object_name})
                raise FileNotFoundError(f"GCS object {source_object_name} not found in bucket {source_bucket_name}.")

            # Construct DLQ object name: original_name_timestamp_uuid.suffix
            timestamp_str = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d%H%M%S")
            base_name, extension = os.path.splitext(source_object_name)
            dlq_object_name = f"{base_name}_{timestamp_str}_{uuid.uuid4().hex[:8]}{extension}"
            
            # Add a prefix based on error type or source if desired
            dlq_prefix = "general_error/"
            if error_details and error_details.get("pipeline_stage"):
                dlq_prefix = f"{error_details.get('pipeline_stage', 'unknown_stage').lower()}/"
            if error_details and error_details.get("detected_issue_type"):
                dlq_prefix += f"{error_details.get('detected_issue_type', 'unknown_error').lower()}/"
            
            full_dlq_object_name = f"{dlq_prefix}{dlq_object_name}"


            dlq_bucket = self._client.bucket(dlq_bucket_name)
            
            # Copy to DLQ
            destination_blob = source_bucket.copy_blob(
                source_blob, dlq_bucket, full_dlq_object_name
            )
            if not destination_blob: # copy_blob returns None on failure in some cases, or raises error
                 raise Exception(f"Failed to copy blob {source_object_name} to DLQ {dlq_bucket_name}/{full_dlq_object_name}. Copy operation returned None.")


            # Delete original blob after successful copy
            source_blob.delete()

            dlq_path = f"gs://{dlq_bucket_name}/{full_dlq_object_name}"
            self.logger.log_info(f"File {source_object_name} from bucket {source_bucket_name} moved to DLQ: {dlq_path}.",
                                 details={"error_context": error_details, "dlq_path": dlq_path})
            return dlq_path

        except NotFound:
             self.logger.log_error(f"GCS resource not found during DLQ operation.", 
                                   details={"source_bucket": source_bucket_name, "source_object": source_object_name})
             raise
        except GoogleAPICallError as e:
            self.logger.log_error(f"GCS API error moving {source_object_name} to DLQ: {e}", e, 
                                  details={"source_bucket": source_bucket_name, "source_object": source_object_name})
            raise
        except Exception as e:
            self.logger.log_error(f"Unexpected error moving {source_object_name} to DLQ: {e}", e, 
                                  details={"source_bucket": source_bucket_name, "source_object": source_object_name})
            raise

    def list_files(self, bucket_name: str, prefix: Optional[str] = None) -> List[str]:
        if not self._is_configured or not self._client:
            self.logger.log_error("GCSStorageHandler not configured. Cannot list files.", details={"bucket": bucket_name, "prefix": prefix})
            raise ConnectionError("GCSStorageHandler not configured.")
        
        try:
            blobs = self._client.list_blobs(bucket_name, prefix=prefix)
            return [blob.name for blob in blobs]
        except NotFound:
            self.logger.log_error(f"GCS bucket {bucket_name} not found during list_files.", details={"bucket": bucket_name, "prefix": prefix})
            return [] # Or raise error, depending on desired behavior for non-existent bucket
        except GoogleAPICallError as e:
            self.logger.log_error(f"GCS API error listing files in {bucket_name} with prefix {prefix}: {e}", e, {"bucket": bucket_name, "prefix": prefix})
            raise
        except Exception as e:
            self.logger.log_error(f"Unexpected error listing files in {bucket_name} with prefix {prefix}: {e}", e, {"bucket": bucket_name, "prefix": prefix})
            raise
