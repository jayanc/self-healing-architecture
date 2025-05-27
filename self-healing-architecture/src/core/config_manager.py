import yaml
import os
from typing import Dict, Any, Optional, List

class ConfigManagerError(Exception):
    """Custom exception for ConfigManager errors."""
    pass

class ConfigManager:
    """
    Manages loading and accessing configuration from policy_config.yaml.
    """
    _instance = None

    def __new__(cls, config_path: Optional[str] = None):
        if cls._instance is None:
            cls._instance = super(ConfigManager, cls).__new__(cls)
            cls._instance._config = None
            if config_path:
                cls._instance.load_config(config_path)
            else:
                # Default path relative to this file's location in src/core/
                # config/policy_config.yaml
                base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
                default_path = os.path.join(base_dir, 'config', 'policy_config.yaml')
                cls._instance.load_config(default_path)
        return cls._instance

    def load_config(self, config_path: str) -> None:
        """
        Loads the YAML configuration file.

        Args:
            config_path: Path to the policy_config.yaml file.

        Raises:
            ConfigManagerError: If the file is not found or cannot be parsed.
        """
        if not os.path.exists(config_path):
            raise ConfigManagerError(f"Configuration file not found at path: {config_path}")
        try:
            with open(config_path, 'r') as f:
                self._config = yaml.safe_load(f)
            if not isinstance(self._config, dict):
                raise ConfigManagerError("Configuration file is not a valid YAML dictionary.")
            self._validate_essential_keys()
            print(f"Configuration loaded successfully from {config_path}")
        except yaml.YAMLError as e:
            raise ConfigManagerError(f"Error parsing YAML configuration file: {e}")
        except Exception as e:
            raise ConfigManagerError(f"An unexpected error occurred while loading configuration: {e}")

    def _validate_essential_keys(self) -> None:
        """
        Validates that essential top-level keys are present in the config.
        Sub-validations can be added as needed.
        """
        essential_keys = [
            "global_settings",
            "data_sources",
            "ingestion_settings",
            "gcs_settings",
            "bigquery_settings",
            "alerting",
            "tracking_and_logging"
        ]
        if not self._config:
            raise ConfigManagerError("Configuration is not loaded.")
        
        missing_keys = [key for key in essential_keys if key not in self._config]
        if missing_keys:
            raise ConfigManagerError(f"Missing essential configuration keys: {', '.join(missing_keys)}")

    def get_config(self) -> Dict[str, Any]:
        """Returns the entire configuration dictionary."""
        if self._config is None:
            raise ConfigManagerError("Configuration not loaded. Call load_config() first.")
        return self._config

    def get_section(self, section_name: str) -> Dict[str, Any]:
        """
        Retrieves a specific top-level section from the configuration.

        Args:
            section_name: The name of the configuration section.

        Returns:
            A dictionary representing the configuration section.

        Raises:
            ConfigManagerError: If the section is not found or config not loaded.
        """
        config = self.get_config()
        if section_name not in config:
            raise ConfigManagerError(f"Configuration section '{section_name}' not found.")
        return config[section_name]

    def get_parameter(self, section_name: str, parameter_name: str, default: Optional[Any] = None) -> Any:
        """
        Retrieves a specific parameter from a given section.

        Args:
            section_name: The name of the configuration section.
            parameter_name: The name of the parameter within the section.
            default: Optional default value if the parameter is not found.

        Returns:
            The parameter value or the default if provided.

        Raises:
            ConfigManagerError: If the section or parameter is not found and no default is provided.
        """
        section = self.get_section(section_name)
        if parameter_name not in section:
            if default is not None:
                return default
            raise ConfigManagerError(f"Parameter '{parameter_name}' not found in section '{section_name}'.")
        return section[parameter_name]

    # --- Specific Getters for Major Sections (examples) ---

    def get_global_settings(self) -> Dict[str, Any]:
        """Returns the 'global_settings' configuration section."""
        return self.get_section("global_settings")

    def get_data_source_config(self, source_id: str) -> Dict[str, Any]:
        """
        Returns the configuration for a specific data source,
        falling back to default if not found.
        """
        sources_config = self.get_section("data_sources")
        default_config = sources_config.get("default", {})
        source_specific_config = sources_config.get(source_id, {})
        
        # Merge default with source-specific, source-specific taking precedence
        merged_config = {**default_config, **source_specific_config}
        if not merged_config:
             raise ConfigManagerError(f"No configuration found for data source '{source_id}' and no default defined.")
        return merged_config

    def get_ingestion_settings(self, pattern_type: Optional[str] = None) -> Dict[str, Any]:
        """
        Returns ingestion settings, optionally for a specific pattern type (cdc, streaming, etc.).
        """
        ingestion_config = self.get_section("ingestion_settings")
        if pattern_type:
            if pattern_type not in ingestion_config:
                raise ConfigManagerError(f"Ingestion pattern type '{pattern_type}' not found in 'ingestion_settings'.")
            return ingestion_config[pattern_type]
        return ingestion_config # Returns all ingestion settings if no type specified

    def get_gcs_settings(self, layer: Optional[str] = None) -> Dict[str, Any]:
        """Returns GCS settings, optionally for a specific layer (bronze, silver)."""
        gcs_config = self.get_section("gcs_settings")
        if layer:
            if layer not in gcs_config:
                raise ConfigManagerError(f"GCS layer '{layer}' not found in 'gcs_settings'.")
            return gcs_config[layer]
        return gcs_config

    def get_bigquery_settings(self, layer: Optional[str] = None) -> Dict[str, Any]:
        """Returns BigQuery settings, optionally for a specific layer (silver, gold)."""
        bq_config = self.get_section("bigquery_settings")
        if layer:
            if layer not in bq_config: # e.g. 'silver_layer' not 'silver'
                 # Attempt with _layer suffix if simple layer name not found
                layer_key_suffix = f"{layer}_layer"
                if layer_key_suffix in bq_config:
                    return bq_config[layer_key_suffix]
                raise ConfigManagerError(f"BigQuery layer '{layer}' or '{layer_key_suffix}' not found in 'bigquery_settings'.")
            return bq_config[layer]
        return bq_config

    def get_alerting_config(self) -> Dict[str, Any]:
        """Returns the 'alerting' configuration section."""
        return self.get_section("alerting")
        
    def get_alert_provider_config(self, provider_name: str) -> Dict[str, Any]:
        """Returns configuration for a specific alert provider."""
        alerting_conf = self.get_alerting_config()
        providers = alerting_conf.get("providers", {})
        if provider_name not in providers:
            raise ConfigManagerError(f"Alert provider '{provider_name}' not found in alerting providers config.")
        return providers[provider_name]

    def get_tracking_and_logging_config(self) -> Dict[str, Any]:
        """Returns the 'tracking_and_logging' configuration section."""
        return self.get_section("tracking_and_logging")

    def get_error_patterns_config(self, service_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """Returns error patterns, optionally for a specific service."""
        patterns_config = self.get_section("error_patterns")
        if service_name:
            if service_name not in patterns_config:
                # Return empty list if service has no specific patterns defined
                return [] 
            return patterns_config[service_name]
        # If no service name, might return all or handle as error - for now, let's return all
        all_patterns = []
        for service_patterns in patterns_config.values():
            all_patterns.extend(service_patterns)
        return all_patterns

# Example usage (typically done once and shared or re-instantiated as needed):
# if __name__ == "__main__":
#     try:
#         # ConfigManager is a singleton, this will always return the same instance
#         # Path can be omitted if default path is correct
#         # base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
#         # project_root = os.path.dirname(base_dir) # Assuming src is one level down from project root
#         # conf_path = os.path.join(project_root, 'config', 'policy_config.yaml')
#         # print(f"Attempting to load config from: {conf_path}")
        
#         # Relative path from where Python is executed if not specifying absolute.
#         # This example assumes you run a script from the 'self-healing-architecture' root.
#         # For robust path handling, consider using absolute paths or environment variables.
#         cfg_manager = ConfigManager("config/policy_config.yaml")
        
#         print("Global Log Level:", cfg_manager.get_parameter("global_settings", "log_level"))
#         print("Default Source Retries:", cfg_manager.get_data_source_config("default")["retry_attempts"])
#         print("Specific Source Retries:", cfg_manager.get_data_source_config("your_source_system_id")["retry_attempts"])
#         print("CDC Ingestion Config:", cfg_manager.get_ingestion_settings("cdc"))
#         print("Bronze GCS DLQ:", cfg_manager.get_gcs_settings("bronze_layer")["dead_letter_bucket"])
#         print("Silver BQ DLQ Table:", cfg_manager.get_bigquery_settings("silver_layer")["dlq_table"])
#         print("Default Alert Provider:", cfg_manager.get_alerting_config()["default_provider"])
#         print("Slack Critical Channel Secret:", cfg_manager.get_alert_provider_config("slack")["channels"]["data_alerts_critical"])
#         print("Policy Execution Log Table:", cfg_manager.get_tracking_and_logging_config()["policy_execution_log_table_id"])
#         print("BQ Error Patterns:", cfg_manager.get_error_patterns_config("bigquery"))

#     except ConfigManagerError as e:
#         print(f"ConfigManager Error: {e}")
#     except Exception as e:
#         print(f"An unexpected error occurred: {e}")

# To make it runnable from self-healing-architecture directory:
# python src/core/config_manager.py
# Ensure policy_config.yaml is in self-healing-architecture/config/
#
# If you are in self-healing-architecture/src/core and run python config_manager.py,
# the default path calculation in __new__ should correctly find config/policy_config.yaml
# relative to the src/core directory.
# The example __main__ block is more for testing from the project root.
#
# For default path in __new__:
# __file__ is src/core/config_manager.py
# os.path.abspath(__file__) gives absolute path to config_manager.py
# os.path.dirname(os.path.abspath(__file__)) gives src/core
# os.path.dirname(os.path.dirname(os.path.abspath(__file__))) gives src/
# os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))) gives self-healing-architecture/
# then os.path.join(base_dir, 'config', 'policy_config.yaml') is correct.
