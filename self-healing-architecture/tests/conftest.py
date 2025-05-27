import pytest
import os
from pathlib import Path

# Adjust path to import from src
import sys
# __file__ is self-healing-architecture/tests/conftest.py
# Path(__file__).resolve().parent is self-healing-architecture/tests
# Path(__file__).resolve().parent.parent is self-healing-architecture
project_root_path = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root_path / 'src'))

from core.config_manager import ConfigManager, ConfigManagerError

@pytest.fixture(scope="session")
def project_root() -> Path:
    """Returns the project root directory."""
    return project_root_path

@pytest.fixture(scope="session")
def test_config_path(project_root: Path) -> str:
    """
    Returns the path to the main policy_config.yaml.
    Tests will use this, assuming it's valid and present.
    If specific test configurations are needed, this fixture should be
    modified or new ones created (e.g., to point to a temporary test config file).
    """
    config_file = project_root / 'config' / 'policy_config.yaml'
    return str(config_file)

@pytest.fixture(scope="session")
def config_manager_instance(test_config_path: str) -> ConfigManager:
    """
    Provides a session-scoped ConfigManager instance.
    Skips tests if the config file is not found.
    """
    if not Path(test_config_path).exists():
        pytest.skip(f"Main config file not found at {test_config_path}, skipping tests that require ConfigManager.")
    
    try:
        # Ensure ConfigManager is treated as a singleton for the test session
        # by directly calling its constructor which implements the singleton logic.
        cm = ConfigManager(config_path=test_config_path)
        return cm
    except ConfigManagerError as e:
        pytest.fail(f"Failed to initialize ConfigManager for tests: {e}")

# Example of a fixture for a more specific part of the config, if needed frequently
@pytest.fixture(scope="session")
def global_settings(config_manager_instance: ConfigManager) -> dict:
    """Provides the global_settings section from the config."""
    try:
        return config_manager_instance.get_global_settings()
    except ConfigManagerError as e:
        pytest.skip(f"Could not load global_settings from config: {e}")
        return {} # Should be skipped by the above, but as a fallback for type hinting

# You can add more fixtures here as needed, for example, for mock objects
# that are used across multiple tests.
# Example:
# from unittest.mock import MagicMock
# from core.interfaces import AbstractLogger
#
# @pytest.fixture
# def mock_logger() -> MagicMock:
#     return MagicMock(spec=AbstractLogger)
