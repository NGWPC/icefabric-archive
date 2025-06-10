import os
from pathlib import Path

os.environ["PYICEBERG_HOME"] = str(Path(__file__).parents[3])
print(f"PYICEBERG_HOME set to: {os.environ['PYICEBERG_HOME']}")

import pytest  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from app.main import app  # noqa: E402


@pytest.fixture(scope="session")
def client():
    """Create a test client for the FastAPI app."""
    return TestClient(app)


@pytest.fixture(scope="function")
def fresh_client():
    """Create a fresh test client for each test function."""
    return TestClient(app)


def pytest_configure(config):
    """Configure pytest markers."""
    config.addinivalue_line("markers", "performance: marks tests as performance tests")
    config.addinivalue_line("markers", "integration: marks tests as integration tests")
    config.addinivalue_line("markers", "unit: marks tests as unit tests")
