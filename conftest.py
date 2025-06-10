import os
import sys
from pathlib import Path

# Hard coding a reference to the icefabric_api/ folder to test the API
project_root = Path(__file__).parents[1]
sys.path.insert(0, str(project_root / "src" / "icefabric_api"))

os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
os.environ["PYICEBERG_HOME"] = str(Path(__file__).parents[1])

import pandas as pd  # noqa: E402
import pytest  # noqa: E402
from app.main import app  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402
from pyiceberg.catalog import Catalog, load_catalog  # noqa: E402

sample_gauges = [
    "01010000",
    "02450825",
    "03173000",
    "04100500",
    "05473450",
    "06823500",
    "07060710",
    "08070000",
    "09253000",
    "10316500",
    "11456000",
    "12411000",
    "13337000",
    "14020000",
]

sample_hf_uri = [
    "gages-01010000",
    "gages-02450825",
    "gages-03173000",
    "gages-04100500",
    "gages-05473450",
    "gages-06823500",
    "gages-07060710",
    "gages-08070000",
    "gages-09253000",
    "gages-10316500",
    "gages-11456000",
    "gages-12411000",
    "gages-13337000",
    "gages-14020000",
]


@pytest.fixture(params=sample_gauges)
def gauge_ids(request) -> str:
    """Returns individual gauge identifiers for parameterized testing"""
    return request.param


@pytest.fixture(params=sample_hf_uri)
def gauge_hf_uri(request) -> str:
    """Returns individual gauge identifiers for parameterized testing"""
    return request.param


@pytest.fixture
def testing_dir() -> Path:
    """Returns the testing data dir"""
    return Path(__file__).parent / "tests/data/"


@pytest.fixture(scope="session")
def client():
    """Create a test client for the FastAPI app."""
    return TestClient(app)


@pytest.fixture
def local_usgs_streamflow_csv():
    """Returns a locally downloaded CSV file from a specific gauge and time"""
    file_path = (
        Path(__file__).parent / "tests/data/usgs_01010000_data_from_20211231_1400_to_20220101_1400.csv"
    )
    return pd.read_csv(file_path)


@pytest.fixture
def local_usgs_streamflow_parquet():
    """Returns a locally downloaded CSV file from a specific gauge and time"""
    file_path = (
        Path(__file__).parent / "tests/data/usgs_01010000_data_from_20211231_1400_to_20220101_1400.parquet"
    )
    return pd.read_parquet(file_path)


@pytest.fixture
def hydrofabric_catalog() -> Catalog:
    """Returns an iceberg catalog object for the hydrofabric"""
    return load_catalog("glue")


def pytest_addoption(parser):
    """Adds the --run-slow tag"""
    parser.addoption(
        "--run-slow",
        action="store_true",
        default=False,
        help="Run slow tests",
    )


def pytest_collection_modifyitems(config, items):
    """Allows pytest to read the --run-slow tag"""
    if not config.getoption("--run-slow"):
        skipper = pytest.mark.skip(reason="Only run when --run-slow is given")
        for item in items:
            if "slow" in item.keywords:
                item.add_marker(skipper)


def pytest_configure(config):
    """Configure pytest markers."""
    config.addinivalue_line("markers", "performance: marks tests as performance tests")
    config.addinivalue_line("markers", "integration: marks tests as integration tests")
    config.addinivalue_line("markers", "unit: marks tests as unit tests")
