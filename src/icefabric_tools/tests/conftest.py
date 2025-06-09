import os
from pathlib import Path

os.environ["PYICEBERG_HOME"] = str(Path(__file__).parents[3])
print(f"PYICEBERG_HOME set to: {os.environ['PYICEBERG_HOME']}")

import pytest  # noqa: E402
from pyiceberg.catalog import Catalog, load_catalog  # noqa: E402

sample_gauges = [
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


@pytest.fixture
def hydrofabric_catalog() -> Catalog:
    """Returns an iceberg catalog object for the hydrofabric"""
    # try:
    return load_catalog("glue")
    # except Exception as e:
    #     raise type(e)(
    #         f"Cannot find warehouse @ {warehouse_path}. Please make sure your AWS credentials are accurate"
    #     )  # type: ignore


@pytest.fixture(params=sample_gauges)
def gauge_hf_uri(request) -> str:
    """Returns individual gauge identifiers for parameterized testing"""
    return request.param


@pytest.fixture
def testing_dir() -> Path:
    """Returns the testing data dir"""
    return Path(__file__).parent / "data/"


def pytest_configure(config):
    """Configure pytest markers."""
    config.addinivalue_line("markers", "performance: marks tests as performance tests")
    config.addinivalue_line("markers", "integration: marks tests as integration tests")
    config.addinivalue_line("markers", "unit: marks tests as unit tests")
