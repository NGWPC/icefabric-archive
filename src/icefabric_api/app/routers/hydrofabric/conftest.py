from pathlib import Path

import pytest

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


@pytest.fixture(params=sample_gauges)
def gauge_hf_uri(request) -> str:
    """Returns individual gauge identifiers for parameterized testing"""
    return request.param


@pytest.fixture
def testing_dir() -> Path:
    """Returns the testing data dir"""
    return Path(__file__).parents[4] / "icefabric_tools/tests/data/"
