import pytest


@pytest.mark.integration
def test_sources_endpoint(client):
    """Test: GET /observations/sources"""
    response = client.get("/v1/observations/sources")
    assert response.status_code == 200

    data = response.json()
    assert "available_sources" in data
    assert "total_sources" in data

    sources = data["available_sources"]
    usgs_source = next((s for s in sources if s["name"] == "usgs"), None)
    assert usgs_source is not None
    assert usgs_source["description"] == "USGS stream gauge hourly data"
    assert usgs_source["units"] == "cms"


@pytest.mark.integration
def test_available_identifiers_example(client):
    """Test: GET /observations/usgs/available"""
    response = client.get("/v1/observations/usgs/available")

    assert response.status_code in [200, 500]  # Will return if the PyIceberg DB exists in the /tmp/ dir

    if response.status_code == 200:
        data = response.json()
        assert "data_source" in data
        assert "identifiers" in data
        assert "total_identifiers" in data
        assert data["data_source"] == "usgs"


@pytest.mark.integration
def test_available_identifiers_with_limit_example(client):
    """Test: GET /observations/usgs/available?limit=50"""
    response = client.get("/v1/observations/usgs/available?limit=50")

    assert response.status_code in [200, 500]

    if response.status_code == 200:
        data = response.json()
        assert data["showing"] <= 50
