from icefabric.schemas.modules import AlbedoValues


def test_albedo_endpoint(remote_client):
    """Test: GET /v2/modules/topoflow_glacier/albedo - all valid arguments"""
    for landcover, albedo in AlbedoValues.__members__.items():
        response = remote_client.get(f"/v1/modules/topoflow_glacier/albedo?landcover={landcover}")
        assert response.status_code == 200
        assert response.text == str(albedo.value)


def test_albedo_endpoint__422(remote_client):
    """Test: GET /v2/modules/topoflow_glacier/albedo - fails validator"""
    response = remote_client.get("/v1/modules/topoflow_glacier/albedo?landcover=nope")
    assert response.status_code == 422
