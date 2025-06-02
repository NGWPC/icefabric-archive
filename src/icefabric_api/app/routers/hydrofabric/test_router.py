from pathlib import Path

import geopandas as gpd
import pytest


@pytest.mark.integration
def test_subset_hl_uri(client, gauge_hf_uri: str, testing_dir: Path, tmp_path: Path):
    """Test: GET /streamflow_observations/usgs/csv"""
    response = client.get(
        f"/v1/hydrofabric/{gauge_hf_uri}/gpkg",
    )

    assert response.status_code == 200, "Incorrect response"

    if response.status_code == 200:
        temp_gpkg = tmp_path / "structure_test.gpkg"
        temp_gpkg.write_bytes(response.content)

        output_gdf = gpd.read_file(temp_gpkg, layer="network")
        correct_gdf = gpd.read_file(testing_dir / f"gages-{gauge_hf_uri}.gpkg", layer="network")
        assert output_gdf.equals(correct_gdf)

        output_gdf = gpd.read_file(temp_gpkg, layer="divides")
        correct_gdf = gpd.read_file(testing_dir / f"gages-{gauge_hf_uri}.gpkg", layer="divides")
        assert output_gdf.equals(correct_gdf)

        output_gdf = gpd.read_file(temp_gpkg, layer="flowpaths")
        correct_gdf = gpd.read_file(testing_dir / f"gages-{gauge_hf_uri}.gpkg", layer="flowpaths")
        assert output_gdf.equals(correct_gdf)

        output_gdf = gpd.read_file(temp_gpkg, layer="nexus")
        correct_gdf = gpd.read_file(testing_dir / f"gages-{gauge_hf_uri}.gpkg", layer="nexus")
        assert output_gdf.equals(correct_gdf)
