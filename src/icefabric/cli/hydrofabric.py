"""Contains all click CLI code for the hydrofabric"""

from pathlib import Path

import click

from icefabric.builds import build_upstream_json
from icefabric.cli import get_catalog
from icefabric.hydrofabric.subset import subset as hfsubset
from icefabric.hydrofabric.subset_optimized import subset_v2 as hfsubset_v2
from icefabric.schemas.hydrofabric import HydrofabricDomains, IdType


@click.command()
@click.option(
    "--catalog",
    type=click.Choice(["glue", "sql"], case_sensitive=False),
    default="glue",
    help="The pyiceberg catalog type",
)
@click.option(
    "--identifier",
    type=str,
    required=True,
    help="The specific ID you are querying the system from",
)
@click.option(
    "--id-type",
    type=click.Choice([e.value for e in IdType], case_sensitive=False),
    required=True,
    help="The ID type you are querying",
)
@click.option(
    "--domain",
    type=click.Choice([e.value for e in HydrofabricDomains], case_sensitive=False),
    required=True,
    help="The domain you are querying",
)
@click.option(
    "--layers",
    type=str,
    multiple=True,
    help="The layers to include in the geopackage. Will always include ['divides', 'flowpaths', 'network', 'nexus']",
)
@click.option(
    "--output-file",
    "-o",
    type=click.Path(path_type=Path),
    default=Path.cwd() / "subset_v2.gpkg",
    help="Output file. Defaults to ${CWD}/subset.gpkg",
)
def subset_v2(
    catalog: str,
    identifier: str,
    id_type: str,
    domain: str,
    layers: tuple[str],
    output_file: Path,
):
    """Subsets the hydrofabric based on a unique identifier

    Parameters
    ----------
    catalog : str
        The pyiceberg catalog type (glue or sql)
    identifier : str
        The specific ID you are querying the system from. Ex: gages-01010000
    id_type : str
        The ID type you are querying
    domain : str
        The domain you are querying
    layers : tuple[str]
        The layers to include in the subset
    output_file : Path
        Output path for the zip file
    """
    id_type_enum = IdType(id_type)
    domain_enum = HydrofabricDomains(domain)

    layers_list = list(layers) if layers else None

    hfsubset_v2(
        catalog=get_catalog(catalog),
        identifier=identifier,
        id_type=id_type_enum,
        layers=layers_list,
        output_file=output_file,
        domain=domain_enum,
    )
    click.echo(f"Hydrofabric file created successfully in the following folder: {output_file}")


@click.command()
@click.option(
    "--catalog",
    type=click.Choice(["glue", "sql"], case_sensitive=False),
    default="glue",
    help="The pyiceberg catalog type",
)
@click.option(
    "--identifier",
    type=str,
    required=True,
    help="The specific ID you are querying the system from",
)
@click.option(
    "--id-type",
    type=click.Choice([e.value for e in IdType], case_sensitive=False),
    required=True,
    help="The ID type you are querying",
)
@click.option(
    "--domain",
    type=click.Choice([e.value for e in HydrofabricDomains], case_sensitive=False),
    required=True,
    help="The domain you are querying",
)
@click.option(
    "--layers",
    type=str,
    multiple=True,
    help="The layers to include in the geopackage. Will always include ['divides', 'flowpaths', 'network', 'nexus']",
)
@click.option(
    "--output-file",
    "-o",
    type=click.Path(path_type=Path),
    default=Path.cwd() / "subset.gpkg",
    help="Output file. Defaults to ${CWD}/subset.gpkg",
)
def subset(
    catalog: str,
    identifier: str,
    id_type: str,
    domain: str,
    layers: tuple[str],
    output_file: Path,
):
    """Subsets the hydrofabric based on a unique identifier

    Parameters
    ----------
    catalog : str
        The pyiceberg catalog type (glue or sql)
    identifier : str
        The specific ID you are querying the system from. Ex: gages-01010000
    id_type : str
        The ID type you are querying
    domain : str
        The domain you are querying
    layers : tuple[str]
        The layers to include in the subset
    output_file : Path
        Output path for the zip file
    """
    id_type_enum = IdType(id_type)
    domain_enum = HydrofabricDomains(domain)

    layers_list = list(layers) if layers else None

    hfsubset(
        catalog=get_catalog(catalog),
        identifier=identifier,
        id_type=id_type_enum,
        layers=layers_list,
        output_file=output_file,
        domain=domain_enum,
    )
    click.echo(f"Hydrofabric file created successfully in the following folder: {output_file}")


@click.command()
@click.option(
    "--catalog",
    type=click.Choice(["glue", "sql"], case_sensitive=False),
    default="glue",
    help="The pyiceberg catalog type",
)
@click.option(
    "--domain",
    type=click.Choice([e.value for e in HydrofabricDomains], case_sensitive=False),
    required=True,
    help="The domain you are querying",
)
@click.option(
    "--output-file",
    "-o",
    type=click.Path(path_type=Path),
    default=Path.cwd() / "subset.gpkg",
    help="Output file. Defaults to ${CWD}/subset.gpkg",
)
def build_upstream_json_file(
    catalog: str,
    domain: str,
    output_file: Path,
):
    """Creates a JSON file which documents the upstream connections from a particular basin

    Parameters
    ----------
    catalog : str
        The pyiceberg catalog
    domain : str
        the hydrofabric domain
    output_file : Path
        Where the json file should be saved
    """
    build_upstream_json(catalog=get_catalog(catalog), namespace=domain, output_path=output_file)
    click.echo(f"Upstream json file created successfully in the following folder: {output_file}")
