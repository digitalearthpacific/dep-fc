import json
import sys
from itertools import product
from typing import Annotated, Optional

import boto3
import typer
from dep_tools.aws import object_exists
from dep_tools.grids import get_tiles
from dep_tools.namers import S3ItemPath

# python list.py --years 2024 --version 0.0.1 --regions FJI


def main(
    years: Annotated[str, typer.Option()],
    version: Annotated[str, typer.Option()],
    regions: Optional[str] = "ALL",
    base_product: str = "ls",
    output_bucket: Optional[str] = "dep-public-staging",
    # output_prefix: Optional[str] = None,
    overwrite: Annotated[bool, typer.Option()] = False,
) -> None:
    country_codes = None if regions.upper() == "ALL" else regions.split(",")

    tile_buffer_kms = 0
    tiles = get_tiles(
        country_codes=country_codes, buffer_distance=tile_buffer_kms * 1000
    )

    years = years.split("-")
    if len(years) == 2:
        years = range(int(years[0]), int(years[1]) + 1)
    elif len(years) > 2:
        ValueError(f"{years} is not a valid value for --years")

    tasks = [
        {
            "tile-id": ",".join([str(i) for i in tile[0]]),
            "year": year,
            "version": version,
        }
        for tile, year in product(list(tiles), years)
    ]

    json.dump(tasks, sys.stdout)


if __name__ == "__main__":
    typer.run(main)
