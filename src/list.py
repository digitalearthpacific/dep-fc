import json
import sys
from itertools import product
from typing import Annotated, Optional

import typer
from cloud_logger import CsvLogger, filter_by_log, S3Handler
from dep_tools.landsat_utils import landsat_grid
from dep_tools.namers import S3ItemPath
from dep_tools.parsers import bool_parser, datetime_parser

from grid import grid as dep_grid
from config import BUCKET, VERSION


def main(
    years: Annotated[list, typer.Option(parser=datetime_parser)],
    version: Annotated[str, typer.Option()] = VERSION,
    limit: Optional[str] = None,
    retry_errors: Annotated[str, typer.Option(parser=bool_parser)] = "True",
    grid: Optional[str] = "dep",
    dataset_id: Optional[str] = "dep_fc_summary_annual", 
    overwrite_existing_log: Annotated[str, typer.Option(parser=bool_parser)] = "False",
) -> None:
    this_grid = dep_grid if grid == "dep" else landsat_grid()
    first_name = dict(dep="column", ls="path")
    second_name = dict(dep="row", ls="row")

    params = list()
    for year in years:
        itempath = S3ItemPath(
            bucket=BUCKET,
            sensor="ls",
            dataset_id=dataset_id,
            version=version,
            time=str(year).replace("/", "_"),
        )

        logger = CsvLogger(
            name=dataset_id,
            path=f"{itempath.bucket}/{itempath.log_path()}",
            overwrite=overwrite_existing_log,
            header="time|index|status|paths|comment\n",
            cloud_handler=S3Handler,
        )

        grid_subset = filter_by_log(this_grid, logger.parse_log(), retry_errors)

        these_params = [
            {
                # we could take fromthe grid if we name the dep_grid multiindex
                first_name[grid]: region[0][0],
                second_name[grid]: region[0][1],
                "year": region[1],
            }
            for region in product(grid_subset.index, [year])
        ]
        params += these_params

    if limit is not None:
        params = params[0 : int(limit)]

    json.dump(params, sys.stdout)


if __name__ == "__main__":
    typer.run(main)
