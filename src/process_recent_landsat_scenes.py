from datetime import datetime, timedelta, timezone
import json
import sys
from typing_extensions import Annotated
import warnings

from distributed import Client
from odc.stac import configure_s3_access
import pystac_client
from typer import Option, Typer

from cloud_logger import CsvLogger
from dep_tools.exceptions import EmptyCollectionError
from dep_tools.landsat_utils import landsat_grid
from dep_tools.namers import S3ItemPath
from dep_tools.stac_utils import use_alternate_s3_href
from dep_tools.utils import search_across_180

from config import BUCKET, DATASET_ID, VERSION
from process_fc_scene import process_fc_scene

app = Typer()


@app.command()
def list():
    """List all Landsat tiles."""
    json.dump(
        [{"path": pr[0], "row": pr[1]} for pr in landsat_grid().index.tolist()],
        sys.stdout,
    )


@app.command()
def process_tile(
    path: Annotated[str, Option(parser=int)],
    row: Annotated[str, Option(parser=int)],
    number_of_days: Annotated[int, Option(parser=int)],
    version: Annotated[str, Option()] = VERSION,
) -> None:
    configure_s3_access(cloud_defaults=True, requester_pays=True)
    id = (path, row)
    cell = landsat_grid().loc[[id]]

    client = pystac_client.Client.open(
        "https://landsatlook.usgs.gov/stac-server",
        modifier=use_alternate_s3_href,
    )

    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(days=number_of_days)

    time = f"{start_time.isoformat()}/{end_time.isoformat()}"

    itempath = S3ItemPath(
        bucket=BUCKET,
        sensor="ls",
        dataset_id=DATASET_ID,
        version=version,
        time=time,
    )

    logger = CsvLogger(
        name=DATASET_ID,
        path=f"{itempath.bucket}/{itempath.log_path()}",
        overwrite=False,
        header="time|index|status|paths|comment\n",
    )

    try:
        items = search_across_180(
            cell,
            client=client,
            query={
                "landsat:wrs_row": dict(eq=str(row).zfill(3)),
                "landsat:wrs_path": dict(eq=str(path).zfill(3)),
            },
            datetime=time,
            collections=["landsat-c2l2-sr"],
        )
    except EmptyCollectionError as e:
        logger.error([id, "no items found", e])
        warnings.warn("No stac items found, exiting")
        # Don't reraise, it just means there's no data
        return None

    paths = [process_fc_scene(item, version=version) for item in items]

    logger.info([id, "complete", paths])


if __name__ == "__main__":
    with Client():
        app()
