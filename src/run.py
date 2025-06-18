import odc.geo.xr  # noqa: F401

from logging import INFO, Formatter, Logger, StreamHandler, getLogger
import boto3
import odc.geo.xr  # noqa: F401
import typer
from dea_tools.dask import create_local_dask_cluster

from dask.distributed import Client
from dep_tools.aws import write_stac_s3
from dep_tools.grids import PACIFIC_GRID_30
from dep_tools.namers import S3ItemPath
from dep_tools.stac_utils import StacCreator, set_stac_properties
from dep_tools.writers import (
    AwsDsCogWriter,
)
from fc import fractional_cover
from odc.stac import configure_s3_access
from typing_extensions import Annotated, Optional
import numpy as np
from odc.algo import geomedian_with_mads
import platform
import util

# NIU uv run src/run.py --tile-id 77,19 --year 2024 --version 0.0.1
# NRU uv run src/run.py --tile-id 50,41 --year 2024 --version 0.0.1
# FJI_Coral_Coast uv run src/run.py --tile-id 84,63 --year 2024 --version 0.0.1


# Main
def main(
    tile_id: Annotated[str, typer.Option()],
    year: Annotated[str, typer.Option()],
    version: Annotated[str, typer.Option()],
    # coastal_buffer: Optional[float] = 0.002,  # 0.002 - 0.005
    cloud_cover: Optional[str] = "50",
    cadence: Optional[str] = "3MS",  # quarterly
    output_bucket: Optional[str] = "dep-public-staging",
    dataset_id: str = "fc",
    base_product: str = "ls",
    memory_limit: str = "64GB",
    workers: int = 4,
    threads_per_worker: int = 32,
) -> None:
    log = get_logger(tile_id)
    log.info("Starting processing...")

    # dask and aws
    client = create_local_dask_cluster(return_client=True)

    """
    client = DaskClient(
        n_workers=workers,
        threads_per_worker=threads_per_worker,
        memory_limit=memory_limit,
    )
    """
    log.info(client)

    configure_s3_access(cloud_defaults=True, requester_pays=True)

    # get aoi
    grid = PACIFIC_GRID_30
    tile_index = tuple(int(i) for i in tile_id.split(","))
    aoi = grid.tile_geobox(tile_index)

    log.info(f"{tile_id} [{year}]")

    # generate product
    ds = util.get_s2_data(aoi, year, cloud_cover=cloud_cover)

    grouped = ds.resample(time=cadence)
    ds_median = grouped.median("time")  # median composite
    geomedian = grouped.map(geomedian_with_mads)  # geomedian

    if platform.platform() == "Darwin":
        ds_median = ds_median.compute()

    """
    if platform.platform() == "Linux":
        ds_median = geomedian.compute()
    """

    index = 0
    for t in ds_median.time.to_numpy():
        ds_source = ds_median.isel(time=index).squeeze()
        datetime = np.datetime_as_string(t, unit="D")
        dfc = fractional_cover.fractional_cover(ds_source)
        publish(
            dfc,
            ds_source,
            base_product,
            dataset_id,
            log,
            output_bucket,
            tile_id,
            version,
            datetime,
        )
        index = index + 1

    # write locally
    # log.info("Saving Outputs Locally...")
    # util.write_locally(ds, tile_id=tile_id, year=year)

    # finish
    log.info(f"{tile_id} - {year} Processed.")
    client.close()


def publish(
    ds,
    ds_source,
    base_product,
    dataset_id,
    log,
    output_bucket,
    tile_id,
    version,
    datetime,
):
    aws_client = boto3.client("s3")
    # itempath
    itempath = S3ItemPath(
        bucket=output_bucket,
        sensor=base_product,
        dataset_id=dataset_id,
        version=version,
        time=datetime,
        prefix="dep",
    )
    stac_document = itempath.stac_path(tile_id)
    # write externally
    output_data = set_stac_properties(ds_source, ds)
    writer = AwsDsCogWriter(
        itempath=itempath,
        overwrite=True,
        convert_to_int16=False,
        extra_attrs=dict(dep_version=version),
        write_multithreaded=True,
        client=aws_client,
    )
    paths = writer.write(output_data, tile_id) + [stac_document]
    stac_creator = StacCreator(
        itempath=itempath, remote=True, make_hrefs_https=True, with_raster=True
    )
    stac_item = stac_creator.process(output_data, tile_id)
    write_stac_s3(stac_item, stac_document, output_bucket)
    if paths is not None:
        log.info(f"Completed writing to {paths[-1]}")
    else:
        log.warning("No paths returned from writer")


# Logger
def get_logger(region_code: str) -> Logger:
    """Set Logger"""
    console = StreamHandler()
    time_format = "%Y-%m-%d %H:%M:%S"
    console.setFormatter(
        Formatter(
            fmt=f"%(asctime)s %(levelname)s ({region_code}):  %(message)s",
            datefmt=time_format,
        )
    )
    log = getLogger("FRACTIONAL_COVER")
    log.addHandler(console)
    log.setLevel(INFO)
    return log


# Run
if __name__ == "__main__":
    typer.run(main)
