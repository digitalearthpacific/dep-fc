"""Create fractional cover for a single Landsat scene."""

from pathlib import Path
import traceback
import warnings

import boto3
from dep_tools.aws import object_exists, s3_dump
from dep_tools.loaders import OdcLoader
from dep_tools.namers import DailyItemPath
from dep_tools.task import AwsDsCogWriter, ItemStacTask
from dep_tools.writers import AwsStacWriter
from dep_tools.stac_utils import StacCreator
from fc.virtualproduct import FractionalCover
from pystac import Item
from xarray import Dataset

from config import BUCKET, DATASET_ID, OUTPUT_COLLECTION_ROOT, VERSION


def process_fc_scene(item: Item, tile_id: tuple[int, ...], version=VERSION):
    """Create fractional cover for a single Landsat scene.

    The output data is saved to AWS S3.

    Args:
        item: A STAC Item for a single landsat scene.
        tile_id: The landsat tile id as (path, row)
        version: The output version.

    """
    itempath = DailyItemPath(
        bucket=BUCKET,
        sensor="ls",
        dataset_id=DATASET_ID,
        version=version,
        time=item.get_datetime(),
    )
    if not object_exists(bucket=BUCKET, key=itempath.stac_path(tile_id)):
        try:
            loader = OdcLoader(
                dtype="uint16",
                bands=["green", "red", "nir08", "swir16", "swir22"],
                chunks=dict(band=5, time=1, x=1024, y=1024),
                stac_cfg={
                    "landsat-c2l2-sr": {
                        "assets": {"*": {"nodata": 0}},
                    }
                },
                anchor="center",
            )
            return ItemStacTask(
                id=tile_id,
                item=item,
                loader=loader,
                processor=FCProcessor(c2_scaling=True),
                writer=AwsDsCogWriter(itempath),
                stac_creator=StacCreator(
                    itempath,
                    collection_url_root=OUTPUT_COLLECTION_ROOT,
                    with_raster=True,
                    with_eo=True,
                    set_geometry_from_input=True,
                ),
                stac_writer=AwsStacWriter(itempath),
            ).run()

        except Exception as e:
            log_path = Path(itempath.log_path()).with_suffix(".error.txt")
            warnings.warn(
                f"Error while processing item. Log file copied to s3://{BUCKET}/{log_path}"
            )
            boto3_client = boto3.client("s3")

            s3_dump(
                data=traceback.format_exc(),
                bucket=BUCKET,
                key=str(log_path),
                client=boto3_client,
            )


class FCProcessor(FractionalCover):
    """The Fractional Cover processor."""

    def process(self, data: Dataset) -> Dataset:
        """Create fractional cover from input landsat data.

        This is a slim wrapper around :func:`FractionalCover().compute`.

        Args:
            data: An :class:`xarray.Dataset` with Landsat data and variables
            "nir08", "swir16", and "swir22"

        Returns:
            A dataset with variables "bs", "pv", "npv", and "ue", representing
            percentages of bare soil, photosynthetic vegetation,
            non-photosynthetic vegetation and unmixing error. Output is a
            unsigned 8 bit integer with a nodata value of 255.

        """
        data = (
            data.rename(dict(nir08="nir", swir16="swir1", swir22="swir2"))
            .assign_attrs(dict(crs=data.odc.crs))
            .compute()
        )

        output = super().compute(data)
        # To convert from int8 with nodata = -1 to uint8 with nodata=255
        # we have to do it this way. I tried to alter the "Measurements"
        # var in the fc code but there are places where -1 is hardcoded
        # so it's not respected entirely
        # Converting to uint8
        # 1. Makes it easier to load alongside WOfS when calculating percentiles
        # 2. matches DE Africa data
        OLD_NODATA = -1
        NODATA = 255
        for var in output:
            output[var] = (
                output[var]
                .astype("int16")
                .where(output[var] != OLD_NODATA, NODATA)
                .astype("uint8")
            )
            output[var].attrs["nodata"] = NODATA
        return output
