from pathlib import Path
import traceback
import warnings

import boto3
from datacube.virtual import Measurement
from dep_tools.aws import object_exists, s3_dump
from dep_tools.loaders import OdcLoader
from dep_tools.namers import DailyItemPath
from dep_tools.task import AwsDsCogWriter, ItemStacTask
from dep_tools.writers import AwsStacWriter
from dep_tools.stac_utils import StacCreator
from fc.virtualproduct import FractionalCover
from pystac import Item

from config import BUCKET, DATASET_ID, VERSION


class FCProcessor(FractionalCover):
    #    @property
    #    def measurements(self):
    #        # Only way to override hardcoded measurements in fc.virtualproduct
    #        fc_measurements = [
    #            {"name": "pv", "dtype": "uint8", "nodata": 255, "units": "percent"},
    #            {"name": "npv", "dtype": "uint8", "nodata": 255, "units": "percent"},
    #            {"name": "bs", "dtype": "uint8", "nodata": 255, "units": "percent"},
    #            {"name": "ue", "dtype": "uint8", "nodata": 255, "units": ""},
    #        ]
    #        return [Measurement(**m) for m in fc_measurements]

    def process(self, data):
        data = (
            data.rename(dict(nir08="nir", swir16="swir1", swir22="swir2"))
            .assign_attrs(dict(crs=data.odc.crs))
            .compute()
        )

        output = super().compute(data)
        output.attrs["nodata"] = -1
        return output


def process_fc_scene(item: Item, tile_id, version=VERSION):
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
                stac_creator=StacCreator(itempath),
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
            # raise e
