from collections import defaultdict
from typing_extensions import Annotated

import boto3
from distributed import Client
import numpy as np
from odc.algo import keep_good_only, mask_cleanup
from odc.stats.plugins.fc_percentiles import StatsFCP
from typer import Option, run
from xarray import Dataset, merge

from cloud_logger import CsvLogger, S3Handler
from dep_tools.loaders import OdcLoader, StacLoader
from dep_tools.namers import S3ItemPath
from dep_tools.processors import XrPostProcessor
from dep_tools.searchers import PystacSearcher
from dep_tools.stac_utils import StacCreator
from dep_tools.task import AwsStacTask as Task
from dep_tools.utils import mask_to_gadm
from dep_tools.writers import AwsDsCogWriter

from config import BUCKET, OUTPUT_COLLECTION_ROOT, NODATA
from grid import grid


class MultiCollectionLoader(StacLoader):
    """Allows loading of data from multiple collections into the same dataset.
    The sets of items in each collection should have the same dimensions (x, y & time),
    but can have different variables."""
    def __init__(self, fill_value=NODATA, **kwargs):
        self._fill_value = fill_value
        self._kwargs = kwargs

    def load(self, items, areas) -> Dataset:
        # split collections
        collections = defaultdict(list)
        for item in items:
            collections[item.collection_id].append(item)

        # load items in each and merge
        return merge(
            [
                OdcLoader(**self._kwargs).load(items, areas)
                for items in collections.values()
            ],
            fill_value=self._fill_value,
        )




class FCPercentiles(StatsFCP):
    """A processor to create annual summaries from individual Fractional Cover layers.
    """
    
    send_area_to_processor = True
    # These are those used for DE Africa
    BAD_BITS_MASK = {"cloud": 1 << 6, "cloud_shadow": 1 << 5}

    def native_transform(self, xx):
        """
        Loads data in its native projection. It performs the following:

        1. Load all fc and WOfS bands
        2. Set the high terrain slope flag to 0
        3. Set all pixels that are not clear and dry to NODATA
        4. Calculate the clear wet pixels
        5. Drop the WOfS band
        """

        # not mask against bit 4: terrain high slope
        # This is why a subclass was needed: without casting the bitmask to
        # uint8, the inversion (~) created a negative number. It must
        # compare to the left operand and has trouble with a dask array?
        mask = xx["water"] & ~np.uint8(1 << 4)
        # Pick out the dry and wet pixels
        valid = mask == 0
        wet = mask == 128

        # dilate both 'valid' and 'water'
        for key, val in self.BAD_BITS_MASK.items():
            if self.cloud_filters.get(key) is not None:
                raw_mask = (xx["water"] & val) > 0
                raw_mask = mask_cleanup(
                    raw_mask, mask_filters=self.cloud_filters.get(key)
                )
                valid &= ~raw_mask
                wet &= ~raw_mask

        xx = xx.drop_vars(["water"])

        # Pick out the pixels that have an unmixing error of less than the threshold
        if self.ue_threshold is not None:
            # No QA
            valid &= xx.ue < self.ue_threshold
        xx = xx.drop_vars(["ue"])

        # If there's a sum limit or clip range, implement these
        if self.max_sum_limit is not None or self.clip_range is not None:
            sum_bands = 0
            for band in xx.data_vars.keys():
                attributes = xx[band].attrs
                mask = xx[band] == NODATA
                band_data = keep_good_only(xx[band], ~mask, nodata=0)

                if self.max_sum_limit is not None:
                    sum_bands = sum_bands + band_data

                if self.clip_range is not None:
                    # No QA
                    clipped = np.clip(xx[band], self.clip_range[0], self.clip_range[1])
                    # Set masked values back to NODATA
                    xx[band] = clipped.where(~mask, NODATA)
                    xx[band].attrs = attributes

            if self.max_sum_limit is not None:
                valid &= sum_bands < self.max_sum_limit

        xx = keep_good_only(xx, valid, nodata=NODATA)
        xx["wet"] = wet
        xx["valid"] = valid

        return xx

    def process(self, input_ds: Dataset, area=None):
        transformed = self.native_transform(input_ds)
        fused = transformed.groupby("time").apply(self.fuser)
        output = self.reduce(fused)
        if area is not None:
            output = mask_to_gadm(output, area)

        return output

def main(
    row: Annotated[str, Option(parser=int)],
    column: Annotated[str, Option(parser=int)],
    datetime: Annotated[str, Option()],
    version: Annotated[str, Option()],
    dataset_id: str = "fc_summary_annual",
) -> None:
    boto3.setup_default_session()
    id = (column, row)
    cell = grid.loc[id].geobox.tolist()[0]

    itempath = S3ItemPath(
        bucket=BUCKET,
        sensor="ls",
        dataset_id=dataset_id,
        version=version,
        time=datetime,
    )

    # fc and wofl are needed for all scenes
    searcher = PystacSearcher(
        catalog=f"https://stac.staging.digitalearthpacific.io",
        datetime=datetime,
        collections=["dep_ls_fc", "dep_ls_wofl"],
    )

    stacloader = MultiCollectionLoader(
        dtype="uint8",
        chunks=dict(x=1600, y=1600),
        fail_on_error=False,
    )

    processor = FCPercentiles(
        cloud_filters=dict(cloud=[("dilation", 6)], cloud_shadow=[("dilation", 6)]),
    )

    post_processor = XrPostProcessor(
        convert_to_int16=False,
        extra_attrs=dict(dep_version=version),
    )

    # load before write here since many of the derivatives share the same
    # input data and this will help minimize the number of reads from cloud storage
    writer = AwsDsCogWriter(itempath=itempath, load_before_write=True)

    logger = CsvLogger(
        name=dataset_id,
        path=f"{itempath.bucket}/{itempath.log_path()}",
        overwrite=False,
        header="time|index|status|paths|comment\n",
        cloud_handler=S3Handler,
    )

    try:
        paths = Task(
            itempath=itempath,
            id=id,
            area=cell,
            searcher=searcher,
            loader=stacloader,
            processor=processor,
            post_processor=post_processor,
            writer=writer,
            logger=logger,
            stac_creator=StacCreator(
                itempath=itempath,
                collection_url_root=OUTPUT_COLLECTION_ROOT,
                with_raster=True,
                with_eo=True,
            ),
        ).run()
    except Exception as e:
        logger.error([id, "error", e])
        raise e

    logger.info([id, "complete", paths])


if __name__ == "__main__":
    with Client():
        run(main)
