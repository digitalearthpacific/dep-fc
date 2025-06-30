from collections import defaultdict
from typing_extensions import Annotated

import boto3
from distributed import Client
from odc.stats.plugins.fc_percentiles import StatsFCP
from typer import Option, run
from xarray import Dataset, merge

from cloud_logger import CsvLogger, S3Handler
from dep_tools.loaders import OdcLoader
from dep_tools.namers import S3ItemPath
from dep_tools.processors import XrPostProcessor
from dep_tools.searchers import PystacSearcher
from dep_tools.stac_utils import StacCreator
from dep_tools.task import AwsStacTask as Task

from config import BUCKET, OUTPUT_COLLECTION_ROOT
from grid import grid


class MultiCollectionLoader:
    def __init__(self, **kwargs):
        self._kwargs = kwargs

    def load(self, items, areas) -> Dataset:
        # split collections
        collections = defaultdict(list)
        for item in items:
            collections[item.collection_id].append(item)

        # load items in each
        output = []
        for items_in_collection in collections.values():
            loader = OdcLoader(**self._kwargs)
            output.append(loader.load(items_in_collection, areas))
        return merge(output, fill_value=255)


NODATA = 255


class FCPercentiles:
    send_area_to_processor = False
    BAD_BITS_MASK = {"cloud": 1 << 6, "cloud_shadow": 1 << 5, "terrain_shadow": 1 << 3}
    cloud_filters = {}
    ue_threshold = None
    max_sum_limit = None
    clip_range = None
    count_valid = None

    def native_transform(self, xx):
        """
        Loads data in its native projection. It performs the following:

        1. Load all fc and WOfS bands
        2. Set the high terrain slope flag to 0
        3. Set all pixels that are not clear and dry to NODATA
        4. Calculate the clear wet pixels
        5. Drop the WOfS band
        """
        from odc.algo import keep_good_only

        # not mask against bit 4: terrain high slope
        # Pick out the dry and wet pixels
        valid = (xx["water"] & (1 << 4)) == 0
        wet = (xx["water"] & (1 << 7)) == 128
        breakpoint()

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
                band_data = eep_good_only(xx[band], ~mask, nodata=0)

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

    def process(self, fc: Dataset):
        summarizer = StatsFCP()
        prepped = fc.groupby("time").apply(lambda ds: self.native_transform(ds))
        output = summarizer.fuser(prepped)
        return summarizer.reduce(output)


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

    searcher = PystacSearcher(
        catalog=f"https://stac.staging.digitalearthpacific.io",
        datetime=datetime,
        collections=["dep_ls_fc", "dep_ls_wofl"],
    )

    stacloader = MultiCollectionLoader(
        dtype="uint8",
        chunks=dict(x=4096, y=4096),
        fail_on_error=False,
        #        stac_cfg=dict(
        #            dep_ls_fc=dict(assets=dict(bs={}, pv={},npv={}, ue={})),
        #            dep_ls_wofl=dict(assets=dict(water={}))
        #            ),
    )

    processor = FCPercentiles()

    post_processor = XrPostProcessor(
        convert_to_int16=False,
        extra_attrs=dict(dep_version=version),
    )

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
