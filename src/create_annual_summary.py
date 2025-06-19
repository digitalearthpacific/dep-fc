from typing_extensions import Annotated

import boto3
from distributed import Client
from odc.stats.plugins.fc_percentiles import StatsFCP
from typer import Option, run
from xarray import Dataset

from cloud_logger import CsvLogger, S3Handler
from dep_tools.loaders import OdcLoader
from dep_tools.namers import S3ItemPath
from dep_tools.processors import XrPostProcessor
from dep_tools.searchers import PystacSearcher
from dep_tools.stac_utils import StacCreator
from dep_tools.task import AwsStacTask as Task

from config import BUCKET, OUTPUT_COLLECTION_ROOT
from grid import grid


class FCPercentiles(StatsFCP):
    def process(self, input: Dataset):
        output = super().native_transform(input)
        output = super().fuser(output)
        return super().reduce(output)


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
        collections=["dep_ls_fc"],
    )

    stacloader = OdcLoader(
        dtype="uint8",
        chunks=dict(x=4096, y=4096),
        fail_on_error=False,
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
