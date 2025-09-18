import os

BUCKET = os.environ.get("FC_BUCKET", "dep-public-staging")
DATASET_ID = "fc"
SENSOR = "ls"
OUTPUT_COLLECTION_ROOT = os.environ.get(
    "OUTPUT_COLLECTION_ROOT", "https://stac.staging.digitalearthpacific.org"
)
VERSION = "test"
NODATA = 255
