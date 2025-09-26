import os

BUCKET = os.environ.get("FC_BUCKET", "dep-public-data")
DATASET_ID = "fc"
SENSOR = "ls"
OUTPUT_COLLECTION_ROOT = os.environ.get(
    "OUTPUT_COLLECTION_ROOT", "https://stac.digitalearthpacific.org"
)
VERSION = "0.1.0"
NODATA = 255
