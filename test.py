import sys
sys.path.append("/usr/bin")
import argparse
import ee
import gdal_merge as gm
from google.cloud.storage import Client
from pathlib import Path
from task_base import HIITask, EETaskError


BUCKET = "hii-export"
NODATA = -9999


class Test(HIITask):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # TODO: handle loading cloud creds as in classification/osm
        self.gclient = Client()

    def download_from_cloudstorage(self, blob_path: str, local_path: str) -> str:
        bucket = self.gclient.get_bucket(BUCKET)
        blob = bucket.blob(blob_path)
        blob.download_to_filename(local_path)
        return local_path

    def calc(self):
        for i in range(2001, 2021):
            hii = ee.Image(f"projects/HII/v1/hii/hii_{i}-01-01")
            if i == 2001:
                prefix = f"hii/{i}"
                self.export_image_cloudstorage(hii.toInt().unmask(NODATA), BUCKET, f"{prefix}/hii")
                self.wait()
                Path(f"/{prefix}").mkdir(parents=True, exist_ok=True)

                tiffs = []
                for blob in self.gclient.list_blobs(BUCKET, prefix=prefix):
                    blob_path = blob.name
                    local_path = f"/{prefix}/{blob_path.split('/')[-1]}"
                    self.download_from_cloudstorage(blob_path, local_path)
                    tiffs.append(local_path)

                print(tiffs)
                gm.main(["", "-n", f"{NODATA}", "-a_nodata", f"{NODATA}", "-o", f"/{prefix}/hii_{i}.tif"] + tiffs)

    def check_inputs(self):
        super().check_inputs()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--taskdate")
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="overwrite existing outputs instead of incrementing",
    )
    options = parser.parse_args()
    test_task = Test(**vars(options))
    try:
        test_task.run()
    except EETaskError as e:
        statuses = list(e.ee_statuses.values())
        if statuses:
            message = statuses[0]["error_message"]
            if message.lower() == "table is empty.":
                test_task.status = test_task.RUNNING
            else:
                raise e
