import re
import subprocess
import ee
from google.cloud.exceptions import NotFound
from pathlib import Path
from typing import Optional, Union


class ConversionException(Exception):
    pass


class DataTransferMixin(object):
    DEFAULT_BUCKET = "scl-pipeline"

    def _parse_task_id(self, output: Union[str, bytes]) -> Optional[str]:
        text = output.decode("utf-8") if isinstance(output, bytes) is True else output
        task_id_regex = re.compile(r"(?<=ID: ).*", flags=re.IGNORECASE)
        try:
            matches = task_id_regex.search(text)
            if matches is None:
                return None
            return matches[0]
        except TypeError:
            return None

    def upload_to_cloudstorage(
        self,
        local_path: Union[str, Path],
        blob_path: Union[str, Path],
        bucketname: Optional[str] = None
    ) -> str:
        bucketname = bucketname or self.DEFAULT_BUCKET
        bucket = self.gcsclient.get_bucket(bucketname)
        blob = bucket.blob(str(blob_path))
        blob.upload_from_filename(str(local_path), timeout=3600)
        return f"gs://{bucketname}/{blob_path}"

    def download_from_cloudstorage(
        self, blob_path: Union[str, Path], local_path: Union[str, Path], bucketname: Optional[str] = None
    ) -> str:
        bucketname = bucketname or self.DEFAULT_BUCKET
        bucket = self.gcsclient.get_bucket(bucketname)
        blob = bucket.blob(str(blob_path))
        blob.download_to_filename(str(local_path))
        return local_path

    def remove_from_cloudstorage(
        self, blob_path: str, bucketname: Optional[str] = None
    ):
        bucketname = bucketname or self.DEFAULT_BUCKET
        bucket = self.gcsclient.get_bucket(bucketname)
        try:  # don't fail entire task if this fails
            bucket.delete_blob(blob_path)
        except NotFound:
            print(f"{blob_path} not found")

    def storage2image(
        self, blob_uri: str, image_asset_id: str, nodataval: Optional[int] = None
    ) -> str:
        try:
            cmd_args = [
                "/usr/local/bin/earthengine",
                f"--service_account_file={self.google_creds_path}",
                "upload image",
                f"--asset_id={image_asset_id}",
            ]
            if nodataval:
                cmd_args.append(f"--nodata_value={nodataval}")
            cmd = cmd_args + [blob_uri]
            output = subprocess.check_output(
                " ".join(cmd), stderr=subprocess.STDOUT, shell=True
            )
            task_id = self._parse_task_id(output)
            if task_id is None:
                raise TypeError("task_id is None")
            self.ee_tasks[task_id] = {}
            return task_id
        except subprocess.CalledProcessError as err:
            raise ConversionException(err.stdout)

    def storage2table(
        self, blob_uri: str, table_asset_id: str, geometry_column: Optional[str] = None
    ) -> str:
        try:
            cmd_args = [
                "/usr/local/bin/earthengine",
                f"--service_account_file={self.google_creds_path}",
                "upload table",
                f"--asset_id={table_asset_id}",
            ]
            if geometry_column:
                cmd_args.append(f"--primary_geometry_column {geometry_column}")

            cmd = cmd_args + [blob_uri]
            output = subprocess.check_output(
                " ".join(cmd), stderr=subprocess.STDOUT, shell=True
            )
            task_id = self._parse_task_id(output)
            if task_id is None:
                raise TypeError("task_id is None")
            self.ee_tasks[task_id] = {}
            return task_id
        except subprocess.CalledProcessError as err:
            raise ConversionException(err.stdout)

    def image2storage(self, image, bucket, asset_path, region=None):
        image = self.set_export_metadata(image)
        blob = asset_path.split("/")[-1]
        region = region or self.extent
        if isinstance(region, list):
            region = ee.Geometry.Polygon(region, proj=self.crs, geodesic=False)

        image_export = ee.batch.Export.image.toCloudStorage(
            image=image,
            description=blob,
            bucket=bucket,
            fileNamePrefix=asset_path,
            region=region,
            fileFormat="GeoTIFF",
            formatOptions={"cloudOptimized": True},
            scale=self.scale,
            crs=self.crs,
            maxPixels=self.ee_max_pixels,
        )
        image_export.start()
        self.ee_tasks[image_export.id] = {}
        return image_export.id

    def table2storage(
        self,
        featurecollection,
        bucket,
        asset_path,
        file_format="GeoJSON",
        selectors=None,
    ):
        featurecollection = self.set_export_metadata(
            featurecollection, ee_type=self.FEATURECOLLECTION
        )
        blob = asset_path.split("/")[-1]

        fc_export = ee.batch.Export.table.toCloudStorage(
            featurecollection,
            description=blob,
            bucket=bucket,
            fileNamePrefix=asset_path,
            fileFormat=file_format,
            selectors=selectors,
        )
        fc_export.start()
        self.ee_tasks[fc_export.id] = {}
        return fc_export.id
