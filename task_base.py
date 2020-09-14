import os
import re
import json
import time
from datetime import datetime, timezone, timedelta
import ee


PROJECTS = "projects"


class Task(object):
    DATE_FORMAT = "%Y-%m-%d"
    ASSET_TIMESTAMP_PROPERTY = "system:time_start"

    # possible statuses associated with a Task instance -- which is different from ee task statuses
    NOTSTARTED = "not started"
    FAILED = "failed"
    RUNNING = "running"
    COMPLETE = "complete"
    status = NOTSTARTED
    inputs = {}
    wait_for_outputs = True

    def _set_inputs(self):
        for input_key, i in self.inputs.items():
            for key, val in i.items():
                if not isinstance(val, str) or not hasattr(self.__class__, val):
                    continue
                func = getattr(self.__class__, val)
                if callable(func):
                    self.inputs[input_key][key] = func(self)

    def __init__(self, *args, **kwargs):
        _taskdate = datetime.now(timezone.utc).date()
        try:
            _taskdate = datetime.strptime(
                kwargs.pop("taskdate", None), self.DATE_FORMAT
            ).date()
        except (TypeError, ValueError):
            pass
        self.taskdate = _taskdate

        self.wait_for_outputs = kwargs.pop("wait_for_outputs", True)

        self._set_inputs()

    def check_inputs(self):
        pass

    def calc(self):
        raise NotImplementedError("`calc` must be defined")

    def wait(self):
        pass

    def run(self, **kwargs):
        self.status = self.RUNNING
        self.check_inputs()
        if self.status != self.FAILED:
            try:
                self.calc()
                self.wait()
                self.status = self.COMPLETE
            except Exception as e:
                self.status = self.FAILED
                raise type(e)(str(e)) from e
        print("status: {}".format(self.status))


class GeoTask(Task):
    crs = "EPSG:4326"
    scale = 1000
    aoi = [
        [
            [
                [-180.0, -90.0],
                [180.0, -90.0],
                [180.0, 90.0],
                [-180.0, 90.0],
                [-180.0, -90.0],
            ]
        ]
    ]
    extent = aoi[0][0]

    def check_inputs(self):
        super().check_inputs()
        if (
            not hasattr(self, "aoi")
            or not hasattr(self, "scale")
            or not hasattr(self, "crs")
            or not self.aoi
            or not self.scale
            or not self.crs
        ):
            self.status = self.FAILED
            raise NotImplementedError("Undefined input: aoi, scale, or crs")


class EETask(GeoTask):
    service_account_key = os.environ.get("SERVICE_ACCOUNT_KEY")
    ee_project = None
    ee_rootdir = None
    ee_tasks = {}
    ee_max_pixels = 500000000000

    EEREADY = "READY"
    EE = "RUNNING"
    EECOMPLETED = "COMPLETED"
    EEFAILED = "FAILED"
    EECANCELLED = "CANCELLED"
    EEUNKNOWN = "UNKNOWN"
    EEFINISHED = [EECOMPLETED, EEFAILED, EECANCELLED, EEUNKNOWN]

    IMAGECOLLECTION = ee.data.ASSET_TYPE_IMAGE_COLL
    FEATURECOLLECTION = "FeatureCollection"
    IMAGE = "Image"
    EEDATATYPES = [IMAGECOLLECTION, FEATURECOLLECTION, IMAGE]

    def _create_ee_path(self, asset_path, image_collection=False):
        path_segments = asset_path.split("/")
        # first two segments are user/project root (e.g. projects/HII)
        path_length = len(path_segments)
        for i in range(2, path_length):
            path = "/".join(path_segments[: i + 1])
            if ee.data.getInfo(path):
                continue
            if i == path_length - 1 and image_collection:
                ee.data.createAsset({"type": "ImageCollection"}, opt_path=path)
            else:
                ee.data.createAsset({"type": "Folder"}, opt_path=path)

    def _canonicalize_assetid(self, assetid):
        if not ee.data.getInfo(assetid):
            return assetid
        i = 1
        new_assetid = "{}-{}".format(assetid, i)
        while ee.data.getInfo(new_assetid):
            i += 1
            new_assetid = "{}-{}".format(assetid, i)
        return new_assetid

    def _initialize_ee_client(self):
        if self.service_account_key is None:
            ee.Initialize("persistent")
        else:
            service_account_name = json.loads(self.service_account_key)["client_email"]
            credentials = ee.ServiceAccountCredentials(
                service_account_name, key_data=self.service_account_key
            )
            ee.Initialize(credentials)

    def _list_assets(self, eedir):
        assets = None
        # possible ee api bug requires prepending
        assetdir = f"projects/earthengine-legacy/assets/{eedir}"
        try:
            assets = ee.data.listAssets({"parent": assetdir})["assets"]
        except ee.ee_exception.EEException:
            print(f"Folder {eedir} does not exist or is not a folder.")
        return assets

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._initialize_ee_client()

        if not self.ee_project:
            self.status = self.FAILED
            raise NotImplementedError("`ee_project` must be defined")

        self.ee_rootdir = kwargs.pop("ee_rootdir", None)
        if not self.ee_rootdir:
            self.ee_rootdir = f"{PROJECTS}/{self.ee_project}"
        self.ee_rootdir = self.ee_rootdir.strip("/")
        self._create_ee_path(self.ee_rootdir)

    # def rm(self, asset_path):
    #     asset = ee.data.getInfo(asset_path)
    #     if not asset:
    #         return
    #
    #     if (
    #         asset["type"].capitalize() == ee.data.ASSET_TYPE_FOLDER
    #         or asset["type"].capitalize() == ee.data.ASSET_TYPE_IMAGE_COLL
    #     ):
    #         print(asset["type"].capitalize(), ee.data.ASSET_TYPE_FOLDER)
    #         for child_asset in self._list_assets(asset_path):
    #             self.rm(f"{asset_path}/{child_asset}")
    #     print(asset)
    #
    #     ee.data.deleteAsset(asset_path)
    #     return True
    #
    # def cp(self, source_id, destination_id, overwrite=True):
    #     destination_dir = "/".join(destination_id.split("/")[:-1])
    #     if not ee.data.getInfo(source_id) or not ee.data.getInfo(destination_dir):
    #         print(f"{source_id} or {destination_id} is not valid")
    #         return
    #     ee.data.copyAsset(source_id, destination_id, overwrite)
    #
    def set_aoi_from_ee(self, asset):
        try:  # seeting aoi from FeatureCollection
            ee_aoi = ee.Geometry.MultiPolygon(
                ee.FeatureCollection(asset).first().geometry().coordinates()
            )
            self.aoi = ee_aoi.getInfo()["coordinates"]
            self.extent = ee.Geometry.MultiPolygon(self.aoi).bounds()
        except ee.ee_exception.EEException:  # setting aoi from Image
            ee_aoi = ee.Image(asset)
            self.aoi = self.extent = ee_aoi.geometry().bounds()
        except Exception as e:
            self.status = self.FAILED
            raise type(e)(str(e) + " `set_aoi_from_ee` asset is neither a FeatureCollection nor an Image path") from e

    # All inputs MUST have `system:time_start` set
    def get_most_recent_image(self, imagecollection):
        # ensure date max filter uses 24-hour period of self.taskdate
        filterdate = self.taskdate + timedelta(days=1)
        filterdate = ee.Date(filterdate.strftime(self.DATE_FORMAT))
        most_recent_image = (
            imagecollection.filterDate("1900-01-01", filterdate)
            .sort(self.ASSET_TIMESTAMP_PROPERTY, False)
            .first()
        )
        most_recent_date = None
        if most_recent_image.getInfo():
            system_timestamp = most_recent_image.get(
                self.ASSET_TIMESTAMP_PROPERTY
            ).getInfo()
            if system_timestamp:
                most_recent_date = ee.Date(system_timestamp)
        return most_recent_image, most_recent_date

    # only use on fcs with SCL naming convention ending in `YYYY-mm-dd`
    def get_most_recent_featurecollection(self, eedir):
        most_recent_fc = None
        most_recent_date = None
        if not ee.data.getInfo(eedir):
            return None, None
        assets = self._list_assets(eedir)

        for asset in assets:
            if asset["type"] == "TABLE":
                match = re.search(r"\d{4}-\d{2}-\d{2}$", asset["id"])
                if match:
                    fcdate = datetime.strptime(match.group(), self.DATE_FORMAT).date()
                    if (
                        not most_recent_fc or fcdate > most_recent_date
                    ) and fcdate <= self.taskdate:
                        most_recent_fc = ee.FeatureCollection(asset["id"])
                        most_recent_date = fcdate

        return most_recent_fc, ee.Date(most_recent_date.strftime(self.DATE_FORMAT))

    def check_inputs(self):
        super().check_inputs()

        # TODO: test aoi validity outside ee and move this into GeoTask?
        try:
            ee_aoi = ee.Geometry.MultiPolygon(coords=self.aoi)
        except Exception as e:
            self.status = self.FAILED
            raise type(e)(str(e) + " `aoi` incorrect: {}".format(self.aoi)) from e

        for key, ee_input in self.inputs.items():
            if "ee_path" not in ee_input:  # not an EE input
                continue

            if "ee_type" not in ee_input or ee_input["ee_type"] not in self.EEDATATYPES:
                self.status = self.FAILED
                print("Missing or invalid ee_type for {}".format(ee_input["ee_path"]))
                continue

            if not ee.data.getInfo(ee_input["ee_path"]):
                self.status = self.FAILED
                print("{} does not exist".format(ee_input["ee_path"]))
                continue

            ee_taskdate = ee.Date(self.taskdate.strftime(self.DATE_FORMAT))
            asset = None
            asset_date = None
            if ee_input["ee_type"] == self.FEATURECOLLECTION:
                continue  # TODO: implement fc maxage checking
            if ee_input["ee_type"] == self.IMAGE:
                asset = ee.Image(ee_input["ee_path"])
                if "static" in ee_input and ee_input["static"] is True:
                    asset_date = ee.Date(self.taskdate.strftime(self.DATE_FORMAT))
                else:
                    system_timestamp = asset.get(
                        self.ASSET_TIMESTAMP_PROPERTY
                    ).getInfo()
                    if system_timestamp:
                        asset_date = ee.Date(system_timestamp)
            if ee_input["ee_type"] == self.IMAGECOLLECTION:
                ic = ee.ImageCollection(ee_input["ee_path"])
                asset, asset_date = self.get_most_recent_image(ic)

            if asset.getInfo() is None or asset_date is None:
                self.status = self.FAILED
                print(
                    f"Asset {ee_input['ee_path']} has no `{self.ASSET_TIMESTAMP_PROPERTY}` property, "
                    f"or has a date more recent than taskdate {self.taskdate}"
                )
                continue
            else:
                age = ee_taskdate.difference(asset_date, "year").getInfo()
                if age < 0:
                    self.status = self.FAILED
                    print(
                        f"Asset {ee_input['ee_path']} has a date more recent than taskdate {self.taskdate}"
                    )
                    continue
                if "maxage" in ee_input and age > ee_input["maxage"]:
                    self.status = self.FAILED
                    print(
                        f"Asset {ee_input['ee_path']} is {age} years old (maxage: {ee_input['maxage']})"
                    )
                    continue

    # ee asset property values must currently be numbers or strings
    def flatten_inputs(self):
        return_properties = {}
        # inputs = dict(itertools.islice(self.inputs.items(), self.EE_MAX_PROPERTIES))
        for inputkey, properties in self.inputs.items():
            for propkey, propval in properties.items():
                # Delimiters: I couldn't get ee to accept normal delimiters in key (tried: :|>/~-)
                # inputkey[:10] Limiting length of key:
                # - without limiting: 100 inputs -> 2 inputs converted to properties
                # - limiting to 10: 20 inputs -> 4 inputs converted to properties
                # - limiting to 10: 100 inputs -> 10 inputs converted to properties
                # if I don't export, all properties are set
                key = "inputs__{}__{}".format(inputkey[:10], propkey)
                if type(propval) != int and type(propval) != str:
                    propval = str(propval)
                return_properties[key] = propval
        return return_properties

    def set_export_metadata(self, element, ee_type=IMAGE):
        tasktime = time.strptime(
            self.taskdate.strftime(self.DATE_FORMAT), self.DATE_FORMAT
        )
        epoch = int(time.mktime(tasktime) * 1000)
        element = element.set(self.ASSET_TIMESTAMP_PROPERTY, epoch)
        # setMulti returns an Element, not an Image or FeatureCollection
        element = element.setMulti(self.flatten_inputs())
        if ee_type == self.IMAGE:
            return ee.Image(element)
        elif ee_type == self.FEATURECOLLECTION:
            return ee.FeatureCollection(element)
        return None

    def export_image_ee(self, image, asset_path, image_collection=True):
        image = self.set_export_metadata(image)
        image_name = asset_path.split("/")[-1]
        self._create_ee_path(
            "{}/{}".format(self.ee_rootdir, asset_path), image_collection
        )
        asset_id = "{}/{}/{}_{}".format(
            self.ee_rootdir, asset_path, image_name, self.taskdate
        )
        asset_id = self._canonicalize_assetid(asset_id)

        image_export = ee.batch.Export.image.toAsset(
            image,
            description=image_name,
            assetId=asset_id,
            region=self.extent,
            scale=self.scale,
            crs=self.crs,
            maxPixels=self.ee_max_pixels,
        )
        image_export.start()
        self.ee_tasks[image_export.id] = {}

    def export_fc_ee(self, featurecollection, asset_path):
        featurecollection = self.set_export_metadata(
            featurecollection, ee_type=self.FEATURECOLLECTION
        )
        # print(featurecollection.getInfo()["properties"])
        asset_id = "{}/{}".format(self.ee_rootdir, asset_path)
        asset_path_segments = asset_id.split("/")
        fc_name = asset_path_segments[-1]
        self._create_ee_path("/".join(asset_path_segments[:-1]))
        asset_id = self._canonicalize_assetid(asset_id)

        fc_export = ee.batch.Export.table.toAsset(
            featurecollection, description=fc_name, assetId=asset_id
        )
        fc_export.start()
        self.ee_tasks[fc_export.id] = {}

    def export_fc_cloudstorage(
        self, featurecollection, bucket, asset_path, file_format="GeoJSON"
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
        )
        fc_export.start()
        self.ee_tasks[fc_export.id] = {}

    def wait(self):
        super().wait()
        max_sleep = 600
        counter = 3
        while self.ee_tasks:
            self.update_ee_tasks()
            if not self.ee_tasks:
                break
            counter += 1
            sleeptime = 2 ** counter
            if sleeptime > max_sleep:
                sleeptime = max_sleep
            time.sleep(sleeptime)

    def update_ee_tasks(self):
        if self.ee_tasks:
            # possible ee task states: READY, RUNNING, COMPLETED, FAILED, CANCELLED, UNKNOWN
            statuses = ee.data.getTaskStatus(self.ee_tasks.keys())
            print(statuses)
            for s in statuses:
                if s["state"] in self.EEFINISHED:
                    del self.ee_tasks[s["id"]]
                    # TODO: log and update self.status based on EECOMPLETED, EEFAILED, EECANCELLED, EEUNKNOWN
                else:
                    self.ee_tasks[s["id"]] = s


class SCLTask(EETask):
    SPECIES = "species"
    RESTORATION = "restoration"
    SURVEY = "survey"
    FRAGMENT = "fragment"
    LANDSCAPE_TYPES = [SPECIES, RESTORATION, SURVEY, FRAGMENT]
    CANONICAL = "canonical"

    ee_project = "SCL/v1"
    species = None
    scenario = None
    ee_aoi = "historical_range_img_200914"

    def _scl_path(self, scltype):
        if scltype is None or scltype not in self.inputs:
            raise TypeError("Missing or incorrect scltype for setting scl path")
        return f"{self.ee_rootdir}/scl_poly/{self.taskdate}/{scltype}"

    def scl_path_species(self):
        return self._scl_path(f"scl_{self.SPECIES}")

    def scl_path_restoration(self):
        return self._scl_path(f"scl_{self.RESTORATION}")

    def scl_path_survey(self):
        return self._scl_path(f"scl_{self.SURVEY}")

    def scl_path_fragment(self):
        return self._scl_path(f"scl_{self.FRAGMENT}")

    def __init__(self, *args, **kwargs):
        self.species = kwargs.pop("species", None)
        if not self.species:
            # remove this line when we move beyond tigers
            self.species = "Panthera_tigris"
            # raise NotImplementedError('`species` must be defined')

        self.scenario = kwargs.pop("scenario", self.CANONICAL)

        super().__init__(*args, **kwargs)
        self.ee_rootdir = f"{self.ee_rootdir}/{self.species}"
        self.set_aoi_from_ee("{}/{}".format(self.ee_rootdir, self.ee_aoi))


class HIITask(EETask):
    ee_project = "HII/v1"
