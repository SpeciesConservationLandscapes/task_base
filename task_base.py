import os
import json
import re
import subprocess
import time
import ee
import git
from datetime import date, datetime, timezone, timedelta
from pathlib import Path

PROJECTS = "projects"


class EETaskError(Exception):
    def __init__(self, ee_statuses, *args, **kwargs):
        self.ee_statuses = ee_statuses or dict()

    def __str__(self):
        num_failed_tasks = len(list(self.ee_statuses.keys()))
        return f"Failed Earth Engine tasks: {num_failed_tasks}"


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
        _taskdatestr = kwargs.get("taskdate") or os.environ.get("taskdate")
        try:
            _taskdate = datetime.strptime(_taskdatestr, self.DATE_FORMAT).date()
        except (TypeError, ValueError):
            pass
        self.taskdate = _taskdate

        self.overwrite = kwargs.get("overwrite") or os.environ.get("overwrite") or False

        self._set_inputs()

    def check_inputs(self):
        pass

    def calc(self):
        raise NotImplementedError("`calc` must be defined")

    def wait(self):
        pass

    def clean_up(self, **kwargs):
        pass

    def run(self, **kwargs):
        try:
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
        finally:
            self.clean_up()
        print("status: {}".format(self.status))


class GeoTask(Task):
    crs = "EPSG:4326"
    scale = 1000
    aoi = [
        [
            [
                [-180.0, -58.0],
                [180.0, -58.0],
                [180.0, 84.0],
                [-180.0, 84.0],
                [-180.0, -58.0],
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
    google_creds_path = "/.google_creds"
    ee_project = None
    ee_rootdir = None
    ee_tasks = {}
    _failed_ee_tasks = {}
    ee_max_pixels = 10000000000000

    EEREADY = "READY"
    EE = "RUNNING"
    EECOMPLETED = "COMPLETED"
    EESUCCEEDED = "SUCCEEDED"
    EEFAILED = "FAILED"
    EECANCELLED = "CANCELLED"
    EEUNKNOWN = "UNKNOWN"
    EEFINISHED = [EECOMPLETED, EESUCCEEDED, EEFAILED, EECANCELLED, EEUNKNOWN]

    IMAGECOLLECTION = "ImageCollection"
    IMAGE = "Image"
    EEDIR = "Folder"
    FEATURECOLLECTION = "FeatureCollection"
    EEDATATYPES = [IMAGECOLLECTION, IMAGE, EEDIR, FEATURECOLLECTION]

    def _canonicalize_assetid(self, assetid):
        path_segments = [s.replace(" ", "_") for s in assetid.split("/")]
        assetid = "/".join(path_segments)
        new_assetid = assetid
        if ee.data.getInfo(assetid):
            i = 1
            while ee.data.getInfo(new_assetid):
                new_assetid = f"{assetid}-{i}"
                i += 1

        if self.overwrite and assetid != new_assetid:
            self.transaction_assets.append((assetid, new_assetid))

        return new_assetid

    def _prep_asset_id(self, asset_path, image_collection=False):
        asset_path = f"{self.ee_rootdir}/{asset_path}"
        asset_name = asset_path.split("/")[-1]
        asset_id = self._canonicalize_assetid(
            f"{asset_path}/{asset_name}_{self.taskdate}"
        )

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

        return asset_name, asset_id

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

    def _rm_ee(self, asset_id, dry_run=False):
        asset = ee.data.getInfo(asset_id)
        if not asset:
            print(f"{asset_id} does not exist")
            return False

        asset_type = asset["type"].capitalize()
        cmd_args = [
            "earthengine",
            f"--service_account_file {self.google_creds_path}",
            "rm",
        ]
        if dry_run:
            cmd_args.append("--dry_run")

        if (
            asset_type == ee.data.ASSET_TYPE_FOLDER
            or asset_type == ee.data.ASSET_TYPE_IMAGE_COLL
            or asset_type == ee.data.ASSET_TYPE_FOLDER_CLOUD
            or asset_type == ee.data.ASSET_TYPE_IMAGE_COLL_CLOUD
        ):
            cmd_args.append("-r")
        cmd_args.append(asset_id)

        subprocess.run(" ".join(cmd_args), stderr=subprocess.STDOUT, shell=True)
        return True

    def _mv_ee(self, old_assetid, new_assetid):
        old_asset = ee.data.getInfo(old_assetid)
        if not old_asset:
            print(f"{old_assetid} does not exist")
            return False
        new_asset = ee.data.getInfo(new_assetid)
        if new_asset:
            print(f"{new_assetid} already exists")
            return False

        cmd_args = [
            "earthengine",
            f"--service_account_file {self.google_creds_path}",
            "mv",
            old_assetid,
            new_assetid,
        ]
        subprocess.run(" ".join(cmd_args), stderr=subprocess.STDOUT, shell=True)

        return True

    def __init__(self, *args, **kwargs):
        self._initialize_ee_client()

        if not self.ee_project:
            self.status = self.FAILED
            raise NotImplementedError("`ee_project` must be defined")

        self.ee_rootdir = kwargs.pop("ee_rootdir", None)
        if not self.ee_rootdir:
            self.ee_rootdir = f"{PROJECTS}/{self.ee_project}"
        self.ee_rootdir = self.ee_rootdir.strip("/")

        self.transaction_assets = []

        creds_path = Path(self.google_creds_path)
        if creds_path.exists() is False:
            with open(creds_path, "w") as f:
                f.write(self.service_account_key)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.google_creds_path

        super().__init__(*args, **kwargs)

    def rm_ee(self, asset_path, dry_run=False):
        asset_path = f"{self.ee_rootdir}/{asset_path}"
        return self._rm_ee(asset_path, dry_run)

    def mv_ee(self, old_asset_path, new_asset_path):
        old_asset_path = f"{self.ee_rootdir}/{old_asset_path}"
        new_asset_path = f"{self.ee_rootdir}/{new_asset_path}"
        return self._mv_ee(old_asset_path, new_asset_path)

    # def cp(self, source_id, destination_id, overwrite=True):
    #     destination_dir = "/".join(destination_id.split("/")[:-1])
    #     if not ee.data.getInfo(source_id) or not ee.data.getInfo(destination_dir):
    #         print(f"{source_id} or {destination_id} is not valid")
    #         return
    #     ee.data.copyAsset(source_id, destination_id, overwrite)
    #
    def set_aoi_from_ee(self, asset):
        try:  # setting aoi from FeatureCollection
            ee_aoi = ee.Geometry.MultiPolygon(
                ee.FeatureCollection(asset).first().geometry().coordinates(),
                proj=self.crs,
                geodesic=False,
            )
            self.aoi = ee_aoi.getInfo()["coordinates"]
            self.extent = (
                ee.Geometry.MultiPolygon(self.aoi, proj=self.crs, geodesic=False)
                .bounds()
                .getInfo()["coordinates"]
            )
        except ee.ee_exception.EEException:  # setting aoi from Image
            ee_aoi = ee.Image(asset)
            self.aoi = self.extent = ee_aoi.geometry().bounds().getInfo()["coordinates"]
        except Exception as e:
            self.status = self.FAILED
            raise type(e)(
                str(e)
                + " `set_aoi_from_ee` asset is neither a FeatureCollection nor an Image path"
            ) from e

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
        return_image = None
        most_recent_date = None
        if most_recent_image.getInfo():
            return_image = most_recent_image
            system_timestamp = most_recent_image.get(
                self.ASSET_TIMESTAMP_PROPERTY
            ).getInfo()
            if system_timestamp:
                most_recent_date = ee.Date(system_timestamp)
        return return_image, most_recent_date

    def get_most_recent_fullyear_imagecollection(
        self, imagecollection, maxage, filterdate=None, iteration=1
    ):
        filterdate = filterdate or self.taskdate + timedelta(days=1)
        previousyear_start = date(filterdate.year - 1, 1, 1)
        previousyear_end = date(filterdate.year - 1, 12, 31)
        most_recent_ic = imagecollection.filterDate(
            previousyear_start.strftime(self.DATE_FORMAT),
            previousyear_end.strftime(self.DATE_FORMAT),
        )
        images = most_recent_ic.getInfo()["features"]
        if len(images) > 0:
            return most_recent_ic, ee.Date(
                previousyear_start.strftime(self.DATE_FORMAT)
            )

        if iteration < maxage:
            return self.get_most_recent_fullyear_imagecollection(
                imagecollection, maxage, previousyear_start, iteration + 1
            )

        return None, None

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

        if most_recent_date is not None:
            most_recent_date = ee.Date(most_recent_date.strftime(self.DATE_FORMAT))
        return most_recent_fc, most_recent_date

    def check_inputs(self):
        super().check_inputs()

        # TODO: test aoi validity outside ee and move this into GeoTask?
        try:
            ee_aoi = ee.Geometry.MultiPolygon(
                coords=self.aoi, proj=self.crs, geodesic=False
            )
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
            # no abstract featureCollection maxage checking; implement in inheritor specific to input
            if (
                ("static" in ee_input and ee_input["static"] is True)
                or ee_input["ee_type"] == self.FEATURECOLLECTION
                or ee_input["ee_type"] == self.EEDIR
            ):
                asset_date = ee.Date(self.taskdate.strftime(self.DATE_FORMAT))
                continue
            else:
                if ee_input["ee_type"] == self.IMAGE:
                    asset = ee.Image(ee_input["ee_path"])
                    system_timestamp = asset.get(
                        self.ASSET_TIMESTAMP_PROPERTY
                    ).getInfo()
                    if system_timestamp:
                        asset_date = ee.Date(system_timestamp)
                if ee_input["ee_type"] == self.IMAGECOLLECTION:
                    ic = ee.ImageCollection(ee_input["ee_path"])
                    asset, asset_date = self.get_most_recent_image(ic)

            if asset is None or asset.getInfo() is None or asset_date is None:
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

        try:  # pass in `-v $PWD/.git:/app/.git` to docker command to write commit SHA to asset properties
            repo = git.Repo(search_parent_directories=True)
            sha = repo.head.object.hexsha
            element = element.set("sha", sha)
        except Exception as e:
            pass

        # setMulti returns an Element, not an Image or FeatureCollection
        element = element.setMulti(self.flatten_inputs())
        if ee_type == self.IMAGE:
            return ee.Image(element)
        elif ee_type == self.FEATURECOLLECTION:
            return ee.FeatureCollection(element)
        return None

    # export_image_ee - appends date to asset name AND sets system:start in properties.
    #   get_most_recent_image and check_inputs use the latter
    # export_fc_ee - ONLY appends date to asset name (can't set fc meta properties)
    #   get_most_recent_featurecollection uses date appended to name; check_inputs not implemented
    # export_fc_cloudstorage - no date or other name manipulation. No implementation of `most_recent` or check_inputs.

    def export_image_ee(
        self, image, asset_path, image_collection=True, region=None, pyramiding=None
    ):
        image = self.set_export_metadata(image)
        image_name, asset_id = self._prep_asset_id(asset_path, image_collection)
        if region is None:
            region = self.extent
        if isinstance(region, list):
            region = ee.Geometry.Polygon(region, proj=self.crs, geodesic=False)
        if pyramiding is None:
            pyramiding = {".default": "mean"}

        image_export = ee.batch.Export.image.toAsset(
            image,
            description=image_name,
            assetId=asset_id,
            region=region,
            scale=self.scale,
            crs=self.crs,
            maxPixels=self.ee_max_pixels,
            pyramidingPolicy=pyramiding,
        )
        image_export.start()
        self.ee_tasks[image_export.id] = {}
        return image_export.id

    def export_fc_ee(self, featurecollection, asset_path):
        featurecollection = self.set_export_metadata(
            featurecollection, ee_type=self.FEATURECOLLECTION
        )
        # print(featurecollection.getInfo()["properties"])
        fc_name, asset_id = self._prep_asset_id(asset_path)

        fc_export = ee.batch.Export.table.toAsset(
            featurecollection, description=fc_name, assetId=asset_id
        )
        fc_export.start()
        self.ee_tasks[fc_export.id] = {}
        return fc_export.id

    def export_fc_cloudstorage(
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

    def wait(self):
        super().wait()

        max_sleep = 600
        counter = 3
        self._failed_ee_tasks = dict()
        while self.ee_tasks:
            self.update_ee_tasks()
            if not self.ee_tasks:
                break
            counter += 1
            sleeptime = 2 ** counter
            if sleeptime > max_sleep:
                sleeptime = max_sleep
            time.sleep(sleeptime)

        if bool(self._failed_ee_tasks) is True:
            raise EETaskError(ee_statuses=self._failed_ee_tasks)

    def update_ee_tasks(self):
        if self.ee_tasks:
            try:
                # possible ee task states: READY, RUNNING, COMPLETED, FAILED, CANCELLED, UNKNOWN
                statuses = ee.data.getTaskStatus(self.ee_tasks.keys())
                print(statuses)
                for s in statuses:
                    ee_task_state = s["state"]
                    ee_task_id = s["id"]

                    if ee_task_state in self.EEFINISHED:
                        if ee_task_state == self.EEFAILED:
                            self._failed_ee_tasks[ee_task_id] = s
                        del self.ee_tasks[ee_task_id]
                    else:
                        self.ee_tasks[s["id"]] = s
            except ConnectionResetError:
                pass  # assume intermittent connectivity issue

    def clean_up(self, **kwargs):
        if self.status != self.FAILED and self.overwrite:
            for old_assetid, new_assetid in self.transaction_assets:
                self._rm_ee(old_assetid)
                self._mv_ee(new_assetid, old_assetid)


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
    ee_aoi = "historical_range"

    def _scl_path(self, scltype):
        if scltype is None or scltype not in self.LANDSCAPE_TYPES:
            raise TypeError("Missing or incorrect scltype for setting scl path")
        return f"{self.ee_rootdir}/pothab/scl_{scltype}"

    def scl_path_species(self):
        return self._scl_path(self.SPECIES)

    def scl_path_restoration(self):
        return self._scl_path(self.RESTORATION)

    def scl_path_survey(self):
        return self._scl_path(self.SURVEY)

    def scl_path_fragment(self):
        return self._scl_path(self.FRAGMENT)

    def __init__(self, *args, **kwargs):
        self.species = kwargs.get("species") or os.environ.get("species")
        if not self.species:
            # remove this line when we move beyond tigers
            self.species = "Panthera_tigris"
            # raise NotImplementedError('`species` must be defined')

        self.scenario = (
            kwargs.get("scenario") or os.environ.get("scenario") or self.CANONICAL
        )

        self.speciesdir = f"{PROJECTS}/{self.ee_project}/{self.species}"
        ee_rootdir = f"{self.speciesdir}/{self.scenario}"
        path_segments = [s.replace(" ", "_") for s in ee_rootdir.split("/")]
        ee_rootdir = "/".join(path_segments)
        super().__init__(*args, ee_rootdir=ee_rootdir, **kwargs)
        self.set_aoi_from_ee(
            f"{self.speciesdir}/{self.ee_aoi}"
        )


class HIITask(EETask):
    scale = 300
    ee_project = "HII/v1"
    _popdens_relative = "source/population_density"
    common_inputs = {
        "population_density": {
            "ee_type": EETask.IMAGECOLLECTION,
            "ee_path": f"projects/HII/v1/{_popdens_relative}",
            "maxage": 1,
        },
        "worldpop": {
            "ee_type": EETask.IMAGECOLLECTION,
            "ee_path": "WorldPop/GP/100m/pop",
            "maxage": 2,
        },
    }

    @property
    def population_density(self):
        # If population density for previous year has already been calculated and stored, use it
        popdens = self.common_inputs["population_density"]["ee_path"]
        population_density, _ = self.get_most_recent_image(ee.ImageCollection(popdens))
        if population_density:
            ee_taskdate = ee.Date(self.taskdate.strftime(self.DATE_FORMAT))
            system_timestamp = population_density.get(self.ASSET_TIMESTAMP_PROPERTY).getInfo()
            age = ee_taskdate.difference(system_timestamp, "year").getInfo()
            if 0 <= age <= self.common_inputs["population_density"]["maxage"]:
                return population_density

        # Otherwise, calculate and store it before returning it
        worldpop_ic, worldpop_date = self.get_most_recent_fullyear_imagecollection(
            ee.ImageCollection(self.common_inputs["worldpop"]["ee_path"]),
            self.common_inputs["worldpop"]["maxage"],
        )

        if worldpop_ic:
            worldpop_scale = worldpop_ic.first().projection().nominalScale()
            area_km2 = (
                ee.Image.pixelArea()
                .multiply(0.000001)
                .reproject(self.crs, None, worldpop_scale)
            )

            population_density = (
                worldpop_ic.mosaic()
                .divide(area_km2)
                .setDefaultProjection(self.crs, None, worldpop_scale)
            )
            self.export_image_ee(population_density, self._popdens_relative)
            self.wait()
            saved_population_density = self.get_most_recent_image(ee.ImageCollection(popdens))
            return saved_population_density

        return None

    def check_inputs(self):
        super().check_inputs()
        if self.population_density is None:
            self.status = self.FAILED
            print(f"Could not get population density for {self.taskdate}")
