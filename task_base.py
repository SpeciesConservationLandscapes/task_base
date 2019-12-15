import os
import json
import ee
import time
from datetime import datetime, timezone


class Task(object):
    DATE_FORMAT = '%Y-%m-%d'
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
            _taskdate = datetime.strptime(kwargs.pop('taskdate', None), self.DATE_FORMAT).date()
        except (TypeError, ValueError):
            pass
        self.taskdate = _taskdate

        self.wait_for_outputs = kwargs.pop('wait_for_outputs', True)

        self._set_inputs()

    def check_inputs(self):
        pass

    def calc(self):
        raise NotImplementedError('`calc` must be defined')

    def wait(self):
        pass

    def run(self, **kwargs):
        self.status = self.RUNNING
        self.check_inputs()
        try:
            self.calc()
            self.wait()
            self.status = self.COMPLETE
            print('status: {}'.format(self.status))
        except Exception as e:
            self.status = self.FAILED
            raise type(e)(str(e)) from e


class GeoTask(Task):
    crs = 'EPSG:4326'
    scale = 300
    aoi = [[[[-180.0, -90.0],
             [180.0, -90.0],
             [180.0, 90.0],
             [-180.0, 90.0],
             [-180.0, -90.0]]]]

    def check_inputs(self):
        super().check_inputs()
        if (not hasattr(self, 'aoi') or not hasattr(self, 'scale') or not hasattr(self, 'crs') or
                not self.aoi or not self.scale or not self.crs):
            self.status = self.FAILED
            raise NotImplementedError('Undefined input: aoi, scale, or crs')


class EETask(GeoTask):
    service_account_key = os.environ.get('SERVICE_ACCOUNT_KEY')
    ee_rootdir = None
    ee_tasks = {}
    ee_max_pixels = 500000000000

    EEREADY = 'READY'
    EE = 'RUNNING'
    EECOMPLETED = 'COMPLETED'
    EEFAILED = 'FAILED'
    EECANCELLED = 'CANCELLED'
    EEUNKNOWN = 'UNKNOWN'
    EEFINISHED = [EECOMPLETED, EEFAILED, EECANCELLED, EEUNKNOWN]

    IMAGECOLLECTION = "ImageCollection"
    FEATURECOLLECTION = "FeatureCollection"
    IMAGE = "Image"
    EEDATATYPES = [IMAGECOLLECTION, FEATURECOLLECTION, IMAGE]

    def _create_ee_path(self, asset_path, image_collection=False):
        path_segments = asset_path.split('/')
        # first two segments are user/project root (e.g. projects/HII)
        path_length = len(path_segments)
        for i in range(2, path_length):
            path = '/'.join(path_segments[:i + 1])
            if ee.data.getInfo(path):
                continue
            if i == path_length - 1 and image_collection:
                ee.data.createAsset({'type': 'ImageCollection'}, opt_path=path)
            else:
                ee.data.createAsset({'type': 'Folder'}, opt_path=path)

    def _canonicalize_assetid(self, assetid):
        if not ee.data.getInfo(assetid):
            return assetid
        i = 1
        new_assetid = '{}-{}'.format(assetid, i)
        while ee.data.getInfo(new_assetid):
            i += 1
            new_assetid = '{}-{}'.format(assetid, i)
        return new_assetid

    def _initialize_ee_client(self):
        if self.service_account_key is None:
            ee.Initialize('persistent')
        else:
            service_account_name = json.loads(self.service_account_key)['client_email']
            credentials = ee.ServiceAccountCredentials(service_account_name,
                                                       key_data=self.service_account_key)
            ee.Initialize(credentials)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._initialize_ee_client()

        if not self.ee_rootdir:
            self.status = self.FAILED
            raise NotImplementedError('`ee_rootdir` must be defined')
        self.ee_rootdir = self.ee_rootdir.strip('/')
        self._create_ee_path(self.ee_rootdir)

    def set_aoi_from_ee(self, fc):
        ee_aoi = ee.Geometry.MultiPolygon(
            ee.FeatureCollection(fc).first().geometry().coordinates()
        )
        self.aoi = ee_aoi.getInfo()['coordinates']

    def check_inputs(self):
        super().check_inputs()

        # TODO: test aoi validity outside ee and move this into GeoTask?
        try:
            ee_aoi = ee.Geometry.MultiPolygon(coords=self.aoi)
        except Exception as e:
            self.status = self.FAILED
            raise type(e)(str(e) + ' `aoi` incorrect: {}'.format(self.aoi)) from e

        for key, ee_input in self.inputs.items():
            if 'ee_path' not in ee_input:
                continue

            if not ee.data.getInfo(ee_input['ee_path']):
                print('{} does not exist'.format(ee_input['ee_path']))
                break

            if ee_input['ee_type'] == self.IMAGECOLLECTION:
                ic = ee.ImageCollection(ee_input['ee_path'])
                ee_taskdate = ee.Date(self.taskdate.strftime(self.DATE_FORMAT))
                most_recent = ic.filterDate('1900-01-01', ee_taskdate)\
                    .sort('system:time_start', False)\
                    .first()
                most_recent_date = ee.Date(most_recent.get("system:time_start"))
                age = ee_taskdate.difference(most_recent_date, 'year').getInfo()
                if 'maxage' in ee_input and age > ee_input['maxage']:
                    self.status = self.FAILED
                    print('{} most recent image is {} years old (maxage: {})'.format(
                        ee_input['ee_path'], age, ee_input['maxage']))
                    break

    # ee asset property values must currently be numbers or strings
    def flatten_inputs(self):
        return_properties = {}
        for inputkey, properties in self.inputs.items():
            for propkey, propval in properties.items():
                # I couldn't get ee to accept normal delimiters in key (tried: :|>/~-)
                key = 'inputsXXX{}XXX{}'.format(inputkey, propkey)
                return_properties[key] = propval
        return return_properties

    def export_image_ee(self, image, asset_path, image_collection=True):
        image = image.set(self.flatten_inputs())

        image_name = asset_path.split('/')[-1]
        self._create_ee_path('{}/{}'.format(self.ee_rootdir, asset_path), image_collection)
        asset_id = '{}/{}/{}_{}'.format(self.ee_rootdir, asset_path, image_name, self.taskdate)
        asset_id = self._canonicalize_assetid(asset_id)

        image_export = ee.batch.Export.image.toAsset(
            image,
            description=image_name,
            assetId=asset_id,
            region=self.aoi,
            scale=self.scale,
            crs=self.crs,
            maxPixels=self.ee_max_pixels,
        )
        image_export.start()
        self.ee_tasks[image_export.id] = {}

    def export_fc_cloudstorage(self, featurecollection, bucket, asset_path, file_format="GeoJSON"):
        featurecollection = featurecollection.set(self.flatten_inputs())
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
                if s['state'] in self.EEFINISHED:
                    del self.ee_tasks[s['id']]
                    # TODO: log and update self.status based on EECOMPLETED, EEFAILED, EECANCELLED, EEUNKNOWN
                else:
                    self.ee_tasks[s['id']] = s


class SCLTask(EETask):
    species = None
    ee_aoi = 'aoi'

    def __init__(self, *args, **kwargs):
        self.species = kwargs.pop('species', None)
        if not self.species:
            # remove this line when we move beyond tigers
            self.species = 'Panthera_tigris'
            # raise NotImplementedError('`species` must be defined')

        super().__init__(*args, **kwargs)
        self.set_aoi_from_ee("{}/{}/{}".format(self.ee_rootdir, self.species, self.ee_aoi))
