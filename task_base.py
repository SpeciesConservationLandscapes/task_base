import os
import json
import ee
from datetime import datetime, timezone


class Task(object):
    DATE_FORMAT = '%Y-%m-%d'
    # list of possible statuses associated with a Task instance -- which is different from ee task statuses
    NOTSTARTED = "not started"
    SKIPPED = "skipped"
    RUNNING = "running"
    COMPLETE = "complete"

    status = NOTSTARTED
    inputs = {}
    crs = 'EPSG:4326'
    scale = 300
    aoi = [[[[-180.0, -90.0],
             [180.0, -90.0],
             [180.0, 90.0],
             [-180.0, 90.0],
             [-180.0, -90.0]]]]

    def __init__(self, *args, **kwargs):
        _taskdate = datetime.now(timezone.utc).date()
        try:
            _taskdate = datetime.strptime(kwargs.pop('taskdate', None), self.DATE_FORMAT).date()
        except (TypeError, ValueError):
            pass
        self.taskdate = _taskdate

    def check_inputs(self):
        if (not hasattr(self, 'aoi') or not hasattr(self, 'scale') or not hasattr(self, 'crs') or
                not self.aoi or not self.scale or not self.crs):
            raise NotImplementedError('Undefined input: aoi, scale, or crs')

    def run(self):
        self.status = self.RUNNING
        self.check_inputs()


class EETask(Task):
    service_account_key = os.environ.get('SERVICE_ACCOUNT_KEY')
    ee_rootdir = None
    ee_taskids = []
    ee_max_pixels = 500000000000

    @staticmethod
    def _create_ee_path(asset_path):
        path_segments = asset_path.split('/')
        # first two segments are user/project root (e.g. projects/HII)
        for i in range(2, len(path_segments)):
            path = '/'.join(path_segments[:i + 1])
            if not ee.data.getInfo(path):
                ee.data.createAsset({'type': 'Folder'}, opt_path=path)

    @staticmethod
    def _canonicalize_assetid(assetid):
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
        super().__init__()
        self._initialize_ee_client()

        if not self.ee_rootdir:
            raise NotImplementedError('`ee_rootdir` must be defined')
        self.ee_rootdir = self.ee_rootdir.strip('/')
        self._create_ee_path(self.ee_rootdir)

    def set_aoi_from_ee(self, fc):
        ee_aoi = ee.Geometry.Polygon(
            ee.FeatureCollection(fc).first().geometry().coordinates()
        )
        self.aoi = ee_aoi.getInfo()['coordinates']

    def check_inputs(self):
        super().check_inputs()

        # TODO: test aoi validity outside ee and move this into parent
        try:
            ee_aoi = ee.Geometry.Polygon(coords=self.aoi)
        except Exception as e:
            raise type(e)(str(e) + ' `aoi` incorrect: {}'.format(self.aoi)) from e

        for key, ee_input in self.inputs.items():
            if 'ee_path' in ee_input:
                if not ee.data.getInfo(ee_input['ee_path']):
                    self.status = self.SKIPPED
                    print('{} does not exist; task skipped'.format(ee_input['ee_path']))
                    break

                ic = ee.ImageCollection(ee_input['ee_path'])
                ee_taskdate = ee.Date(self.taskdate.strftime(self.DATE_FORMAT))
                most_recent = ic.filterDate('1900-01-01', ee_taskdate)\
                    .sort('system:time_start', False)\
                    .first()
                most_recent_date = ee.Date(most_recent.get("system:time_start"))
                age = ee_taskdate.difference(most_recent_date, 'year').getInfo()
                if age > ee_input['maxage']:
                    self.status = self.SKIPPED
                    print('{} most recent image is {} years old (maxage: {}); task skipped'.format(
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

    def export_image_ee(self, image, asset_path):
        image = image.set(self.flatten_inputs())

        image_name = asset_path.split('/')[-1]
        self._create_ee_path('{}/{}'.format(self.ee_rootdir, asset_path))
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
        self.ee_taskids.append(image_export.id)

    def run(self):
        super().run()

    def get_unfinished_ee_tasks(self):
        unfinished_tasks = []
        if self.ee_taskids:
            # possible ee task states: READY, RUNNING, COMPLETED, FAILED, CANCELLED, UNKNOWN
            statuses = ee.data.getTaskStatus(self.ee_taskids)
            print(statuses)
            for s in statuses:
                if s['state'] in ['READY', 'RUNNING']:
                    unfinished_tasks.append(s['id'])
                elif s['state'] == 'UNKOWN':
                    raise ValueError('unknown task {}'.format(s['id']))

        return unfinished_tasks


class SCLTask(EETask):
    species = None
    ee_aoi = 'aoi'

    def __init__(self, *args, **kwargs):
        super().__init__()
        try:
            self.species = kwargs.pop('species', None)
        except (TypeError, ValueError):
            # remove this line when we move beyond tigers
            self.species = 'Panthera_tigris'

        if not self.species:
            raise NotImplementedError('`species` must be defined')
        self.set_aoi_from_ee("{}/{}/{}".format(self.ee_rootdir, self.species, self.ee_aoi))
