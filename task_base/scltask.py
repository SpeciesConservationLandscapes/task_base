import ee
import os
from .eetask import EETask, PROJECTS


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
    common_inputs = {
        "countries": {
            "ee_type": EETask.FEATURECOLLECTION,
            "ee_path": "projects/SCL/v1/source/esri_countries_generalized",
            "static": True,  # TODO: make dynamic
        },
        "ecoregions": {
            "ee_type": EETask.FEATURECOLLECTION,
            "ee_path": "RESOLVE/ECOREGIONS/2017",
            "static": True,
        },
        "pas": {
            "ee_type": EETask.FEATURECOLLECTION,
            "ee_path": "WCMC/WDPA/current/polygons",
            "static": True,
        },
        "historical_range": {
            "ee_type": EETask.FEATURECOLLECTION,
            "ee_path": "historical_range_path",
            "static": True,
        },
        "watermask": {
            "ee_type": EETask.IMAGE,
            "ee_path": "projects/HII/v1/source/phys/watermask_jrc70_cciocean",
            "static": True,
        },
    }

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

    def historical_range_path(self):
        return f"{self.speciesdir}/historical_range"

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

        self.historical_range_fc = ee.FeatureCollection(
            self.common_inputs["historical_range"]["ee_path"]
        )
        self.historical_range = self.historical_range_fc.reduceToImage(
            ["FID"], ee.Reducer.first()
        ).unmask(0)
        self.countries = ee.FeatureCollection(
            self.common_inputs["countries"]["ee_path"]
        ).filterBounds(self.historical_range_fc.geometry())
        self.ecoregions = ee.FeatureCollection(
            self.common_inputs["ecoregions"]["ee_path"]
        ).filterBounds(self.historical_range_fc.geometry())
        taskyear = ee.Date(self.taskdate.strftime(self.DATE_FORMAT)).get("year")
        self.pas = (
            ee.FeatureCollection(self.inputs["pas"]["ee_path"])
            .filterBounds(self.historical_range_fc.geometry())
            .filter(ee.Filter.neq("STATUS", "Proposed"))
            .filter(ee.Filter.lte("STATUS_YR", taskyear))
        )
        self.watermask = ee.Image(self.common_inputs["watermask"]["ee_path"])

        self.set_aoi_from_ee(f"{self.speciesdir}/{self.ee_aoi}")
