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
        self.set_aoi_from_ee(f"{self.speciesdir}/{self.ee_aoi}")
