import ee
import os
import json
from .eetask import EETask, PROJECTS


class HIITask(EETask):
    scale = 300
    ee_project = "HII/v1"
    cloud_project = "hii3-246517"
    ee_cloud_asset_root = f"{PROJECTS}/{cloud_project}/assets"
    _popdens_relative = "source/population_density"
    common_inputs = {
        "countries": {
            "ee_type": EETask.FEATURECOLLECTION,
            "ee_path": f"{PROJECTS}/SCL/v1/source/gadm404_country_simp",
            "static": True,  # TODO: make dynamic
        },
        "population_density": {
            "ee_type": EETask.IMAGECOLLECTION,
            "ee_path": f"{PROJECTS}/HII/v1/{_popdens_relative}",
            "maxage": 1,
            "endcoverage": 2020
        },
        "worldpop": {
            "ee_type": EETask.IMAGECOLLECTION,
            "ee_path": "WorldPop/GP/100m/pop",
            "maxage": 2,
            "endcoverage": 2020
        },
        "watermask": {
            "ee_type": EETask.IMAGE,
            "ee_path": f"{PROJECTS}/HII/v1/source/phys/watermask_jrc70_cciocean",
            "static": True,
        },
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.countries = ee.FeatureCollection(
            self.common_inputs["countries"]["ee_path"]
        ).filter(ee.Filter.neq("iso2", "AQ"))
        self.watermask = ee.Image(self.common_inputs["watermask"]["ee_path"])

    @property
    def population_density(self):
        """
        If population density for previous year has already been calculated from WorldPop, use it. 
        Note: WorldPop coverage ends in 2020; therefore, can calculate popdensity ee.Images up to 2020. Any runtimes with a
         taskdate > 2021-01-01 are going to automatically retrieve the 2020 popdensity ee.Image
        
        Returns:
            ee.Image
        """
        # this whole function's logic assumes that worldpop data is available within 'maxage' years of taskdate, which, after 2022 is not true
        # popdens maxage is 1
        # worldpop maxage is 2
        # WorldPop data coverage up to 2020
        # logic doesn't break the code until taskdate is beyond 2023-12-31 because someone ran this code a few times after 2020 
        # and it generated popdensity images for 2021 and 2022 from 2020 WorldPop, exported them with the runtime's taskdate suffix, 
        popdens = self.common_inputs["population_density"]["ee_path"]
        popdens_endcoverage = self.common_inputs["population_density"]["endcoverage"]
        population_density, popdens_date = self.get_most_recent_image(ee.ImageCollection(popdens)) # we will leave this fn alone as its a lower-level class fn
        if population_density:
            taskyear = self.taskdate.year
            if taskyear >= popdens_endcoverage:
                last_valid_popdens = (ee.ImageCollection(popdens)
                                      .filterDate(str(popdens_endcoverage)+'-01-01',str(popdens_endcoverage)+'-12-31')
                                      .limit(1,'system:time_start')
                                      .first())
                population_density = last_valid_popdens
                print(f"Warning: Population Density from WorldPop is only available up to 2020. This does not break the OSM pipeline. Your taskdate: {self.taskdate}")
                return population_density
            else:
                popdensyear = popdens_date.get("year").getInfo()
                if (
                    0
                    <= (taskyear - popdensyear)
                    <= self.common_inputs["population_density"]["maxage"]
                ):
                    return population_density

        # Otherwise, calculate and store it before returning it
        # worldpop IC stops at 2020 and maxage is set to 2, so this returns None,None which does not meet the below if condition 
        worldpop_ic, worldpop_date = self.get_most_recent_fullyear_imagecollection(
            ee.ImageCollection(self.common_inputs["worldpop"]["ee_path"]),
            self.common_inputs["worldpop"]["maxage"],
        )
        # Kyle: this is where you'd create a new popdensity image in the popdensity collection from worldpop 
        # but the if condition makes us skip the process and return popdens=none
        # the worldpop IC used only has data out to 2020
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
            saved_population_density, _ = self.get_most_recent_image(
                ee.ImageCollection(popdens)
            )
            return saved_population_density

        
        return None

    def check_inputs(self):
        super().check_inputs()
        if self.population_density is None:
            self.status = self.FAILED
            print(f"Could not get population density for {self.taskdate}")
