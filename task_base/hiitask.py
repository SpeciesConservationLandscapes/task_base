import ee
from .eetask import EETask, PROJECTS


class HIITask(EETask):
    scale = 300
    ee_project = "HII/v1"
    _popdens_relative = "source/population_density"
    common_inputs = {
        "population_density": {
            "ee_type": EETask.IMAGECOLLECTION,
            "ee_path": f"{PROJECTS}/HII/v1/{_popdens_relative}",
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
            taskyear = self.taskdate.year
            popdensyear = _.get("year").getInfo()
            if (
                0
                <= (taskyear - popdensyear)
                <= self.inputs["population_density"]["maxage"]
            ):
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
