from .task import Task


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
