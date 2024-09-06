import pystac
import rasterio
from shapely.geometry import Polygon, mapping
from datetime import datetime
import json

import boto3
from pystac import Link
from pystac.stac_io import DefaultStacIO, StacIO
from typing import Union, Any
from urllib.parse import urlparse

## Credentials in Environment:
# export AWS_ENDPOINT_URL=https://s3.mesocentre.uca.fr
# export AWS_S3_ENDPOINT=s3.mesocentre.uca.fr
# export AWS_ACCESS_KEY_ID=XXX
# export AWS_SECRET_ACCESS_KEY=YYY


## class S3StacIO : read / write STAC catalog with S3 API
# cf. https://pystac.readthedocs.io/en/stable/concepts.html#i-o-in-pystac
class S3StacIO(DefaultStacIO):
    def __init__(self):
        self.s3 = boto3.resource("s3")
        super().__init__()

    def read_text(self, source: Union[str, Link], *args: Any, **kwargs: Any) -> str:
        parsed = urlparse(source)
        if parsed.scheme == "s3":
            bucket = parsed.netloc
            key = parsed.path[1:]

            obj = self.s3.Object(bucket, key)
            return obj.get()["Body"].read().decode("utf-8")
        else:
            return super().read_text(source, *args, **kwargs)

    def write_text(
        self, dest: Union[str, Link], txt: str, *args: Any, **kwargs: Any
    ) -> None:
        parsed = urlparse(dest)
        if parsed.scheme == "s3":
            bucket = parsed.netloc
            key = parsed.path[1:]
            self.s3.Object(bucket, key).put(Body=txt, ContentEncoding="utf-8")
        else:
            super().write_text(dest, txt, *args, **kwargs)

StacIO.set_default(S3StacIO)

## Extract geo bounding box from GeoTIFF metadata
# cf. https://pystac.readthedocs.io/en/latest/tutorials/how-to-create-stac-catalogs.html
def get_bbox_and_footprint(raster_uri):
    with rasterio.open(raster_uri) as ds:
        bounds = ds.bounds
        bbox = [bounds.left, bounds.bottom, bounds.right, bounds.top]
        footprint = Polygon(
            [
                [bounds.left, bounds.bottom],
                [bounds.left, bounds.top],
                [bounds.right, bounds.top],
                [bounds.right, bounds.bottom],
            ]
        )
        
        # The GeoJSON-like mapping of a geometric object can be obtained using shapely.geometry.mapping().
        geojson = mapping(footprint)
        
        return (bbox, geojson)


## Creation du catalogue
catalog = pystac.Catalog(id="uca-catalog", description="UCA demo catalog.")

# Ajout des elements
images = [
    "Copernicus_DSM_COG_30_N00_00_E006_00_DEM",
    "Copernicus_DSM_COG_30_N00_00_E009_00_DEM",
    "Copernicus_DSM_COG_30_N00_00_E010_00_DEM"
]

for image_id in images:

    url = "s3://uca-eoscfe-data/{}.tif".format(image_id)
    bbox, footprint = get_bbox_and_footprint(url)

    item = pystac.Item(
        id=image_id,
        geometry=footprint,
        bbox=bbox,
        datetime=datetime.utcnow(),
        properties={},
    )
    
    item.add_asset(
        key="image", asset=pystac.Asset(href=url, media_type=pystac.MediaType.GEOTIFF)
    )
    
    print(json.dumps(item.to_dict(), indent=2))

    catalog.add_item(item)


## Sauvegarde du catalogue sur le S3
catalog.normalize_hrefs("s3://uca-eoscfe-catalog/")
catalog.validate_all()
catalog.save(catalog_type=pystac.CatalogType.ABSOLUTE_PUBLISHED)
