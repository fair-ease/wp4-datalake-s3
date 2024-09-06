import re
import sys
import json
import boto3
import pika
import pystac
import rasterio
import jsonschema

from shapely.geometry import Polygon, mapping
from datetime import datetime
from pystac import Link, Catalog
from pystac.stac_io import DefaultStacIO, StacIO
from typing import Union, Any
from urllib.parse import urlparse
import pathlib

##########################################################################
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

##########################################################################
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


##########################################################################
def add_item_from_notif(catalog, record):
    bucket = record['s3']['bucket']['name']
    key = record['s3']['object']['key']
    url = "s3://{}/{}".format(bucket,key)
    metadata = {}
    metadata_re = re.compile(r"^x-amz-meta-(fairease(?:[.]([^.]+))+)$")
    
    for m in record['s3']['object']['metadata']:
        regexp = metadata_re.match(m['key'])
        if regexp is not None:
            meta_key = regexp.group(1)
            metadata[meta_key] = m['val']
    
    if 'fairease.catalog.mediatype' in metadata and (metadata['fairease.catalog.mediatype']=='COG'):
        
        item_id = pathlib.Path(key).stem
        if any(True for _ in catalog.get_items(item_id)):
            print("Item {} already exists".format(item_id))
        else:
            print("Item {} has to be inserting in catalog".format(item_id))
            bbox, footprint = get_bbox_and_footprint(url)
            
            item = pystac.Item(
                id=item_id,
                geometry=footprint,
                bbox=bbox,
                datetime=datetime.utcnow(),
                properties={},
            )
        
            item.add_asset(
                key="image", asset=pystac.Asset(href=url, media_type=pystac.MediaType.COG)
            )
            
            print(item)
            
            catalog.add_item(item)
            catalog.validate_all()
            catalog.describe()
            catalog.save(catalog_type=pystac.CatalogType.ABSOLUTE_PUBLISHED)
            print("Catalog updated".format(item_id))
    return 0

##########################################################################
def delete_item_from_notif(catalog, record):
    return
    
##########################################################################
## Callback function for each received messages
def stac_callback(catalog, ch, method, properties, body):
    print(f" [x] {body}")

    msg = json.loads(body)    
    for r in msg['Records']:
        event_name =  r['eventName']
        event_id = r['eventId']
        print("\n{} ({})".format(event_name, event_id))
        
        if event_name.startswith('ObjectCreated:'):
          add_item_from_notif(catalog, r)


##########################################################################
def main() -> int:
    
    # Load catalog from S3
    StacIO.set_default(S3StacIO)
    catalog = pystac.Catalog.from_file('s3://uca-eoscfe-catalog/catalog.json')
    
    # Open a channel with RabbitMQ server on localhost
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    
    # Declare a queue for the AMQP consummer
    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue
    channel.queue_bind(exchange='fairease-s3-events', queue=queue_name)
    
    # callback function
    def callback(ch, method, properties, body):
        nonlocal catalog
        return stac_callback(catalog, ch, method, properties, body)
    
    # Loop for consuming messages
    print(' [*] Waiting for logs. To exit press CTRL+C')
    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
    channel.start_consuming()
    
    return 0

if __name__ == '__main__':
    sys.exit(main())


## Command Line to add item with aws cli:
# aws s3 cp --content-type image/tiff --metadata "fairease.catalog.mediatype=COG" Copernicus_DSM_COG_30_N00_00_E006_00_DEM.tif  s3://uca-eoscfe-data/Copernicus_DSM_COG_30_N00_00_E006_00_DEM.tif