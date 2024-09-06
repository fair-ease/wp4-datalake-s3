# wp4-datalake-s3

message2.json: it is an example of AMQP message exchanged for automatic file registering

S3_examples: an example that explain how to use S3 in order to load a file, do some data transformations, transform it in geoparquet, write it on S3 and then query it using duckDB

test1-stac-catalog-s3: an example showing how to manually add a new asset in a static STAC catalog (with some metadatas) onto S3. Two buckets are used (one for the data, one for STAC)

test2-s3-amqp-update-stac-catalog: an example showing how to automatically update a STAC catalog (stored on S3) using user metadata and AMQP message. It requires to install RabbitMQ, and a python script (not available here)
