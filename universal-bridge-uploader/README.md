# Lumeo Universal Bridge Uploader

The Lumeo Universal Bridge Uploader is a tool that uploads media files to the Lumeo cloud, (optionally) associates them with a virtual camera, and queues them for processing. 

If clips uploaded via the Universal bridge are associated with a camera, using the Lumeo console, you can then specify a default pipeline to be applied to each uploaded clip for that camera, thereby automating the processing of clips with video analytics. Learn more : https://docs.lumeo.com/docs/universal-bridge

Alternatively you can also provide a pipeline, deployment configuration that is applied to create deployments.

## Installation

```
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Usage

The script can be run from the command line with the following arguments:

usage: upload.py [-h] --app_id APP_ID --token TOKEN [--pattern PATTERN] [--file_list FILE_LIST] [--csv_file CSV_FILE] [--s3_bucket S3_BUCKET] [--s3_access_key_id S3_ACCESS_KEY_ID]
                 [--s3_secret_access_key S3_SECRET_ACCESS_KEY] [--s3_region S3_REGION] [--s3_endpoint_url S3_ENDPOINT_URL] [--s3_prefix S3_PREFIX] [--tag TAG] [--camera_id CAMERA_ID]
                 [--camera_external_id CAMERA_EXTERNAL_ID] [--pipeline_id PIPELINE_ID] [--deployment_config DEPLOYMENT_CONFIG] [--deployment_prefix DEPLOYMENT_PREFIX] [--delete_processed DELETE_PROCESSED]
                 [--log_level LOG_LEVEL] [--batch_size BATCH_SIZE] [--queue_size]

options:
  -h, --help            show this help message and exit

Authentication Args:
  --app_id APP_ID       Application (aka Workspace) ID
  --token TOKEN         Access (aka API) Token.

Source Files (one of pattern, file_list, csv_file, s3_bucket or tag is required):
  --pattern PATTERN     Glob pattern for files to upload
  --file_list FILE_LIST
                        Comma separated list of file URIs to queue
  --csv_file CSV_FILE   CSV file containing file_uri and corresponding camera_external_id or camera_id
  --s3_bucket S3_BUCKET
                        S3 bucket name to use as source for files
  --s3_access_key_id S3_ACCESS_KEY_ID
                        S3 Access key ID
  --s3_secret_access_key S3_SECRET_ACCESS_KEY
                        S3 secret access key
  --s3_region S3_REGION
                        S3 region if using AWS S3 bucket. Either s3_region or s3_endpoint_url must be specified.
  --s3_endpoint_url S3_ENDPOINT_URL
                        S3 endpoint URL. Either s3_region or s3_endpoint_url must be specified.
  --s3_prefix S3_PREFIX
                        S3 path prefix to filter files. Optional.
  --tag TAG             Tag to apply to uploaded files. Can be tag uuid, tag name or tag path (e.g. "tag1/tag2/tag3").If specified without pattern/file_list/csv_file/s3_bucket, will process existing files with
                        that tag.

Associate with Camera (gets pipeline & deployment config from camera):
  --camera_id CAMERA_ID
                        Camera ID of an existing camera, to associate with the uploaded files
  --camera_external_id CAMERA_EXTERNAL_ID
                        Use your own unique camera id to find or create a virtual camera, and associate with the uploaded files

Deployment Args (applied only when camera not specified):
  --pipeline_id PIPELINE_ID
                        Pipeline ID to queue deployment for processing. Required if camera_id / camera_external_id not specified.
  --deployment_config DEPLOYMENT_CONFIG
                        String containing a Deployment config JSON object. Video source in the config will be overridden by source files specified in this script. Ignored if camera_id or camera_external_id
                        specified. Optional.

General Args:
  --deployment_prefix DEPLOYMENT_PREFIX
                        Prefix to use for deployment name. Optional.
  --delete_processed DELETE_PROCESSED
                        Delete successfully processed files from the local folder after uploading
  --log_level LOG_LEVEL
                        Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
  --batch_size BATCH_SIZE
                        Number of concurrent uploads to process at a time. Default 5.
  --queue_size          Print the current queue size


## Universal Bridge
Uses the Universal Bridge capability in Lumeo to set the default pipeline and reference deployment configuration in Lumeo Console.

1. Create a virtual camera in Lumeo console, and assign a Universal bridge pipeline to it in Camera's settings tab. 
   *If you want to use your own camera identifier, assign a unique External source identifier in Camera settings -> Univeral bridge section. This will be the same as camera_external_id here.*
2. Create a reference deployment with a SINGLE media clip. 
   ```
   python3 upload.py --app_id 'd413586b-0ccb-4aaa-9fdf-3df7404f716d' --token 'xxxxxxx' --camera_id 'd4e00207-8203-43b8-99e0-4de8cb3cff02' --pattern '/path/to/file.mp4'
   ```
3. Tweak the reference deployment in Lumeo Console to adjust ROIs and deploy time parameters
4. Bulk upload files using one of the usage options below.

### Upload local files using a pattern
Uploads all files that match the pattern and associates them with virtual camera that has a specific external id.
```
python3 upload.py --app_id 'd413586b-0ccb-4aaa-9fdf-3df7404f716d' --token 'xxxxxxx' --camera_external_id 'test-ub-uploader' --pattern '/Users/username/media/lumeo-*.mp4'
```

### Upload self-hosted files using list
Creates input streams for URLs in the list and associates them with virtual camera that has a specific external id.
```
python3 upload.py --app_id 'd413586b-0ccb-4aaa-9fdf-3df7404f716d' --token 'xxxxxxx' --camera_external_id 'test-ub-uploader' --file_list 'https://assets.lumeo.com/media/parking_lot/mall-parking-1.mp4,https://assets.lumeo.com/media/sample/sample-people-car-traffic.mp4'
```

### Upload self-hosted or local files using a CSV manifest
Creates input streams for URLs in the list / uploads local files, and associates them with virtual camera that has a specific external id.
```
python3 upload.py --app_id 'd413586b-0ccb-4aaa-9fdf-3df7404f716d' --token 'xxxxxxx' --csv_file ./manifest.csv
```

CSV format:
```
file_uri, camera_external_id, camera_id, pipeline_id, deployment_config
/Users/devarshi/Downloads/warehouse2.mp4,test-camera-1, 
https://assets.lumeo.com/media/parking_lot/mall-parking-1.mp4,test-camera-2, 
https://storage.googleapis.com/lumeo-public-media/samples/mall-guest-svcs.mp4,test-camera-3, 
https://storage.googleapis.com/lumeo-public-media/demos/warehouse5.mp4,,d4e00207-8203-43b8-99e0-4de8cb3cff02 
```

### Upload self-hosted files from a S3 bucket
Creates input streams for signed S3 URLs from your S3 bucket, associates them with a virtual camera.
```
./upload.py --s3_endpoint_url='https://sfo2.digitaloceanspaces.com' --s3_prefix=universal-bridge-testing --s3_bucket=lumeo-test --s3_access_key_id=xxxx --s3_secret_access_key='xxxx' --app_id=bc655947-45da-43cb-a254-a3a5e69ec084 --token='xxx' --camera_external_id=lpr-bulk-processing
```



## Standard Deployment
In this mode, the pipeline to use and (optionally) deployment configuration overrides are specified as script arguments.

### Upload local files using a pattern
Uploads all files that match the pattern and deploy a specific pipeline.
```
python3 upload.py --app_id 'd413586b-0ccb-4aaa-9fdf-3df7404f716d' --token 'xxxxxxx' --pipeline_id ee55c234-b3d5-405f-b904-cfb2bd6f2e06 --pattern '/Users/username/media/lumeo-*.mp4'
```

Uploads all files that match the pattern and deploy a specific pipeline, and tag all uploaded files + resulting deployments
```
python3 upload.py --app_id 'd413586b-0ccb-4aaa-9fdf-3df7404f716d' --token 'xxxxxxx' --pipeline_id ee55c234-b3d5-405f-b904-cfb2bd6f2e06 --pattern '/Users/username/media/lumeo-*.mp4' --tag 'bulk-uploads/2024-07-31'
```

Uploads all files that match the pattern and deploy a specific pipeline with deployment config override
```
python3 upload.py --app_id 'd413586b-0ccb-4aaa-9fdf-3df7404f716d' --token 'xxxxxxx' --pipeline_id ee55c234-b3d5-405f-b904-cfb2bd6f2e06 --pattern '/Users/username/media/lumeo-*.mp4' --deployment_config='{"overlay_meta2": {"text": "my-test-run","show_frame_count":true}}'
```


### Upload self-hosted files using list
Creates input streams for URLs in the list and deploys with a specific pipeline.
```
python3 upload.py --app_id 'd413586b-0ccb-4aaa-9fdf-3df7404f716d' --token 'xxxxxxx' --pipeline_id ee55c234-b3d5-405f-b904-cfb2bd6f2e06 --file_list 'https://assets.lumeo.com/media/parking_lot/mall-parking-1.mp4,https://assets.lumeo.com/media/sample/sample-people-car-traffic.mp4'
```

### Upload self-hosted or local files using a CSV manifest
Creates input streams for URLs in the list / uploads local files, and deploys with a specific pipeline/deployment config specified in the csv file.
```
python3 upload.py --app_id 'd413586b-0ccb-4aaa-9fdf-3df7404f716d' --token 'xxxxxxx' --csv_file ./manifest.csv
```

Creates input streams for URLs in the list / uploads local files, and deploys with a specific pipeline/deployment config specified in the csv file, falling back to command line options.
```
python3 upload.py --app_id 'd413586b-0ccb-4aaa-9fdf-3df7404f716d' --token 'xxxxxxx' --csv_file ./manifest.csv --pipeline_id ee55c234-b3d5-405f-b904-cfb2bd6f2e06 --deployment_config '{"overlay_meta2": {"text": "my-test-run-default","show_frame_count":false}}'
```


CSV format (note the double quoted JSON):
```
file_uri, camera_external_id, camera_id, pipeline_id, deployment_config
/Users/devarshi/Downloads/warehouse2.mp4,,,ee55c234-b3d5-405f-b904-cfb2bd6f2e06
https://assets.lumeo.com/media/parking_lot/mall-parking-1.mp4,,ee55c234-b3d5-405f-b904-cfb2bd6f2e06
https://storage.googleapis.com/lumeo-public-media/samples/mall-guest-svcs.mp4,,,ee55c234-b3d5-405f-b904-cfb2bd6f2e06 
https://storage.googleapis.com/lumeo-public-media/demos/warehouse5.mp4,,,ee55c234-b3d5-405f-b904-cfb2bd6f2e06,"{""overlay_meta2"": {""text"": ""my-test-run"",""show_frame_count"":true}}" 
```

### Upload self-hosted files from a S3 bucket
Creates input streams for signed S3 URLs from your S3 bucket, and deploys with a specific pipeline, tagging them in the process.
```
./upload.py --s3_endpoint_url='https://sfo2.digitaloceanspaces.com' --s3_prefix=universal-bridge-testing --s3_bucket=lumeo-test --s3_access_key_id=xxxx --s3_secret_access_key='xxxx' --app_id=bc655947-45da-43cb-a254-a3a5e69ec084 --token='xxx' --pipeline_id ee55c234-b3d5-405f-b904-cfb2bd6f2e06 --tag 's3-file-uploads/2024-03-01/run1'
```

### Process existing Lumeo cloud files tagged with a specific tag
Queues up all the files with the specific tag in Lumeo Cloud for processing using the specified pipeline id and deployment configuration.
```
./upload.py --app_id=bc655947-45da-43cb-a254-a3a5e69ec084 --token='xxx' --pipeline_id ee55c234-b3d5-405f-b904-cfb2bd6f2e06 --tag 'test-bench/person-model-testing' --deployment_config '{"overlay_meta2": {"text": "person-model-testing-2024-08-18","show_frame_count":false}}'
```

```
./upload.py --app_id=bc655947-45da-43cb-a254-a3a5e69ec084 --token='xxx' --pipeline_id ee55c234-b3d5-405f-b904-cfb2bd6f2e06 --tag 'test-bench/person-model-testing'
```


## Check queue length
```
python3 upload.py --app_id 'd413586b-0ccb-4aaa-9fdf-3df7404f716d' --token 'xxxxxxx' --queue_size
```