# Lumeo Universal Bridge Uploader

The Lumeo Universal Bridge Uploader is a tool that uploads media files to the Lumeo cloud, associates them with a virtual camera, and queues them for processing. 
Clips uploaded via the Universal bridge are associated with a camera in Lumeo. Using the Lumeo console, you can then specify a default pipeline, to be applied to each uploaded clip for that camera, thereby automating the processing of clips with video analytics.

Learn more : https://docs.lumeo.com/docs/universal-bridge

## Installation

```
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Usage

The script can be run from the command line with the following arguments:

- `--app_id`: (required) The Application (aka Workspace) ID.
- `--token`: (required) The Access (aka API) Token.
- `--pattern`: A glob pattern for files to upload.
- `--file_list`: A comma-separated list of file URIs to queue.
- `--camera_id`: The Camera ID of an existing camera, to associate with the uploaded files.
- `--camera_external_id`: Use your own unique camera identifier to find or create a virtual camera, and associate with the uploaded files.
- `--csv_file`: A CSV file containing file_uri and corresponding camera_external_id or camera_id.
- `--delete_processed`: Delete successfully processed files from the local folder after uploading.
- `--log_level`: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
- `--batch_size`: Number of concurrent uploads to process at a time. Default is 5.
- `--queue_size`: Print the current queue size.

## Getting Started

1. Create a virtual camera in Lumeo console, and assign a Universal bridge pipeline to it in Camera's settings tab. 
   *If you want to use your own camera identifier, assign a unique External source identifier in Camera settings -> Univeral bridge section. This will be the same as camera_external_id here.*
2. Create a reference deployment with a SINGLE media clip. 
   ```
   python3 upload.py --app_id 'd413586b-0ccb-4aaa-9fdf-3df7404f716d' --token 'xxxxxxx' --camera_id 'd4e00207-8203-43b8-99e0-4de8cb3cff02' --pattern '/path/to/file.mp4'
   ```
3. Tweak the reference deployment in Lumeo Console to adjust ROIs and deploy time parameters
4. Bulk upload files using one of the usage options below.

## Examples

### Upload local files using a pattern
Uploads all files that match the pattern and associates them with virtual camera that has a specific external id.
```
python3 upload.py --app_id 'd413586b-0ccb-4aaa-9fdf-3df7404f716d' --token 'xxxxxxx' --camera_external_id 'test-ub-uploader' --pattern '/Users/username/media/lumeo-*.mp4'
```

### Upload self-hosted files using list
Creates input streams for URLs in the list and associates them with virtual camera that has a specific external id.
```
python3 upload.py --app_id 'd413586b-0ccb-4aaa-9fdf-3df7404f716d' --token 'xxxxxxx' --camera_external_id 'test-ub-uploader' --file_list 'https://abc.com/media1.mp4,https://abd.com/media2.mp4'
```

### Upload self-hosted or local files using a CSV manifest
Creates input streams for URLs in the list / uploads local files, and associates them with virtual camera that has a specific external id.
```
python3 upload.py --app_id 'd413586b-0ccb-4aaa-9fdf-3df7404f716d' --token 'xxxxxxx' --csv_file ./manifest.csv
```

CSV format:
```
file_uri, camera_external_id, camera_id
/Users/username/media/lumeo1.mp4,test-camera-1, 
/Users/username/media/lumeo2.mp4,test-camera-2, 
https://assets.lumeo.com/media/lumeo1.mp4,test-camera-3, 
https://assets.lumeo.com/media/lumeo1.mp4,,d4e00207-8203-43b8-99e0-4de8cb3cff02 
```

### Check queue length
```
python3 upload.py --app_id 'd413586b-0ccb-4aaa-9fdf-3df7404f716d' --token 'xxxxxxx' --queue_size
```