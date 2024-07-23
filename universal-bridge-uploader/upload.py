#!python3

import argparse
import asyncio
import boto3
import colorlog
import csv
import glob
import logging
import time
from pathlib import Path

from api import LumeoApiClient
from botocore.client import Config
from fileuploader import LumeoUniversalBridgeUploader

handler = logging.StreamHandler()
handler.setFormatter(colorlog.ColoredFormatter(
    "%(log_color)s%(asctime)s %(levelname)-8s %(message)s",
    datefmt='%Y-%m-%d %H:%M:%S',
    log_colors={
        'DEBUG': 'cyan',
        'INFO': 'green',
        'WARNING': 'yellow',
        'ERROR': 'red,bg_white',
        'CRITICAL': 'red,bg_white',
    },
))

logger = colorlog.getLogger()
logger.addHandler(handler)
logger.setLevel(logging.INFO)

async def main():    
    parser = argparse.ArgumentParser(description="""Lumeo Universal Bridge Uploader uploads media files to Lumeo cloud, \
                                                    associates them with a virtual camera and queues them for processing. \
                                                    Learn more at https://docs.lumeo.com/docs/universal-bridge """)
    parser.add_argument('--app_id', required=True, help='Application (aka Workspace) ID')
    parser.add_argument('--token', required=True, help='Access (aka API) Token.')
    parser.add_argument('--camera_id', help='Camera ID of an existing camera, to associate with the uploaded files')
    parser.add_argument('--camera_external_id', help='Use your own unique camera id to find or create a virtual camera, and associate with the uploaded files')
    
    parser.add_argument('--pattern', help='Glob pattern for files to upload')
    parser.add_argument('--file_list', help='Comma separated list of file URIs to queue')
    parser.add_argument('--csv_file', help='CSV file containing file_uri and corresponding camera_external_id or camera_id')
    parser.add_argument('--s3_bucket', help='S3 bucket name to use as source for files')
    parser.add_argument('--s3_access_key_id', help='S3 Access key ID')
    parser.add_argument('--s3_secret_access_key', help='S3 secret access key')
    parser.add_argument('--s3_region', help='S3 region if using AWS S3 bucket. Either s3_region or s3_endpoint_url must be specified.')
    parser.add_argument('--s3_endpoint_url', help='S3 endpoint URL. Either s3_region or s3_endpoint_url must be specified.')
    parser.add_argument('--s3_prefix', help='S3 path prefix to filter files. Optional.')

    parser.add_argument('--delete_processed', help='Delete successfully processed files from the local folder after uploading')
    parser.add_argument('--log_level', default='INFO', help='Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)')
    parser.add_argument('--batch_size', type=int, default=5, help='Number of concurrent uploads to process at a time. Default 5.')
    parser.add_argument('--queue_size', action='store_true', help='Print the current queue size')
    args = parser.parse_args()
    
    if len(vars(args)) <= 1:
        parser.print_usage()
        return
    
    logger.setLevel(args.log_level.upper())
    api_client = LumeoApiClient(args.app_id, args.token)
    queuing_lock = asyncio.Lock()
    tasks = []
    start_time = time.time()

    if not (args.pattern or args.file_list or args.csv_file or args.s3_bucket):
        if args.queue_size:
            await print_queue_size(api_client)
        else:
            print("Please provide either a glob pattern, a file list, a csv file, or an S3 bucket")
        return        
    
    if args.csv_file:
        with open(args.csv_file, 'r') as f:
            reader = csv.reader(f)
            next(reader)  # Skip the header
            tasks = []
            for row in reader:
                if len(row) == 3:
                    file_uri, camera_external_id, camera_id = row
                    file_uri = file_uri.strip() if len(file_uri.strip()) > 0 else None
                    camera_external_id = camera_external_id.strip() if len(camera_external_id.strip()) > 0 else None
                    camera_id = camera_id.strip() if len(camera_id.strip()) > 0 else None
                    tasks.append(LumeoUniversalBridgeUploader(api_client, queuing_lock, file_uri, camera_external_id, camera_id).process())
                    
    elif args.file_list:
        if not args.camera_id and not args.camera_external_id:
            print("Please provide either a camera_id or camera_external_id when specifying a file_list")
            return
        
        file_list = args.file_list.split(',')
        tasks = []
        for file_uri in file_list:
            tasks.append(LumeoUniversalBridgeUploader(api_client,  queuing_lock, file_uri, args.camera_external_id, args.camera_id).process())
    
    elif args.pattern:
        if not args.camera_id and not args.camera_external_id:
            print("Please provide either a camera_id or camera_external_id when specifying a glob pattern")
            return
        
        for file_path in glob.glob(args.pattern):
            tasks.append(LumeoUniversalBridgeUploader(api_client, queuing_lock, file_path, args.camera_external_id, args.camera_id).process())
    
    elif args.s3_bucket:
        if not args.camera_id and not args.camera_external_id:
            print("Please provide either a camera_id or camera_external_id when specifying an S3 bucket")
            return
        
        if not args.s3_access_key_id or not args.s3_secret_access_key:
            print("Please provide AWS credentials when using an S3 bucket")
            return
        
        if not args.s3_region and not args.s3_endpoint_url:
            print("Please provide AWS S3 region OR endpoint URL when using an S3 bucket")
            return
        
        s3_file_list = await get_s3_file_list(args.s3_bucket, 
                                              args.s3_access_key_id, args.s3_secret_access_key, 
                                              args.s3_region, args.s3_endpoint_url, 
                                              args.s3_prefix)
        for signed_url in s3_file_list:
            tasks.append(LumeoUniversalBridgeUploader(api_client, queuing_lock, signed_url, args.camera_external_id, args.camera_id).process())
        
    # Wait for processing to finish
    results = []
    sem = asyncio.Semaphore(args.batch_size)
    async def process_with_limit(task):
        async with sem:
            return await task

    # Process tasks concurrently in batches of size args.batch_size
    tasks = [process_with_limit(task) for task in tasks]
    for completed in asyncio.as_completed(tasks):
        result = await completed
        results.append(result)

    # Log results
    end_time = time.time()
    successful_tasks = results.count(True)
    failed_tasks = results.count(False)    
    print(f"Finished queueing. Results : Total {len(tasks)}, Successful {successful_tasks}, Failed {failed_tasks}.")    
    print(f"Total processing time: {round(end_time - start_time, 2)} seconds")
    
    # Get the deployment queue for this app
    await print_queue_size(api_client)
    return   
    

async def get_s3_file_list(bucket_name, access_key_id, secret_access_key, region, endpoint_url=None, prefix=None):
    file_list = []
    s3_config = {
        'aws_access_key_id': access_key_id,
        'aws_secret_access_key': secret_access_key,
        'config': Config(signature_version='s3v4')
    }
    
    if endpoint_url:
        s3_config['endpoint_url'] = endpoint_url
    else:
        s3_config['region_name'] = region

    s3 = boto3.client('s3', **s3_config)

    paginator = s3.get_paginator('list_objects_v2')
    pagination_params = {'Bucket': bucket_name}
    if prefix:
        pagination_params['Prefix'] = prefix

    for page in paginator.paginate(**pagination_params):
        for obj in page.get('Contents', []):
            signed_url = s3.generate_presigned_url('get_object',
                                                    Params={'Bucket': bucket_name,
                                                            'Key': obj['Key']},
                                                    ExpiresIn=604800)  # 1 week in seconds    
            file_list.append(signed_url)
            
    return file_list
    
    
async def print_queue_size(api_client: LumeoApiClient):
    # Get the deployment queue for this app
    deployment_queue_id = await api_client.get_deployment_queue_id()
    queue_entries = await api_client.get_queue_entries(deployment_queue_id)    
    print(f"Current Queue size: {queue_entries['total_elements']}")    
    
        
if __name__ == "__main__":
    asyncio.run(main())
