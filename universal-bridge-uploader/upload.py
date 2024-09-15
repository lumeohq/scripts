#!python3

import argparse
import asyncio
import boto3
import colorlog
import csv
import glob
import json
import logging
import time
import uuid
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

    tasks = []    
    queuing_lock = asyncio.Lock()
    start_time = time.time()
    
    parser = create_args_parser()
    args = parser.parse_args()
    
    if not validate_args(args):
        parser.print_usage()
        return
    
    api_client = LumeoApiClient(args.app_id, args.token)

    if args.queue_size and not (args.pattern or args.file_list or args.csv_file or args.s3_bucket or args.tag):
        await print_queue_size(api_client)
        return
        
    if args.tag:
        # tag is to be applied. get or create tag path.
        try:
            uuid.UUID(args.tag)
            tag_id = args.tag
        except ValueError:
            tag_id = await api_client.get_tag_id_by_path(args.tag)
            if not tag_id and (args.csv_file or args.file_list or args.pattern or args.s3_bucket):
                logging.info(f"Tag path '{args.tag}' not found. Creating...")
                tag_id = await api_client.create_tag_path(args.tag)
            if not tag_id:
                logging.error(f"Tag path '{args.tag}' not found.")
                return
        args.tag = tag_id
          
    
    if args.csv_file:
        tasks = await process_csv_file(args, api_client, queuing_lock)
    elif args.file_list:
        tasks = await process_file_list(args, api_client, queuing_lock)
    elif args.pattern:
        tasks = await process_glob_pattern(args, api_client, queuing_lock)
    elif args.s3_bucket:
        tasks = await process_s3_bucket(args, api_client, queuing_lock)
    elif args.tag:
        tasks = await process_existing_files_with_tag(args, api_client, queuing_lock)
    else:
        tasks = []
        
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

def create_args_parser():
    parser = argparse.ArgumentParser(description="""Lumeo Universal Bridge Uploader uploads media files to Lumeo cloud, \
                                                    (optionally) associates them with a virtual camera, and queues them for processing. \
                                                    Learn more at https://docs.lumeo.com/docs/universal-bridge """)
    
    required_group = parser.add_argument_group('Authentication Args')
    required_group.add_argument('--app_id', required=True, help='Application (aka Workspace) ID')
    required_group.add_argument('--token', required=True, help='Access (aka API) Token.')
    
    file_source_group = parser.add_argument_group('Source Files (one of pattern, file_list, csv_file, s3_bucket or tag is required)')
    file_source_group.add_argument('--pattern', help='Glob pattern for files to upload')
    file_source_group.add_argument('--file_list', help='Comma separated list of file URIs to queue')
    file_source_group.add_argument('--csv_file', help='CSV file containing file_uri and corresponding camera_external_id or camera_id')
    file_source_group.add_argument('--s3_bucket', help='S3 bucket name to use as source for files')
    file_source_group.add_argument('--s3_access_key_id', help='S3 Access key ID')
    file_source_group.add_argument('--s3_secret_access_key', help='S3 secret access key')
    file_source_group.add_argument('--s3_region', help='S3 region if using AWS S3 bucket. Either s3_region or s3_endpoint_url must be specified.')
    file_source_group.add_argument('--s3_endpoint_url', help='S3 endpoint URL. Either s3_region or s3_endpoint_url must be specified.')
    file_source_group.add_argument('--s3_prefix', help='S3 path prefix to filter files. Optional.')
    file_source_group.add_argument('--tag', help='Tag to apply to uploaded files. Can be tag uuid, tag name or tag path (e.g. "tag1/tag2/tag3").'
                                      'If specified without pattern/file_list/csv_file/s3_bucket, will process existing files with that tag.')
    
    camera_group = parser.add_argument_group('Associate with Camera (gets pipeline & deployment config from camera)')    
    camera_group.add_argument('--camera_id', help='Camera ID of an existing camera, to associate with the uploaded files')
    camera_group.add_argument('--camera_external_id', help='Use your own unique camera id to find or create a virtual camera, and associate with the uploaded files')    
    
    pipeline_group = parser.add_argument_group('Deployment Args (applied only when camera not specified)')
    pipeline_group.add_argument('--pipeline_id', help='Pipeline ID to queue deployment for processing. Required if camera_id / camera_external_id not specified.')    
    pipeline_group.add_argument('--deployment_config', help='String containing a Deployment config JSON object. Video source in the config will be overridden by source files specified in this script. Ignored if camera_id or camera_external_id specified. Optional.')

    other_group = parser.add_argument_group('General Args')
    other_group.add_argument('--deployment_prefix', help='Prefix to use for deployment name. Optional.')
    other_group.add_argument('--delete_processed', help='Delete successfully processed files from the local folder after uploading')
    other_group.add_argument('--log_level', default='INFO', help='Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)')
    other_group.add_argument('--batch_size', type=int, default=5, help='Number of concurrent uploads to process at a time. Default 5.')
    other_group.add_argument('--queue_size', action='store_true', help='Print the current queue size')
    
    return parser

def validate_args(args):
    if len(vars(args)) <= 1:
        return False

    logger.setLevel(args.log_level.upper())

    if not args.queue_size:
        if not any([args.pattern, args.file_list, args.csv_file, args.s3_bucket, args.tag]):
            logging.error("Please provide either a tag for already uploaded files, glob pattern, a file list, a csv file, or an S3 bucket.")
            return False
        elif not (args.csv_file or any([args.camera_id, args.camera_external_id, args.pipeline_id])):
            logging.error("Please provide either a camera_id or camera_external_id or pipeline_id if source isnt a csv file.")
            return False
        elif args.s3_bucket and not all([args.access_key_id, args.secret_access_key]):
            logging.error("Please provide AWS credentials when using an S3 bucket")
            return False
        elif args.s3_bucket and not any([args.region, args.endpoint_url]):
            print("Please provide AWS S3 region OR endpoint URL when using an S3 bucket")
            return False
        
    
    if args.deployment_config:
        try:
            args.deployment_config = json.loads(args.deployment_config)
        except json.JSONDecodeError:
            logging.error("Invalid deployment config JSON provided")
            return False
    else:
        args.deployment_config = {}
    
    return True
    

async def process_csv_file(args, api_client, queuing_lock):
    tasks = []
    with open(args.csv_file, 'r') as f:
        reader = csv.reader(f)
        next(reader)  # Skip the header
        for row in reader:
            file_uri = None
            camera_external_id = args.camera_external_id
            camera_id = args.camera_id
            pipeline_id = args.pipeline_id
            deployment_config = args.deployment_config
            file_uri = row[0].strip() if len(row[0].strip()) > 0 else None
            if file_uri and not file_uri.startswith('#'):
                if len(row) > 1:
                    camera_external_id = row[1].strip() if len(row[1].strip()) > 0 else camera_external_id
                if len(row) > 2:
                    camera_id = row[2].strip() if len(row[2].strip()) > 0 else camera_id
                if len(row) > 3:
                    pipeline_id = row[3].strip() if len(row[3].strip()) > 0 else pipeline_id
                if len(row) > 4:
                    deployment_config_str = row[4].strip() if len(row[4].strip()) > 0 else "{}"
                    try:
                        deployment_config_dict = json.loads(deployment_config_str)
                    except json.JSONDecodeError:
                        logging.error(f"Invalid deployment config JSON provided in row {reader.line_num} : {row}. Skipping.")
                        continue
                    deployment_config = deployment_config_dict
                
                if not any([camera_external_id, camera_id, pipeline_id]):
                    logging.error(f"No camera_id, camera_external_id or pipeline_id provided in row {reader.line_num} : {row}. Skipping.")
                    continue
                
                tasks.append(LumeoUniversalBridgeUploader(api_client, queuing_lock, file_uri, camera_external_id, camera_id,
                                                            None, pipeline_id, deployment_config, file_tag_id=args.tag, 
                                                            deployment_prefix=args.deployment_prefix).process())
                
            else:
                logging.error(f"Invalid file URI provided in CSV file row {reader.line_num}: {row}. Skipping.")
    return tasks

async def process_file_list(args, api_client, queuing_lock):    
    tasks = []
    for file_uri in args.file_list.split(','):
        tasks.append(LumeoUniversalBridgeUploader(api_client, queuing_lock, file_uri, args.camera_external_id, args.camera_id,
                                                  None, args.pipeline_id, args.deployment_config, file_tag_id=args.tag, 
                                                            deployment_prefix=args.deployment_prefix).process())
    return tasks

async def process_glob_pattern(args, api_client, queuing_lock):
    tasks = []
    for file_path in glob.glob(args.pattern):
        tasks.append(LumeoUniversalBridgeUploader(api_client, queuing_lock, file_path, args.camera_external_id, args.camera_id,
                                                  None, args.pipeline_id, args.deployment_config, file_tag_id=args.tag, 
                                                            deployment_prefix=args.deployment_prefix).process())
    return tasks

async def process_s3_bucket(args, api_client, queuing_lock):    
    s3_file_list = await get_s3_file_list(args.bucket, args.access_key_id, args.secret_access_key, args.region, args.endpoint_url, args.prefix)
    tasks = []
    for signed_url in s3_file_list:
        tasks.append(LumeoUniversalBridgeUploader(api_client, queuing_lock, signed_url, args.camera_external_id, args.camera_id,
                                                  None, args.pipeline_id, args.deployment_config, file_tag_id=args.tag, 
                                                            deployment_prefix=args.deployment_prefix).process())
    return tasks


async def process_existing_files_with_tag(args, api_client, queuing_lock):
    tasks = []
    
    # Get all file streams with the specified tag ID
    logging.info(f"Getting existing file streams with tag {args.tag}")
    file_streams = await api_client.get_file_streams(args.tag)    
    for stream in file_streams:
        # Create a task for processing this file
        tasks.append(LumeoUniversalBridgeUploader(api_client, queuing_lock, file_stream_id=stream['id'], pipeline_id=args.pipeline_id,
                                                  deployment_config=args.deployment_config, 
                                                 deployment_prefix=args.deployment_prefix).process())

    return tasks

    

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
