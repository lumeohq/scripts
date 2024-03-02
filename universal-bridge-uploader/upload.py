#!python3

import argparse
import asyncio
import colorlog
import csv
import glob
import logging
import time
from pathlib import Path

from api import LumeoApiClient
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
    parser.add_argument('--pattern', help='Glob pattern for files to upload')
    parser.add_argument('--file_list', help='Comma separated list of file URIs to queue')
    parser.add_argument('--camera_id', help='Camera ID of an existing camera, to associate with the uploaded files')
    parser.add_argument('--camera_external_id', help='Use your own unique camera id to find or create a virtual camera, and associate with the uploaded files')
    parser.add_argument('--csv_file', help='CSV file containing file_uri and corresponding camera_external_id or camera_id')
    parser.add_argument('--delete_processed', help='Delete successfully processed files from the local folder after uploading')
    parser.add_argument('--log_level', default='INFO', help='Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)')
    parser.add_argument('--batch_size', type=int, default=5, help='Number of concurrent uploads to process at a time. Default 5.')
    parser.add_argument('--queue_size', action='store_true', help='Print the current queue size')
    args = parser.parse_args()
    
    logger.setLevel(args.log_level.upper())
    api_client = LumeoApiClient(args.app_id, args.token)
    queuing_lock = asyncio.Lock()
    tasks = []
    start_time = time.time()

    if not (args.pattern or args.file_list or args.csv_file):
        if args.queue_size:
            await print_queue_size(api_client)
        else:
            print("Please provide either a glob pattern or a file list or a csv file")
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
    
    
async def print_queue_size(api_client: LumeoApiClient):
    # Get the deployment queue for this app
    deployment_queue_id = await api_client.get_deployment_queue_id()
    queue_entries = await api_client.get_queue_entries(deployment_queue_id)    
    print(f"Current Queue size: {queue_entries['total_elements']}")    
    
        
if __name__ == "__main__":
    asyncio.run(main())
