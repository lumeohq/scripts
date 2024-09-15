import asyncio
from typing import Union
import logging
import os

from api import LumeoApiClient
from lumeo_types import JsonObject

class LumeoUniversalBridgeUploader():
    
    def __init__(self, 
                 api_client: LumeoApiClient,
                 queuing_lock: asyncio.Lock,
                 file_uri: str = None,
                 camera_external_id: str = None, 
                 camera_id: str = None,
                 
                 file_stream_id: str = None,
                 pipeline_id: str = None,
                 deployment_config: JsonObject = None,
                 
                 file_tag_id: str = None,
                 
                 deployment_prefix: str = None,
                 delete_processed_files: bool = False):
        
        self.api_client = api_client
        self.queuing_lock = queuing_lock
        self.file_uri = file_uri
        self.camera_external_id = camera_external_id
        self.camera_id = camera_id
        
        self.file_stream_id = file_stream_id
        self.pipeline_id = pipeline_id
        self.deployment_config = deployment_config
        
        self.file_tag_id = file_tag_id
        self.deployment_prefix = deployment_prefix or ''
        self.delete_processed_files = delete_processed_files        
        
    def log_debug(self, message: str) -> None:
        logging.debug(f"[{self.file_uri or self.file_stream_id}] {message}")
        
    def log_info(self, message: str) -> None:
        logging.info(f"[{self.file_uri or self.file_stream_id}] {message}")

    def log_warning(self, message: str) -> None:
        logging.warning(f"[{self.file_uri or self.file_stream_id}] {message}")   

    def log_error(self, message: str) -> None:
        logging.error(f"[{self.file_uri or self.file_stream_id}] {message}")   
        
    async def get_file_size(self) -> int:
        # Get the size of the file
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, os.path.getsize, self.file_uri)
    
    async def delete_file(self) -> None:
        # Delete the file
        self.log_info(f"Deleting file {self.file_uri} from local folder")
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, os.remove, self.file_uri)

    async def get_deployment_queue_id(self) -> str:
        # Get the deployment queue for this app
        deployment_queue_id = await self.api_client.get_deployment_queue_id()
        if not deployment_queue_id:
            err = f"Deployment queue not found for app {self.api_client.application_id}"
            self.log_error(err)
            raise Exception(err)
        return deployment_queue_id                


    async def get_or_create_camera_with_external_id(self, camera_external_id) -> JsonObject:
        # Get or create a camera using the camera external identifier
        camera = await self.api_client.get_camera_with_external_id(camera_external_id)
        if not camera:
            camera = await self.api_client.create_virtual_camera(camera_external_id, camera_external_id)
        return camera


    async def get_camera_with_id(self, camera_id) -> Union[JsonObject, None]:
        # Get a camera using the camera id. If doesnt exist, return None
        return await self.api_client.get_camera_with_id(camera_id)
    

    async def upload_and_create_input_stream(self, file_path: str, camera: Union[JsonObject,None]) -> Union[JsonObject, None]:
        input_stream = None
        camera_id = camera["id"] if camera else None   
        try:
            log_msg = f"Uploading file {file_path}, creating input stream"
            if camera_id:
                log_msg += f" for camera {camera_id}"
            self.log_info(log_msg)
            
            file_name = file_path.split("/")[-1]

            # Upload the file to Lumeo cloud and delete from local dir
            file_size = await self.get_file_size()
            file_object = await self.api_client.create_file(file_name, file_size, camera_id)
            file_uploaded = await self.api_client.upload_file(file_object["data_url"], file_object["metadata_url"], file_path)
            if file_uploaded:
                await self.api_client.set_file_status(file_object["id"], "uploaded")
                # Create input stream from uploaded file
                input_stream = await self.api_client.create_lumeo_file_stream(file_object, camera_id)
                if self.file_tag_id:
                    await self.api_client.add_tag_to_file(file_object["id"], self.file_tag_id)
                    await self.api_client.add_tag_to_stream(input_stream["id"], self.file_tag_id)                                
        except Exception as e:
            self.log_error(f"File {file_path} does not exist. Skipping. Error: {e}")
            
        return input_stream
    
    
    async def create_input_stream(self, file_url: str, camera: Union[JsonObject,None]) -> Union[JsonObject, None]:
        camera_id = None
        log_msg = f"Creating input stream for file {file_url}"
        if camera:
            log_msg += f" for camera {camera['id']}"
            camera_id = camera['id']
            
        self.log_info(log_msg)
        file_name = file_url.split("/")[-1].split("?")[0]        
        input_stream = await self.api_client.create_file_stream(file_name, file_url, camera_id)        
        if input_stream:
            if self.file_tag_id:
                await self.api_client.add_tag_to_stream(input_stream["id"], self.file_tag_id)
        return input_stream
                                                
                                                
    async def queue_universal_bridge_deployment(self, input_stream, camera):   
        self.log_info(f"Queuing universal bridge deployment with input stream {input_stream['id']}")

        # Get the camera's default pipeline. Clone a reference
        # deployment if one exists, otherwise, we queue a new deployment and
        # set it as the reference.
        
        async with self.queuing_lock: 
            camera = await self.get_camera_with_id(camera["id"])        
            default_pipeline_id = camera["reference_pipeline_ids"][0] if camera["reference_pipeline_ids"] else None
            reference_deployment_id = camera["reference_deployment_ids"][0] if camera["reference_deployment_ids"] else None

            if default_pipeline_id:
                # Get pipeline name
                pipeline = await self.api_client.get_pipeline(default_pipeline_id)
                pipeline_name = pipeline["name"] if pipeline else default_pipeline_id
                deployment_configuration = {}
                deployment_name = f"{self.deployment_prefix}{camera['name']}-{pipeline_name}"
                
                if reference_deployment_id:
                    # Clone the reference deployment's configuration
                    reference_deployment = await self.api_client.get_deployment(reference_deployment_id)
                    if reference_deployment:
                        deployment_configuration = reference_deployment["configuration"]
                        deployment_configuration["video1"]["source_id"] = input_stream["id"]
                        deployment_configuration["video1"]["source_type"] = "stream"
                else:
                    # Create a new deployment configuration
                    deployment_configuration = {"video1": {"source_type": "stream", "source_id": input_stream["id"]}}
                    deployment_name += " reference deployment"

                deployment_queue_id = await self.get_deployment_queue_id()
                queued_deployment = await self.api_client.queue_deployment(
                    deployment_queue_id, default_pipeline_id, deployment_configuration, deployment_name
                )

                if queued_deployment:                    
                    if "deployment_id" in queued_deployment:
                        deployment_id = queued_deployment["deployment_id"]
                        entry_id = queued_deployment["id"]
                    else:
                        deployment_id = queued_deployment["id"]
                        entry_id = None

                    if not reference_deployment_id:
                        # Set the queued deployment as the reference deployment
                        await self.api_client.set_camera_reference_deployments(camera["id"], deployment_id)
                                
                    # Log a deployed event
                    await self.api_client.create_event(
                        "api.deployment_queued", "info", f"Queued deployment for {camera['external_id']}.", None, camera["id"], None
                    )    
                
                    self.log_info(f"Success queueing deployment. Entry ID: {entry_id}, Deployment ID: {deployment_id}")            
                else:
                    self.log_error(f"Failed queueing deployment for camera {camera['id']} with input stream {input_stream['id']}")
                    
                return queued_deployment is not None
            
            else:
                self.log_warning(f"Camera {camera['id']} does not have a default pipeline. Cannot queue deployment.")
        
        return False
            
                        
    async def process_new_file(self):                        
        # If the file is not a valid file, skip it.
        file_name = self.file_uri.split("/")[-1].split("?")[0]
        if file_name.split(".")[-1] not in ["mp4", "avi", "mov", "mkv", "wmv", "flv", "webm", "jpg", "jpeg", "png"]:
            self.log_warning(f"File is not a video file. Skipping : {self.file_uri}")
            return False
                        
        self.log_info(f"Processing file {file_name} with camera_external_id={self.camera_external_id} / camera_id={self.camera_id}")        
        
        # Get or create a camera using the camera external identifier
        camera = None
        if self.camera_external_id:
            camera = await self.get_or_create_camera_with_external_id(self.camera_external_id)
        elif self.camera_id:
            camera = await self.get_camera_with_id(self.camera_id)

        if camera:
            self.log_debug(f"Camera found: {camera['id']} {camera['external_id']} {camera['name']}")
        elif self.pipeline_id is None and (self.camera_external_id or self.camera_id):
            self.log_error(f"Camera with external id {self.camera_external_id} or id {self.camera_id} not found or could not be created.")
            return False

        # Get the size of the file
        if not self.file_uri.startswith("http"):
            input_stream = await self.upload_and_create_input_stream(self.file_uri, camera)
        else:
            input_stream = await self.create_input_stream(self.file_uri, camera)
            
        #Queue the deployment
        if input_stream and camera:
            success = await self.queue_universal_bridge_deployment(input_stream, camera)
        elif input_stream and self.pipeline_id:
            success = await self.queue_regular_deployment(input_stream)
        else:
            success = False
                
        # Delete the file if it was processed successfully and delete_processed_files is True
        if success and self.delete_processed_files and not self.file_uri.startswith("http"):
            self.log_info(f"Deleting file from local folder")
            await self.delete_file()
                                    
        self.log_info(f"Processed file.")

        return success


    async def process_existing_file(self):
        self.log_info(f"Queueing file stream deployment with pipeline_id={self.pipeline_id}")
        success = False

        input_stream = await self.api_client.get_stream_with_id(self.file_stream_id)
        
        success = await self.queue_regular_deployment(input_stream) if input_stream else False
        
        self.log_info(f"Processed existing file.")
        return success
    
    
    async def queue_regular_deployment(self, input_stream):
        self.log_info(f"Queuing regular deployment with input stream {input_stream['id']} / pipeline_id={self.pipeline_id}")
        success = False
        
        async with self.queuing_lock: 
            deployment_configuration = self.deployment_config if self.deployment_config else {}
            
            pipeline = await self.api_client.get_pipeline(self.pipeline_id)                        
            if pipeline:
                deployment_name = f"{self.deployment_prefix}{input_stream['name']}-{pipeline['name']}"
                
                if 'video1' not in deployment_configuration:
                    deployment_configuration['video1'] = {}
                deployment_configuration['video1']["source_id"] = input_stream['id']
                deployment_configuration['video1']["source_type"] = "stream"
                
                deployment_queue_id = await self.get_deployment_queue_id()
                self.log_info(f"Queueing deployment, queue_id={deployment_queue_id} / pipeline_id={self.pipeline_id} / deployment_name={deployment_name}, deployment_config={deployment_configuration}")
                # success = True
                queued_deployment = await self.api_client.queue_deployment(
                    deployment_queue_id, self.pipeline_id, deployment_configuration, deployment_name
                )                

                if queued_deployment:                    
                    if "deployment_id" in queued_deployment:
                        deployment_id = queued_deployment["deployment_id"]
                        entry_id = queued_deployment["id"]
                    else:
                        deployment_id = queued_deployment["id"]
                        entry_id = None
                                
                    # Log a deployed event
                    await self.api_client.create_event(
                        "api.deployment_queued", "info", f"Queued deployment for {input_stream['id']}.", None, None, input_stream['id']
                    )    
                
                    self.log_info(f"Success queueing deployment. Entry ID: {entry_id}, Deployment ID: {deployment_id}")  
                    success = True                 
            else:
                self.log_error(f"Failed queueing deployment for file_stream_id={input_stream['id']} : Pipeline with id {self.pipeline_id} not found.")                                        
        return success
    
    async def process(self):
        if self.file_uri:
            return await self.process_new_file()
        elif self.file_stream_id and self.pipeline_id:
            return await self.process_existing_file()
        else:
            self.log_error("Either new file path/url and camera id/external id must be provided, or existing file tag and pipeline id must be provided.")
            return False    