import logging
from typing import Dict, Optional, Tuple, Union, cast

import aiofiles
import backoff
import httpx
from aiocache import cached
from backoff import _typing as backoff_typing
from httpx import HTTPError, HTTPStatusError, Timeout

from lumeo_types import JsonObject

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("aiocache").setLevel(logging.WARNING)

def backoff_handler(details: backoff_typing.Details) -> None:
    logging.warning(
        f"Backing off {details['wait']:0.1f} seconds after {details['tries']} tries"
        f" calling function {details['target'].__name__} with args {details['args']} and kwargs {details['kwargs']}"
    )

def backoff_no_giveup(exc: Exception) -> bool:
    return False

class LumeoApiClient:
    client: httpx.AsyncClient = httpx.AsyncClient(timeout=Timeout(60.0))

    def __init__(self, application_id: str, token: str, api_base_url: str = 'https://api.lumeo.com') -> None:
        self.headers: Dict[str, str] = {"Authorization": f"Bearer {token}"}
        self.base_url: str = api_base_url
        self.application_id = application_id
        self.client.base_url = self.base_url        
        self.client.headers = self.headers
        logging.info(f"Initialized Lumeo API client with base url: {self.base_url}, app_id: {application_id}, token: xxxx{token[-4:]}")

    def log_debug(self, message: str) -> None:
        logging.debug(f"{message}")
        
    def log_info(self, message: str) -> None:
        logging.info(f"{message}")

    def log_warning(self, message: str) -> None:
        logging.warning(f"{message}")

    def log_error(self, message: str) -> None:
        logging.error(f"{message}")
        
    async def close(self) -> None:
        await self.client.aclose()
        
    @backoff.on_exception(backoff.expo, HTTPError, max_time=60, on_backoff=backoff_handler, giveup=backoff_no_giveup, logger=None)        
    async def request(self, request_desc: str, method: str, url: str, timeout: Optional[Union[float, httpx.Timeout]] = None, **kwargs) -> Union[httpx.Response, None]:
        try:
            self.log_debug(f"API request: {request_desc} : {url}")
            response = await self.client.request(method, url, headers=self.headers, timeout=timeout, **kwargs)
            response.raise_for_status()
            return response
        except HTTPError as err:            
            # Log error    
            print(f"{err} : {err.response.text}")
            message = err.response.status_code if isinstance(err, HTTPStatusError) else err #error.response.text
            self.log_warning(f"API request failed. Will retry. {request_desc}: {message}")
            
            # If the error is a 400, 401, 403, 404, 409, dont raise exception, return None
            if isinstance(err, HTTPStatusError) and err.response.status_code in {400, 401, 403, 404, 409}:
                return None                    
            
            # Re-raise the exception to be caught by backoff
            raise
        except Exception as e:
            # This will catch the HTTPError re-raised by backoff after max_time is reached
            self.log_error(f"API Error {request_desc}: {message}")
            return None
        
    async def create_event(
        self, event_type: str, severity: str, payload: str, deployment_id: Union[str, None], camera_id: Union[str, None]
    ) -> None:
        event_json = {
            "category": "ftp-gateway",
            "event_type": event_type,
            "severity": severity,
            "payload": payload,
            "context": None,
            "related_entities": {
                "deployment_id": None,
                "camera_id": None,
                "stream_id": None,
                "gateway_id": None,
                "file_id": None,
                "node_id": None,
            },
        }

        if deployment_id:
            event_json["object"] = "deployment"
            event_json["object_id"] = deployment_id
            event_json["related_entities"]["deployment_id"] = deployment_id

        if camera_id:
            event_json["object"] = "camera"
            event_json["object_id"] = camera_id
            event_json["related_entities"]["camera_id"] = camera_id

        await self.request(f"Creating event {event_type}", "POST", f"/v1/apps/{self.application_id}/events", json=event_json)

        return

    async def get_camera_with_external_id(self, external_id: str) -> Union[JsonObject, None]:
        response = await self.request(
            f"Getting camera with external id {external_id}",
            "GET",
            f"/v1/apps/{self.application_id}/cameras",
            params={"pagination": "cursor", "limit": 1, "external_ids[]": external_id},
        )
        response_json = response.json()
        if "data" in response_json and len(response_json["data"]) > 0:
            return response_json["data"][0]
        else:
            return None
        
    async def get_camera_with_id(self, camera_id: str) -> Union[JsonObject, None]:
        response = await self.request(
            f"Getting camera with id {camera_id}",
            "GET",
            f"/v1/apps/{self.application_id}/cameras/{camera_id}"
        )        
        if response:            
            return response.json()
        else:
            return None        

    async def create_virtual_camera(self, external_id: str, name: str) -> JsonObject:
        response = await self.request(
            f"Creating virtual camera with external id {external_id}",
            "POST",
            f"/v1/apps/{self.application_id}/cameras",
            json={
                "external_id": external_id,
                "name": name,
                "model": "Virtual",
                "status": "unknown",
                "uri": None,
                "conn_type": "virtual",
            },
        )
        response_json = response.json()
        return response_json

    async def set_camera_reference_deployments(self, camera_id: str, deployment_id: str) -> None:
        await self.request(
            f"Setting camera reference deployment for camera {camera_id}",
            "POST",
            f"/v1/apps/{self.application_id}/cameras/{camera_id}/reference_deployments",
            json=[deployment_id],
        )

    @cached(ttl=3600)
    async def get_deployment_queue_id(self) -> str:
        response = await self.request(f"Getting deployment queue", "GET", f"/v1/apps/{self.application_id}/deployment_queues")
        response_json = response.json()
        return response_json and response_json[0]["id"]

    async def create_file(self, file_name: str, file_size: int, camera_id: str) -> JsonObject:
        response = await self.request(
            f"Creating file with file name {file_name}",
            "POST",
            f"/v1/apps/{self.application_id}/files",
            json={
                "name": file_name,
                "size": file_size,
                "gateway_id": None,
                "pipeline_id": None,
                "node_id": None,
                "deployment_id": None,
                "camera_id": camera_id,
                "stream_id": None,
                "metadata": None,
                "description": None,
            },
        )
        response_json = response.json()
        return response_json

    async def upload_file(self, data_url: str, metadata_url: str, file_path: str) -> None:
        self.log_debug(f"API Uploading file {file_path} ...")
        try:
            async with aiofiles.open(file_path, "rb") as f:
                file_content = await f.read()
                response = await self.client.put(data_url, data=file_content)
                response.raise_for_status()

            await self.client.put(metadata_url, data="null", timeout=3600.0)
            return True
        except HTTPError as error:
            message = error.response.text if isinstance(error, HTTPStatusError) else error
            self.log_error(f"Error while uploading file {file_path}: {message}")

        return False

    async def set_file_status(self, file_id: str, status: str) -> None:
        await self.request(
            f"Setting file status for file {file_id}",
            "PUT",
            f"/v1/apps/{self.application_id}/files/{file_id}/cloud_status",
            data=status,
        )

    async def create_lumeo_file_stream(self, file: JsonObject, camera_id: str) -> JsonObject:
        return await self.create_file_stream(file['name'], f"lumeo://{file['id']}", camera_id)
    
    async def create_file_stream(self, name: str, url: str, camera_id: str) -> JsonObject:        
        response = await self.request(
            f"Creating file stream for file {name}",
            "POST",
            f"/v1/apps/{self.application_id}/streams",
            json={
                "name": name[:200],
                "uri": url,
                "source": "uri_stream",
                "stream_type": "file",
                "camera_id": camera_id,
                "gateway_id": None,
                "status": "unknown",
            },
        )
        response_json = response.json()
        return response_json
    
    @cached(ttl=3600)
    async def get_pipeline(self, pipeline_id: str) -> Union[JsonObject, None]:
        response = await self.request(
            f"Getting pipeline {pipeline_id}", "GET", f"/v1/apps/{self.application_id}/pipelines/{pipeline_id}"
        )
        response_json = response.json()
        return response_json

    @cached(ttl=3600)
    async def get_deployment(self, deployment_id: str) -> Union[JsonObject, None]:
        response = await self.request(
            f"Getting deployment {deployment_id}", "GET", f"/v1/apps/{self.application_id}/deployments/{deployment_id}"
        )
        response_json = response.json()
        return response_json

    async def queue_deployment(
        self, queue_id: str, pipeline_id: str, deployment_configuration: JsonObject, deployment_name: Union[str, None]
    ) -> JsonObject:
        response = await self.request(
            f"Queueing deployment for pipeline {pipeline_id} with name '{deployment_name}', configuration {deployment_configuration}",
            "POST",
            f"/v1/apps/{self.application_id}/deployment_queues/{queue_id}/entries",
            json={
                "pipeline_id": pipeline_id,
                "deployment_name": deployment_name,
                "deployment_configuration": deployment_configuration,
            },
        )
        response_json = response.json()
        return response_json

    async def get_queue_entries(self, queue_id: str) -> JsonObject:
        response = await self.request(
            f"Getting queue info for queue {queue_id}", 
            "GET", 
            f"/v1/apps/{self.application_id}/deployment_queues/{queue_id}/entries",
            params={"pagination": "offset", "page": 1, "limit": 1},            
        )
        response_json = response.json()
        return response_json
