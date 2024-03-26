import requests
import argparse

state = {
    "lumeo_app_id": "",
    "lumeo_app_token": "",
    "verkada_org_id": "",
    "verkada_api_key": "",
    "verkada_region": "api",
    "session": requests.Session(),
    "cameras": []
}

def fetch_cameras():
    """
    This script fetches a list of Verkada cameras, presents the list to the user,
    asks for the cameras to be imported, and then uses the Lumeo API to create streams with a specific stream URL.
    """
    # Query Verkada API for list of cameras
    camera_fetch_complete = False
    next_page = None
    while not camera_fetch_complete:
        get_cameras_url = (
            "https://api.lumeo.com/integrations/verkada/cameras?"
            f"api_key={state['verkada_api_key']}&"
            f"region={state['verkada_region']}"
        )
        if next_page:
            get_cameras_url += f"&page_token={next_page}"
        
        response = state['session'].get(get_cameras_url)
        
        if response.ok:
            response_json = response.json()
            cameras = response_json['cameras']
            next_page = response_json.get('next_page_token', None)
            cameras.sort(key=lambda x: (x.get('site', ''), x.get('name', '')))
            state['cameras'].extend(cameras)
            
            if not next_page:
                camera_fetch_complete = True
        else:
            print(f"Failed to fetch cameras from Verkada. Error: {response.text}")
            break

            
    if state['cameras']:        
        # Present the list of cameras to the user
        print("Available cameras:")
        print("{:<10} {:<40} {:<20} {:<20} {:<20} {:<20}".format('Index', 'Name', 'Site', 'Mac', 'Model', 'Serial'))
        for index, camera in enumerate(state['cameras']):
            print("{:<10} {:<40} {:<20} {:<20} {:<20} {:<20}".format(index, 
                                                                     camera.get('name',''), 
                                                                     camera.get('site',''), 
                                                                     camera.get('mac',''), 
                                                                     camera.get('model',''), 
                                                                     camera.get('serial','')))

def import_cameras():
    # Ask for the cameras to be imported
    camera_indices_to_import = input("Enter the indices or names of the cameras to import, separated by commas (Ctrl-c to exit) : ").split(',')

    # Use the Lumeo API to create streams with a specific stream URL
    for camera_index_or_name in camera_indices_to_import:
        camera = None
        if camera_index_or_name.isdigit():
            camera_index = int(camera_index_or_name)
            if 0 <= camera_index < len(state['cameras']):
                camera = state['cameras'][camera_index]
        else:
            camera = next((c for c in state['cameras'] if c['name'] == camera_index_or_name), None)

        if camera:
            import_camera(camera)
        else:
            print(f"No camera found with index or name {camera_index_or_name}")    


def import_camera(camera):
    """
    This function uses the Lumeo API to create a stream with a specific stream URL.
    """
    stream_url = (
        "https://api.lumeo.com/integrations/verkada/stream?"
        f"api_key={state['verkada_api_key']}&"
        f"org_id={state['verkada_org_id']}&"
        f"camera_id={camera['camera_id']}&"
        f"quality={state['quality']}&"
        f"region={state['verkada_region']}"        
    )
    lumeo_payload = {
        "name": f"{camera['site']} - {camera['name']} ({state['quality']})",
        "source": "uri_stream",
        "stream_type": "rtsp",
        "uri": stream_url
    }
    response = state['session'].post(f"https://api.lumeo.com/v1/apps/{state['lumeo_app_id']}/streams", 
                                     json=lumeo_payload,
                                     headers={"Authorization": f"Bearer {state['lumeo_app_token']}"})
    if response.status_code == 201:
        print(f"Successfully imported camera {camera['name']}")
    else:
        print(f"Failed to import camera {camera['name']}. Error: {response.text}")
    

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description='Fetch Verkada cameras and import them to Lumeo.')
    parser.add_argument("-l", "--lumeo_app_id", required=True, help='Your Lumeo application ID.')
    parser.add_argument("-t", "--lumeo_app_token", required=True, help='Your Lumeo application token.')
    parser.add_argument("-o", "--verkada_org_id", required=True, help='Your Verkada organization ID.')
    parser.add_argument("-a", "--verkada_api_key", required=True, help='Verkada API Key. Must have Camera fetch and Streaming - Live/historical permissions.')
    parser.add_argument("-r", "--verkada_region", default="api", required=False, help='Verkada region. Default is api. For EU use api.eu')
    parser.add_argument("-q", "--quality", default='low_res', help='Options: high_res/low_res. Determines quality of video stream. Default is low_res.')    
    args = parser.parse_args()

    state['lumeo_app_id'] = args.lumeo_app_id
    state['lumeo_app_token'] = args.lumeo_app_token
    state['verkada_org_id'] = args.verkada_org_id
    state['verkada_api_key'] = args.verkada_api_key
    state['verkada_region'] = args.verkada_region    
    state['quality'] = args.quality

    fetch_cameras()
    
    while True:
        import_cameras()        
        if input("Do you want to import more cameras? (y/n): ").lower() != 'y':
            break
        

