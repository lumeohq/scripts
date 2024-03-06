import requests
import argparse

state = {
    "lumeo_app_id": "",
    "lumeo_app_token": "",
    "avigilon_server": "",
    "avigilon_username": "",
    "avigilon_password": "",
    "session": requests.Session(),
    "cameras": []
}

def fetch_cameras():
    """
    This script fetches a list of Avigilon cameras from the specified server, presents the list to the user,
    asks for the cameras to be imported, and then uses the Lumeo API to create streams with a specific stream URL.
    """
    # Query Avigilon API for list of cameras
    avigilon_cameras_url = (
        "https://api.lumeo.com/integrations/avigilon/cameras?"
        f"server={state['avigilon_server']}&"
        f"user={state['avigilon_username']}&"
        f"password={state['avigilon_password']}"
    )
    response = state['session'].get(avigilon_cameras_url)
    
    if response.ok:
        cameras = response.json()
        cameras.sort(key=lambda x: (x.get('location', ''), x.get('name', '')))
        state['cameras'] = cameras
        
        # Present the list of cameras to the user
        print("Available cameras:")
        print("{:<10} {:<40} {:<20} {:<20} {:<20} {:<20}".format('Index', 'Name', 'Location', 'Manufacturer', 'Model', 'Serial'))
        for index, camera in enumerate(cameras):
            print("{:<10} {:<40} {:<20} {:<20} {:<20} {:<20}".format(index, 
                                                                     camera.get('name',''), 
                                                                     camera.get('location',''), 
                                                                     camera.get('manufacturer',''), 
                                                                     camera.get('model',''), 
                                                                     camera.get('serial','')))
    else:
        print(f"Failed to fetch cameras from Avigilon. Error: {response.text}")

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
        "https://api.lumeo.com/integrations/avigilon/stream?"
        f"server={state['avigilon_server']}&"
        f"user={state['avigilon_username']}&"
        f"password={state['avigilon_password']}&"
        f"camera_id={camera['id']}&"
        f"quality={state['quality']}"
    )
    lumeo_payload = {
        "name": f"{camera['location']} - {camera['name']} - {camera['serial']}",
        "source": "uri_stream",
        "stream_type": "rtsp",
        "uri": stream_url,
    }
    response = state['session'].post(f"https://api.lumeo.com/v1/apps/{state['lumeo_app_id']}/streams", 
                                     json=lumeo_payload,
                                     headers={"Authorization": f"Bearer {state['lumeo_app_token']}"})
    if response.status_code == 201:
        print(f"Successfully imported camera {camera['name']}")
    else:
        print(f"Failed to import camera {camera['name']}. Error: {response.text}")
    

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description='Fetch Avigilon cameras and import them to Lumeo. The Avigilon system must be reachable from the internet, and the Avigilon Web API must be enabled.')
    parser.add_argument("-l", "--lumeo_app_id", required=True, help='Your Lumeo application ID.')
    parser.add_argument("-t", "--lumeo_app_token", required=True, help='Your Lumeo application token.')
    parser.add_argument("-s", "--avigilon_server", required=True, help='The server IP/FQDN of your Avigilon system. Format: IP:PORT or FQDN:PORT.')
    parser.add_argument("-u", "--avigilon_username", required=True, help='Your Avigilon username.')
    parser.add_argument("-p", "--avigilon_password", required=True, help='Your Avigilon password.')
    parser.add_argument("-q", "--quality", default='low', help='Options: high/low. Determines quality of video stream. Default is low.')    
    args = parser.parse_args()

    state['lumeo_app_id'] = args.lumeo_app_id
    state['lumeo_app_token'] = args.lumeo_app_token
    state['avigilon_server'] = args.avigilon_server
    state['avigilon_username'] = args.avigilon_username
    state['avigilon_password'] = args.avigilon_password    
    state['quality'] = args.quality

    fetch_cameras()
    
    while True:
        import_cameras()        
        if input("Do you want to import more cameras? (y/n): ").lower() != 'y':
            break
        

