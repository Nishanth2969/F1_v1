import requests
import json
import time
import urllib3
import sys

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# NiFi API configuration
NIFI_URL = "https://localhost:8443/nifi-api"
USERNAME = "73598372-7087-4a18-8e3d-d514115aa009"
PASSWORD = "jl/UQahVks/PpNGSWKincTanAPpVVBHm"

# REST API operations
session = requests.Session()

def login():
    """Login to NiFi and establish session"""
    try:
        # Try to access a protected endpoint to trigger login
        response = session.get(f"{NIFI_URL}/flow/status", 
                              auth=(USERNAME, PASSWORD),
                              verify=False)
        if response.status_code == 200:
            print("Login successful")
            return True
        else:
            print(f"Login failed: {response.status_code}, {response.text}")
            return False
    except Exception as e:
        print(f"Error during login: {str(e)}")
        return False

def import_flow_template():
    """Import the F1 flow template"""
    try:
        # Read the template file
        with open('nifi/f1_raw_data.json', 'r') as file:
            template_content = file.read()
        
        # Create a process group
        process_group_response = session.post(
            f"{NIFI_URL}/process-groups/root/process-groups",
            json={
                "component": {
                    "name": "F1 Telemetry",
                    "position": {
                        "x": 300,
                        "y": 300
                    }
                },
                "revision": {
                    "version": 0
                }
            },
            auth=(USERNAME, PASSWORD),
            verify=False
        )
        
        if process_group_response.status_code == 201:
            process_group_data = process_group_response.json()
            process_group_id = process_group_data['component']['id']
            print(f"Created process group with ID: {process_group_id}")
            
            # Import the template into the process group
            template_obj = json.loads(template_content)
            
            import_response = session.post(
                f"{NIFI_URL}/process-groups/{process_group_id}/flow-content",
                json=template_obj,
                auth=(USERNAME, PASSWORD),
                verify=False
            )
            
            if import_response.status_code == 200:
                print("Flow template imported successfully")
                return process_group_id
            else:
                print(f"Failed to import template: {import_response.status_code}, {import_response.text}")
                return None
        else:
            print(f"Failed to create process group: {process_group_response.status_code}, {process_group_response.text}")
            return None
    except Exception as e:
        print(f"Error importing flow template: {str(e)}")
        return None

def enable_controller_services(process_group_id):
    """Enable all controller services in the process group"""
    try:
        # Get all controller services
        services_response = session.get(
            f"{NIFI_URL}/flow/process-groups/{process_group_id}/controller-services",
            auth=(USERNAME, PASSWORD),
            verify=False
        )
        
        if services_response.status_code == 200:
            services_data = services_response.json()
            controller_services = services_data.get('controllerServices', [])
            
            for service in controller_services:
                service_id = service['id']
                service_name = service['component']['name']
                
                # Update controller service state to ENABLED
                enable_response = session.put(
                    f"{NIFI_URL}/controller-services/{service_id}/run-status",
                    json={
                        "state": "ENABLED",
                        "revision": {
                            "version": service['revision']['version']
                        }
                    },
                    auth=(USERNAME, PASSWORD),
                    verify=False
                )
                
                if enable_response.status_code == 200:
                    print(f"Enabled controller service: {service_name}")
                else:
                    print(f"Failed to enable controller service {service_name}: {enable_response.status_code}, {enable_response.text}")
            
            return True
        else:
            print(f"Failed to get controller services: {services_response.status_code}, {services_response.text}")
            return False
    except Exception as e:
        print(f"Error enabling controller services: {str(e)}")
        return False

def start_all_processors(process_group_id):
    """Start all processors in the process group"""
    try:
        start_response = session.put(
            f"{NIFI_URL}/flow/process-groups/{process_group_id}",
            json={
                "id": process_group_id,
                "state": "RUNNING",
                "disconnectedNodeAcknowledged": False
            },
            auth=(USERNAME, PASSWORD),
            verify=False
        )
        
        if start_response.status_code == 200:
            print("Started all processors")
            return True
        else:
            print(f"Failed to start processors: {start_response.status_code}, {start_response.text}")
            return False
    except Exception as e:
        print(f"Error starting processors: {str(e)}")
        return False

def main():
    """Main function to setup NiFi flow"""
    if not login():
        sys.exit(1)
        
    process_group_id = import_flow_template()
    if not process_group_id:
        sys.exit(1)
        
    # Sleep to allow NiFi to fully process the import
    print("Waiting for flow to be fully imported...")
    time.sleep(5)
        
    if not enable_controller_services(process_group_id):
        sys.exit(1)
        
    # Sleep to allow controller services to stabilize
    print("Waiting for controller services to stabilize...")
    time.sleep(5)
        
    if not start_all_processors(process_group_id):
        sys.exit(1)
        
    print("NiFi flow setup completed successfully!")

if __name__ == "__main__":
    main() 