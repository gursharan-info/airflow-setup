from shareplum import Office365
from shareplum import Site
from shareplum.site import Version
from pathlib import Path
import os
from dotenv import load_dotenv 


def upload(source_file_path, remote_file_name, sector_name, dataset_name):
    
    # ENV_FILE = Path('.') / '.env'
    ENV_FILE = os.path.join(os.path.dirname(os.path.realpath(__file__)), '.env')
    load_dotenv(dotenv_path = ENV_FILE)

    server_url = f"https://{os.getenv('SHAREPOINT_HOSTNAME')}/"
    site_url = server_url + "personal/idp_isb_edu"
    indiaPulse_folder = f"{os.getenv('MAIN_FOLDER')}/{os.getenv('INDIAPULSE_FOLDER')}"
    upload_folder = f"{indiaPulse_folder}/{sector_name}/{dataset_name}"
    print(upload_folder)
    
    try:
        authcookie = Office365(server_url, username = os.getenv('SHAREPOINT_USERNAME'),
                               password=os.getenv('SHAREPOINT_PWD')).GetCookies()
        
        site = Site(site_url, version=Version.v365, authcookie=authcookie)
        folder = site.Folder(upload_folder)
        with open(source_file_path, mode='rb') as file:
            fileContent = file.read()
        folder.upload_file(fileContent, remote_file_name)
        
        return True

    except Exception as e:
        print(e, "Couldn't upload file", source_file_path)
        return False
        