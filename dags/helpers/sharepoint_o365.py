from pathlib import Path
import os
from dotenv import load_dotenv 
from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.files.file_creation_information import FileCreationInformation


def print_upload_progress(offset):
    print("Uploaded '{0}' Kbytes...".format(offset*1000))


def upload_file(source_file_path, remote_file_name, sector_name, dataset_name):
    
    # ENV_FILE = Path('.') / '.env'
    ENV_FILE = os.path.join(os.path.dirname(os.path.realpath(__file__)), '.env')
    load_dotenv(dotenv_path = ENV_FILE)

    server_url = f"https://{os.getenv('SHAREPOINT_HOSTNAME')}/"
    site_url = server_url + "personal/idp_isb_edu/"
    main_IndiaPulse_folder = f"{os.getenv('MAIN_FOLDER')}/{os.getenv('INDIAPULSE_FOLDER')}"
    upload_folder = f"{main_IndiaPulse_folder}/{sector_name}/{dataset_name}"
    print(upload_folder)
    
    try:
        ctx_auth = AuthenticationContext(url=server_url)
        if ctx_auth.acquire_token_for_user(username=os.getenv('SHAREPOINT_USERNAME'),
                                               password=os.getenv('SHAREPOINT_PWD')):
            ctx = ClientContext(site_url, ctx_auth)

            target_folder = ctx.web.get_folder_by_server_relative_url(upload_folder)
            
            with open(source_file_path, 'rb') as content_file:
                file_content = content_file.read()
            result_file = target_folder.upload_file(remote_file_name, file_content)
            ctx.execute_query()
            print('File {0} has been uploaded successfully'.format(result_file.serverRelativeUrl))
        return True

    except Exception as e:
        print(e, "Couldn't upload file", source_file_path)
        return False



def upload_large_file(source_file_path, remote_file_name, sector_name, dataset_name):
    
    # ENV_FILE = Path('.') / '.env'
    ENV_FILE = os.path.join(os.path.dirname(os.path.realpath(__file__)), '.env')
    load_dotenv(dotenv_path = ENV_FILE)

    server_url = f"https://{os.getenv('SHAREPOINT_HOSTNAME')}/"
    site_url = server_url + "personal/idp_isb_edu/"
    main_IndiaPulse_folder = f"{os.getenv('MAIN_FOLDER')}/{os.getenv('INDIAPULSE_FOLDER')}"
    upload_folder = f"{main_IndiaPulse_folder}/{sector_name}/{dataset_name}"
    print(upload_folder)
    
    size_chunk = 1000000
    file_size = os.path.getsize(source_file_path)
    
    try:
        ctx_auth = AuthenticationContext(url=server_url)
        if ctx_auth.acquire_token_for_user(username=os.getenv('SHAREPOINT_USERNAME'),
                                               password=os.getenv('SHAREPOINT_PWD')):
            ctx = ClientContext(site_url, ctx_auth)

            target_folder = ctx.web.get_folder_by_server_relative_url(upload_folder)
            
            if file_size > size_chunk:
                result_file = target_folder.files.create_upload_session(source_file_path, size_chunk, print_upload_progress)
            else:
                with open(source_file_path, 'rb') as content_file:
                    file_content = content_file.read()
                result_file = target_folder.upload_file(remote_file_name, file_content)
            ctx.execute_query()
            print('File {0} has been uploaded successfully'.format(result_file.serverRelativeUrl))
        return True

    except Exception as e:
        print(e, "Couldn't upload file", source_file_path)
        return False