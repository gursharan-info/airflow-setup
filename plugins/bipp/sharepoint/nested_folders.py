import os

from dotenv import load_dotenv
from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext

# target_folder_url = "/Shared Documents/Archive/2020/09"
# target_folder_url = ctx.web.ensure_folder_path(target_folder_url).execute_query()
# print(target_folder_url.serverRelativeUrl)


def ensure_folders(target_folder_path, dataset_name, cluster_type):
    ENV_FILE = os.path.join(os.path.dirname(os.path.realpath(__file__)), ".env")
    load_dotenv(dotenv_path=ENV_FILE)

    server_url = f"https://{os.getenv('SHAREPOINT_HOSTNAME')}/"
    site_url = server_url + "personal/idp_isb_edu/"
    if cluster_type == "india_pulse":
        data_folder = f"{os.getenv('MAIN_FOLDER')}/{os.getenv('INDIAPULSE_FOLDER')}"
    if cluster_type == "idp":
        data_folder = f"{os.getenv('MAIN_FOLDER')}/{os.getenv('IDP_FOLDER')}"
    upload_folder = f"{data_folder}/{dataset_name}/{target_folder_path}"
    # print(upload_folder)
    # target_folder_path = f"{upload_folder}/{target_folder_path}"
    try:
        ctx_auth = AuthenticationContext(url=server_url)
        if ctx_auth.acquire_token_for_user(
            username=os.getenv("SHAREPOINT_USERNAME"),
            password=os.getenv("SHAREPOINT_PWD"),
        ):
            ctx = ClientContext(site_url, ctx_auth)
            # ensuring the folder's presence
            ctx.web.ensure_folder_path(upload_folder).execute_query()
            print(upload_folder, " : Folders Exist Now")
    except Exception as e:
        print(e, "Couldn't create folders", upload_folder)
        return False
