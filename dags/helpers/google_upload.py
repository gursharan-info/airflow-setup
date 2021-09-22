from __future__ import print_function
import pickle
import os.path
import googleapiclient
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.errors import ResumableUploadError

from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery_cache.base import Cache

scopes = ['https://www.googleapis.com/auth/drive.file']


class MemoryCache(Cache):
    _CACHE = {}

    def get(self, url):
        return MemoryCache._CACHE.get(url)

    def set(self, url, content):
        MemoryCache._CACHE[url] = content

def upload(source_file, remote_file_name, folder_id):
    # print(source_file)
    # print(os.path.dirname(os.path.realpath(__file__)))
    # print(sys.path[0])
    JSON_FILE = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'fiery-cistern-304207-e73d9c66538a.json')

    try:
        credentials = ServiceAccountCredentials.from_json_keyfile_name(JSON_FILE, scopes=scopes)
    except FileNotFoundError as e:
        print("Credentials file not found")
        print(e)
        return False

    # https://developers.google.com/drive/api/v3/quickstart/python
    drive_service = build('drive', 'v3', credentials=credentials, cache=MemoryCache())

    file_metadata = {
        'name': remote_file_name,
        'parents': [folder_id]
    }
    
    try:
        media = googleapiclient.http.MediaFileUpload(source_file,
                                mimetype='text/csv',
                                resumable=True)
        file = drive_service.files().create(body=file_metadata,
                                            media_body=media,
                                            fields='id').execute()
        print('File ID: %s' % file.get('id'))
        return True
    except ResumableUploadError as e:
        print(e, "Couldn't upload file for this date")
        return False
        

