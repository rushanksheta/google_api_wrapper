from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
from ..auth.authenticate import Authenticator  # You should have a credentials.py with `authenticate()` method
import io

class GDrive():
    def __init__(self, **kwargs):
        self.authenticator = Authenticator(**kwargs)

    def get_drive_service(self):
        creds = self.authenticator.authenticate()
        return build('drive', 'v3', credentials=creds)


    def list_files_in_folder(self, folder_id: str, mime_type_filter: str = None) -> list:
        """
        Lists files in a given Google Drive folder.
        Optionally filters by mimeType (e.g., 'image/png').
        """
        service = self.get_drive_service()
        query = f"'{folder_id}' in parents and trashed = false"
        if mime_type_filter:
            query += f" and mimeType = '{mime_type_filter}'"

        files = []
        page_token = None
        while True:
            response = service.files().list(
                q=query,
                spaces='drive',
                fields="nextPageToken, files(id, name, mimeType, modifiedTime)",
                pageToken=page_token
            ).execute()
            files.extend(response.get('files', []))
            page_token = response.get('nextPageToken', None)
            if not page_token:
                break
        return files


    def download_drive_file_to_memory(self, file_id: str) -> dict:
        """
        Downloads a Google Drive file into memory and returns a dict with content and metadata.

        Returns:
            {
                'content': BytesIO,
                'file_name': str,
                'mime_type': str
            }
        """
        service = self.get_drive_service()

        # Fetch file metadata first
        metadata = service.files().get(fileId=file_id, fields="name, mimeType").execute()
        file_name = metadata["name"]
        mime_type = metadata["mimeType"]

        # Download file content to BytesIO
        buffer = io.BytesIO()
        request = service.files().get_media(fileId=file_id)
        downloader = MediaIoBaseDownload(buffer, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()

        buffer.seek(0)

        return {
            "content": buffer,
            "file_name": file_name,
            "mime_type": mime_type
        }

    def move_file_to_folder(self, file_id: str, new_folder_id: str):
        """
        Moves a file to a new folder in Google Drive by updating its parents.

        Args:
            file_id: ID of the file to move
            new_folder_id: ID of the destination folder
        """
        service = self.get_drive_service()

        # Step 1: Get current parents
        file = service.files().get(fileId=file_id, fields='parents').execute()
        current_parents = ",".join(file.get('parents', []))

        # Step 2: Update parents to move the file
        updated_file = service.files().update(
            fileId=file_id,
            addParents=new_folder_id,
            removeParents=current_parents,
            fields='id, parents'
        ).execute()

        return updated_file
