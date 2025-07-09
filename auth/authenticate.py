import os
import pickle
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

class Authenticator():
    def __init__(self, token_fpath):
        self.token_fpath = token_fpath

        self.default_scopes = [
            "https://www.googleapis.com/auth/forms.responses.readonly",
            "https://www.googleapis.com/auth/drive.readonly"
        ]

    def authenticate(self, token_fname='token.pkl'):
        # Resolve the directory where *this module* resides
        # base_dir = os.path.dirname(os.path.abspath(__file__))
        token_path = os.path.join(self.token_fpath, token_fname)

        # if not os.path.exists(token_path):
        #     raise RuntimeError(f"Token file not found at: {token_path}. Please generate it locally and upload config folder.")

        try:
            with open(token_path, 'rb') as token_file:
                creds = pickle.load(token_file)

            if creds and creds.expired and creds.refresh_token:
                print("Token is expired, refreshing...")
                creds.refresh(Request())

                with open(token_path, 'wb') as token_file:
                    pickle.dump(creds, token_file)
                print(f"✅ {token_path} updated successfully.")

        except Exception as e:
            print(f"Error Loading {token_path}: {e}")

        return creds
    
    def generate_token(self, client_secret_path, token_fname='token.pkl', SCOPES: list=[]):
        # Base path of the current module's directory
        # base_dir = os.path.dirname(os.path.abspath(__file__))

        # Resolve full paths relative to the module
        # client_secret_path = os.path.join(base_dir, client_secret_dir, 'client_secret.json')
        # token_save_path = os.path.join(base_dir, token_save_dir, 'token.pkl')
        token_path = os.path.join(self.token_fpath, token_fname)
        # client_secret_path = os.path.join(client_secret_fpath, client_secret_fpath)

        # Start OAuth flow
        flow = InstalledAppFlow.from_client_secrets_file(
            client_secret_path,
            scopes=self.default_scopes if not SCOPES else SCOPES
        )
        creds = flow.run_local_server(port=8080, prompt='consent', access_type='offline')

        # Save credentials to token file
        with open(token_path, 'wb') as token_file:
            pickle.dump(creds, token_file)

        print(f"✅ {self.token_fpath} generated successfully.")