import pickle
from pathlib import Path
from beartype import beartype

from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2 import service_account


@beartype
class Authenticator:
    def __init__(self, secrets_dir: str,
                 service_account_fname: str = 'databricks-ingestion-sa.json',
                 oauth_token_fname: str = 'token.pkl',
                 IMPERSONATE_USER: str = None):
        
        self.secrets_dir = Path(secrets_dir)
        self.service_account_path = self.secrets_dir / service_account_fname
        self.oauth_token_path = self.secrets_dir / oauth_token_fname

        self.IMPERSONATE_USER = IMPERSONATE_USER

        self.default_scopes = [
            "https://www.googleapis.com/auth/drive",  # full access
            "https://www.googleapis.com/auth/forms.responses.readonly"
        ]
        self.creds = None

    def authenticate(self, method: str = 'service_account', SCOPES: list = []):
        scopes = self.default_scopes if not SCOPES else SCOPES

        if method == 'service_account':
            if not self.IMPERSONATE_USER: 
                raise AttributeError("Missing attribute: IMPERSONATE_USER")
            try:
                self.creds = service_account.Credentials.from_service_account_file(
                    self.service_account_path,
                    scopes=scopes
                ).with_subject(self.IMPERSONATE_USER)
                print(f"✅ Authenticated with service account: {self.service_account_path}")
                return self.creds
            except Exception as e:
                print(f"XXXX> Service account authentication failed: {e}")
                return None

        elif method == 'oauth2':
            try:
                with open(self.oauth_token_path, 'rb') as token_file:
                    self.creds = pickle.load(token_file)

                if self.creds and self.creds.expired and self.creds.refresh_token:
                    print("Token is expired, refreshing...")
                    self.creds.refresh(Request())
                    with open(self.oauth_token_path, 'wb') as token_file:
                        pickle.dump(self.creds, token_file)
                    print(f"✅ {self.oauth_token_path} updated successfully.")

                print(f"✅ Authenticated with OAuth token: {self.oauth_token_path}")
                return self.creds

            except Exception as e:
                print(f"XXXX> OAuth2 authentication failed: {e}")
                return None

        else:
            print(f"XXXX> Invalid method '{method}'. Use 'service_account' or 'oauth2'.")
            return None

    def generate_token(self, client_secret_dir: str, client_secret_fname: str = 'client_secret.json', SCOPES: list = []):
        client_secret_path = Path(client_secret_dir) / client_secret_fname
        scopes = self.default_scopes if not SCOPES else SCOPES

        flow = InstalledAppFlow.from_client_secrets_file(
            str(client_secret_path),
            scopes=scopes
        )
        self.creds = flow.run_local_server(port=8080, prompt='consent', access_type='offline')

        with open(self.oauth_token_path, 'wb') as token_file:
            pickle.dump(self.creds, token_file)

        print(f"✅ Token generated and saved to {self.oauth_token_path}")
        return self.creds