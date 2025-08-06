# google_api_wrapper

A lightweight, scalable Python wrapper for private **Google Forms and Google Drive APIs**, designed for **ETL pipelines, data workflows, and automation** in data engineering and analytics environments.

---

## Features

- Simplified authentication using OAuth 2.0 with seamless token refresh.  
- Fetch Google Forms responses in a structured DataFrame-friendly format.  
- Manage and download files from Google Drive programmatically.  
- Modular, extensible structure for adding Sheets, Calendar, or Gmail integrations.  
- Ready for use in **production pipelines** and **educational projects**.


## Installation

```bash
pip install git+https://github.com/rushanksheta/google_api_wrapper.git
```

## Usage

### 0. Imports
```python
from google_api_wrapper import GForms, GDrive, Authenticator
```

### 1. Create Object of class Autenticator(Service Account or Oauth2)
- #### 1.1. Using Service Account
    Default Authentication Method 
    ```python
    # Initialize object # default: (sectrets_fname = 'databricks-ingestion-sa.json)'
    auth_object = Authenticator(secrets_dir='/home/spark/google_creds', secrets_fname='google-sa.json')
    
    # Test authentication: success returns Credentials object
    auth_object.authenticate(method='service_account') # default: (method='service_account')
    # retruns <google.oauth2.service_account.Credentials at 0x7fefd6c0e7b0>
    ```
- #### 1.2. Using Oauth2
- 
    Generate a token file for authentication from client secrets(refer to client_secrets-template.json) is expired else use refresh token
    ```python
    # Initialize Object
    auth_object = Authenticator(secrets_dir='/home/spark/google_creds', secrets_fname='token.pkl')
    
    # Test authentication: success returns Credentials object
    auth_object.authenticate(method='oauth2')
    # retruns <google.oauth2.service_account.Credentials at 0x7fefd6c0e7b0>
    
    # Create new pkl file if credentials expired
    # default scopes are forms.responses.readonly, drive.readonly
    allow_scopes = [
            "https://www.googleapis.com/auth/forms",
            "https://www.googleapis.com/auth/drive"
        ]
    auth_object.generate_token(client_secret_dir='/home/spark/google_creds', client_secret_fname='databricks-client-secret.json', SCOPES=allow_scopes)
    # creates token.pkl file
    ```

Get Google Form responses for forms with file upload questions
``` python
import pandas as pd
from google_api_wrapper import GForms

responses = GForms(token_dir='/home/spark/google_creds')\
    .extract_form_data(form_id='(*-*)', include_fields=['question_title', 'textAnswers', 'fileUploadAnswers'])

df = pd.DataFrame(responses)

df_exploded = df.explode(column=['fileIds', 'fileNames', 'mimeTypes'])
```
## License 
This project is licensed under the **Apache License 2.0**

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
