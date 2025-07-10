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
```python
# Generate a token file for authentication from client secrets(refer to client_secrets-template.json), token_fname is optional, default='token.pkl'
Authenticator(token_dir='/home/spark/google_creds',  token_fname='databricks_google_token.pkl').\
    generate_token(client_secret_dir='/home/spark/google_creds', client_secret_fname='databricks_google_client_secrets.json')

# Get Google Form responses for forms with file upload questions
responses = GForms(token_dir='/home/spark/google_creds')\
    .extract_form_data(form_id='(*-*)',\
                       include_fields=['question_title', 'textAnswers', 'fileUploadAnswers'])

df = pd.DataFrame(responses)

columns_to_explode = ['fileIds', 'fileNames', 'mimeTypes']
df_exploded = df.explode(column=columns_to_explode)
```
## License 
This project is licensed under the **Apache License 2.0**

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
