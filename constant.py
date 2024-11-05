import os

def is_running_locally():
    # Check if the 'LAMBDA_TASK_ROOT' environment variable is set.
    # Lambda sets this variable, so if it's not set, the code is likely running locally.
    return 'LAMBDA_TASK_ROOT' not in os.environ

# if the code is running locally then only the dotenv module will be imported
if is_running_locally():
    from dotenv import load_dotenv, find_dotenv
    load_dotenv(find_dotenv())

# Fetch the credentials from the environment variables
AWS_ACCESS_KEY_ID = os.environ.get("ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("SECRET_ACCESS_KEY")

BUCKET_NAME = "supabase-db-json-backup"

GOOGLE_CREDENTIALS_BUCKET_NAME = "learners-progress-report-credentials"
GOOGLE_CREDENTIALS_FILE_KEY = "google_credentials_file/generated-report-77c0d34eb762.json"

# enqurious database credentials
DBNAME = os.environ.get("DBNAME")
USER = os.environ.get("USER")
PASSWORD = os.environ.get("PASSWORD")
HOST = os.environ.get("HOST")