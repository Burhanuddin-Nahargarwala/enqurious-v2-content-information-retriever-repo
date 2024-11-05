from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe
import json
import gspread
from constant import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    GOOGLE_CREDENTIALS_BUCKET_NAME,
    GOOGLE_CREDENTIALS_FILE_KEY,
)
from s3 import S3


def reflect_data_to_gsheet(df):
    # Fetch the contents of the file from S3
    try:
        # Fetch data from s3
        s3_obj = S3(
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        )
        service_account_info = json.loads(
            s3_obj.get_data(
                bucket_name=GOOGLE_CREDENTIALS_BUCKET_NAME,
                key=GOOGLE_CREDENTIALS_FILE_KEY,
            )
        )

        # # Print or process the file content as needed
        # print(file_content.decode('utf-8'))  # Assuming it's a text file
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        exit()

    # # Initialize the Google Drive API with your credentials
    # console.cloud.google.com:- ```generated-report@generated-report.iam.gserviceaccount.com```
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    credentials = Credentials.from_service_account_info(
        service_account_info, scopes=scopes
    )

    gc = gspread.authorize(credentials)

    file_name = "content-master"  # "content-master" # "Enqurious Content Details"
    folder_id = "1n9Q-mfixt9x1X8DXOlEPziO4e9Cy80EB"

    # try:
    # First try to open the spreadsheet, if spreadsheet is not there, it will throw the error
    sh = gc.open(title=file_name, folder_id=folder_id)
    # except gspread.exceptions.SpreadsheetNotFound as err:
    # When it throws the error, create the given spreadsheet
    # sh = gc.create(
    # title=file_name, folder_id=folder_id
    # )  # . -> to create

    sheet_name = "Enqurious_Content_Details"

    # check whether the sheet exists in the google spreadsheet or not
    try:
        # If yes, then open the given sheet
        sheet = sh.worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound as err:
        # Else add the sheet and then open it
        sheet = sh.add_worksheet(title=sheet_name, rows=len(df), cols=len(df.columns))

    set_with_dataframe(
        worksheet=sheet,
        dataframe=df,
        include_index=False,
        include_column_header=True,
        resize=True,
    )
