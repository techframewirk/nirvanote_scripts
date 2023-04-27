from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


import json
import audio_metadata
import csv
from pathlib import Path
import os.path
import pandas as pd
import argparse
import time
import validators


SCOPES = ['https://www.googleapis.com/auth/drive',
          'https://www.googleapis.com/auth/spreadsheets']

def write_to_json(json_filepath, data):
    with open(json_filepath, 'w') as fp:
        json.dump(data, fp, indent=4)

def main():
    """Shows basic usage of the Drive v3 API.
    Prints the names and ids of the first 10 files the user has access to.
    """
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    try:
        sheet_service = build('sheets', 'v4', credentials=creds)
        spreadsheets = sheet_service.spreadsheets()
        gsheets = spreadsheets.get(spreadsheetId="1MyHnJn80acykpslAcKSP-8qQrH8bDKbHj7hzw6O_h0o").execute()['sheets']
        sheet_list = []
        for gsheet in gsheets:
            sheet_list.append(gsheet['properties']['title'])
        
        write_to_json("gsheets.json",sheet_list)
        values = spreadsheets.values().get(spreadsheetId="1MyHnJn80acykpslAcKSP-8qQrH8bDKbHj7hzw6O_h0o",range="p2ab1").execute().get('values',[])
        print(values)
        # spreadsheet_values = spreadsheets.values()
        # print("1", json.dumps(dict(spreadsheet_values), separators=(',', ':')))
        # result_obj = spreadsheet_values.get(spreadsheetId=tab['id'],range=tab['name'])
        # print("2", dir(result_obj))
        # values = result_obj.execute().get('values',[])
        # print("3", dir(values))
        # # values = executed_obj.get('values',[])
        # print("4",dir(values))
    except HttpError as error:
        print(f'An error occurred: {error}')



main()