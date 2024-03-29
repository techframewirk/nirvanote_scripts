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
import numpy as np
from tkinter import Tk, filedialog
import datetime
import time


SCOPES = ['https://www.googleapis.com/auth/drive',
          'https://www.googleapis.com/auth/spreadsheets']
def parse_arguments():
    parser = argparse.ArgumentParser(description='')
    parser.add_argument(
        '-n', '--foldername', help="id of the drive folder", type=str)
    parser.add_argument(
        '-i', '--folderid', help="id of the drive folder", type=str)
    parser.add_argument(
        '-s', '--sheetfolderid', help="id of the sheet drive folder", type=str)
    parser.add_argument(
        '-b', '--batchname', help="batch name", type=str)
    parser.add_argument(
        '-c', '--intracsvdir', help="batch name", type=str)
    return parser.parse_args()

def get_audio_duration(filepath):
    return audio_metadata.load(filepath)['streaminfo']['duration']


def write_to_csv(csv_filepath, data):
    with open(csv_filepath, 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerows(data)


def write_to_json(json_filepath, data):
    with open(json_filepath, 'w') as fp:
        json.dump(data, fp, indent=4)


def read_csv(csv_filepath):
    data = csv.reader(open(csv_filepath))
    csv_data = []
    for row in data:
        csv_data.append(row)
    csv_data = np.delete(np.array(csv_data),0,1).tolist()
    return csv_data


def get_from_json(filepath):
    with open(filepath, 'r') as JSON:
        return json.load(JSON)


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
        drive_service = build('drive', 'v3', credentials=creds)
        sheet_service = build('sheets', 'v4', credentials=creds)
        
        args = parse_arguments()
        folder_name = args.foldername
        sheet_folder_id =  args.sheetfolderid
        folder_id = args.folderid
        batch_name = args.batchname
        intra_csv_dir = args.intracsvdir

        root_url = 'https://drive.google.com/file/d/'
        intra_folder = intra_csv_dir+folder_name+"/"
        intra_files = [Path(intra_folder)/file for file in os.listdir(intra_folder)
                       if Path(Path(intra_folder)/file).is_file() and file != 'inter.xlsx']
        intra_csv_data = {}
        for file in intra_files:
            intra_csv_data[Path(file).stem] = read_csv(file)

        files = []
        audio_files = {}
        tsv_files = {}
        pageToken = ""
        state_dists = []
        if Path('audio_files_shaip.json').exists():
            audio_files = get_from_json('audio_files_shaip.json')
        else:
            while pageToken is not None: 
                results = drive_service.files().list(pageSize=1000, q="'"+folder_id+"' in parents",pageToken=pageToken, fields="nextPageToken, files(id, name, mimeType)").execute()
                state_dists.extend(results.get('files',[]))
                pageToken = results.get('nextPageToken')
            for state_dist in state_dists:
                pageToken = ""
                audio_folder = []
                while pageToken is not None: 
                    results = drive_service.files().list(pageSize=1000, q="'"+state_dist['id']+"' in parents",pageToken=pageToken, fields="nextPageToken, files(id, name, mimeType)").execute()
                    audio_folder.extend(results.get('files',[]))
                    pageToken = results.get('nextPageToken')
                for audio_fold in audio_folder:
                    pageToken = ""
                    batches = []
                    while pageToken is not None: 
                        results = drive_service.files().list(pageSize=1000, q="'"+audio_fold['id']+"' in parents",pageToken=pageToken, fields="nextPageToken, files(id, name, mimeType)").execute()
                        batches.extend(results.get('files',[]))
                        pageToken = results.get('nextPageToken')
                    for batch in batches:
                        pageToken = ""
                        districts = []
                        while pageToken is not None: 
                            results = drive_service.files().list(pageSize=1000, q="'"+batch['id']+"' in parents",pageToken=pageToken, fields="nextPageToken, files(id, name, mimeType)").execute()
                            districts.extend(results.get('files',[]))
                            pageToken = results.get('nextPageToken')
                        for dist in districts:
                            pageToken = ""
                            speakers = []
                            while pageToken is not None: 
                                results = drive_service.files().list(pageSize=1000, q="'"+dist['id']+"' in parents",pageToken=pageToken, fields="nextPageToken, files(id, name, mimeType)").execute()
                                speakers.extend(results.get('files',[]))
                                pageToken = results.get('nextPageToken')
                            for speaker in speakers:
                                pageToken = ""
                                while pageToken is not None: 
                                    results = drive_service.files().list(pageSize=1000, q="'"+speaker['id']+"' in parents",pageToken=pageToken, fields="nextPageToken, files(id, name, mimeType)").execute()
                                    files.extend(results.get('files',[]))
                                    pageToken = results.get('nextPageToken')
            for file in files:
                if file['name'].endswith('.wav'):
                    audio_files[file['name']] = file['id']
                elif file['name'].endswith('.tsv'):
                    tsv_files[file['name']] = file['id']
            write_to_json("audio_files_shaip.json",audio_files)
        sheet = sheet_service.spreadsheets()
        sheet_name = "shaip_"+batch_name+"_"+folder_name+"_intra"
        sheet_body = {
                "properties": {
                "title": sheet_name
            }
        }
        created_file = sheet.create(body=sheet_body).execute()
        sheetId = created_file["spreadsheetId"]
        
        
        for filename, data in intra_csv_data.items():
            time.sleep(1.3)
            new_data = []
            header = data[0]
            ref1_link = root_url+audio_files[header[1]+'.wav']
            ref2_link = root_url+audio_files[header[2]+'.wav']
            ref3_link = root_url+audio_files[header[3]+'.wav']
            ref4_link = root_url+audio_files[header[4]+'.wav']
            ref5_link = root_url+audio_files[header[5]+'.wav']
            header.insert(1, "FileName link")
            header.insert(9, "Minimum_Score_Reference link")
            drive_link_row = ["", "", ref1_link, ref2_link,
                        ref3_link, ref4_link, ref5_link]
            new_data.append(header)
            new_data.append(drive_link_row)
            data.pop(0)
            speakerId = filename.split("_")[2]
            request_body = {
                'requests': [
                    {
                        'addSheet': {
                            'properties': {
                                'title': speakerId,
                                'sheetType': 'GRID',
                                'hidden': False
                            }
                        }
                    }
                ]
            }
            sheet.batchUpdate(spreadsheetId=sheetId,
                              body=request_body).execute()
            for row in data:
                row.insert(1, root_url+audio_files[row[0]+'.wav'])
                min_file_name = root_url+audio_files[row[8]+'.wav']
                row.insert(9,min_file_name)
                new_data.append(row)
            sheet.values().update(spreadsheetId=sheetId, range=speakerId+"!A1",
                                  valueInputOption="USER_ENTERED", body={"values": new_data}).execute()
      
        sheets_data = sheet.get(spreadsheetId=sheetId).execute()['sheets']
        local_sheet_data = {}
        for sheet_data in sheets_data:
            tab_name = sheet_data['properties']['title']
            tab_link = "https://docs.google.com/spreadsheets/d/"+sheetId+"/edit#gid="+str(sheet_data['properties']['sheetId'])
            local_sheet_data[tab_name] = tab_link
        summary_data = [["Speaker Id","Link","No of rows", "Assignee"]]
        for filename, data in intra_csv_data.items():
            speakerId = filename.split("_")[2]
            if len(data) == 0:
                no_of_rows = 0
            else:
                no_of_rows = len(data)
            summary_data.append([speakerId,local_sheet_data[speakerId],no_of_rows,""])
        request_body = {
                'requests': [
                    {
                        'addSheet': {
                            'properties': {
                                'title': "summary",
                                'sheetType': 'GRID',
                                'index': 0,
                                'hidden': False
                            }
                        }
                    }
                ]
            }
        sheet.batchUpdate(spreadsheetId=sheetId,
                            body=request_body).execute()
        sheet.values().update(spreadsheetId=sheetId, range="summary!A1",
                                  valueInputOption="USER_ENTERED", body={"values": summary_data}).execute()
        drive_service.files().update(fileId=sheetId, addParents=sheet_folder_id, removeParents='root').execute()
        request_body = {
                'requests': [
                    {
                        'deleteSheet': {
                            'sheetId': 0
                        }
                    }
                ]
        }
        sheet.batchUpdate(spreadsheetId=sheetId,
                              body=request_body).execute()        

    except HttpError as error:
        print(f'An error occurred: {error}')


main()
