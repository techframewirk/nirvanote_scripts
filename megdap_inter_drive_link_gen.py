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
import argparse
import numpy as np
import subprocess

SCOPES = ['https://www.googleapis.com/auth/drive',
          'https://www.googleapis.com/auth/spreadsheets']


def parse_arguments():
    parser = argparse.ArgumentParser(description='')
    parser.add_argument(
        '-i', '--folderid', help="id of the drive folder", type=str)
    parser.add_argument(
        '-b', '--batchname', help="batchname", type=str)
    parser.add_argument(
        '-v', '--vendorname', help="vendorname", type=str)
    parser.add_argument(
        '-p', '--parentid', help="parent folder id", type=str)
    parser.add_argument(
        '-c', '--csvdir', help="csv folder path", type=str)
    return parser.parse_args()

def get_audio_duration(filepath):
    return audio_metadata.load(filepath)['streaminfo']['duration']

def write_to_csv(csv_filepath, data):
    with open(csv_filepath, 'w',newline='') as csvfile: 
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
    csv_data.pop(0)
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
        root_url = 'https://drive.google.com/file/d/'
        audio_batch_folder_id = args.folderid
        date_folder_name = args.batchname
        parent_folder_id = args.parentid
        csv_files_dir = args.csvdir
        districts = [Path(csv_files_dir)/district for district in os.listdir(csv_files_dir) if Path(Path(csv_files_dir)/district).is_dir()]
        for district in districts:
            folder_name = os.path.basename(district)
            batch_folder = Path(district)/date_folder_name
            if Path(batch_folder).exists():
                inter_file = Path(batch_folder)/"inter.csv"
                inter_csv_data = []
                if Path(inter_file).exists():
                    inter_csv_data = read_csv(inter_file)
                file_metadata = {
                    'name': folder_name,
                    'mimeType': 'application/vnd.google-apps.folder',
                    'parents': [parent_folder_id]
                }
                file = drive_service.files().create(body=file_metadata, fields='id, name'
                                            ).execute()
                sheet_folder_id =  file['id']
                files = []
                audio_files = {}
                tsv_files = {}
                pageToken = ""
                dates = []
                if Path("audio_files_megdap.json").exists():
                    audio_files = get_from_json("audio_files_megdap.json")
                else:
                    while pageToken is not None: 
                        results = drive_service.files().list(pageSize=1000, q="'"+audio_batch_folder_id+"' in parents",pageToken=pageToken, fields="nextPageToken, files(id, name, mimeType)").execute()
                        dates.extend(results.get('files',[]))
                        pageToken = results.get('nextPageToken')
                    write_to_json("dates.json",dates)
                    for date in dates:
                        pageToken = ""
                        states = []
                        if date['name'] == date_folder_name:
                            while pageToken is not None: 
                                results = drive_service.files().list(pageSize=1000, q="'"+date['id']+"' in parents",pageToken=pageToken, fields="nextPageToken, files(id, name, mimeType)").execute()
                                states.extend(results.get('files',[]))
                                pageToken = results.get('nextPageToken')
                            for state in states:
                                pageToken = ""
                                districts = []
                                while pageToken is not None: 
                                    results = drive_service.files().list(pageSize=1000, q="'"+state['id']+"' in parents",pageToken=pageToken, fields="nextPageToken, files(id, name, mimeType)").execute()
                                    districts.extend(results.get('files',[]))
                                    pageToken = results.get('nextPageToken')
                                for district in districts:
                                    pageToken = ""
                                    speakers = []
                                    while pageToken is not None: 
                                        results = drive_service.files().list(pageSize=1000, q="'"+district['id']+"' in parents",pageToken=pageToken, fields="nextPageToken, files(id, name, mimeType)").execute()
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
                    write_to_json("audio_files_megdap.json",audio_files)
                    
                inter_csv_data_with_drive_link = [["File1","File2","Cosine Similarity","Result","Confidence","Detailed sheet link","File1 link", "File2 link"]]
                for row in inter_csv_data:
                    try:
                        audio1_drive_link = [root_url+audio_files[row[0]+'.wav']]
                        audio2_drive_link = [root_url+audio_files[row[1]+'.wav']]
                        if len(row) == 5:
                            inter_csv_data_with_drive_link.append(row+[""]+audio1_drive_link+audio2_drive_link)
                    except:
                        print(row)
                sheet = sheet_service.spreadsheets()
                sheet_name = "megdap_"+date_folder_name+"_"+folder_name+"_inter"
                sheet_body = {
                    "properties":{
                        "title": sheet_name
                    },
                    "sheets": [
                        {
                            "properties": {
                                "title": sheet_name
                            }
                        }
                    ]
                }
                created_file = sheet.create(body=sheet_body).execute()
                created_file_id = created_file["spreadsheetId"]
                sheet.values().update(spreadsheetId=created_file_id, range=sheet_name+"!A1",
                                        valueInputOption="USER_ENTERED", body={"values": inter_csv_data_with_drive_link}).execute()
                
                drive = build('drive', 'v3', credentials=creds)
                drive.files().update(fileId=created_file_id, addParents=sheet_folder_id, removeParents='root').execute()
                cmd = "python3 megdap_intra_drive_link_gen.py -n "+folder_name+" -i "+audio_batch_folder_id+" -s "+sheet_folder_id+" -d "+date_folder_name+ " -c "+ csv_files_dir
                subprocess.run(cmd,shell=True)

    
    except HttpError as error:
        print(f'An error occurred: {error}')



main()