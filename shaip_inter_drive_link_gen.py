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
import shutil
import subprocess

SCOPES = ['https://www.googleapis.com/auth/drive',
          'https://www.googleapis.com/auth/spreadsheets']


def parse_arguments():
    parser = argparse.ArgumentParser(description='')
    parser.add_argument(
        '-n', '--foldername', help="name of the drive folder", type=str)
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
        service = build('drive', 'v3', credentials=creds)
        root_url = 'https://drive.google.com/file/d/'
        folder_id = "1QHBItss-i0BSFyajHWfRzuKZk6oW6YeE"
        date_folder_name = "28-03-2023"
        shutil.rmtree("./intra/")
        shutil.rmtree("./intra_csv/")
        shutil.rmtree("./intra_csv_with_drive_link/")
        csv_files_dir = "./xlsx_csv_files/shaip/"
        districts = [Path(csv_files_dir)/district for district in os.listdir(csv_files_dir) if Path(Path(csv_files_dir)/district).is_dir()]
        for district in districts:
            folder_name = os.path.basename(district)
            intra_files_folder = Path(district)/date_folder_name
            if Path(intra_files_folder).exists():
                os.makedirs("./intra/"+folder_name)
                os.makedirs("./intra_csv/"+folder_name)
                os.makedirs("./intra_csv_with_drive_link/"+folder_name)
                intra_files = [Path(intra_files_folder)/file for file in os.listdir(intra_files_folder) if Path(Path(intra_files_folder)/file).is_file() and not file.startswith('inter')]
                for file in intra_files:
                    shutil.copy(file,"./intra/"+folder_name)
                inter_file = Path(intra_files_folder)/"inter.csv"
                inter_csv_data = []
                if Path(inter_file).exists():
                    inter_csv_data = read_csv(inter_file)
                files = []
                audio_files = {}
                tsv_files = {}
                pageToken = ""
                state_dists = []
                if Path('audio_files_shaip.json').exists():
                    audio_files = get_from_json('audio_files_shaip.json')
                else:
                    while pageToken is not None: 
                        results = service.files().list(pageSize=1000, q="'"+folder_id+"' in parents",
                            pageToken=pageToken, fields="nextPageToken, files(id, name, mimeType)").execute()
                        state_dists.extend(results.get('files',[]))
                        pageToken = results.get('nextPageToken')
                    for state_dist in state_dists:
                        print(state_dist["name"])
                        pageToken = ""
                        audio_folder = []
                        while pageToken is not None: 
                            results = service.files().list(pageSize=1000, q="'"+state_dist['id']+"' in parents",
                                                           pageToken=pageToken, fields="nextPageToken, files(id, name, mimeType)").execute()
                            audio_folder.extend(results.get('files',[]))
                            pageToken = results.get('nextPageToken')
                        for audio_fold in audio_folder:
                            pageToken = ""
                            batches = []
                            while pageToken is not None: 
                                results = service.files().list(pageSize=1000, q="'"+audio_fold['id']+"' in parents",
                                pageToken=pageToken, fields="nextPageToken, files(id, name, mimeType)").execute()
                                batches.extend(results.get('files',[]))
                                pageToken = results.get('nextPageToken')
                            for batch in batches:
                                pageToken = ""
                                districts = []
                                while pageToken is not None: 
                                    results = service.files().list(pageSize=1000, q="'"+batch['id']+"' in parents",
                                    pageToken=pageToken, fields="nextPageToken, files(id, name, mimeType)").execute()
                                    districts.extend(results.get('files',[]))
                                    pageToken = results.get('nextPageToken')
                                for dist in districts:
                                    pageToken = ""
                                    speakers = []
                                    while pageToken is not None: 
                                        results = service.files().list(pageSize=1000, q="'"+dist['id']+"' in parents",
                                        pageToken=pageToken, fields="nextPageToken, files(id, name, mimeType)").execute()
                                        speakers.extend(results.get('files',[]))
                                        pageToken = results.get('nextPageToken')
                                    for speaker in speakers:
                                        pageToken = ""
                                        while pageToken is not None: 
                                            results = service.files().list(pageSize=1000, q="'"+speaker['id']+"' in parents",
                                            pageToken=pageToken, fields="nextPageToken, files(id, name, mimeType)").execute()
                                            files.extend(results.get('files',[]))
                                            pageToken = results.get('nextPageToken')
                    for file in files:
                        if file['name'].endswith('.wav'):
                            audio_files[file['name']] = file['id']
                        elif file['name'].endswith('.tsv'):
                            tsv_files[file['name']] = file['id']
                    write_to_json("audio_files_shaip.json",audio_files)
                
                drive_service = build('drive', 'v3', credentials=creds)
                folder_metadata = {
                    'name': folder_name,
                    'mimeType': 'application/vnd.google-apps.folder',
                    'parents': ['1yRuK2aiVUuJ-BN8bGqTckZrFTvysNHxh']
                }
                file = drive_service.files().create(body=folder_metadata, fields='id, name'
                                            ).execute()
                created_district_folder_id =  file['id']
                inter_csv_data_with_drive_link = [["File1","File2","Cosine Similarity",
                                                   "Result","Confidence","Detailed sheet link",
                                                   "File1 link", "File2 link"]]
                for row in inter_csv_data:
                    try:
                        audio1_drive_link = [root_url+audio_files[row[0]+'.wav']]
                        audio2_drive_link = [root_url+audio_files[row[1]+'.wav']]
                        if len(row) == 5:
                            inter_csv_data_with_drive_link.append(row+[""]+
                                                       audio1_drive_link+audio2_drive_link)
                        elif len(row) == 4:
                            inter_csv_data_with_drive_link.append(row+
                                                                  audio1_drive_link+audio2_drive_link)
                    except:
                        print(row)
                sheet_service = build('sheets', 'v4', credentials=creds)
                sheet = sheet_service.spreadsheets()
                sheet_name = "shaip_"+date_folder_name+"_"+folder_name+"_inter"
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
                                        valueInputOption="USER_ENTERED",
                                          body={"values": inter_csv_data_with_drive_link}).execute()
                drive_service.files().update(fileId=created_file_id, addParents=created_district_folder_id
                                             , removeParents='root').execute()

                cmd = "python3 intra_drive_link_gen.py -n "+folder_name+" -i "+folder_id+" -s "+created_district_folder_id + " -b "+ date_folder_name+ " -c "+ csv_files_dir
                subprocess.run(cmd,shell=True)

    
    except HttpError as error:
        print(f'An error occurred: {error}')



main()