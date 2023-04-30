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
import time

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
    if 'inter' in str(csv_filepath):
        csv_data.pop(0)
    if len(csv_data) != 0:
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
        vendor_name = args.vendorname

        pair_audio_data_folder = Path("./pair_audio_data")
        if not pair_audio_data_folder.exists():
            os.makedirs(pair_audio_data_folder)

        data_megdap_audio_files_filename = "audio_files_"+vendor_name+"_"+date_folder_name+".json"
        data_megdap_audio_files_filepath = Path(pair_audio_data_folder)/data_megdap_audio_files_filename
        
        data_shaip_audio_files_filename = 'audio_files_shaip.json'
        data_shaip_audio_files_filepath = Path(pair_audio_data_folder)/data_shaip_audio_files_filename

        data_intra_csv_data_filename = "intra_csv_data_"+vendor_name+"_"+date_folder_name+".json"
        data_intra_csv_data_filepath = Path(pair_audio_data_folder)/data_intra_csv_data_filename

        districts = [Path(csv_files_dir)/district for district in os.listdir(csv_files_dir) if Path(Path(csv_files_dir)/district).is_dir()]
        for district in districts:
            folder_name = os.path.basename(district)
            batch_folder = Path(district)/date_folder_name
            print(folder_name)
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
                sheet_folder = drive_service.files().create(body=file_metadata, fields='id, name'
                                            ).execute()
                sheet_folder_id =  sheet_folder['id']
                files = []
                state_dists = []

                audio_files = {}
                tsv_files = {}
                pageToken = ""
                dates = []
                file_data_exists = False
                if vendor_name == "megdap":
                    if Path(data_megdap_audio_files_filepath).exists():
                        audio_files = get_from_json(data_megdap_audio_files_filepath)
                        file_data_exists = True
                elif vendor_name == "shaip":
                    if Path(data_shaip_audio_files_filepath).exists():
                        audio_files = get_from_json(data_shaip_audio_files_filepath)
                        file_data_exists = True
                if file_data_exists == False:
                    if vendor_name == "megdap":
                        while pageToken is not None: 
                            results = drive_service.files().list(pageSize=1000, q="'"+audio_batch_folder_id+"' in parents",pageToken=pageToken, fields="nextPageToken, files(id, name, mimeType)").execute()
                            dates.extend(results.get('files',[]))
                            pageToken = results.get('nextPageToken')
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
                    elif vendor_name == "shaip":
                        while pageToken is not None: 
                            results = drive_service.files().list(pageSize=1000, q="'"+audio_batch_folder_id+"' in parents",
                            pageToken=pageToken, fields="nextPageToken, files(id, name, mimeType)").execute()
                            state_dists.extend(results.get('files',[]))
                            pageToken = results.get('nextPageToken')
                            for state_dist in state_dists:
                                pageToken = ""
                                audio_folder = []
                                while pageToken is not None: 
                                    results = drive_service.files().list(pageSize=1000, q="'"+state_dist['id']+"' in parents",
                                                                pageToken=pageToken, fields="nextPageToken, files(id, name, mimeType)").execute()
                                    audio_folder.extend(results.get('files',[]))
                                    pageToken = results.get('nextPageToken')
                                for audio_fold in audio_folder:
                                    pageToken = ""
                                    batches = []
                                    while pageToken is not None: 
                                        results = drive_service.files().list(pageSize=1000, q="'"+audio_fold['id']+"' in parents",
                                        pageToken=pageToken, fields="nextPageToken, files(id, name, mimeType)").execute()
                                        batches.extend(results.get('files',[]))
                                        pageToken = results.get('nextPageToken')
                                    for batch in batches:
                                        pageToken = ""
                                        districts = []
                                        while pageToken is not None: 
                                            results = drive_service.files().list(pageSize=1000, q="'"+batch['id']+"' in parents",
                                            pageToken=pageToken, fields="nextPageToken, files(id, name, mimeType)").execute()
                                            districts.extend(results.get('files',[]))
                                            pageToken = results.get('nextPageToken')
                                        for dist in districts:
                                            pageToken = ""
                                            speakers = []
                                            while pageToken is not None: 
                                                results = drive_service.files().list(pageSize=1000, q="'"+dist['id']+"' in parents",
                                                pageToken=pageToken, fields="nextPageToken, files(id, name, mimeType)").execute()
                                                speakers.extend(results.get('files',[]))
                                                pageToken = results.get('nextPageToken')
                                            for speaker in speakers:
                                                pageToken = ""
                                                while pageToken is not None: 
                                                    results = drive_service.files().list(pageSize=1000, q="'"+speaker['id']+"' in parents",
                                                    pageToken=pageToken, fields="nextPageToken, files(id, name, mimeType)").execute()
                                                    files.extend(results.get('files',[]))
                                                    pageToken = results.get('nextPageToken')
                    else:
                        print("WRONG vendor name, only megdap and shaip works")
                        exit()
                    for file in files:
                        if file['name'].endswith('.wav'):
                            audio_files[file['name']] = file['id']
                        elif file['name'].endswith('.tsv'):
                            tsv_files[file['name']] = file['id']
                    if vendor_name == "shaip":
                        write_to_json(data_shaip_audio_files_filepath,audio_files)
                    elif vendor_name == "megdap":
                        write_to_json(data_megdap_audio_files_filepath,audio_files)
                inter_csv_data_with_drive_link = [["File1","File2","Cosine Similarity","Result","Confidence","Detailed sheet link","File1 link", "File2 link"]]
                for row in inter_csv_data:
                    try:
                        audio1_drive_link = [root_url+audio_files[row[0]+'.wav']]
                        audio2_drive_link = [root_url+audio_files[row[1]+'.wav']]
                        if len(row) == 5:
                            inter_csv_data_with_drive_link.append(row+[""]+audio1_drive_link+audio2_drive_link)
                    except:
                        print("EXCEPTION OCCURED")
                        print(folder_name)
                        exit()
                
                sheet = sheet_service.spreadsheets()
                sheet_name = vendor_name+"_"+date_folder_name+"_"+folder_name+"_inter"
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
                drive_service.files().update(fileId=created_file_id, addParents=sheet_folder_id, removeParents='root').execute()
                
        
                intra_folder = Path(args.csvdir)/folder_name/date_folder_name
                intra_files = [Path(intra_folder)/file for file in os.listdir(intra_folder)
                            if Path(Path(intra_folder)/file).is_file() and file != 'inter.csv' and file != '.DS_Store']
                intra_csv_data = {}
                for file in intra_files:
                    intra_csv_data[Path(file).stem] = read_csv(file)
                write_to_json(data_intra_csv_data_filepath, intra_csv_data)
                print("intra starts")
                sheet = sheet_service.spreadsheets()
                intra_csv_data_items = list(intra_csv_data.items())
                intra_files_length =  len(intra_csv_data)
                count = 1
                for file_no in range(0,intra_files_length,200):
                    current_items = intra_csv_data_items[file_no:file_no+200]
                    sheet_name = vendor_name+"_"+folder_name+"_"+date_folder_name+"_intra_"+str(count)
                    sheet_body = {
                            "properties": {
                            "title": sheet_name
                        }
                    }
                    created_file = sheet.create(body=sheet_body).execute()
                    sheetId = created_file["spreadsheetId"]
                    count += 1
                    for filename, data in current_items:
                        time.sleep(1.3)
                        new_data = []
                        header = data[0]
                        ref1_name = root_url+audio_files[header[1]+'.wav']
                        ref2_name = root_url+audio_files[header[2]+'.wav']
                        ref3_name = root_url+audio_files[header[3]+'.wav']
                        ref4_name = root_url+audio_files[header[4]+'.wav']
                        ref5_name = root_url+audio_files[header[5]+'.wav']
                        header.insert(1, "FileName link")
                        header.insert(9, "Minimum_Score_Reference link")
                        link_row = ["", "", ref1_name, ref2_name,
                                    ref3_name, ref4_name, ref5_name]
                        new_data.append(header)
                        new_data.append(link_row)
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
                    for filename, data in current_items:
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
                    drive = build('drive', 'v3', credentials=creds)
                    drive.files().update(fileId=sheetId, addParents=sheet_folder_id, removeParents='root').execute()
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
                    gsheets = sheet.get(spreadsheetId=sheetId).execute()
                    print(len(gsheets['sheets']))
    except HttpError as error:
        print(f'An error occurred: {error}')
main()