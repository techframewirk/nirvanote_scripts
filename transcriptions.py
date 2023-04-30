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

SCOPES = ['https://www.googleapis.com/auth/drive',
          'https://www.googleapis.com/auth/spreadsheets']


def parse_arguments():
    parser = argparse.ArgumentParser(description='')
    parser.add_argument(
        '-f', '--folderpath', help="Path of the vendor folder containing segmented files", type=str)
    parser.add_argument(
        '-t', '--transcriptionpath', help="Path of the vendor folder containing segmented files", type=str)
    parser.add_argument(
        '-i', '--folderid', help="id of the drive folder", type=str)
    parser.add_argument(
        '-p', '--parentid', help="id of the drive folder under which the sheets would be created", type=str)
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
    return csv_data

def get_from_json(filepath):
    with open(filepath, 'r') as JSON:
       return json.load(JSON)

def read_tsv(filepath):
    data = []
    with open(filepath,'r') as file:
        for line in file:
            row = line.split("\t")
            data.append(row)
    return data

 
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
        folder_id = args.folderid
        processed_seg_files = args.folderpath
        sheet_folder_id = args.parentid
        transcriptions_path = args.transcriptionpath
        
        transcriptions_data_folder = Path("./transcriptions_data")
        if not Path(transcriptions_data_folder).exists():
            os.makedirs(transcriptions_data_folder)
        
        data_local_segmented_audio_files_filename = "local_segmented_audio_files.json"
        data_local_segmented_audio_files_filepath = Path(transcriptions_data_folder)/data_local_segmented_audio_files_filename
        
        data_local_segmented_audio_duration_filename = "local_segmented_audio_duration.json"
        data_local_segmented_audio_duration_filepath = Path(transcriptions_data_folder)/data_local_segmented_audio_duration_filename

        data_transcription_audionames_vs_drivelinks_filename = "transcription_audionames_vs_drivelinks.json"
        data_transcription_audionames_vs_drivelinks_filepath = Path(transcriptions_data_folder)/data_transcription_audionames_vs_drivelinks_filename

        sheet = sheet_service.spreadsheets()
        dates = [Path(processed_seg_files)/date for date in os.listdir(processed_seg_files) if Path(Path(processed_seg_files)/date).is_dir()]
        local_audio_files = {}
        if Path(data_local_segmented_audio_files_filepath).exists():
            local_audio_files = get_from_json(data_local_segmented_audio_files_filepath)
        else:
            for date in dates:
                states = [Path(date)/state/"AUDIO" for state in os.listdir(date) if Path(Path(date)/state).is_dir()  and state != 'data_prep']
                for state in states:
                    batches = [Path(state)/batch for batch in os.listdir(state) if Path(Path(state)/batch).is_dir()]
                    for batch in batches:
                        districts = [Path(batch)/district for district in os.listdir(batch) if Path(Path(batch)/district).is_dir()]
                        for district in districts:
                            speakers = [Path(district)/speaker for speaker in os.listdir(district) if Path(Path(district)/speaker).is_dir()]
                            for speaker in speakers:
                                audios = [Path(speaker)/audio_file for audio_file in os.listdir(speaker) if audio_file.endswith('.wav')]
                                for audio_file_path in audios:
                                    local_audio_files[os.path.basename(str(audio_file_path))] = str(audio_file_path)
            write_to_json(data_local_segmented_audio_files_filepath,local_audio_files)
        duration_data = {}
        if Path(data_local_segmented_audio_duration_filepath).exists():
            duration_data = get_from_json(data_local_segmented_audio_duration_filepath)
        else:
            for local_audio,local_path in local_audio_files.items():
                duration_data[local_audio] = audio_metadata.load(local_path)['streaminfo']['duration']
            write_to_json(data_local_segmented_audio_duration_filepath,duration_data)
        dist_wise_duration = {}
        dist_wise_audio_count = {}
        for local_audio_name, duration in duration_data.items():
            name_arr = local_audio_name.split("_")
            dist_name = name_arr[0]+"_"+name_arr[1]
            if dist_name not in dist_wise_duration.keys():
                dist_wise_duration[dist_name] = duration
            else:
                dist_wise_duration[dist_name] += duration
            if dist_name not in dist_wise_audio_count.keys():
                dist_wise_audio_count[dist_name] = 1
            else:
                dist_wise_audio_count[dist_name] += 1        
        dates = []
        audio_files = []
        data = {}
        pageToken = ""
        if Path(data_transcription_audionames_vs_drivelinks_filepath).exists():
            data = get_from_json(data_transcription_audionames_vs_drivelinks_filepath)
        else:
            print("Drive traversal started")
            while pageToken is not None: 
                results = drive_service.files().list(pageSize=1000, q="'"+folder_id+"' in parents",pageToken=pageToken, fields="nextPageToken, files(id, name, mimeType)").execute()
                dates.extend(results.get('files',[]))
                pageToken = results.get('nextPageToken')
            for date in dates:
                pageToken = ""
                states = []
                while pageToken is not None: 
                    results = drive_service.files().list(pageSize=1000, q="'"+date['id']+"' in parents",pageToken=pageToken, fields="nextPageToken, files(id, name, mimeType)").execute()
                    states.extend(results.get('files',[]))
                    pageToken = results.get('nextPageToken')
                for state in states:
                    pageToken = ""
                    audio = []
                    while pageToken is not None: 
                        results = drive_service.files().list(pageSize=1000, q="'"+state['id']+"' in parents",pageToken=pageToken, fields="nextPageToken, files(id, name, mimeType)").execute()
                        audio.extend(results.get('files',[]))
                        pageToken = results.get('nextPageToken')                    
                    for aud in audio:
                        pageToken = ""
                        bactches = []
                        while pageToken is not None: 
                            results = drive_service.files().list(pageSize=1000, q="'"+aud['id']+"' in parents",pageToken=pageToken, fields="nextPageToken, files(id, name, mimeType)").execute()
                            bactches.extend(results.get('files',[]))
                            pageToken = results.get('nextPageToken')
                        for batch in bactches:
                            pageToken = ""
                            district = []
                            while pageToken is not None: 
                                results = drive_service.files().list(pageSize=1000, q="'"+batch['id']+"' in parents",pageToken=pageToken, fields="nextPageToken, files(id, name, mimeType)").execute()
                                district.extend(results.get('files',[]))
                                pageToken = results.get('nextPageToken')                            
                            for dist in district:
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
                                        audio_files.extend(results.get('files',[]))
                                        pageToken = results.get('nextPageToken')                                    
            print("Drive traversal ended")
            for audio_file in audio_files:
                if audio_file['name'].endswith('.wav'):
                    data[audio_file['name']] = root_url + audio_file['id']
            write_to_json(data_transcription_audionames_vs_drivelinks_filepath,data)
        files = [Path(transcriptions_path)/file for file in os.listdir(transcriptions_path) if Path(Path(transcriptions_path)/file).is_file() and file.endswith('.txt') and not file.startswith(".")]
        audio_file_names = data.keys()
        count = 0
        missed_csvs = []
        file_wise_duration_data = {}
        for file in files:
            dataa = read_tsv(file)
            csv_data = [["Image name",	"Audio name",	"Segment ID",	"Start time",	"End time",	"Audio link",	"Assignee",	"Transcription"	,"Transcriber Metadata"]]
            row_no = 0
            for row in dataa:
                row_no += 1
                oaudio_name = row[1]
                ot1 = row[3]
                ot2 = row[4]
                t1 = int(str(row[3]).replace(".",""))
                t2 = int(str(row[4]).replace(".",""))
                audio_name = row[1].replace(".wav","_"+str(t1)+"_"+str(t2)+".wav")
                if audio_name not in audio_file_names:
                    missed_csvs.append([audio_name,str(Path(file).stem),row_no])
                    count += 1
                    continue
                if os.path.basename(str(file)) not in file_wise_duration_data.keys():
                    file_wise_duration_data[os.path.basename(str(file))] = duration_data[audio_name]
                else:
                    file_wise_duration_data[os.path.basename(str(file))] += duration_data[audio_name]
                csv_data.append([row[0],oaudio_name,row[2],ot1,ot2,data[audio_name]])
            sheet_name = os.path.basename(str(file).replace(".txt","-transcription-links"))
            sheet_body = {
                    "properties": {
                    "title": sheet_name
                }
            }
            created_file = sheet.create(body=sheet_body).execute()
            sheetId = created_file["spreadsheetId"]
            drive_service.files().update(fileId=sheetId, addParents=sheet_folder_id, removeParents='root').execute()
            request_body = {
                'requests': [
                    {
                        'addSheet': {
                            'properties': {
                                'title': sheet_name,
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
            sheet.values().update(spreadsheetId=sheetId, range=sheet_name+"!A1",
                                    valueInputOption="USER_ENTERED", body={"values": csv_data}).execute()
        sheet_name = "missing-transcription-audio-files"
        sheet_body = {
                "properties": {
                "title": sheet_name
            }
        }
        created_file = sheet.create(body=sheet_body).execute()
        sheetId = created_file["spreadsheetId"]
        drive_service.files().update(fileId=sheetId, addParents=sheet_folder_id, removeParents='root').execute()
        request_body = {
                'requests': [
                    {
                        'addSheet': {
                            'properties': {
                                'title': sheet_name,
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
        sheet.values().update(spreadsheetId=sheetId, range=sheet_name+"!A1",
                                valueInputOption="USER_ENTERED", body={"values": missed_csvs}).execute()
    except HttpError as error:
        print(f'An error occurred: {error}')



main()
