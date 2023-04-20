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


def parse_arguments():
    parser = argparse.ArgumentParser(description='')
    parser.add_argument(
        '-v', '--vendorname', help="vendor name", type=str)
    parser.add_argument(
        '-b', '--batchname', help="batch name", type=str)
    parser.add_argument(
        '-i', '--folderid', help="drive folder id", type=str)
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
        args = parse_arguments()
        folder_id = args.folderid
        batch_name = args.batchname
        vendor_name = args.vendorname
        districts = []
        data = {}
        pageToken = ""
        if Path("district_vs_filenames.json").exists():
            os.remove("district_vs_filenames.json")
        if Path("sheetnames_vs_tab_metadata.json").exists():
            os.remove("sheetnames_vs_tab_metadata.json")
        if Path("district_vs_sheets.json").exists():
            os.remove("district_vs_sheets.json")
        if Path("district_vs_filenames.json").exists():
            data = get_from_json("district_vs_filenames.json")
        else:
            while pageToken is not None: 
                results = drive_service.files().list(pageSize=1000, q="'"+folder_id+"' in parents",
                                                     pageToken=pageToken, 
                                                     fields="nextPageToken, files(id, name, mimeType)"
                                                     ).execute()
                districts.extend(results.get('files',[]))
                pageToken = results.get('nextPageToken')
            for dist in districts:
                if dist['mimeType'] == "application/vnd.google-apps.folder":
                    pageToken = ""
                    files = []
                    while pageToken is not None: 
                        results = drive_service.files().list(pageSize=1000,
                                                              q="'"+dist['id']+"' in parents",
                                                              pageToken=pageToken, 
                                                              fields="nextPageToken, files(id, name, mimeType)"
                                                              ).execute()
                        files.extend(results.get('files',[]))
                        pageToken = results.get('nextPageToken')
                    data[dist['name']] = files
            write_to_json('district_vs_filenames.json',data)
        district_vs_sheets = {}
        for district,sheets in data.items():
            district_vs_sheets[district] = []
            for sheet in sheets:
                if sheet['mimeType'] == "application/vnd.google-apps.spreadsheet":
                    district_vs_sheets[district].append(sheet)
        write_to_json('district_vs_sheets.json',district_vs_sheets)
        sheet_service = build('sheets', 'v4', credentials=creds)
        spreadsheets = sheet_service.spreadsheets()
        sheetnames_vs_tab_metadata = {}
        if Path("sheetnames_vs_tab_metadata.json").exists():
            sheetnames_vs_tab_metadata = get_from_json("sheetnames_vs_tab_metadata.json")
        else:
            for district, sheets in district_vs_sheets.items():
                sheetnames_vs_tab_metadata[district] = {}
                for sheet in sheets:
                    time.sleep(1.3)
                    sheetId = sheet['id']
                    gsheets = spreadsheets.get(spreadsheetId=sheetId).execute()
                    sheetnames_vs_tab_metadata[district][sheet['name']] = []
                    for gsheet in gsheets['sheets']:
                        sheet_item = {
                            'id': sheet['id'],
                            'name': gsheet['properties']['title']
                        }
                        sheetnames_vs_tab_metadata[district][sheet['name']].append(sheet_item)
        write_to_json("sheetnames_vs_tab_metadata.json",sheetnames_vs_tab_metadata)

        excel_post_process = "./excel_delivery/"+vendor_name+"/" 
        
        for district, sheets in sheetnames_vs_tab_metadata.items():
            for sheet_name ,tabs in sheets.items():
                for tab in tabs:
                    district = district.replace("-","_")
                    if not Path(excel_post_process+district).exists():
                        os.makedirs(excel_post_process+district)
                    excel_dist_folder = excel_post_process + district+"/"+batch_name+"/"
                    excel_inter_speaker_pairs = excel_post_process + district+"/"+batch_name+"/interSpkrPairs/"
                    if not Path(excel_dist_folder).exists():
                        os.makedirs(excel_dist_folder)
                    if sheet_name.startswith('inter'):
                        if not Path(excel_inter_speaker_pairs).exists():
                            os.makedirs(excel_inter_speaker_pairs)
                        if Path(excel_inter_speaker_pairs+sheet_name+'.xlsx').exists():
                            print("interspkr exists")
                        else:
                            time.sleep(1.5)
                            values = spreadsheets.values().get(spreadsheetId=tab['id'],range=tab['name']).execute().get('values',[])
                            for i in range(len(values)):
                                for j in range(len(values[i])):
                                    if validators.url(values[i][j]):
                                        values[i][j] = '=HYPERLINK("'+values[i][j]+'")'
                            excel_data = pd.DataFrame(values)
                            excel_data.to_excel(excel_inter_speaker_pairs+sheet_name+'.xlsx', index=False, header=False)
                    if 'intra' in sheet_name:
                        if tab['name'] != "summary":
                            district_name = district.replace("-","_")
                            if Path(excel_dist_folder+"_"+tab['name']+'.xlsx').exists():
                                print("intra exists")
                            else:
                                time.sleep(1.3)
                                values = spreadsheets.values().get(spreadsheetId=tab['id'],range=tab['name']).execute().get('values',[])
                                for i in range(len(values)):
                                    for j in range(len(values[i])):
                                        if validators.url(values[i][j]):
                                            values[i][j] = '=HYPERLINK("'+values[i][j]+'")'
                                excel_data = pd.DataFrame(values)
                                excel_data.to_excel(excel_dist_folder+district_name+"_"+tab['name']+'.xlsx', index=False, header=False)
                    if sheet_name.endswith('inter'):
                        if Path(excel_dist_folder+'inter.xlsx').exists():
                            print("inter exists")
                        else:
                            time.sleep(1.3)
                            values = spreadsheets.values().get(spreadsheetId=tab['id'],range=tab['name']).execute().get('values',[])
                            for i in range(len(values)):
                                for j in range(len(values[i])):
                                    if validators.url(values[i][j]):
                                        values[i][j] = '=HYPERLINK("'+values[i][j]+'")'
                            excel_data = pd.DataFrame(values)
                            excel_data.to_excel(excel_dist_folder+'/inter.xlsx', index=False, header=False)
        
    except HttpError as error:
        print(f'An error occurred: {error}')



main()
