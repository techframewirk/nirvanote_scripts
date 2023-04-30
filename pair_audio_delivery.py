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
import numpy as np

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
        missed_tabs = []
        pageToken = ""
        excel_delivery_data_folder = Path("./excel_delivery_data")
        if not excel_delivery_data_folder.exists():
            os.makedirs(excel_delivery_data_folder)
        data_district_vs_filenames_filename = "district_vs_filenames_" + vendor_name + "_" + batch_name+".json"
        data_district_vs_filenames_filepath = Path(excel_delivery_data_folder)/data_district_vs_filenames_filename

        data_sheetnames_vs_tab_metadata_filename = "sheetnames_vs_tab_metadata_"+ vendor_name + "_" + batch_name+".json"
        data_sheetnames_vs_tab_metadata_filepath = Path(excel_delivery_data_folder) / data_sheetnames_vs_tab_metadata_filename

        data_district_vs_sheets_filename = "district_vs_sheets_"+ vendor_name + "_" + batch_name+".json"
        data_district_vs_sheets_filepath = Path(excel_delivery_data_folder) / data_district_vs_sheets_filename

        if Path(data_district_vs_sheets_filepath).exists():
            data = get_from_json(data_district_vs_filenames_filepath)
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
            write_to_json(data_district_vs_filenames_filepath,data)
        district_vs_sheets = {}
        for district,sheets in data.items():
            district_vs_sheets[district] = []
            for sheet in sheets:
                if sheet['mimeType'] == "application/vnd.google-apps.spreadsheet":
                    district_vs_sheets[district].append(sheet)
        write_to_json(data_district_vs_sheets_filepath,district_vs_sheets)
        sheet_service = build('sheets', 'v4', credentials=creds)
        spreadsheets = sheet_service.spreadsheets()
        sheetnames_vs_tab_metadata = {}
        if Path(data_sheetnames_vs_tab_metadata_filepath).exists():
            sheetnames_vs_tab_metadata = get_from_json(data_sheetnames_vs_tab_metadata_filepath)
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
        write_to_json(data_sheetnames_vs_tab_metadata_filepath,sheetnames_vs_tab_metadata)

        vendor_folder = Path("./excel_delivery/") / vendor_name
        
        for district, sheets in sheetnames_vs_tab_metadata.items():
            for sheet_name ,tabs in sheets.items():
                for tab in tabs:
                    print(tab['id'],tab['name'])
                    district = district.replace("_","_").replace("-","_")
                    current_district_folder = Path(vendor_folder)/district
                    if not Path(current_district_folder).exists():
                        os.makedirs(current_district_folder)
                    current_batch_folder = Path(current_district_folder)/batch_name
                    inter_speaker_pairs_folder = Path(current_batch_folder)/"/interSpkrPairs/"
                    if not Path(current_batch_folder).exists():
                        os.makedirs(current_batch_folder)
                    if sheet_name.startswith('inter'):
                        if not Path(inter_speaker_pairs_folder).exists():
                            os.makedirs(inter_speaker_pairs_folder)
                        inter_speaker_pairs_file_name = sheet_name+'.xlsx'
                        inter_speaker_pairs_file_path = Path(inter_speaker_pairs_folder) / inter_speaker_pairs_file_name
                        if Path(inter_speaker_pairs_file_path).exists():
                            print("\tinterSpkrPairs exists")
                        else:
                            time.sleep(1.3)
                            try:
                                print("\tinterspkr starts")
                                values = spreadsheets.values().get(spreadsheetId=tab['id'],range=tab['name']).execute().get('values',[])
                                print("\tinterspkr ends\n")  
                            except:
                                missed_tabs.append([sheet_name, tab['name']])
                                continue
                            values = pd.DataFrame(values).to_numpy()
                            
                            df = pd.DataFrame(values[1:],columns=values[0])
                            columns_to_keep = ['File1','File2','Cosine Similarity','Result','Confidence']
                            excel_data = df[columns_to_keep]
                            excel_data.to_excel(inter_speaker_pairs_folder+sheet_name+'.xlsx', index=False, header=True)
                    if 'intra' in sheet_name:
                        if tab['name'] != "summary":
                            district_name = district.replace("-","_")
                            intra_file_name = district_name+"_"+tab['name']+'.xlsx'
                            intra_file_path = Path(current_batch_folder)/intra_file_name

                            if Path(intra_file_path).exists():
                                print("\tintra exists")
                            else:
                                time.sleep(1.3)
                                try:
                                    print("\tintra starts")
                                    values = spreadsheets.values().get(spreadsheetId=tab['id'],range=tab['name']).execute().get('values',[])
                                    print("\tintra ends")
                                except:
                                    missed_tabs.append([sheet_name, tab['name']])
                                    continue
                                values = pd.DataFrame(values).to_numpy().tolist()
                                headers = values[0]
                                values.pop(1)
                                df = pd.DataFrame(values[1:],columns=values[0])
                                headers.pop(1)
                                headers.pop(8)
                                headers = headers[:10]
                                columns_to_keep = headers
                                excel_data = df[columns_to_keep]
                                excel_data.to_excel(intra_file_path, index=False, header=True)
                    if sheet_name.endswith('inter'):
                        inter_file_path = Path(current_batch_folder) / 'inter.xlsx'
                        if Path(inter_file_path).exists():
                            print("\tinter exists")
                        else:
                            time.sleep(1.3)
                            try:
                                print("\tinter starts")
                                values = spreadsheets.values().get(spreadsheetId=tab['id'],range=tab['name']).execute().get('values',[])
                                print("\tinter ends\n")  
                            except:
                                missed_tabs.append([sheet_name,tab['name']])
                                continue
                            values = pd.DataFrame(values).to_numpy()
                            df = pd.DataFrame(values[1:],columns=values[0])
                            columns_to_keep = ['File1','File2','Cosine Similarity','Result','Confidence']
                            excel_data = df[columns_to_keep]
                            excel_data.to_excel(inter_file_path, index=False, header=True)
        
        missing_tabs_filename = "missing_tabs_"+vendor_name+"_"+batch_name+".csv"
        missing_tabs_filepath = Path(excel_delivery_data_folder)/missing_tabs_filename
        write_to_csv(missing_tabs_filepath,missed_tabs)  
    except HttpError as error:
        print(f'An error occurred: {error}')



main()
