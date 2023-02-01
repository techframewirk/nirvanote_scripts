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
import shutil

class GenerateDriveLink:
    def __init__(self):
        self.output_dir = './drive_link_results/'
        if Path(self.output_dir).exists():
            shutil.rmtree(self.output_dir)
        if not Path(self.output_dir).exists():
            os.makedirs(self.output_dir)
        self.SCOPES = ['https://www.googleapis.com/auth/drive.metadata.readonly']
        args = self.parse_arguments()
        self.drivefolderid = args.drivefolderid

        self.get_dir_info()

        timestamp = datetime.datetime.now()
        outputfilename = os.path.basename(self.csv_filepath).replace('.csv',str("_"+str(timestamp)+'.csv'))
        self.outputfilepath = self.output_dir + outputfilename
        self.main()

    def get_dir_info(self):
        root = Tk()
        root.withdraw()
        self.audio_dir = filedialog.askdirectory(title="Select the audio folder")
        root.update()
        print("Select the csv file")
        self.csv_filepath = filedialog.askopenfilename(title="Select the csv file")
        root.update()

    def parse_arguments(self):
        parser = argparse.ArgumentParser(description='')
        parser.add_argument(
            '-i', '--drivefolderid', help="id of the drive folder", type=str)
        return parser.parse_args()

    def write_to_csv(self,csv_filepath, data, col_names):
        with open(csv_filepath, 'w',newline='') as csvfile: 
            csvwriter = csv.writer(csvfile) 
            csvwriter.writerow(col_names) 
            csvwriter.writerows(data)

    def read_csv(self,csv_filepath):
        data = csv.reader(open(csv_filepath))
        return list(data)

    def write_to_json(self,json_filepath, data):
        with open(json_filepath, 'w') as fp:
            json.dump(data, fp, indent=4)

    def get_from_json(self,filepath):
        with open(filepath, 'r') as JSON:
            return json.load(JSON)

    def get_pair_data(self):
        pair_data = {}
        print(self.csv_filepath)
        pair_csv_data = self.get_only_data()
        print(len(pair_csv_data))
        for row in pair_csv_data:
            if row[0] not in pair_data.keys():
                pair_data[row[0]] = [row[1]]
            else:
                pair_data[row[0]].append(row[1])
        self.write_to_json('./drive_link_results/pair_data.json',pair_data)
        return pair_data

    def get_only_data(self):
        data = []
        csv_data = self.read_csv(self.csv_filepath)
        result_data = csv_data
        if csv_data[1][0].isnumeric() or csv_data[0][0] == "":
            result_data = np.delete(result_data,0,1).tolist()
        if csv_data[0][0] == "" or csv_data[0][1].isalpha() or csv_data[0][0].isalpha() or " " in csv_data[0][0]:
            result_data = np.delete(result_data,0,0).tolist()
        for row in result_data:
            if not row[0].startswith('manual_qc'):
                row[0] = 'manual_qc/'+row[0]
                row[1] = 'manual_qc/'+row[1]
            if not row[0].endswith('.wav'):
                row[0] = row[0] + '.wav'
                row[1] = row[1] + '.wav'
            data.append(row)
        return data

    def get_audio_file_names_from_csv(self):
        data = []
        pair_csv_data = self.get_only_data()
        for row in pair_csv_data:
            if row[0] not in data:
                data.append(row[0])
            if row[1] not in data:
                data.append(row[1])
        self.write_to_json('./drive_link_results/pair_audio_filepaths.json',data)
        return data

    def get_audio_duration(self,filepath):
        return audio_metadata.load(filepath)['streaminfo']['duration']


    def get_pair_audio_vs_duration(self):
        data = {}
        audio_filepath_list = self.get_audio_file_names_from_csv()
        for audio_filepath in audio_filepath_list:
            if audio_filepath == "manual_qc/megdap/pair_audio/audio_dump/UP_JyotibaP_55385107_1209530000_TGKFM_8016.wav":
                print("YESSSS")
            duration = self.get_audio_duration(os.path.join(self.audio_dir, audio_filepath))
            data[audio_filepath] = duration
        self.write_to_json('./drive_link_results/pair_audio_duration.json',data)
        return data

    def main(self):
        """Shows basic usage of the Drive v3 API.
        Prints the names and ids of the first 10 files the user has access to.
        """
        creds = None
        # The file token.json stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        if os.path.exists('token.json'):
            creds = Credentials.from_authorized_user_file('token.json', self.SCOPES)
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    'credentials.json', self.SCOPES)
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open('token.json', 'w') as token:
                token.write(creds.to_json())

        try:
            service = build('drive', 'v3', credentials=creds)
            topFolderId = self.drivefolderid 
            print(self.csv_filepath)
            pair_audio_vs_duration = self.get_pair_audio_vs_duration()
            pair_data = self.get_pair_data()
            pair_audios = []
            pageToken = ""
            root_url = 'https://drive.google.com/file/d/'
            while pageToken is not None:
                response = service.files().list(q="'" + topFolderId + "' in parents", pageSize=1000, pageToken=pageToken, fields="nextPageToken, files(id, name)").execute()
                pair_audios.extend(response.get('files', []))
                pageToken = response.get('nextPageToken')
            drive_link_list = []
            drive_link_id_data = {}
            count = 0
    
            
            for pair_audio in pair_audios:
                drive_link_id_data[pair_audio['name']] = root_url + pair_audio['id']
            print(len(drive_link_id_data))
          
            for audio1, pairs in pair_data.items():
                audio1_name = os.path.basename(audio1)
                for audio2 in pairs:
                    audio2_name = os.path.basename(audio2)
                    drive_link_list.append([audio1, pair_audio_vs_duration[audio1],drive_link_id_data[audio1_name],audio2, pair_audio_vs_duration[audio2],drive_link_id_data[audio2_name]])
            print(count)
            self.write_to_csv(self.outputfilepath,drive_link_list,[])
        except HttpError as error:
            print(f'An error occurred: {error}')


if __name__ == '__main__':
    generateDriveLink = GenerateDriveLink()