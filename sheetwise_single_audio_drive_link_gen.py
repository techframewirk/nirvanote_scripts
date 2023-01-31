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
        self.get_dir_info()
        timestamp = datetime.datetime.now()
        outputfilename = os.path.basename(self.csv_filepath).replace('.csv',str("_"+str(timestamp)+'.csv'))
        self.outputfilepath = self.output_dir + outputfilename
        print(self.outputfilepath)
        self.drivefolderid = args.drivefolderid
        self.main()

    def get_dir_info(self):
        root = Tk()
        root.withdraw()
        print("Select the audio folder")
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

    def get_audio_file_names_from_csv(self,csv_data):
        data = []
        result_data = csv_data
        if csv_data[1][0].isnumeric() or csv_data[0][0] == "":
            result_data = np.delete(result_data,0,1).tolist()
        if csv_data[0][0] == "" or csv_data[0][1].isalpha() or csv_data[0][0].isalpha() or " " in csv_data[0][0]:
            result_data = np.delete(result_data,0,0).tolist()
        for row in result_data:
            if not row[0].startswith('manual_qc'):
                row[0] = 'manual_qc/'+row[0]
            if not row[0].endswith('.wav'):
                row[0] = row[0] + '.wav'
            data.append(row[0])
        return data

    def get_audio_duration(self,filepath):
        return audio_metadata.load(filepath)['streaminfo']['duration']

    def get_audio_name_vs_path(self,audio_filepath_list):
        data = {}
        for audio_filepath in audio_filepath_list:
            filename = os.path.basename(audio_filepath).replace('.wav','.mp4')
            data[filename] = audio_filepath
        return data

    def get_audio_vs_duration(self,audio_filepath_list):
        data = {}
        for audio_filepath in audio_filepath_list:
            audio_name = os.path.basename(audio_filepath).replace('.wav','.mp4')
            duration = self.get_audio_duration(os.path.join(self.audio_dir , audio_filepath))
            data[audio_name] = duration
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
            audio_filepath_list = self.get_audio_file_names_from_csv(self.read_csv(self.csv_filepath))
            audio_vs_duration = self.get_audio_vs_duration(audio_filepath_list)
            audioname_vs_audiopath = self.get_audio_name_vs_path(audio_filepath_list)
            dates = []
            pageToken = ""
            root_url = 'https://drive.google.com/file/d/'
            while pageToken is not None:
                response = service.files().list(q="'" + topFolderId + "' in parents", pageSize=1000, pageToken=pageToken, fields="nextPageToken, files(id, name)").execute()
                dates.extend(response.get('files', []))
                pageToken = response.get('nextPageToken')

            image_list = []
            count = 0
            audios = []
            for date in dates:
                pageToken = ""
                root_url = 'https://drive.google.com/file/d/'
                while pageToken is not None:
                    results = service.files().list(pageSize=1000, q="'"+date['id']+"' in parents", fields="nextPageToken, files(id, name, mimeType)").execute()
                    audios.extend( results.get('files',[]))
                    pageToken = results.get('nextPageToken')
            print(len(audios))
            for audio in audios:
                if audio['name'].startswith('._'):
                    continue
                state = audio['name'].split('_')[0]
                district = audio['name'].split('_')[1]
                image_list.append([state,district,audioname_vs_audiopath[audio['name']],audio_vs_duration[audio['name']],root_url+audio['id']])     
            self.write_to_csv(self.outputfilepath,image_list,[])
            print(count)
        except HttpError as error:
            print(f'An error occurred: {error}')


if __name__ == '__main__':
    generateDriveLink = GenerateDriveLink()