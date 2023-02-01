import os
from pathlib import Path
import csv 
import json
import audio_metadata
import argparse
import moviepy.editor as me
import numpy as np
from tkinter import Tk, filedialog
import subprocess
import shutil


class Download():
    def __init__(self):
        if Path('./download_data').exists():
            shutil.rmtree('./download_data')
        if not Path('./download_data').exists():
            os.makedirs('./download_data')
        if not Path('./Audio').exists():
            os.makedirs('./Audio')
        args = self.parse_arguments()
        self.ssh_path = args.sshfilepath
        self.get_dir_info()
        

    def get_dir_info(self):
        root = Tk()
        root.withdraw()
        print("Select the csv file")
        self.csvfilepath = filedialog.askopenfilename(title="Select the csv file")
        root.update()
    
    def parse_arguments(self):
        parser = argparse.ArgumentParser(description='')
        parser.add_argument(
            '-p', '--sshfilepath', help="path of the sshfile", type=str)
        return parser.parse_args()

    def write_to_csv(self,csv_filepath, data):
        with open(csv_filepath, 'w',newline='') as csvfile: 
            csvwriter = csv.writer(csvfile) 
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

    def get_audio_file_names_from_csv(self):
        data = []
        csv_data = self.read_csv(self.csvfilepath)
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
        self.write_to_json('./download_data/single_audio_filepaths.json',data)
        return data

    def download_files(self):
        audio_files = self.get_audio_file_names_from_csv()
        for audio_file in audio_files:
            dir_name = os.path.dirname(audio_file)
            if not Path(os.path.join('./Audio',dir_name)).exists():
                os.makedirs(os.path.join('./Audio',dir_name))
            cmd = f"sudo scp -i {self.ssh_path} venkatesanc@34.93.48.56:/data/Database/{audio_file} ./Audio/{dir_name}"
            subprocess.run(cmd,shell=True)

Download().download_files()