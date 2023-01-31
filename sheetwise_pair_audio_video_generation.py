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


class CreateVideo:
    def __init__(self):
        shutil.rmtree('./video_gen_data')
        if not Path('./video_gen_data').exists():
            os.makedirs('./video_gen_data')
        if not Path('./Videos').exists():
            os.makedirs('./Videos')
        self.count = 0
        self.get_dir_info()
        self.start()

    def get_dir_info(self):
        root = Tk()
        root.withdraw()
        print("Select the audio folder ( folder containing manual_qc )")
        self.audio_dir = filedialog.askdirectory(title="Select the audio folder ( folder containing manual_qc )")
        root.update()
        print("Select the csv file")
        self.csvfilepath = filedialog.askopenfilename(title="Select the csv file")
        root.update()
        print("Select the blank image")
        self.blank_imagepath = filedialog.askopenfilename(title="Select the blank image")
        root.update()

    def create_video(self,audfile, finalfile, duration):
        # Grab the pictures and the audio that we want to use in our target video.
        
        video_id = os.path.dirname(audfile.split(self.audio_dir)[1].replace('.wav','.mp4'))
        
        video_dirname = './Videos/'+video_id
        
        if not Path(video_dirname).exists():
            os.makedirs(video_dirname)

        image_filepath = Path(self.blank_imagepath.replace(" ","\ "))
        audio_filepath = Path(audfile.replace(" ","\ "))
      
        video_filepath = Path('./Videos/'+ audfile.split(self.audio_dir)[1].replace('.wav','.mp4').replace(" ","\ "))
        
        if Path(video_filepath).exists():
            print(f"{video_filepath} exists")
            return

        cmd = f'ffmpeg -i {image_filepath} -i {audio_filepath} -acodec aac -vcodec libx264 -vf "scale=1920:1080:force_original_aspect_ratio=decrease,pad=1920:1080:-1:-1" {video_filepath}'
        subprocess.run(cmd,shell=True)

    def parse_arguments(self):
        parser = argparse.ArgumentParser(description='')
        parser.add_argument(
            '-f', '--csvfile', help="path of the csvfile containing the csv file", type=str)
        parser.add_argument(
            '-u', '--uimode', help="Enable to select files and folders through GUI", type=str)
        parser.add_argument(
            '-a', '--audiodir', help="path containing the manual_qc folder", type=str)
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
                row[1] = 'manual_qc/'+row[1]
            if not row[0].endswith('.wav'):
                row[0] = row[0] + '.wav'
                row[1] = row[1] + '.wav'
            if row[0] not in data:
                data.append(row[0])
            if row[1] not in data:
                data.append(row[1])
        self.write_to_json('./video_gen_data/filepaths.json',data)
        return data

    def get_audio_duration(self,filepath):
        return audio_metadata.load(filepath)['streaminfo']['duration']

    def get_audio_duration_data(self):
        return self.get_from_json('./video_gen_data/ad_data/csv_wise/master_audio_duration_data.json')

    def get_aud_vs_duration(self,audio_filepath_list,audio_dir):
        data = {}
        if Path('./video_gen_data/aud_vs_duration.json').exists():
            data = self.get_from_json('./video_gen_data/aud_vs_duration.json')
            return data
        else:
            for audio_filepath in audio_filepath_list:
                data[audio_filepath] = self.get_audio_duration(os.path.join(audio_dir,audio_filepath))
            self.write_to_json('./video_gen_data/aud_vs_duration.json',data)
            return data

    def start(self):
        csv_filepath = self.csvfilepath
        audio_filepaths = self.get_audio_file_names_from_csv(self.read_csv(csv_filepath))
        aud_vs_duration  = self.get_aud_vs_duration(audio_filepaths,self.audio_dir)
        for audio in audio_filepaths:
            video_path = os.path.basename(audio).replace('.wav','.mp4')
            audio_path = audio.replace(self.audio_dir,'')
            duration = aud_vs_duration[audio_path]
            audio_filepath = os.path.join(self.audio_dir,audio)
            self.create_video(audio_filepath,video_path,duration)

if __name__ == '__main__':
    createVideo = CreateVideo()