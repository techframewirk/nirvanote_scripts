import os
from pathlib import Path
import csv 
import json
import audio_metadata
import argparse
import numpy as np
from tkinter import Tk, filedialog
import subprocess
import shutil
from moviepy.editor import *

class CreateVideo:
    def __init__(self):
        if Path('./video_gen_data').exists():
            shutil.rmtree('./video_gen_data')
        if not Path('./video_gen_data').exists():
            os.makedirs('./video_gen_data')
        if not Path('./Videos').exists():
            os.makedirs('./Videos')
        self.get_dir_info()
        self.start()

    def get_dir_info(self):
        root = Tk()
        root.withdraw()
        print("Select the audio folder")
        self.audio_dir = filedialog.askdirectory(title="Select the audio folder")
        root.update()
        print("Select the specific images folder")
        self.spec_image_dir = filedialog.askdirectory(title="Select the specific images folder")
        root.update()
        print("Select the generic images folder")
        self.gen_image_dir = filedialog.askdirectory(title="Select the generic images folder")
        root.update()
        print("Select the pilot images folder")
        self.pilot_image_dir = filedialog.askdirectory(title="Select the pilot images folder")
        root.update()
        print("Select the csv file")
        self.csvfilepath = filedialog.askopenfilename(title="Select the csv file")
        root.update()

    def create_video(self,picfile, audfile):
        # Grab the pictures and the audio that we want to use in our target video.
        video_id = os.path.dirname(audfile.split(self.audio_dir)[1].replace('.wav','.mp4'))
        
        video_dirname = './Videos/'+video_id
        
        if not Path(video_dirname).exists():
            os.makedirs(video_dirname)

        image_filepath = str(picfile)
        audio_filepath = str(audfile)

        video_filepath = str(Path('./Videos/'+ audfile.split(self.audio_dir)[1].replace('.wav','.mp4').replace(" ","\ ")))

        image = ImageClip(image_filepath)

        # Load the audio file
        audio = AudioFileClip(audio_filepath)

        # Set the duration of the video to match the audio length
        duration = audio.duration

        # Combine the image and audio into a video clip
        video = image.set_duration(duration).set_audio(audio)

        # Write the video to a file


        if Path(video_filepath).exists():
            print(f"{video_filepath} exists")
            return
        video_dirname = '~/Documents/Videos/'+video_id
        
        if not Path(video_dirname).exists():
            os.makedirs(video_dirname)
        video_filepath = str(Path('~/Documents/Videos/'+ audfile.split(self.audio_dir)[1].replace('.wav','.mp4').replace(" ","\ ")))
        
        video.write_videofile(video_filepath, fps=24, codec="libx264")
        




        cmd = f"ffmpeg -loop 1 -i {image_filepath} -i {audio_filepath} -c:v libx264 -tune stillimage -c:a aac -b:a 192k -pix_fmt yuv420p -shortest {video_filepath}"
        
        subprocess.run(cmd,shell=True)

    def parse_arguments(self):
        parser = argparse.ArgumentParser(description='')
        parser.add_argument(
            '-f', '--csvfile', help="path of the csvfile containing the csv file", type=str)
        parser.add_argument(
            '-u', '--uimode', help="Enable to select files and folders through GUI", type=str)
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
            if not row[0].endswith('.wav'):
                row[0] = row[0] + '.wav'
            data.append(row[0])
        return data

    def get_audio_duration(self,filepath):
        return audio_metadata.load(filepath)['streaminfo']['duration']

    def get_audio_duration_data(self):
        return self.get_from_json('./video_gen_data/ad_data/csv_wise/master_audio_duration_data.json')


    def get_spec_img_data(self,spec_image_dir):
        spec_image_dir = self.spec_image_dir
        spec_img_data = {}
        if Path('./video_gen_data/spec_img_data.json').exists():
            spec_img_data = self.get_from_json('./video_gen_data/spec_img_data.json')
            return spec_img_data
        else:
            spec_img_dists = [  district for district in os.listdir(spec_image_dir) if Path(Path(spec_image_dir) / district ).is_dir() and district != '.DS_Store' and district != 'Generic']
            for dist in spec_img_dists:
                dist_spec = Path(spec_image_dir) / dist / 'specific'
                imgs = {img.split('.')[0] : str(Path(dist_spec) / img) for img in os.listdir(dist_spec) if Path(Path(dist_spec) / img).is_file() and img != '.DS_Store' and ( img.endswith('.png') or img.endswith('.jpg') or img.endswith('jpeg'))}
                spec_img_data.update(imgs)
            self.write_to_json('./video_gen_data/spec_img_data.json',spec_img_data)
            return spec_img_data

    def get_gen_img_data(self,gen_img_dir):
        gen_img_data = {}
        if Path('./video_gen_data/gen_img_data.json').exists():
            gen_img_data = self.get_from_json('./video_gen_data/gen_img_data.json')
            return gen_img_data
        else:
            for gen_set in os.listdir(gen_img_dir):
                gen_img_data.update( { img.split('.')[0] : str(Path(gen_img_dir) / gen_set / img) for img in os.listdir(Path(gen_img_dir) / gen_set) if Path(Path(gen_img_dir)/gen_set/img).is_file() and img != '.DS_Store' and ( img.endswith('.png') or img.endswith('.jpg') or img.endswith('jpeg'))})
            self.write_to_json('./video_gen_data/gen_img_data.json',gen_img_data)
            return gen_img_data

    def get_pilot_img_data(self,pilot_img_dir):
        pilot_img_data = {}
        if Path('./video_gen_data/pilot_img_data.json').exists():
            pilot_img_data = self.get_from_json('./video_gen_data/pilot_img_data.json')
            return pilot_img_data
        else:
            for district in os.listdir(pilot_img_dir):
                pilot_img_data.update( {img.split('.')[0]:str(Path(pilot_img_dir) / district / img) for img in os.listdir(Path(pilot_img_dir) / district) if Path(Path(pilot_img_dir) / district / img).is_file() and img != '.DS_Store' or (img.endswith('.png') or img.endswith('.jpg') or img.endswith('.jpeg'))})
            self.write_to_json('./video_gen_data/pilot_img_data.json',pilot_img_data)
            return pilot_img_data

    def get_img_vs_audio(self,audio_filepath_list, audio_dir):
        data = {}
        if Path('./video_gen_data/img_vs_audio.json').exists():
            data = self.get_from_json('./video_gen_data/img_vs_audio.json')
            return data
        else:
            for audio_filepath in audio_filepath_list:
                img_name = ""
                if 'IMG_' in str(audio_filepath):
                   
                    [state, district, speaker_id,unknown, img1, img2,img3, t1,t2] = os.path.basename(audio_filepath).split('_')
                    extension = os.path.splitext(audio_filepath)[1]
                    img_name = img1+'_'+img2+'_'+img3
                elif 'IMG' in str(audio_filepath) and not 'IMG_' in str(audio_filepath):
             
                  
                    [state, district, speaker_id,unknown, img, t1,t2] = os.path.basename(audio_filepath).split('_')
                    
                    extension = os.path.splitext(audio_filepath)[1]
                    
                    img_name = img
                else:
                 
                    [state, district, speaker_id,unknown, img1, img2, t1,t2] = os.path.basename(audio_filepath).split('_')
                    extension = os.path.splitext(audio_filepath)[1]
                    img_name = img1+'_'+img2
                if img_name not in data.keys():
                    data[img_name] = [os.path.join(audio_dir, audio_filepath)]
                else:
                    data[img_name].append(os.path.join(audio_dir, audio_filepath))
            self.write_to_json('./video_gen_data/img_vs_audio.json', data)
            return data

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

    def get_img_vs_path(self):
        total_img_vs_path = {}
        if Path('./video_gen_data/total_img_vs_path.json').exists():
            total_img_vs_path = self.get_from_json('./video_gen_data/total_img_vs_path.json')
            return total_img_vs_path
        else:
            total_img_vs_path.update(self.get_pilot_img_data(self.pilot_image_dir))
            total_img_vs_path.update(self.get_gen_img_data(self.gen_image_dir))
            total_img_vs_path.update(self.get_spec_img_data(self.spec_image_dir))
            self.write_to_json('./video_gen_data/total_img_vs_path.json',total_img_vs_path)
            return total_img_vs_path

    def start(self):
        total_img_vs_path = self.get_img_vs_path()
        csv_filepath = self.csvfilepath
        audio_filepaths = self.get_audio_file_names_from_csv(self.read_csv(csv_filepath))
        img_vs_audio = self.get_img_vs_audio(audio_filepaths,self.audio_dir)
        aud_vs_duration  = self.get_aud_vs_duration(audio_filepaths,self.audio_dir)
        imgs = total_img_vs_path.keys()
        missed_csv = [["District","Image name","Audio"]]
        error_csv = [["Audio name"]]
        for img, audio_filepaths in img_vs_audio.items():
            if img not in imgs:
                missed_csv.append([audio_filepath])
                continue
            image_path = total_img_vs_path[img]
            for audio_filepath in audio_filepaths:
                self.create_video(image_path,audio_filepath)
          
        self.write_to_csv("missed.csv",missed_csv)
        self.write_to_csv("error.csv",error_csv)

if __name__ == '__main__':
    createVideo = CreateVideo()