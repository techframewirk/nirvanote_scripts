import json
import audio_metadata
import csv
from pathlib import Path
import os.path
import pandas as pd

import argparse

def parse_arguments():
    parser = argparse.ArgumentParser(description='')
    parser.add_argument(
        '-c', '--csvdir', help="csv folder path", type=str)
    parser.add_argument(
        '-v', '--vendorname', help="vendor folder path", type=str)
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
    
args = parse_arguments()
folder_path = args.csvdir
vendor_name = args.vendorname
xlsx_info = {}

districts = [Path(folder_path) / district for district in os.listdir(folder_path) if Path(Path(folder_path)/district).is_dir() ]

if Path('xlsx-info.json').exists():
    xlsx_info = get_from_json('xlsx-info.json')
else:
    for district in districts:
        xlsx_info[os.path.basename(district)] = {}

        dates = [ Path(district) / date for date in os.listdir(district) if Path(Path(district)/date).is_dir() ]
        for date in dates:

            intra_files = [str(Path(date)/file) for file in os.listdir(date) if Path(Path(date)/file).is_file() and file != 'inter.xlsx' and file != '.DS_Store' and not file.startswith(".")]
            
            inter_file = str(Path(date) / 'inter.xlsx')
            xlsx_info[os.path.basename(district)][os.path.basename(date)] = {}
            print(date)
            if Path(Path(date) / 'inter.xlsx').exists():
                xlsx_info[os.path.basename(district)][os.path.basename(date)]['inter'] = inter_file
            xlsx_info[os.path.basename(district)][os.path.basename(date)]['intra'] = intra_files
    write_to_json('xlsx-info.json',xlsx_info)

output_csv_shaip = "./xlsx_csv_files/"+vendor_name+"/"

for district, dates in xlsx_info.items():
    district_folder = Path(Path(output_csv_shaip)/district)
    
    for date, file_types in dates.items():
        date_folder = Path(Path(district_folder)/date)
        if not date_folder.exists():
            os.makedirs(date_folder)
        for file_type, files in file_types.items():
            if file_type == "inter":
                file_path = Path(date_folder) / os.path.basename(files.replace(".xlsx",".csv"))
                if Path(file_path).exists():
                        print("exists")
                        continue
                df = pd.read_excel(files)
                
                df.to_csv(file_path)
            elif file_type == "intra":
                print(files)
                for file in files:
                    file_path = Path(date_folder) / os.path.basename(file.replace(".xlsx",".csv"))

                    if Path(file_path).exists():
                        print("exists")

                        continue
                    df = pd.read_excel(file, engine='openpyxl')
                    
                    df.to_csv(file_path)
            elif file_type == "interSpkrPairs":
                for file in files:
                    if not Path(Path(date_folder) / "interSpkrPairs").exists():
                        os.makedirs(Path(Path(date_folder) / "interSpkrPairs"))
                    file_path = Path(date_folder) / "interSpkrPairs" /os.path.basename(file.replace(".xlsx",".csv"))
                    if Path(file_path).exists():
                        print("exists")

                        continue
                    df = pd.read_excel(file)

                    df.to_csv(file_path)
