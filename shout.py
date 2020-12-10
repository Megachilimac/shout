import requests
from zipfile import ZipFile
import pandas as pd 
import csv

file_url = "http://wsprnet.org/archive/wsprspots-2020-12.csv.zip"
file_name = "current.csv.zip"
header_list = [
"Spot ID",
"Timestamp",
"Reporter",
"Reporter's Grid",
"SNR",
"Frequency",
"Call Sign",
"Grid",
"Power",
"Drift",
"Distance",
"Azimuth",
"Band",
"Version",
"Code"]

r = requests.get(file_url, stream = True) 

with open(file_name,"wb") as current: 
    for chunk in r.iter_content(chunk_size=1024): 

        # writing one chunk at a time to pdf file 
        if chunk: 
            current.write(chunk) 

# opening the zip file in READ mode 
with ZipFile(file_name, 'r') as zip: 

    # extracting all the files 
    print('Extracting all the files now...') 
    zip.extractall() 
    print('Done!')

    for info in zip.infolist(): 
        with open(info.filename) as csv_file: 
            print(info.filename) 
            # read the csv file 
            spots = pd.read_csv(info.filename, names=header_list)
            spots['Timestamp'] = pd.to_datetime(spots['Timestamp'], unit='s')

            spots_call = spots[spots['Call Sign']=='8P9DH'].fillna(0)
            print(spots_call.head()) 