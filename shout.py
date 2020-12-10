import requests
from zipfile import ZipFile
import pandas as pd 
import csv
from tqdm import tqdm

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

response = requests.get(file_url, stream = True) 

total_size_in_bytes= int(response.headers.get('content-length', 0))
block_size = 1024 #1 Kibibyte
progress_bar = tqdm(total=total_size_in_bytes, unit='iB', unit_scale=True)
with open(file_name, 'wb') as file:
    for data in response.iter_content(block_size):
        progress_bar.update(len(data))
        file.write(data)
progress_bar.close()
if total_size_in_bytes != 0 and progress_bar.n != total_size_in_bytes:
    print("ERROR, something went wrong")

# opening the zip file in READ mode 
with ZipFile(file_name, 'r') as zip: 

    # extract file 
    print('Extracting the file now...') 
    zip.extractall() 
    print('Extraction complete!')

    for info in zip.infolist(): 
        with open(info.filename) as csv_file: 
            print("Cleaning and filtering data...") 
            # read the csv file 
            spots = pd.read_csv(info.filename, names=header_list)
            spots['Timestamp'] = pd.to_datetime(spots['Timestamp'], unit='s')

            spots_call = spots[spots['Call Sign']=='KE0CCI'].fillna(0)
            print(spots_call.head()) 