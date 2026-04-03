import requests, os
from datetime import datetime, timedelta

MASTER_LIST_URL = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"
TIME_LIMIT = 4

def get_file_list():

    try:
        print("Downloading the GDELT master URL list...")
        r = requests.get(MASTER_LIST_URL)
    except Exception as e:
        print(f"Failed retrieving master URL list: {e}")
        return None
    
    lines = r.text.splitlines()
    export_list = []
    time_range = datetime.now() - timedelta(hours=TIME_LIMIT + 3)
    print(f"Files starting from: {time_range}")
    print(f"Current time: {datetime.now()}")

    for line in lines:
        if ".export.CSV.zip" in line:
            parts = line.split()
            url = parts[2]
            timestamp_str = url.split('/')[-1].split('.')[0]
            file_time = datetime.strptime(timestamp_str, "%Y%m%d%H%M%S")
            
            if file_time > time_range:      
                export_list.append(url)

    print(f"Succesfully created the file list, total {len(export_list)} URL:s")
    return(export_list)

def download_files(file_list):
    os.makedirs("data/raw_zips", exist_ok=True)

    for url in file_list:
        fname = url.split('/')[-1]
        print(f"Downloading {fname}...") 
        with requests.get(url, stream=True) as r_file:
            with open(f"data/raw_zips/{fname}", 'wb') as f:
                for chunk in r_file.iter_content(chunk_size=8192):
                    f.write(chunk)

'''
if __name__=="__main__":
    file_list = get_file_list()
    if file_list:
        download_files(file_list)
'''
#def download_files(url_list, ):