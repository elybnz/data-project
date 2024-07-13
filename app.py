import sys
import json
import time
import concurrent.futures
import schedule
import pandas as pd
from os import environ, remove
from pathlib import Path
from ftplib import FTP_TLS

def get_ftp() -> FTP_TLS:
    # Get FTP details
    FTPHOST = environ["FTPHOST"]
    FTPUSER = environ["FTPUSER"]
    FTPPASS = environ["FTPPASS"]

    # Return authenticated FTP
    ftp = FTP_TLS(FTPHOST, FTPUSER, FTPPASS)
    ftp.prot_p()
    return ftp

def upload_to_ftp(ftp: FTP_TLS, file_source: Path):
    with open(file_source, "rb") as fp:
        ftp.storbinary(f"STOR {file_source.name}", fp)

def delete_file(file_source: str | Path):
    remove(file_source)

def read_csv(config: dict) -> pd.DataFrame:
    url = config["URL"]
    params = config["PARAMS"]
    return pd.read_csv(url, **params)

def pipeline():       
    # Load source configuration     
    with open("config.json", "rb") as fp:
        config = json.load(fp)
    
    ftp =get_ftp()
    
    # Iterate over each item in the config list
    for source in config:
        for source_name, source_config in source.items():
            file_name = Path(source_name + ".csv")
            df = read_csv(source_config)
            df.to_csv(file_name, index=False)
            
            print(f"File{file_name} has been downloaded.")
            
            upload_to_ftp(ftp, file_name)
            print(f"File{file_name} has been uploaded to FTP.")
            
            delete_file(file_name)
            print(f"File{file_name} has been deleted.")
            
if __name__=="__main__":
    
    param = sys.argv[1]

    if param=="manual":
        pipeline()
    
    elif param=="schedule":
             schedule.every().day.at("01:58").do(pipeline)

             while True:
                schedule.run_pending()
                time.sleep(1)
    else:
        print("Invalid parameter. The function will not run.")
