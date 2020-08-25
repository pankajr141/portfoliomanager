import os
import sys
import requests
import pandas as pd
from datetime import datetime
from datetime import timedelta
import logging as log
from settings import *

def convert_cas(casfile, password):
    cmd = f'qpdf --password={password} --decrypt {casfile} converted.pdf'
    log.info(cmd.replace(password, "XXXXXX"))
    os.system(cmd)

def download_mf_nav():
    past_date = datetime.strftime(datetime.now() - timedelta(days=2), "%d-%b-%Y")
    cur_date = datetime.strftime(datetime.now(), "%d-%b-%Y")
    numbers = [3, 4, 6, 9, 10, 13, 17, 18, 20, 21, 22, 25, 26, 28, 32, 33, 37, 41, 42, 45, 46, 47, 48, 53, 54, 55, 56, 57, 58, 59, 61, 62, 63, 64, 69, 70]

    data = []
    for i in numbers:
        webpage = f'http://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?mf={i}&frmdt={past_date}&todt={cur_date}'
        log.info(f'Downloading information from {webpage}')
        response = requests.get(webpage) 
        if not "Direct Plan" in str(response._content):
            continue
        content = str(response._content)
        content = content.split("\\n")
        columns = content[0].strip().replace('\\r', '').split(';')


        for line in content[1:]:
           line = line.strip().replace('\\r', '').split(';')
           if len(line) != len(columns):
               continue
           data.append(line)

    columns[0] = columns[0].replace("b'", '')
    for i, column in enumerate(columns):
        column = column.lower().replace(' ', '_')
        columns[i] = column
    df = pd.DataFrame(data, columns=columns)
    
    if not os.path.exists(os.path.dirname(FILEPATH_NAV)):
        os.makedirs(os.path.dirname(FILEPATH_NAV))
    log.info(f'saving combined nav to disk: {FILEPATH_NAV}')
    df.to_csv(FILEPATH_NAV, index=False)

def cas_mapping():
    df = pd.read_csv(FILEPATH_NAV)
    #print(df)
    #df.sort_values(by="date").drop_duplicates(subset=["scheme_code"], keep="last")
    df = df.sort_values(by="date").groupby(['scheme_code']).last()
    print(df)

if __name__ == "__main__":
    log.basicConfig(format="[ %(levelname)s ] %(message)s", level=log.INFO, stream=sys.stdout)

    casfile = sys.argv[1]
    password = sys.argv[2]
    #convert_cas(casfile, password) 
    #download_mf_nav()
    cas_mapping()
