import os
import sys
import requests
import pandas as pd
from datetime import datetime
from datetime import timedelta
from portfoliomanager import settings
import logging as log

log.basicConfig(format="[ %(levelname)s ] %(message)s", level=log.INFO, stream=sys.stdout)

def download_mf_nav(date=None):
    past_date = datetime.strftime(datetime.now() - timedelta(days=2), "%d-%b-%Y")
    cur_date = datetime.strftime(datetime.now(), "%d-%b-%Y")
    numbers = [3, 4, 6, 9, 10, 13, 17, 18, 20, 21, 22, 25, 26, 28, 32, 33, 37, 41, 42, 45, 46, 47, 48, 53, 54, 55, 56, 57, 58, 59, 61, 62, 63, 64, 69, 70]
    if date:
        past_date = date
        cur_date = date

    data = []
    numbers = range(100)
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

    if date:
        df.to_csv(settings.FILEPATH_NAV + "-" + date, index=False)
        return df

    if not os.path.exists(os.path.dirname(settings.FILEPATH_NAV)):
        os.makedirs(os.path.dirname(settings.FILEPATH_NAV))
    
    log.info(f'saving combined nav to disk: {settings.FILEPATH_NAV}')
    df.to_csv(settings.FILEPATH_NAV, index=False)
