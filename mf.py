import os
import cv2
import sys
import json
import requests
import pdfquery
import pandas as pd
from datetime import datetime
from datetime import timedelta
import logging as log
from settings import *
import subprocess
import pandas as pd
import more_itertools
from PyPDF2 import PdfFileReader
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

def extract_text_by_coords(pdfpath, coordinates):
  '''
  Arguments
  pdfpath - pdffile location from which data needs to be extracted
  coordinates - List of dictionary where each dictionary contains below format
  [{
    'id': id, 'pageno': pageno,
    'x': x, 'y': y, 'w': width, 'h': height
  }]
  '''  
  carg = ""
  for coordinate in coordinates:
    if list(filter(lambda x: x not in coordinate.keys(), ['id', 'pageno', 'x', 'y', 'w', 'h'])):
      raise ValueError("All keys ['id', 'pageno', 'x', 'y', 'w', 'h'] should be present in coordinate passed as argument")
    carg += ",%s:%s:%s:%s:%s:%s" % (coordinate['id'], coordinate['pageno'], coordinate['x'], coordinate['y'], coordinate['w'], coordinate['h'])
  carg = carg.lstrip(',')
  cmd = ["java", "-jar", "libs/pdf2text.jar", "-i" , pdfpath, "-c", carg]
  process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
  out, err = process.communicate()
  return out

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


def process_cas(casfile):
    _, _, PDF_WIDTH, PDF_HEIGHT = PdfFileReader(open(casfile, 'rb')).getPage(0).mediaBox

    pdf = pdfquery.PDFQuery(casfile)
    page = 0
    funds_data = []
    coordinates = []
    transactions_data = []
    while True:
        try:
            pdf.load(page)

            ''' For every page extract the Folio information '''
            folios = pdf.pq('LTTextLineHorizontal:contains("Folio No:")')
            for i in range(len(folios)):
                folio = folios[i].layout
                bbox = folio.bbox
                folio_no = pdf.pq('LTTextLineHorizontal:in_bbox("%s, %s, %s, %s")' % (bbox[0], bbox[1], bbox[2] + 150, bbox[3] + 10)).text()
                fund_name = pdf.pq('LTTextLineHorizontal:in_bbox("%s, %s, %s, %s")' % (bbox[0], bbox[1] - 20, bbox[2] + 150, bbox[3])).text()
                funds_data.append({
                    'folio_no': folio_no, 
                    'fund_name': fund_name,
                    'y': bbox[1],
                    'page': page
                })

            ''' For every year parse the page for dates and then use predefined coordinates to extract the values '''
            for year in range(2000, 2021):
                transactions = pdf.pq('LTTextLineHorizontal:contains("-%d")' % year)
                for i in range(len(transactions)):
                    tnx = transactions[i].layout
                    bbox = tnx.bbox
                    #transaction_date = pdf.pq('LTTextLineHorizontal:in_bbox("%s, %s, %s, %s")' % (bbox[0] - 40, bbox[1] - 5, bbox[2], bbox[3] + 5)).text()
                    #transaction_type = pdf.pq('LTTextLineHorizontal:in_bbox("%s, %s, %s, %s")' % (bbox[0] + 10, bbox[1] - 5, bbox[2] + 300, bbox[3] + 5)).text()
                    #investment_amount = pdf.pq('LTTextLineHorizontal:in_bbox("%s, %s, %s, %s")' % (bbox[0] + 300, bbox[1] - 5, bbox[2] + 350, bbox[3] + 5)).text()
                    #transaction_nav = pdf.pq('LTTextLineHorizontal:in_bbox("%s, %s, %s, %s")' % (bbox[0] + 350, bbox[1] - 5, bbox[2] + 400, bbox[3] + 5)).text()
                    #transaction_units = pdf.pq('LTTextLineHorizontal:in_bbox("%s, %s, %s, %s")' % (bbox[0] + 400, bbox[1] - 5, bbox[2] + 450, bbox[3] + 5)).text()
                   
                    id_ = len(transactions_data)
                    transactions_data.append({
                        'id': id_,
                        #'transaction_date': transaction_date,
                        #'transaction_type': transaction_type,
                        #'investment_amount': investment_amount,
                        #'transaction_nav': transaction_nav,
                        #'transaction_units': transaction_units,
                        'y': bbox[1],
                        'page': page
                    })
                    coordinates.extend([
                     
                        {'id': str(id_) + "-" + 'transaction_date', 'pageno': page, 
                            'x': int(bbox[0]), 'y': int(PDF_HEIGHT) - int(bbox[1]) - 5, 'w': int(tnx.width), 'h': int(tnx.height) + 5}, 
                        {'id': str(id_) + "-" + 'transaction_type', 'pageno': page, 
                            'x': int(bbox[0]) + 40, 'y': int(PDF_HEIGHT) - int(bbox[1]) - 5, 'w': int(tnx.width) + 240, 'h': int(tnx.height) + 5}, 
                        {'id': str(id_) + "-" + 'transaction_amount', 'pageno': page, 
                            'x': int(bbox[0]) + 250, 'y': int(PDF_HEIGHT) - int(bbox[1]) - 5, 'w': int(tnx.width) + 70, 'h': int(tnx.height) + 5}, 
                        {'id': str(id_) + "-" + 'transaction_nav', 'pageno': page, 
                            'x': int(bbox[0]) + 350, 'y': int(PDF_HEIGHT) - int(bbox[1]) - 5, 'w': int(tnx.width) + 50, 'h': int(tnx.height) + 5}, 
                        {'id': str(id_) + "-" + 'transaction_units', 'pageno': page, 
                            'x': int(bbox[0]) + 400, 'y': int(PDF_HEIGHT) - int(bbox[1]) - 5, 'w': int(tnx.width) + 50, 'h': int(tnx.height) + 5}, 
                ])
                #print(bbox, transaction.width, transaction.height)
                #print(coordinates)
            page += 1
        except Exception as err:
            print(err)
            break

    df_funds = pd.DataFrame(funds_data)
    df_transaction_query = pd.DataFrame(transactions_data).astype(int)

    print("Total Tranactions:", len(coordinates))
    def _return_transaction_dict(coordinates):
        coordinates = more_itertools.chunked(coordinates, 1000)
        extracted_items = {}
        for coordinate in coordinates:
            output = extract_text_by_coords(casfile, coordinate)
            output = json.loads(output)
            for key, val in output.items():
                id_, key = key.split("-")
                if not id_ in extracted_items.keys():
                    extracted_items[id_] = {}
                extracted_items[id_][key] = val[0].strip()
                extracted_items[id_]['id'] = id_
        return extracted_items

    extracted_items = _return_transaction_dict(coordinates)

    df_transaction_extraction = pd.DataFrame(extracted_items.values())
    df_transaction_extraction['id'] = df_transaction_extraction['id'].astype(int)

    def _validate_date(x):
        try:
            datetime.strptime(x, '%d-%b-%Y')
        except Exception as err:
            return False
        return True
   
    df_transaction_extraction['is_date'] = df_transaction_extraction['transaction_date'].apply(_validate_date)
    df_transaction_extraction = df_transaction_extraction[df_transaction_extraction['is_date'] == True]
    df_transaction = pd.merge(df_transaction_extraction, df_transaction_query, on='id', how='left')

    df_funds.to_csv('data/funds.csv', index=False)
    df_transaction.to_csv('data/transaction.csv', index=False)

def generate_folio_satement():
    df_funds = pd.read_csv('data/funds.csv')
    df_transaction = pd.read_csv('data/transaction.csv')
    print(df_funds)
    print(df_transaction)

if __name__ == "__main__":
    log.basicConfig(format="[ %(levelname)s ] %(message)s", level=log.INFO, stream=sys.stdout)

    casfile = sys.argv[1]
    password = sys.argv[2]
    #convert_cas(casfile, password) 
    #download_mf_nav()
    #cas_mapping()
    #process_cas("converted.pdf")
    generate_folio_satement()
