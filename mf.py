import os
import cv2
import sys
import json
import random
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

#log.basicConfig(format="[ %(levelname)s ] %(message)s", level=log.INFO, stream=sys.stdout)
log.getLogger('pdfquery').setLevel(log.ERROR)
log.getLogger('PyPDF2').setLevel(log.ERROR)

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
    coordinates_funds = []
    coordinates_transactions = []
    coordinates_closing = []

    closing_id = 0
    fund_id = 0
    transaction_id = 0
    while True:
        try:
            pdf.load(page)

            ''' For every page extract the Folio information '''
            folios = pdf.pq('LTTextLineHorizontal:contains("Folio No:")')
            for i in range(len(folios)):
                folio = folios[i].layout
                bbox = folio.bbox

                id_ = fund_id
                fund_id += 1

                coordinates_funds.extend([
                     {'id': str(id_) + "-" + 'folio_no', 'pageno': page,
                       'x': int(bbox[0]), 'y': int(PDF_HEIGHT) - int(bbox[1]) - 8, 'w': int(folio.width) + 50, 'h': int(folio.height) + 5},
                     {'id': str(id_) + "-" + 'folio_name', 'pageno': page,
                       'x': int(bbox[0]), 'y': int(PDF_HEIGHT) - int(bbox[1]) + 2, 'w': int(folio.width) + 150, 'h': int(folio.height) + 5},
                ])


            ''' Closing Extraction '''
            closings = pdf.pq('LTTextLineHorizontal:contains("Closing Unit Balance:")')
            for i in range(len(closings)):
                closing = closings[i].layout
                bbox = closing.bbox
                id_ = closing_id
                closing_id += 1
                coordinates_closing.extend([
                     {'id': str(id_) + "-" + 'closing_unit', 'pageno': page,
                       'x': int(bbox[0]), 'y': int(PDF_HEIGHT) - int(bbox[1]) - 8, 'w': int(folio.width) + 50, 'h': int(folio.height) + 3},
                     {'id': str(id_) + "-" + 'closing_nav', 'pageno': page,
                       'x': int(bbox[0]) + 200, 'y': int(PDF_HEIGHT) - int(bbox[1]) - 8, 'w': int(folio.width) + 80, 'h': int(folio.height) + 3},
                     {'id': str(id_) + "-" + 'closing_value', 'pageno': page,
                       'x': int(bbox[0]) + 400, 'y': int(PDF_HEIGHT) - int(bbox[1]) - 8, 'w': int(folio.width) + 150, 'h': int(folio.height) + 3},
                ])

            ''' For every year parse the page for dates and then use predefined coordinates to extract the values '''
            for year in range(2000, 2021):
                transactions = pdf.pq('LTTextLineHorizontal:contains("-%d")' % year)
                for i in range(len(transactions)):
                    tnx = transactions[i].layout
                    bbox = tnx.bbox
                   
                    id_ = transaction_id
                    transaction_id += 1
                    coordinates_transactions.extend([
                     
                        {'id': str(id_) + "-" + 'transaction_date', 'pageno': page, 
                            'x': int(bbox[0]), 'y': int(PDF_HEIGHT) - int(bbox[1]) - 8, 'w': int(tnx.width) + 5, 'h': int(tnx.height) + 3}, 
                        {'id': str(id_) + "-" + 'transaction_type', 'pageno': page, 
                            'x': int(bbox[0]) + 40, 'y': int(PDF_HEIGHT) - int(bbox[1]) - 8, 'w': int(tnx.width) + 220, 'h': int(tnx.height) + 3}, 
                        {'id': str(id_) + "-" + 'transaction_amount', 'pageno': page, 
                            'x': int(bbox[0]) + 300, 'y': int(PDF_HEIGHT) - int(bbox[1]) - 8, 'w': int(tnx.width) + 20, 'h': int(tnx.height) + 3}, 
                        {'id': str(id_) + "-" + 'transaction_nav', 'pageno': page, 
                            'x': int(bbox[0]) + 370, 'y': int(PDF_HEIGHT) - int(bbox[1]) - 8, 'w': int(tnx.width) + 20, 'h': int(tnx.height) + 3}, 
                        {'id': str(id_) + "-" + 'transaction_units', 'pageno': page, 
                            'x': int(bbox[0]) + 420, 'y': int(PDF_HEIGHT) - int(bbox[1]) - 8, 'w': int(tnx.width) + 30, 'h': int(tnx.height) + 3}, 
                ])
                #print(bbox, transaction.width, transaction.height)
                #print(coordinates)
            
            #if page > 0:
            #    break
            page += 1
        except Exception as err:
            print(err)
            break


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
                fd = list(filter(lambda x: x['id'].split('-')[0] == id_ and x['id'].split('-')[1] == key, coordinate))
                assert(len(fd) == 1)
                fd = fd[0]
                extracted_items[id_]["page"] = fd['pageno']
                extracted_items[id_]["y"] = fd['y']
                extracted_items[id_][key + "_cx"] = [fd['x'], fd['y'], fd['w'], fd['h']]

        return extracted_items
    ''' Closing Data - pdf2txt data'''
    extracted_closing = _return_transaction_dict(coordinates_closing)
    df_closing = pd.DataFrame(extracted_closing.values())
    print(df_closing)

    ''' Funds Data - Combine both Pdfquery and Pdf2text data '''
    print("Total Funds:", len(coordinates_funds))
    extracted_funds  = _return_transaction_dict(coordinates_funds)
    df_funds         = pd.DataFrame(extracted_funds.values())

    ''' Transaction Data - Combine both Pdfquery and Pdf2text data '''
    print("Total Tranactions:", len(coordinates_transactions))
    extracted_items  = _return_transaction_dict(coordinates_transactions)
    df_transaction   = pd.DataFrame(extracted_items.values())

    def _validate_date(x):
        try:
            datetime.strptime(x, '%d-%b-%Y')
        except Exception as err:
            return False
        return True
   
    df_transaction['is_date'] = df_transaction['transaction_date'].apply(_validate_date)
    df_transaction            = df_transaction[df_transaction['is_date'] == True]
    print(df_funds)
    #print(df_transaction)

    df_funds.to_csv(FILEPATH_FUNDS, index=False)
    df_transaction.to_csv(FILEPATH_TRANSACTION, index=False)
    df_closing.to_csv(FILEPATH_CLOSING, index=False)

def generate_folio_satement():
    df_funds = pd.read_csv(FILEPATH_FUNDS)
    df_transaction = pd.read_csv(FILEPATH_TRANSACTION)
    df_funds = df_funds.sort_values(['id'])
    df_funds = df_funds[list(filter(lambda x: not '_cx' in x, df_funds.columns))]
    df_transaction = df_transaction[list(filter(lambda x: not '_cx' in x, df_transaction.columns))]
    print(df_funds)
    print(df_transaction)
   
    def _map_transaction_2_fund(x):
        #if x.page != 1:
        #    return
        #print('========================') 
        #print(x.page, x.y, x.transaction_date, x.transaction_amount)
        df_f = df_funds[(df_funds['page'] == x.page) & (df_funds['y'] < x.y)]
        if df_f.shape[0] > 0:
            #print(df_f)
            df_f = df_f.tail(1)
            return df_f.id.tolist()[0]
        else:
            #print('Not found')
            df_f = df_funds[df_funds['page'] == x.page - 1]
            df_f = df_f.tail(1)
            return df_f.id.tolist()[0]
            #print('>>>>>>>>>>>:',  df_f.id.tolist()[0])
            #print(df_f)
    df_transaction['fund_id'] = df_transaction[['page', 'y', 'transaction_date', 'transaction_amount']].apply(_map_transaction_2_fund, axis=1) 
    #print(df_transaction)

    i = 6 
    print(df_funds[df_funds['id'] == i])
    print('###################################')
    print(df_transaction[df_transaction['fund_id'] == i].sort_values(['id']))

def visualize(casfile):
    _, _, PDF_WIDTH, PDF_HEIGHT = PdfFileReader(open(casfile, 'rb')).getPage(0).mediaBox
    df_funds = pd.read_csv(FILEPATH_FUNDS)
    df_transaction = pd.read_csv(FILEPATH_TRANSACTION)
    df_closing = pd.read_csv(FILEPATH_CLOSING)
    import cv2
    import ast
    from pdf2jpg import pdf2jpg
    import matplotlib.pyplot as plt

    result = pdf2jpg.convert_pdf2jpg(casfile, "output", dpi=150, pages="0")
    imgfile = result[0]['output_jpgfiles'][0]
    img = cv2.imread(imgfile)
    width_ratio = img.shape[1] / PDF_WIDTH
    height_ratio = img.shape[0] / PDF_HEIGHT
    print(width_ratio, height_ratio)

    for df in [df_funds, df_transaction, df_closing]:
        for _, row in df.iterrows():
            page = row['page']
            if page != 0:
                continue

            for column in list(filter(lambda x: '_cx' in x, df.columns)):
                coords = row[column]
                x, y, w, h = ast.literal_eval(coords)
                x, y, w, h = x * width_ratio, y * height_ratio, w * width_ratio, h * height_ratio
                cv2.rectangle(img, (x, y), (x + w, y + h), random.choice([1,2,3,4,5]), 2)


    img = cv2.resize(img, (0, 0), fx = 0.6, fy = 0.6) 
    cv2.imshow('image', img)
    cv2.waitKey(10000)
if __name__ == "__main__":

    casfile = sys.argv[1]
    password = sys.argv[2]
    #convert_cas(casfile, password) 
    #download_mf_nav()
    #cas_mapping()
    process_cas("converted.pdf")
    #generate_folio_satement()
    visualize('converted.pdf')
