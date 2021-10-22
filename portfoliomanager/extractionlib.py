import os
import sys
import json
import subprocess
import pdfquery
import more_itertools
import pandas as pd
from portfoliomanager import settings
from PyPDF2 import PdfFileReader
import logging as log

log.basicConfig(format="[ %(levelname)s ] %(message)s", level=log.INFO, stream=sys.stdout)

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

def parse_casfile_for_data(casfile):
    log.info("processing {}".format(casfile))
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

            page += 1
        except Exception as err:
            log.error(str(err))
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
    log.info("Total Funds: {}".format(len(coordinates_funds)))
    extracted_funds  = _return_transaction_dict(coordinates_funds)
    df_funds         = pd.DataFrame(extracted_funds.values())

    ''' Transaction Data - Combine both Pdfquery and Pdf2text data '''
    log.info("Total Tranactions: {}".format(len(coordinates_transactions)))
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

    df_funds.to_csv(settings.FILEPATH_FUNDS, index=False)
    df_transaction.to_csv(settings.FILEPATH_TRANSACTION, index=False)
    df_closing.to_csv(settings.FILEPATH_CLOSING, index=False)
