import os
import sys
import logging as log

log.basicConfig(format="[ %(levelname)s ] %(message)s", level=log.INFO, stream=sys.stdout)

def remove_password_from_pdf(pdf, password, outputpdf):
    cmd = f'qpdf --password={password} --decrypt {pdf} {outputpdf}'
    log.info(cmd.replace(password, "XXXXXX"))
    os.system(cmd)
