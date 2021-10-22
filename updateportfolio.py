import sys
import portfoliomanager as pm

def process(casfile, password):
    pm.pdflib.remove_password_from_pdf(casfile, password)
    pm.extractionlib.parse_casfile_for_data("converted.pdf")
    pass

if __name__ == "__main__":
    casfile = sys.argv[1]
    password = sys.argv[2]
    process(casfile, password)
