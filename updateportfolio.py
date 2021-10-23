import sys
import portfoliomanager as pm

def process(casfile, password):

    converted_casfile = "converted.pdf"
    #pm.pdflib.remove_password_from_pdf(casfile, password, converted_casfile)
    #pm.extractionlib.parse_casfile_for_data(converted_casfile)
    #pm.extractionlib.visualize_casfile(converted_casfile)
    #pm.amfi.download_mf_nav()
    pm.mapping.map_cas_with_amc_data()
    pass

if __name__ == "__main__":
    casfile = sys.argv[1]
    password = sys.argv[2]
    process(casfile, password)
