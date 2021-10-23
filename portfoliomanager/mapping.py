import os
import re
import sys
from  fuzzywuzzy  import fuzz
import pandas as pd
from portfoliomanager import settings
import logging as log

log.basicConfig(format="[ %(levelname)s ] %(message)s", level=log.INFO, stream=sys.stdout)

def assign_fundhouse_name(fundname):
    if not fundname:
        return
    fundname = fundname.replace('-', ' ')

    for key in settings.FUND_HOUSE_MAPPING.keys():
        if key in fundname.lower().split():
            return settings.FUND_HOUSE_MAPPING[key]
    return ''

def map_cas_with_amc_data():
    def _clean_name(x):
        try:
            x = str(x)
            x = x.split("-", 1)[1]
            x = re.sub("[^a-zA-Z0-9 ]+", "", x)
            return x.lower()
        except Exception as err:
            return x
    df_cas_funds = pd.read_csv(settings.FILEPATH_CAS_FUNDS)
    df_cas_funds['folio_name_clean'] = df_cas_funds['folio_name'].apply(_clean_name)
    df_cas_funds['fund_house'] = df_cas_funds['folio_name_clean'].apply(assign_fundhouse_name)

    log.info("Total funds in CAS {}".format(df_cas_funds.shape[0]))
    print(df_cas_funds[['fund_house', 'folio_name', 'folio_name_clean']])

    df_nav = pd.read_csv(settings.FILEPATH_NAV)
    df_nav['scheme_name_clean'] = df_nav['scheme_name'].apply(lambda x: x.lower())
    df_nav['fund_house'] = df_nav['scheme_name'].apply(assign_fundhouse_name)
    print(df_nav[['fund_house', 'scheme_name', 'scheme_name_clean']])

    fund_map_generated = []
    log.info('mapping cas funds with amfi provided names')
    for cas_fund_name in df_cas_funds['folio_name_clean'].unique():
        #download_mf_nav(nav_date)

        fund_house = assign_fundhouse_name(cas_fund_name)
        log.info("======================================================================")
        log.info("cas_fund_name:\t\t {} , \tfundhouse(inferred): \t{}".format(cas_fund_name, fund_house))

        df_nav_ = df_nav[df_nav['fund_house'] == fund_house]

        max_score = 0
        max_x = None
        for x in df_nav_['scheme_name_clean'].tolist():
            score = fuzz.partial_ratio(cas_fund_name.lower(), x.lower())
            if score > max_score:
                max_score = score
                max_x = x

#             print(f"Fund House: {fund_house} \t {fund}")
        log.info("mapped_amfi_name:\t {}, \t score: \t {}".format(max_x, max_score))
        fund_map_generated.append({'cas_fund_name': cas_fund_name, 'amfi_fund_name': max_x, 'score' : max_score})

        #, 'cas_nav': row['closing_nav'],
        #    'scheme_nav': df_nav_filt[df_nav_filt['scheme_name'] == max_x]['net_asset_value'].tolist()

        #})
        # print(row['closing_nav'], df_nav_filt[df_nav_filt['scheme_name'] == max_x]['net_asset_value'].tolist())
        # print(f"\t {fund} ================== {max_x}  || Score: {max_score}")
        # break
    # pd.DataFrame(s).to_csv('data/s.csv', index=False)
