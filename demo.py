import pandas as pd
import importlib
from pathlib import Path
import secmmf.mmf_data_loader.api as mmf_api

pwd
data_dir = Path('/Users/yangjuehan/Local Data/mmf_data')
pathfile = 'xmlpath.csv'

#mmf_api.download_sec_index(data_dir, form_name = 'N-MFP2', start_date = '2016-10', end_date = '2021-06')
# Step 2: based on the raw database index info, write a local index file for N-MFP2 forms
#mmf_api.generate_index(data_dir, pathfile)

#mmf_api.scrape(data_dir,pathfile)


url = 'https://sec.report/Document/0000278001-21-000024/primary_doc.xml'
pd.read_xml(url)
