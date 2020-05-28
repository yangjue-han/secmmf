from secmmf.secmmf import *

data_dir = '' ## THE DIRECTORY TO STORE YOUR DATA, ESTIMATED USAGE: 3GB/YR ##
pathfile = 'xmlpath.csv'

download_sec_index(data_dir=data_dir,form_name='N-MFP2',start_date='2020-05')

# generate urls using generate_index(data_dir,pathfile)
generate_index(data_dir,pathfile)

# scrape data
scrape(data_dir,pathfile)

# clean data
gen_table_fund(data_dir,pathfile)

# make portfolio tables
gen_table_holdings(data_dir, pathfile)

# combine all tables together
wrap(data_dir)
