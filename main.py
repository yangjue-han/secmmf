import datetime
import pandas
from sqlalchemy import create_engine
import sqlite3
import requests
import os
import shutil
from NMFP2 import *

# =========================================================================
# Step 1: download index file from SEC EDGAR system
# =========================================================================

current_year = datetime.date.today().year
current_quarter = (datetime.date.today().month - 1) // 3 + 1
start_year = 2016 # NMFP2 starts from Oct 2016
years = list(range(start_year, current_year))
quarters = ['QTR1', 'QTR2', 'QTR3', 'QTR4']
history = [(y, q) for y in years for q in quarters]
for i in range(1, current_quarter + 1):
    history.append((current_year, 'QTR%d' % i))
urls = ['https://www.sec.gov/Archives/edgar/full-index/%d/%s/crawler.idx' %
        (x[0], x[1]) for x in history]
urls.sort()

# Download index files and write content into SQLite
con = sqlite3.connect('edgar_htm_idx.db')
cur = con.cursor()
cur.execute('DROP TABLE IF EXISTS idx')
cur.execute(
    'CREATE TABLE idx (conm TEXT, type TEXT, cik TEXT, date TEXT, path TEXT)')

for url in urls:
    lines = requests.get(url).text.splitlines()
    nameloc = lines[7].find('Company Name')
    typeloc = lines[7].find('Form Type')
    cikloc = lines[7].find('CIK')
    dateloc = lines[7].find('Date Filed')
    urlloc = lines[7].find('URL')
    records = [tuple([line[:typeloc].strip(), line[typeloc:cikloc].strip(), line[cikloc:dateloc].strip(),
                      line[dateloc:urlloc].strip(), line[urlloc:].strip()]) for line in lines[9:]]
    cur.executemany('INSERT INTO idx VALUES (?, ?, ?, ?, ?)', records)
    print(url, 'downloaded and wrote to SQLite')

con.commit()
con.close()

# Write SQLite database to Stata and csv
engine = create_engine('sqlite:///edgar_htm_idx.db')
with engine.connect() as conn, conn.begin():
    data = pandas.read_sql_table('idx', conn)
    #data.to_stata('edgar_htm_idx.dta')
    data.to_csv('edgar_htm_idx.csv')

# Load index information
edgar_idx = pandas.read_csv('edgar_htm_idx.csv')
edgar_idx = edgar_idx[['conm','type','cik','date','path']]
edgar_idx.head()

nmfp2 = edgar_idx[edgar_idx['type']=='N-MFP2'].copy()
nmfp2.to_csv('NMFP2_idx.csv',index=False)

#pd.read_csv('NMFP2_idx.csv')

# remove raw index files
os.remove('edgar_htm_idx.db')
os.remove('edgar_htm_idx.csv')


# =========================================================================
# Step 2: Scraping data
# =========================================================================

data_dir = "/Users/yangjuehan/Local Data/mmf_updated_202003/raw_data/"
os.makedirs(data_dir)
pathfile = 'xmlpath.csv'

# move the index file to this folder
source = os.path.join(os.getcwd(),'NMFP2_idx.csv')
shutil.move(source, data_dir)

# only download filings after 2019-6
os.chdir(data_dir)
idx = pd.read_csv('NMFP2_idx.csv')
idx['selector'] = pd.to_datetime(idx['date'],format='%Y-%m-%d')
idx=idx[idx['selector']>'2019-7']
idx.drop(columns='selector').to_csv('NMFP2_idx.csv',index=False)

# generate urls using generate_index(data_dir,pathfile)
generate_index(data_dir,pathfile)
pd.read_csv('xmlpath.csv').head()

# scrape data
crawl2(data_dir,pathfile)

def crawl2(data_dir , pathfile):

    edgar_root = "https://www.sec.gov/Archives/edgar/data/"

    allpaths = pd.read_csv(data_dir + pathfile, dtype = str)
    cik = [x for x in list(allpaths['cik'].values)]
    acc = [x for x in list(allpaths['accession_num'].values)]
    xmlpaths = [edgar_root + x[0] + '/' + x[1] + '/primary_doc.xml' for x in zip(cik,acc)]

    N = len(xmlpaths)
    N_blocks = 20
    block_len = int(N/N_blocks)
    res_len = N%block_len

    mmf_parser = N_MFP() # initialize XML parser

    print(color.BOLD + color.RED + color.UNDERLINE + 'scraping filing data from SEC website ... ' + color.END)
    for i in range(N_blocks):

        # prepares a block of xml paths
        if i < (N_blocks-1):
            block_paths = xmlpaths[i*block_len:(i+1)*block_len]
        else:
            block_paths = xmlpaths[i*block_len:]

        # set up timer
        start = timeit.default_timer()
        n = 0

        # make a data directory
        blockpath = data_dir + 'block_{}/'.format(i+1)
        os.mkdir(blockpath)

        # scraping loop
        for f in block_paths:
            data = mmf_parser.parse_csv(f)
            cik_acc = f.split('/')[-3:-1]

            with open(blockpath + cik_acc[0] + '_' + cik_acc[1] + '.csv' , 'w', newline='') as log:
                logwriter = csv.writer(log)
                for item in data:
                    keys = [x.strip().replace('\n','') for x in item.split(':')[0].split('_')]
                    value = item.split(':')[1].strip().replace('\n','')
                    logwriter.writerow(keys[1:] + [value])

            # progress tracker
            n += 1
            if n%10 == 0:
                clear_output(wait = True)
                stop = timeit.default_timer()
                perc_run = n/block_len
                multiple_remain = (block_len-n)/n
                mins_elapse = (stop-start)/60

                print('Block: {}'.format(i+1))
                print('{:.2f}% finished'.format(perc_run*100))
                print('Current run time: {:.1f} minutes.'.format(mins_elapse))
                print('Expected remaining run time: {:.1f} minutes'.format(mins_elapse*multiple_remain))
