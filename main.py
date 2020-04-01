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

# Write SQLite database to a csv file
engine = create_engine('sqlite:///edgar_htm_idx.db')
with engine.connect() as conn, conn.begin():
    data = pandas.read_sql_table('idx', conn)
    data.to_csv('edgar_htm_idx.csv')

# Load index information
edgar_idx = pandas.read_csv('edgar_htm_idx.csv')
edgar_idx = edgar_idx[['conm','type','cik','date','path']]

nmfp2 = edgar_idx[edgar_idx['type']=='N-MFP2'].copy()
nmfp2.to_csv('NMFP2_idx.csv',index=False)

# remove raw index files
os.remove('edgar_htm_idx.db')
os.remove('edgar_htm_idx.csv')


# =========================================================================
# Step 2: Scraping data
# =========================================================================

data_dir = "/Users/yangjuehan/Local Data/mmf_updated_202003/raw_data/"
try:
    os.makedirs(data_dir)
except:
    pathfile = 'xmlpath.csv'
    # move the index file to this folder
    source = os.path.join(os.getcwd(),'NMFP2_idx.csv')
    try:
        shutil.move(source, data_dir)
    except:
        # only download filings after 2019-6
        os.chdir(data_dir)

idx = pd.read_csv('NMFP2_idx.csv')
idx['selector'] = pd.to_datetime(idx['date'],format='%Y-%m-%d')
idx=idx[idx['selector']>'2019-7']
idx.drop(columns='selector').to_csv('NMFP2_idx.csv',index=False)

# generate urls using generate_index(data_dir,pathfile)
generate_index(data_dir,pathfile)

# scrape data
scrape(data_dir,pathfile)
