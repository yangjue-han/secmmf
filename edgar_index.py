import datetime
import pandas
from sqlalchemy import create_engine
import sqlite3
import requests

current_year = datetime.date.today().year
current_quarter = (datetime.date.today().month - 1) // 3 + 1
start_year = 1993
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

# Write SQLite database to Stata
engine = create_engine('sqlite:///edgar_htm_idx.db')
with engine.connect() as conn, conn.begin():
    data = pandas.read_sql_table('idx', conn)
    data.to_stata('edgar_htm_idx.dta')
