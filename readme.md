`sec-nmfp2` enables the user to download SEC's NMFP2 reporting files from the EDGAR database. The final output of this package is a csv file that contains fund-level and security-level information of all U.S. money market funds.

Author: Yangjue Han, New York University
First version: 2019.8
Updated: 2020.4

Users can simply download the program into a folder and run **main.py**, which includes the following steps:

- Step 1: download the index file from SEC EDGAR and export raw path information of N-MFP2 filings to **NMFP2_idx.csv**. With this file as input, `generate_index()` generates a csv file that contains all urls of XML filings data.

- Step 2: `scrape()` scrapes filings data from SEC EDGAR system and save each filing as a csv file in a local folder. Note: this step may take several hours. Please keep internet connection.

- Step 3: `clean()` turns raw csv file into formatted series-level and class-level tables.

- Step 4: `make_port()` turns raw csv files into formatted security-level table.
