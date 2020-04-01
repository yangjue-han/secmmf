`sec-nmfp2` enables the user to download SEC's NMFP2 reporting files from the EDGAR database. The final output of this package is a csv file that contains fund-level and security-level information of all U.S. money market funds.

Author: Yangjue Han, New York University
First version: 2019.8
Date updated: 2020.4

Main procedures in **main.py**:

- Step 1
Download the index file from SEC EDGAR and export raw path information of N-MFP2 filings to **NMFP2_idx.csv**.

- Step 2
Run **NMFP2.py** to scrape and clean data:
    - Create a directory by defining `data_dir`, this folder will contain all data files generated later.
    - Create a csv file that contains urls of XML files by defining `pathfile`. Default value is **xmlpath.csv**.
    - `generate_index(data_dir,pathfile)` turns **NMFP2_idx.csv** to paths of .xml files (originally .htm files), generate **xmlpath.csv**
    - `crawl(data_dir,pathfile)` scrapes and parse xml files listed in **xmlpath.csv**, save each filing as a csv file, sorted into 20 blocks  
    - `clean(data_dir,pathfile)` turns raw csv file into formatted series-level and class-level table, one file for each block, **NMFP2_data_i.csv**
    - `make_port(data_dir,pathfile)` turns raw csv file into formatted security-level table, one for each block, **NMFP2_port_i.csv**
