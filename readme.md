`mmf` enables the user to download SEC's NMFP2 reporting files from the EDGAR database. The final output of this package is a csv file that contains fund-level and security-level information of all U.S. money market funds from 2016-10 to 2019-7.

Author: Yangjue Han, New York University

1. Run **edgar_index.py** to download the index file from EDGAR and generate **edgar_htm_idx.dta** (users can save the index information to other file types and skip STATA)
2. Open **edgar_htm_idx.dta**, `keep if type == "N-MFP" | "N-MFP2"`, save to **NMFP_all_idx.dta**
3. Export csv files containing un-modified path information of N-MFP and N-MFP2 to **NMFP_idx.csv** and **NMFP2_idx.csv**
4. Run **NMFP2.py** to scrape and clean data:
    - `generate_index()` turns **NMFP2_idx.csv** to paths of .xml files (originally .htm files), generate **xmlpath.csv**
    - `crawl()` scrapes and parse xml files listed in **xmlpath.csv**, save each filing as a csv file, sorted into 20 blocks  
    - `clean()` turns raw csv file into formatted series-level and class-level table, one file for each block, **NMFP2_data_i.csv**
    - `make_port()` turns raw csv file into formatted security-level table, one for each block, **NMFP2_port_i.csv**
5. Run "combine csv files" notebook to combine 20 blocks of data into a single file
