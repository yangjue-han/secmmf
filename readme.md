About the code

1. Run final python script/edgar_index.py to download the index file from EDGAR and generate "edgar_htm_idx.dta"
2. Open "edgar_htm_idx.dta", keep if type == "N-MFP" or "N-MFP2", save to "NMFP_all_idx.dta"
3. Export csv files containing un-modified path information of N-MFP and N-MFP2 to "NMFP_idx.csv" and "NMFP2_idx.csv"
4. Run "NMFP2.py" to scrape and clean data:
    a. generate_index() turns NMFP2_idx.csv to paths of .xml files (originally .htm files), generate "xmlpath.csv"
    b. crawl() scrapes and parse xml files listed in "xmlpath.csv", save each filing as a csv file, sorted into 20 blocks  
    c. clean() turns raw csv file into formatted series-level and class-level table, one file for each block, "NMFP2_data_i.csv"
    d. make_port() turns raw csv file into formatted security-level table, one for each block, "NMFP2_port_i.csv"
5. Run "combine csv files" notebook to combine 20 blocks of data into a single file, could be incorporated into clean() later
