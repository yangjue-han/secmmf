# SEC N-MFP2 Money Market Fund Holdings Data

- Author: Yangjue Han 
- Date: May 2020

## Introduction
This repository contains code that enables the user to parse and download money market fund holdings information in N-MFP2 filings from SEC EDGAR system. At the end of every month, all U.S. money market funds are required to report their securities holdings to SEC, including identification, maturity, market value, yield to maturity, issuer information, and other features. For repurchase agreement contracts, money market funds also have to report information on collateral securities. The granularity of this dataset provides an unparallel opportunity for financial economists to study questions related to the shadow banking system. 

## Installation

```
pip install secmmf
```

## Usage

The module `secmmf` contains a set of functions that parse and download the information in N-MFP2 filings. The user should first specify the path of a directory to store the downloaded data to `data_dir` and the storage of `data_dir` should be at least 20GBs. 

```
import secmmf

data_dir = ## YOUR DIRECTORY HERE ##
pathfile = 'xmlpath.csv' # no need to change this
```

First we download and extract the paths of filings from SEC EDGAR system using method `download_sec_index()`. By specifying `start_date` and `end_date`, the user will limit the time range to [`start_date`,`end_date`]. The default start date is 2016-10 and end date is the current month. The method will output a csv file named `index_file.csv` in `data_dir`.
```
secmmf.download_sec_index(data_dir, form_name = 'N-MFP2', start_date = '2016-10', end_date = '2020-05')
```


