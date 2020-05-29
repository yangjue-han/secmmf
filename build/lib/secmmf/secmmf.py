import csv
import sys, os
import random
import time
import pandas as pd
import numpy as np
import timeit
import datetime

from sqlalchemy import create_engine
import sqlite3
import requests
import shutil
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from IPython.display import clear_output

from secmmf.parser import N_MFP2

class color:
   PURPLE = '\033[95m'
   CYAN = '\033[96m'
   DARKCYAN = '\033[36m'
   BLUE = '\033[94m'
   GREEN = '\033[92m'
   YELLOW = '\033[93m'
   RED = '\033[91m'
   BOLD = '\033[1m'
   UNDERLINE = '\033[4m'
   END = '\033[0m'


class DictList(dict):
    def __setitem__(self, key, value):
        try:
            # Assumes there is a list on the key
            self[key].append(value)
        except KeyError: # If it fails, because there is no key
            super(DictList, self).__setitem__(key, value)
        except AttributeError: # If it fails because it is not a list
            super(DictList, self).__setitem__(key, [self[key], value])


def download_sec_index(data_dir, form_name, start_date=None, end_date=None):

    try:
        os.makedirs(data_dir)
        os.chdir(data_dir)
    except:
        os.chdir(data_dir)

    if start_date == None:
        start_year = 2016
    else:
        start_year = int(start_date[:4])

    current_year = datetime.date.today().year
    current_quarter = (datetime.date.today().month - 1) // 3 + 1
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
        data = pd.read_sql_table('idx', conn)
        data.to_csv('edgar_htm_idx.csv')

    # Load index information
    edgar_idx = pd.read_csv('edgar_htm_idx.csv')
    edgar_idx = edgar_idx[['conm','type','cik','date','path']]

    nmfp2 = edgar_idx[edgar_idx['type']==form_name].copy()
    nmfp2.to_csv('index_file.csv',index=False)

    # remove raw index files
    os.remove('edgar_htm_idx.db')
    os.remove('edgar_htm_idx.csv')

    # select the date range of interest based on the input argument
    idx = pd.read_csv('index_file.csv')
    idx['selector'] = pd.to_datetime(idx['date'],format='%Y-%m-%d')
    if start_date != None:
        try:
            idx=idx[idx['selector']>=start_date]
        except:
            pass

    if end_date != None:
        try:
            idx=idx[idx['selector']<=end_date]
        except:
            pass

    idx.drop(columns='selector').to_csv('index_file.csv',index=False)


def generate_index(data_dir, pathfile):

    # general a table of paths linked to the XML files, saved to pathfile

    print(color.BOLD + color.RED + 'Building the XML path file.' + color.END + '\n')
    count = 0
    edgar_root = "https://www.sec.gov/Archives/edgar/data/"
    with open(os.path.join(data_dir,pathfile), 'w', newline='') as log:
        logwriter = csv.writer(log)
        with open(os.path.join(data_dir,'index_file.csv'), newline='') as infile:
            records = csv.reader(infile)
            log_row = ['conm', 'type', 'cik', 'accession_num', 'path']

            for r in records:
                if count > 0:
                    xml_url = r[4].replace(edgar_root, '')
                    cik, res = xml_url.split('/')
                    accession_num = ''.join(res.split('-')[:3])
                    xmlpath = edgar_root + cik + '/' + accession_num + '/primary_doc.xml'
                    log_row = r[:2] + [cik,accession_num,xmlpath]

                logwriter.writerow(log_row)
                count += 1
                if count%1000 == 0:
                    print(color.BLUE + 'Finished ' + str(count) + ' records...' + color.END)
    print('\n')


def scrape(data_dir, pathfile, N_blocks=20, start_block=1, end_block=20):

    edgar_root = "https://www.sec.gov/Archives/edgar/data/"

    allpaths = pd.read_csv(os.path.join(data_dir,pathfile), dtype = str)
    cik = [x for x in list(allpaths['cik'].values)]
    acc = [x for x in list(allpaths['accession_num'].values)]
    xmlpaths = [edgar_root + x[0] + '/' + x[1] + '/primary_doc.xml' for x in zip(cik,acc)]

    N = len(xmlpaths)
    block_len = int(N/N_blocks)
    res_len = N%block_len

    mmf_parser = N_MFP2() # initialize XML parser

    print(color.BOLD + color.RED + color.UNDERLINE + 'scraping filing data from SEC website ... ' + color.END)
    for i in range(start_block-1,end_block):

        # prepares a block of xml paths
        if i < (N_blocks-1):
            block_paths = xmlpaths[i*block_len:(i+1)*block_len]
        else:
            block_paths = xmlpaths[i*block_len:]

        # set up timer
        start = timeit.default_timer()
        n = 0

        # make a data directory
        blockpath = os.path.join(data_dir,'block_{}/'.format(i+1))
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

                _ = os.system('clear')
                print('Block: {}'.format(i+1))
                print('{:.2f}% finished'.format(perc_run*100))
                print('Current run time: {:.1f} minutes.'.format(mins_elapse))
                print('Expected remaining run time: {:.1f} minutes'.format(mins_elapse*multiple_remain))


def gen_table_fund(data_dir, pathfile, N_blocks=20):

    # set up paths
    allpaths = pd.read_csv(os.path.join(data_dir,pathfile), dtype = str)
    cik = [x for x in list(allpaths['cik'].values)]
    acc = [x for x in list(allpaths['accession_num'].values)]
    xmlpaths = [x[0]+'_'+x[1]+'.csv' for x in zip(cik,acc)]

    N = len(xmlpaths)
    block_len = int(N/N_blocks)
    res_len = N%block_len

    # set up special names
    outliers = [ 'classLevelInfo_fridayWeek{}_weeklyGrossSubscriptions'.format(i) for i in range(1,6)]
    outliers.extend(['classLevelInfo_fridayWeek{}_weeklyGrossRedemptions'.format(i) for i in range(1,6)])
    outliers.extend(['classLevelInfo_totalForTheMonthReported_weeklyGrossSubscriptions',
                    'classLevelInfo_totalForTheMonthReported_weeklyGrossRedemptions'])

    stubs = ['totalValueDailyLiquidAssets',
         'percentageDailyLiquidAssets',
         'totalValueWeeklyLiquidAssets',
         'percentageWeeklyLiquidAssets',
         'netAssetValue',
         'netAssetPerShare',
         'weeklyGrossRedemptions',
         'weeklyGrossSubscriptions']

    series_stubs = ['totalValueDailyLiquidAssets',
             'percentageDailyLiquidAssets',
             'totalValueWeeklyLiquidAssets',
             'percentageWeeklyLiquidAssets',
             'netAssetValue']

    class_stubs = ['netAssetPerShare',
             'weeklyGrossRedemptions',
             'weeklyGrossSubscriptions']

    stubs_long = ['seriesLevelInfo_'+ x for x in series_stubs] + ['classLevelInfo_'+ x for x in class_stubs]

    _stubs = [x+'_fridayWeek{}'.format(i) for x in stubs for i in range(1,6)]

    fundflow = ['classLevelInfo_weeklyGrossSubscriptions/fridayWeek5',
                'classLevelInfo_weeklyGrossRedemptions/fridayWeek5']

    print(color.BOLD + color.RED + color.UNDERLINE + 'Combining fund-level data file into a single one ... ' + color.END)

    N_error = 0
    N_processed = 0

    for i in range(N_blocks):
        if i < (N_blocks-1):
            block_paths = xmlpaths[i*block_len:(i+1)*block_len]
        else:
            block_paths = xmlpaths[i*block_len:]

        blockpath = os.path.join(data_dir,'block_{}/'.format(i+1))
        data_path = os.path.join(data_dir,'NMFP2_data_' + str(i+1) + '.csv')

        # set up progress tracker
        start = timeit.default_timer()
        n = 0

        # initialize buffer
        data = pd.DataFrame()

        for f in block_paths:
            n += 1
            gsc = DictList()
            port = DictList()
            '''
            Data converting block...

            '''
            if os.path.exists(blockpath+f):

                with open(blockpath+f, 'r', newline='') as infile:
                    print(blockpath+f)
                    records = csv.reader(infile)
                    count = 0
                    for r in records:
                        # reshuffle the position for certain stubs
                        if r[0] in ['generalInfo','seriesLevelInfo','classLevelInfo']:
                            if '_'.join(r[:-1]) in outliers:
                                key = [r[0],r[2],r[1]]
                                val = r[-1]
                            else:
                                key = r[:-1]
                                val = r[-1]

                            if key[1] == 'classesId':
                                count += 1

                            if key[1] == 'totalShareClassesInSeries':
                                N_class = int(val)

                            if len(key) > 2:
                                key[2] = key[2].replace('fridayDay','fridayWeek')

                            if '_'.join(key[1:]) not in _stubs:
                                # if the item is an identification item, combine two layers into one name
                                gsc['_'.join(key)] = val

                                if '_'.join(key) == 'classLevelInfo_personPayForFundFlag':
                                    if val == 'N':
                                        gsc['classLevelInfo_nameOfPersonDescExpensePay'] = 'N/A'
                            else:
                                if '_'.join(key) in ['classLevelInfo_weeklyGrossSubscriptions_fridayWeek5',
                                                    'classLevelInfo_weeklyGrossRedemptions_fridayWeek5']:
                                    val = val + '_{}'.format(count)
                                gsc['_'.join(key[:-1])+'/'+key[-1]] = val


                new_gsc = DictList()

                error_names = ['seriesLevelInfo_adviser_adviserName',
                             'seriesLevelInfo_adviser_adviserFileNumber',
                             'seriesLevelInfo_subAdviser_adviserName',
                             'seriesLevelInfo_subAdviser_adviserFileNumber',
                             'seriesLevelInfo_administrator_administratorName',
                             'seriesLevelInfo_securitiesActFileNumber',
                             'seriesLevelInfo_indpPubAccountant_name',
                             'seriesLevelInfo_indpPubAccountant_city',
                             'seriesLevelInfo_indpPubAccountant_stateCountry',
                             'seriesLevelInfo_transferAgent_name',
                             'seriesLevelInfo_transferAgent_cik',
                             'seriesLevelInfo_transferAgent_fileNumber',
                             'seriesLevelInfo_feederFundFlag',
                             'seriesLevelInfo_masterFundFlag',
                             'seriesLevelInfo_seriesFundInsuCmpnySepAccntFlag',
                             'seriesLevelInfo_moneyMarketFundCategory',
                             'seriesLevelInfo_fundExemptRetailFlag',
                             'seriesLevelInfo_feederFund_cik',
                             'seriesLevelInfo_feederFund_name',
                             'seriesLevelInfo_feederFund_fileNumber',
                             'seriesLevelInfo_feederFund_seriesId',
                             'seriesLevelInfo_masterFund_cik',
                             'seriesLevelInfo_masterFund_name',
                             'seriesLevelInfo_masterFund_fileNumber',
                             'seriesLevelInfo_masterFund_seriesId']

                # some identification items might have multiple entries, combine them
                for name in error_names:
                    try:
                        new_gsc[name] = ''.join(gsc[name])
                    except:
                        # if no such item, then pass
                        pass

                # assign values to those items with no bugs
                for key in gsc:
                    if (key not in error_names) & (key not in fundflow):
                        new_gsc[key] = gsc[key]

                # calculate the depth upon a pre-mature dictionary
                if True:
                    num_val = []
                    for key in new_gsc:
                        # a sub task that calculates the number of classes
                        if type(new_gsc[key]) == str:
                            num_val.append(1)
                        else:
                            num_val.append(len(new_gsc[key]))
                    N_class = max(num_val)

                # sometimes even if one class enters week5 data, another might not
                for item in fundflow:
                    try:
                        week5val = gsc[item]
                    except:
                        pass
                    else:
                        if type(gsc[item]) == str:
                            newval = gsc[item].split('_')[0]
                        else:
                            pos = []
                            vals = []
                            for x in gsc[item]:
                                pos.append(int(x.split('_')[1]))
                                vals.append(x.split('_')[0])

                            newval = [np.nan for x in range(N_class)]

                            for x in zip(pos,vals):
                                newval[x[0]-1] = x[1]
                        new_gsc[item] = newval


                try:
                    N_processed += 1
                    df = pd.DataFrame(new_gsc, index = range(N_class))

                except Exception as e:
                    N_error += 1
                    print('Error: ' + color.RED + e.args[0] + color.END)

                else:
                    try:
                        df2 = pd.wide_to_long(df, stubnames = stubs_long,
                                          i = ['classLevelInfo_classesId'],
                                          j = 'week', sep = '/', suffix = '\w+')
                    except Exception as e:
                        print('Error: ' + color.RED + e.args[0] + color.END)
                        pass
                    else:
                        df2.columns = [x.replace('generalInfo_','').replace('LevelInfo','').replace('series_','')
                                       for x in df2.columns]
                        df2.index.set_names(['classID','week'], inplace = True)
                        df2.reset_index(inplace = True)
                        df2.rename(columns = {'reportDate':'date'}, inplace = True)
                        df2['week'] = df2['week'].apply(lambda x: x.replace('fridayWeek',''))
                        df2['date'] = pd.to_datetime(df2['date'],format = '%Y-%m-%d')
                        df2['month'] = pd.DatetimeIndex(df2['date']).month
                        df2['year'] = pd.DatetimeIndex(df2['date']).year
                        '''
                        Finish converting...
                        '''

                        # append new dataframe
                        data = data.append(df2, ignore_index = True, sort = False)
            else:
                print('File: ' + blockpath + f + ' does not exist!')

            # progress tracker
            if n%100 == 0:
                #clear_output(wait = True)
                stop = timeit.default_timer()
                perc_run = n/block_len
                multiple_remain = (block_len-n)/n
                mins_elapse = (stop-start)/60
                error_rate = N_error/N_processed * 100

                print('Block: {}'.format(i+1))
                print('{:.2f}% finished'.format(perc_run*100))
                print('Current error rate: {:.2f}%'.format(error_rate))
                print('Current run time: {:.1f} minutes.'.format(mins_elapse))
                print('Expected remaining run time: {:.1f} minutes \n'.format(mins_elapse*multiple_remain))

        data.to_csv(data_path)


def parse_port(filepath):
    seclist = []
    general = DictList()
    sec = DictList()
    guarantor = DictList()
    sec_rating = DictList()
    has_repo = False
    repo_n = 10
    with open(filepath , 'r', newline='') as infile:
        records = csv.reader(infile)
        for r in records:
            repo_n += 1

            if r[:-1] == ['signature','registrant']:
                seclist.append(sec)

            # include the identification info
            if r[0] in ['generalInfo']:
                key = r[:-1]
                val = r[-1]
                general['_'.join(key)] = val

            # portfolio info
            # we want a single DictList() for each page of portfolio information
            # every time encounters a new entry of portfolio, create a new dictionary

            if '_'.join(r[:2]) == 'scheduleOfPortfolioSecuritiesInfo_nameOfIssuer':

                # append rating info
                try:
                    text = [ x[0]+': '+x[1]
                            for x in zip(sec_rating['ratingAgency'],sec_rating['ratingScore'])]
                    sec['NRSRO'] = '/'.join(text)
                except:
                    pass

                # append guarantor info
                for key in guarantor:
                    #print(guarantor[key], type(guarantor[key]))
                    if type(guarantor[key]) != list:
                        sec[key] = guarantor[key]
                    else:
                        sec[key] = '/'.join(guarantor[key])

                seclist.append(sec)

                # initialize new dictionaries
                sec = DictList()
                guarantor = DictList()
                sec_rating = DictList()

                for key in general:
                    sec[key] = general[key]

            if (
                r[0] == 'scheduleOfPortfolioSecuritiesInfo'
            ) & (
                r[1] not in ['NRSRO', 'guarantor', 'enhancementProvider', 'repurchaseAgreement', 'demandFeature']
            ):
                sec[r[1]] = r[-1]

            if r[0] == 'scheduleOfPortfolioSecuritiesInfo':

                # guarantor info
                if '_'.join(r[1:-1]) == 'guarantor_identityOfTheGuarantor':
                    guarantor['guarantor_name'] = r[-1]
                elif '_'.join(r[1:-1]) == 'guarantor_amountProvidedByGuarantor':
                    guarantor['guarantor_amount'] = r[-1]

                elif '_'.join(r[1:-1]) == 'guarantor_guarantorRatingOrNRSRO_nameOfNRSRO':
                    guarantor['guarantor_ratingAgency'] = r[-1]
                elif '_'.join(r[1:-1]) == 'guarantor_guarantorRatingOrNRSRO_rating':
                    guarantor['guarantor_ratingScore'] = r[-1]

                # rating info
                elif '_'.join(r[1:-1]) == 'NRSRO_nameOfNRSRO':
                    sec_rating['ratingAgency'] = r[-1]
                elif '_'.join(r[1:-1]) == 'NRSRO_rating':
                    sec_rating['ratingScore'] = r[-1]

                elif (r[1] == 'securityEligibilityFlag') & has_repo:
                    # append the last entry of repo collateral info, if any
                    for key in memo:
                            sec[key] = memo[key]
                    has_repo = False

                # repo info
                elif r[1] == 'repurchaseAgreement':

                    if r[2] == 'repurchaseAgreementOpenFlag':
                        sec['repo_OpenFlag'] = r[-1]
                        has_repo = True
                        memo = dict(
                            zip(['repo_Collateral_Issuer',
                                'repo_Collateral_MaturityDate',
                                'repo_Collateral_CouponOrYield',
                                'repo_Collateral_PrincipalAmount',
                                'repo_Collateral_Value',
                                'repo_Collateral_type'
                                ]
                                ,
                                [None for x in range(6)]
                            )

                        )

                    elif '_'.join(r[2:-1]) == 'collateralIssuers_nameOfCollateralIssuer':
                        for key in memo:
                            sec[key] = memo[key]
                        memo['repo_Collateral_Issuer'] = r[-1]
                    elif '_'.join(r[2:-1]) == 'collateralIssuers_maturityDate_date':
                        memo['repo_Collateral_MaturityDate'] = datetime.datetime.strptime(r[-1], '%Y-%m-%d')
                    elif '_'.join(r[2:-1]) == 'collateralIssuers_couponOrYield':
                        memo['repo_Collateral_CouponOrYield'] = r[-1]
                    elif '_'.join(r[2:-1]) == 'collateralIssuers_principalAmountToTheNearestCent':
                        memo['repo_Collateral_PrincipalAmount'] = r[-1]
                    elif '_'.join(r[2:-1]) == 'collateralIssuers_valueOfCollateralToTheNearestCent':
                        memo['repo_Collateral_Value'] = r[-1]
                    elif '_'.join(r[2:-1]) == 'collateralIssuers_ctgryInvestmentsRprsntsCollateral':
                        memo['repo_Collateral_type'] = r[-1]

    mmf = pd.DataFrame()
    for item in seclist[1:]:
        max_N = 1
        for k,v in item.items():
            if type(v) == list:
                max_N = max(1,len(v))

        for k,v in item.items():
            if type(v) != list:
                for i in range(max_N-1):
                    item[k] = v
        try:
            df = pd.DataFrame(item, index = range(max_N))
        except Exception as e:
            print(color.BLUE + e.args[0] + color.END)
            pass
        else:
            if 'repo_OpenFlag' in df.columns:
                df = df.drop(0)
            mmf = mmf.append(df, ignore_index = True, sort = False)

    try:
        mmf['investmentCategory'] = mmf['investmentCategory'].apply(lambda x: x.split(',')[0])
    except:
        pass
    tonum = ['percentageOfMoneyMarketFundNetAssets',
             'yieldOfTheSecurityAsOfReportingDate']
    for k in tonum:
        try:
            mmf[k] = mmf[k].astype(float)
        except:
            pass

    todate = ['finalLegalInvestmentMaturityDate', 'generalInfo_reportDate',
                  'investmentMaturityDateWAL','investmentMaturityDateWAM']
    for x in todate:
        try:
            mmf[x] = mmf[x].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d'))
        except:
            pass

    return mmf


def gen_table_holdings(data_dir, pathfile):

    # set up paths
    allpaths = pd.read_csv(os.path.join(data_dir,pathfile), dtype = str)
    cik = [x for x in list(allpaths['cik'].values)]
    acc = [x for x in list(allpaths['accession_num'].values)]
    xmlpaths = [x[0]+'_'+x[1]+'.csv' for x in zip(cik,acc)]

    N = len(xmlpaths)
    N_blocks = 20
    block_len = int(N/N_blocks)
    res_len = N%block_len

    print(color.BOLD + color.RED + color.UNDERLINE + 'Combining fund-level portfolio data into a single one ... ' + color.END)

    N_error = 0
    N_processed = 0

    for i in range(N_blocks):
        if i < (N_blocks-1):
            block_paths = xmlpaths[i*block_len:(i+1)*block_len]
        else:
            block_paths = xmlpaths[i*block_len:]

        blockpath = os.path.join(data_dir,'block_{}/'.format(i+1))
        data_path = os.path.join(data_dir,'NMFP2_port_' + str(i+1) + '.csv')

        # set up progress tracker
        start = timeit.default_timer()
        n = 0

        # initialize buffer
        data = pd.DataFrame()

        for f in block_paths:
            n += 1
            N_processed += 1
            if os.path.exists(blockpath+f):
                try:
                    print(blockpath+f)
                    df = parse_port(blockpath+f)
                    data = data.append(df, ignore_index = True, sort = False)
                except Exception as e:
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    print(color.BLUE + str(exc_tb.tb_lineno) + color.END + ' --> ' + color.RED + e.args[0] + color.END)

                    N_error +=1
                    pass
            else:
                print('File: ' + blockpath + f + ' does not exist!')

            # progress tracker
            if n%100 == 0:
                #clear_output(wait = True)
                stop = timeit.default_timer()
                perc_run = n/block_len
                multiple_remain = (block_len-n)/n
                mins_elapse = (stop-start)/60
                error_rate = N_error/N_processed * 100

                print('Block: {}'.format(i+1))
                print('{:.2f}% finished'.format(perc_run*100))
                print('Current error rate: {:.2f}%'.format(error_rate))
                print('Current run time: {:.1f} minutes.'.format(mins_elapse))
                print('Expected remaining run time: {:.1f} minutes \n'.format(mins_elapse*multiple_remain))

        data.to_csv(data_path)


def combine_fund(data_dir,N_blocks=20):
    os.chdir(os.path.join(data_dir,'fund-level'))
    df = pd.read_csv('NMFP2_data_1.csv')
    for i in range(1,N_blocks):
        df = df.append(
            pd.read_csv('NMFP2_data_{}.csv'.format(i+1),low_memory=False),
            ignore_index=True,
            sort=False
        )
    df.drop(columns='Unnamed: 0').to_csv(os.path.join(data_dir,'NMFP2_fund.csv'))
    os.chdir(data_dir)

def combine_port(data_dir,N_blocks=20):
    os.chdir(os.path.join(data_dir,'holdings-level'))
    df = pd.read_csv('NMFP2_port_1.csv',low_memory=False)
    for i in range(1,N_blocks):
        df = df.append(
            pd.read_csv('NMFP2_port_{}.csv'.format(i+1),low_memory=False),
            ignore_index=True,
            sort=False
        )
    df.drop(columns='Unnamed: 0').to_csv(os.path.join(data_dir,'NMFP2_port.csv'))
    os.chdir(data_dir)

def wrap(data_dir, N_blocks=20):
    os.chdir(data_dir)
    try:
        # create a folder to collect fund-level data
        os.makedirs(os.path.join(data_dir,'fund-level'))
    except:
        pass
        print("Fund info folder exists. Combining datasets...")

    # if the folder already exists and no existing combined file, create one
    if os.path.exists('NMFP2_fund.csv') == False:
        # first move all the files into the created folder
        for i in range(N_blocks):
            filepath = 'NMFP2_data_{}.csv'.format(i+1)
            if os.path.exists(filepath):
                shutil.move(filepath, os.path.join(data_dir,'fund-level'))

        # second create a combined file in data_dir
        combine_fund(data_dir,N_blocks)
    else:
        print("Fund-level files already combined.")

    try:
        # create a folder to collect holdings-level data
        os.makedirs(os.path.join(data_dir,'holdings-level'))
    except:
        print("Holdings folder exists. Combining portfolio holdings datasets...")

    # similarly for portfolio holdings
    if os.path.exists('NMFP2_port.csv') == False:
        # first move all the files into the created folder
        for i in range(N_blocks):
            filepath = 'NMFP2_port_{}.csv'.format(i+1)
            if os.path.exists(filepath):
                shutil.move(filepath, os.path.join(data_dir,'holdings-level'))

        # second create a combined file in data_dir
        combine_port(data_dir,N_blocks)
    else:
        print("Holdings-level files already combined.")
