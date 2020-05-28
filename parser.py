import pandas as pd
import bs4 as bs
import untangle as ut
import urllib.request as rq
from collections import OrderedDict

class N_MFP2:

    def __init__(self):
        self.select_cols()

    def born(self, tag):
        # if tag is a single-node tag contains a navigable string, return a list with that string
        # if tag has multiple element, needs to further born them
        childs = []
        for x in tag:
            if (x != '\n') & (type(x) != bs.element.Comment):
                childs.append(x)
        return childs

    def dive(self, root, surname=''):
        name = surname + root.name
        sons = []
        for son in self.born(root):
            if type(son) == bs.element.NavigableString:
                text = ': '.join([name, son])
                sons.append(text)
            elif type(son) == bs.element.Tag:
                sons.extend(self.dive(son, surname=name + '_'))
        return sons

    def teach(self, root):
        sons = []
        for son in self.born(root):
            if len(self.born(son)) == 1:
                sons.append((son.name, son.get_text().replace('\n', '')))
            elif len(self.born(son)) > 1:
                for grandson in self.born(son):
                    sons.append((son.name + '_' + grandson.name,
                                 grandson.get_text().replace('\n', '')))
        return sons

    def teach_rec(self, root):
        sons = []
        for son in self.born(root):
            if len(self.born(son)) == 1:
                sons.append((son.name, son.get_text().replace('\n', '')))
            elif len(self.born(son)) > 1:
                sons.append(teach_rec(son))
        return sons

    def parse(self, url='https://www.sec.gov/Archives/edgar/data/759667/000070217219000020/primary_doc.xml'):

        stubs = self.stubs
        #_tonum = self._tonum
        #series_level_names = self.series_level_names
        #class_level_names = self.class_level_names

        source = rq.urlopen(url).read()
        soup = bs.BeautifulSoup(source, 'xml')

        # parse XML info into a list of dictionaries
        mmf = []
        for tag in self.born(soup.formData):
            if tag.name in ['classLevelInfo', 'generalInfo', 'seriesLevelInfo']:
                mmf.append((tag.name, self.teach(tag)))

        general_series_class = []
        general_series = mmf[0][1] + mmf[1][1]

        for i, x in enumerate(general_series):
            if x[0] == 'numberOfSharesOutstanding':
                y = list(x)
                y[0] = 'series_numberOfSharesOutstanding'
                general_series[i] = tuple(y)

        for x in mmf[2:]:
            general_series_class.append(OrderedDict(general_series + x[1]))

        df = pd.DataFrame(general_series_class)
        if 'nameOfPersonDescExpensePay' in df.columns:
            df.drop(columns='nameOfPersonDescExpensePay', inplace=True)

        # rename those columns that have reversed patterns
        namemap = []
        for x in ['weeklyGrossRedemptions', 'weeklyGrossSubscriptions']:
            namemap.append(dict([('fridayWeek' + str(i + 1) + '_' + x,
                                  x + '_' + 'fridayWeek' + str(i + 1)) for i in range(5)]))
        for x in ['totalValueDailyLiquidAssets', 'percentageDailyLiquidAssets']:
            namemap.append(dict([(x + '_' + 'fridayDay' + str(i + 1),
                                  x + '_' + 'fridayWeek' + str(i + 1)) for i in range(5)]))

        for i in range(4):
            df = df.rename(columns=namemap[i])

        # make data wide to long on weekly holding statistics
        df = pd.wide_to_long(df, stubnames=self.stubs,
                             i='classesId', j='week', sep='_', suffix='\w+')
        df.reset_index(inplace=True)
        df['week'] = df['week'].apply(
            lambda x: int(x.replace('fridayWeek', '')))

        #df = df[['week']+series_level_names+class_level_names]

        # change the type of numeric data to float
        #df[_tonum] = df[_tonum].astype(dtype = float)

        return df

    def parse_csv(self, url):
        source = rq.urlopen(url).read()
        soup = bs.BeautifulSoup(source, 'xml')
        return self.dive(soup.formData)

    def select_cols(self):

        self.stubs = ['totalValueDailyLiquidAssets', 'percentageDailyLiquidAssets',
                      'totalValueWeeklyLiquidAssets', 'percentageWeeklyLiquidAssets',
                      'netAssetValue', 'netAssetPerShare',
                      'weeklyGrossRedemptions', 'weeklyGrossSubscriptions']

        self._tonum = ['totalShareClassesInSeries',
                       'averagePortfolioMaturity',
                       'averageLifeMaturity',
                       'cash',
                       'totalValuePortfolioSecurities',
                       'amortizedCostPortfolioSecurities',
                       'totalValueOtherAssets',
                       'totalValueLiabilities',
                       'netAssetOfSeries',
                       'numberOfSharesOutstanding',
                       'stablePricePerShare',
                       'sevenDayGrossYield',
                       'minInitialInvestment',
                       'netAssetsOfClass',
                       'totalForTheMonthReported_weeklyGrossSubscriptions',
                       'totalForTheMonthReported_weeklyGrossRedemptions',
                       'sevenDayNetYield'] + self.stubs

        self.series_level_names = ['reportDate',
                                   'cik',
                                   'seriesId',
                                   'totalShareClassesInSeries',
                                   'finalFilingFlag',
                                   'fundAcqrdOrMrgdWthAnthrFlag',
                                   'securitiesActFileNumber',
                                   'adviser_adviserName',
                                   'adviser_adviserFileNumber',
                                   'indpPubAccountant_name',
                                   'indpPubAccountant_city',
                                   'indpPubAccountant_stateCountry',
                                   'administrator',
                                   'transferAgent_name',
                                   'transferAgent_cik',
                                   'transferAgent_fileNumber',
                                   'feederFundFlag',
                                   'masterFundFlag',
                                   'seriesFundInsuCmpnySepAccntFlag',
                                   'moneyMarketFundCategory',
                                   'fundExemptRetailFlag',
                                   'averagePortfolioMaturity',
                                   'averageLifeMaturity',
                                   'totalValueDailyLiquidAssets',
                                   'totalValueWeeklyLiquidAssets',
                                   'percentageDailyLiquidAssets',
                                   'percentageWeeklyLiquidAssets',
                                   'cash',
                                   'totalValuePortfolioSecurities',
                                   'amortizedCostPortfolioSecurities',
                                   'totalValueOtherAssets',
                                   'totalValueLiabilities',
                                   'netAssetOfSeries',
                                   'series_numberOfSharesOutstanding',
                                   'stablePricePerShare',
                                   'sevenDayGrossYield',
                                   'netAssetValue']
        self.class_level_names = ['classesId',
                                  'minInitialInvestment',
                                  'netAssetsOfClass',
                                  'numberOfSharesOutstanding',
                                  'netAssetPerShare',
                                  'weeklyGrossSubscriptions',
                                  'weeklyGrossRedemptions',
                                  'totalForTheMonthReported_weeklyGrossSubscriptions',
                                  'totalForTheMonthReported_weeklyGrossRedemptions',
                                  'sevenDayNetYield',
                                  'personPayForFundFlag']
