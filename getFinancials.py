
# coding: utf-8

# In[3]:


#!/usr/bin/env python
# -*- coding: utf-8 -*-
''' Retrieves 10-year historical stock prices from Quandl given ticker symbol. 
    Optional start date argument may be used to define date range of interest.
'''
import os
#import json
import re
#import random as rnd
from datetime import date, timedelta, datetime
from dateutil.relativedelta import relativedelta
import requests as req
import numpy as np
import pandas as pd

import sqlalchemy as sa
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, exc
from sqlalchemy.sql import select, update, insert, table

from sqlalchemy.ext.declarative import declarative_base # extract classes into tables
from sqlalchemy import Column, Integer, String, Float, Date, MetaData 

import pymysql
import csv
import matplotlib.pyplot as plt


# In[4]:


# Create SQL database for cleaned data
Base = declarative_base() 

class stock(Base):
    __tablename__= "historicalprice"
    ticker = Column(String(255), primary_key=True)
    date = Column(Date)
    open = Column(Float)
    high = Column(Float)
    low_close = Column(Float)
    volume = Column(Float)
    ex_dividend = Column(Float)
    split_ratio = Column(Float)
    adj_open = Column(Float)
    adj_low = Column(Float)
    adj_close = Column(Float)
    adj_volume = Column(Float)
    
    __tablename__= "performancedata"
    ticker = Column(String(255), primary_key=True)
    date = Column(Date)
    Revenue_USD_Mil = Column(Float)
    Gross_Margin = Column(Float)
    Operating_Income_USD_Mil = Column(Float)
    Operating_Margin = Column(Float)
    Net_Income_USD_Mil = Column(Float)
    Earnings_Per_Share_USD = Column(Float)
    Dividends_USD = Column(Float)
    Payout_Ratio = Column(Float)
    Shares_Mil = Column(Float)
    Book_Value_Per_Share_USD = Column(Float)
    
engine = create_engine('sqlite:///stockAI.sqlite')
Base.metadata.create_all(engine)
Base.metadata.tables


# In[8]:


def urlhistprice_quandl(ticker, start_date=None):
    '''create url for historical price for quandl query
    '''
    try:
        assert start_date != None
    except AssertionError:
        #start_date = (datetime.now() + timedelta(-30)).date().isoformat()
        #if no start date is provided asumme 10 year financial history 
        start_date = (datetime.now() + relativedelta(years=-10)).date().isoformat()
    end_date = date.today().isoformat() #.strftime('%Y-%m-%d')
    report_range = 'start_date=' + start_date + '&end_date=' + end_date

    url = 'https://www.quandl.com/api/v3/datasets/WIKI/'+ticker+             '.json?start_date=' + report_range
    return start_date, url

def getfinancials_quandl(ticker, start_date=None):
    '''gets historical price from quandl
    '''
    ticker = ticker.lower()
    try:
        assert start_date != None
    except AssertionError:
        #start_date = (datetime.now() + timedelta(-30)).date().isoformat()
        #if no start date is provided asumme 10 year financial history 
        start_date = (datetime.now() + relativedelta(years=-10)).date().isoformat()

    start_date, query_url = urlhistprice_quandl(ticker , start_date)
    #Retrieve historical prices
    res = req.get(query_url).json()
    column_names = ('Date,Open,High,Low,Close,Volume,Ex_Dividend,Split_Ratio,Adj_Open,'+                                     'Adj_High,Adj_Low,Adj_Close,Adj_Volume').lower().split(',')
    df_histprice = pd.DataFrame(data=res['dataset']['data'], columns=column_names) #res['dataset']['column_names'])
    ##TODO:
        #Add ticker column if one sql table will be used for all tickers
        
    #connect to DB
    #_, Base , _ = connect_to_db()
    ###df_histprice.set_index('Date', inplace=True)
    #df_histprice.to_sql(ticker, engine, index=False, if_exists='replace',  \
                        #dtype={'date': Date})
    #conn.execute('CREATE INDEX idx_'+ticker+'_date ON '+db+'.'+ticker+' (date)')
    return df_histprice

def urlkeyratios(ticker):
    """ Build URL for required to get key ratio performance data for given ticker"""
    return 'http://financials.morningstar.com/ajax/exportKR2CSV.html?t='+ ticker

def getperformance(ticker):
    '''save performance data, use a boolan function for this process 
        determine curr5ent portfolio
        if new portfolio create a new directory
        save save performance data
    '''
    b_keyratios = req.get(urlkeyratios(ticker))
    str_keyratios = (b_keyratios.content.decode("utf-8-sig"))
    #write to file
    f = open(ticker+'_performance_key-ratios.txt', 'w')
    f.write(str_keyratios)
    f.close()

    fn = ticker+'_performance_key-ratios.txt'
    if not os.path.isfile(fn):
        print('{} does not exist' .format(fn))
        exit()
    dict_perf = {}
    pef_indx = 0
    #var_pef = ['fin', 'key_ratio', 'prof', 'growth', 'cash_flow', 'bal_sht', \
    #                                                            'liqu', 'eff']
    var_pef = ['financials', 'key_ratio', 'prof', 'growth', 'cash_flow', 'bal_sht',                                                                 'liqu', 'eff']                                                                
    with open(fn, 'r') as f:
        lines = f.readlines()
        csv_ln = csv.reader(lines)
        pgstrt = [k for k in range(len(lines)) if lines[k][-4:].strip() =='TTM' or lines[k][-4:].strip() =='Qtr']
        pgbrk = [x-1 for x in range(len(lines)) if lines[x].strip()=='']
        pgbrk.append(len(lines)) 
        #pgstrt=2; pgbrk=17
        for ln in csv_ln:
            perf_key = var_pef[pef_indx]
        #loop thru perfromance characteristics and record dictionary
            if 'TTM' in ln or 'Latest Qtr' in ln:
                while csv_ln.line_num >= pgstrt[pef_indx]  and csv_ln.line_num <= pgbrk[pef_indx]:
                    #add all values of the current perf characteristics to the dict
                    if perf_key not in dict_perf.keys():
                        #add the performace variable to the dict
                        dict_perf['{}'.format(perf_key)] = []
                        dict_perf[perf_key].append(ln)
                        #print(dict_perf.keys())
                    else:
                        try:
                            dict_perf[perf_key].append(csv_ln.__next__())
                            #print(csv_ln.__next__(), csv_ln.line_num)
                        except StopIteration:
                            pgbrk[pef_indx] = pgbrk[pef_indx]-1

                else:
                    #convert dict into np array
                    np_tmp = np.array(dict_perf[perf_key])
                    if dict_perf[perf_key][0][0] == '' and  perf_key == 'financials':
                        np_tmp[0][0] = 'Financial Categories'
                    elif dict_perf[perf_key][0][0] == '' and  len(dict_perf[perf_key][1]) == 1:
                        table_names = []
                        rows = []
                        for row in range(len(np_tmp)):
                            if len(np_tmp[row]) == 1:
                                table_names.append(np_tmp[row])
                                rows.append(row)
                        np_ttmp = np.delete(np_tmp, rows) 
                        np_tmp = np_ttmp
                    try:
                        #set index to first element from rows starting for row 1
                        index = list(np.reshape(np_tmp[:,0], np_tmp[:,0].size))
                        #set cols to all elements from row 0
                        cols = list(np.reshape(np_tmp[0,:], np_tmp[0,:].size))
                        #set data to all elements from row 1
                        data = np_tmp[1:,:]
                        dates = np.array(cols[1:]).reshape(len(cols[1:]),1)
                        data3 = data.T
                        data3 = np.char.replace(data3,u',', u'')
                        data3 = np.char.replace(data3,' ', '_')
                        tmp_ = np.insert(dates, 0, 'date', axis=0)
                        tmp_ = np.insert(tmp_, 1, ticker, axis=1)
                        tmp_[0][1]='symbol'
                        data3 = np.concatenate((tmp_,data3), axis=1)
                        data3[data3=='']=np.nan
                        df_finan = pd.DataFrame(data=data3[1:][:], columns=data3[0][:])
                        df_finan[df_finan=='nan']=np.nan
                        df_finan.iloc[:,2:].apply(pd.to_numeric)
                    except IndexError as growth:
                        pass
                    ticker = ticker.lower()
                    #df_finan.to_sql(perf_key, engine, index=False, if_exists='append')
                    #print(df_finan)
                    pef_indx+=1
    f.close()
    #return dict_perf
    return df_finan


# In[10]:

def tickersymbols():
    """define list of tickers
    """
    tickers =['MU', 'GIB', 'BIIB', 'UTHR', 'OZRK', 'BX', 'UFPO', 'TX', 'GOOG', 'APPL']
    s_and_p = ['MMM','ABT','ABBV','ACN','ATVI','AYI','ADBE','AMD','AAP','AES','AET',
		'AMG','AFL','A','APD','AKAM','ALK','ALB','ARE','ALXN','ALGN','ALLE',
		'AGN','ADS','LNT','ALL','GOOGL','GOOG','MO','AMZN','AEE','AAL','AEP',
		'AXP','AIG','AMT','AWK','AMP','ABC','AME','AMGN','APH','APC','ADI','ANDV',
		'ANSS','ANTM','AON','AOS','APA','AIV','AAPL','AMAT','APTV','ADM','ARNC',
		'AJG','AIZ','T','ADSK','ADP','AZO','AVB','AVY','BHGE','BLL','BAC','BK',
		'BAX','BBT','BDX','BRK.B','BBY','BIIB','BLK','HRB','BA','BWA','BXP','BSX',
		'BHF','BMY','AVGO','BF.B','CHRW','CA','COG','CDNS','CPB','COF','CAH','CBOE',
		'KMX','CCL','CAT','CBG','CBS','CELG','CNC','CNP','CTL','CERN','CF','SCHW',
		'CHTR','CHK','CVX','CMG','CB','CHD','CI','XEC','CINF','CTAS','CSCO','C','CFG',
		'CTXS','CLX','CME','CMS','KO','CTSH','CL','CMCSA','CMA','CAG','CXO','COP',
		'ED','STZ','COO','GLW','COST','COTY','CCI','CSRA','CSX','CMI','CVS','DHI',
		'DHR','DRI','DVA','DE','DAL','XRAY','DVN','DLR','DFS','DISCA','DISCK','DISH',
		'DG','DLTR','D','DOV','DWDP','DPS','DTE','DRE','DUK','DXC','ETFC','EMN','ETN',
		'EBAY','ECL','EIX','EW','EA','EMR','ETR','EVHC','EOG','EQT','EFX','EQIX','EQR',
		'ESS','EL','ES','RE','EXC','EXPE','EXPD','ESRX','EXR','XOM','FFIV','FB','FAST',
		'FRT','FDX','FIS','FITB','FE','FISV','FLIR','FLS','FLR','FMC','FL','F','FTV',
		'FBHS','BEN','FCX','GPS','GRMN','IT','GD','GE','GGP','GIS','GM','GPC','GILD',
		'GPN','GS','GT','GWW','HAL','HBI','HOG','HRS','HIG','HAS','HCA','HCP','HP','HSIC',
		'HSY','HES','HPE','HLT','HOLX','HD','HON','HRL','HST','HPQ','HUM','HBAN','HII',
		'IDXX','INFO','ITW','ILMN','IR','INTC','ICE','IBM','INCY','IP','IPG','IFF','INTU',
		'ISRG','IVZ','IQV','IRM','JEC','JBHT','SJM','JNJ','JCI','JPM','JNPR','KSU','K','KEY',
		'KMB','KIM','KMI','KLAC','KSS','KHC','KR','LB','LLL','LH','LRCX','LEG','LEN','LUK',
		'LLY','LNC','LKQ','LMT','L','LOW','LYB','MTB','MAC','M','MRO','MPC','MAR','MMC','MLM',
		'MAS','MA','MAT','MKC','MCD','MCK','MDT','MRK','MET','MTD','MGM','KORS','MCHP','MU',
		'MSFT','MAA','MHK','TAP','MDLZ','MON','MNST','MCO','MS','MOS','MSI','MYL','NDAQ',
		'NOV','NAVI','NTAP','NFLX','NWL','NFX','NEM','NWSA','NWS','NEE','NLSN','NKE','NI',
		'NBL','JWN','NSC','NTRS','NOC','NCLH','NRG','NUE','NVDA','ORLY','OXY','OMC','OKE',
		'ORCL','PCAR','PKG','PH','PDCO','PAYX','PYPL','PNR','PBCT','PEP','PKI','PRGO','PFE',
		'PCG','PM','PSX','PNW','PXD','PNC','RL','PPG','PPL','PX','PCLN','PFG','PG','PGR',
		'PLD','PRU','PEG','PSA','PHM','PVH','QRVO','PWR','QCOM','DGX','RRC','RJF','RTN','O',
		'RHT','REG','REGN','RF','RSG','RMD','RHI','ROK','COL','ROP','ROST','RCL','CRM','SBAC',
		'SCG','SLB','SNI','STX','SEE','SRE','SHW','SIG','SPG','SWKS','SLG','SNA','SO','LUV',
		'SPGI','SWK','SBUX','STT','SRCL','SYK','STI','SYMC','SYF','SNPS','SYY','TROW','TPR',
		'TGT','TEL','FTI','TXN','TXT','TMO','TIF','TWX','TJX','TMK','TSS','TSCO','TDG','TRV',
		'TRIP','FOXA','FOX','TSN','UDR','ULTA','USB','UAA','UA','UNP','UAL','UNH','UPS','URI',
		'UTX','UHS','UNM','VFC','VLO','VAR','VTR','VRSN','VRSK','VZ','VRTX','VIAB','V','VNO',
		'VMC','WMT','WBA','DIS','WM','WAT','WEC','WFC','HCN','WDC','WU','WRK','WY','WHR','WMB',
		'WLTW','WYN','WYNN','XEL','XRX','XLNX','XL','XYL','YUM','ZBH','ZION','ZTS']

# loop thru tickers and get historical price date
#for ticker in tickers:
    #getfinancials_quandl(ticker)


# In[9]:

def main():
    res = getperformance("goog")
    print(res)

if __name__ == '__main__':
    main()


