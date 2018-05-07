
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
import time
from datetime import date, timedelta, datetime
from dateutil.relativedelta import relativedelta
import requests as req
import numpy as np
import pandas as pd
from pandas.plotting import scatter_matrix

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

import utili.symbols as symbols
import utili.keys as keychain



# In[4]:
def createDB():
    # Create SQL database for cleaned data
    Base = declarative_base() 

    class historicalprice(Base):
        __tablename__= "historicalprice"
        symbol = Column(String(255), primary_key=True)
        date = Column(Date, primary_key=True)
        open = Column(Float)
        high = Column(Float)
        low = Column(Float)
        close = Column(Float)
        volume = Column(Float)
        ex_dividend = Column(Float)
        split_ratio = Column(Float)
        adj_open = Column(Float)
        adj_high = Column(Float)
        adj_low = Column(Float)
        adj_close = Column(Float)
        adj_volume = Column(Float)
        RMA_9 = Column(Float)
        RMA_21 = Column(Float)

    class performancedata(Base):    
        __tablename__= "performancedata"
        BasicEPSNetIncome = Column(Float)
        CashandCashEquivalents = Column(Float)
        CommonStock = Column(Float)
        CostofRevenue = Column(Float)
        CurrentRatio = Column(Float)
        CurrentRatioFQ = Column(Float)
        CurrentRatioFY = Column(Float)
        DebttoEquity = Column(Float)
        DebttoEquityFQ = Column(Float)
        DebttoEquityFY = Column(Float)
        DilutedEPSNetIncome = Column(Float)
        EBIT = Column(Float)
        Goodwill = Column(Float)
        GrossMarginPctFQ = Column(Float)
        GrossMarginPctFY = Column(Float)
        GrossMarginPctTTM = Column(Float)
        GrossProfit= Column(Float)
        IntangibleAssets = Column(Float)
        MarketCapBasic = Column(Float)
        MarketCapTSO = Column(Float)
        NetIncome= Column(Float)
        OperatingMarginPctFQ = Column(Float)
        OperatingMarginPctFY = Column(Float)
        OperatingMarginPctTTM= Column(Float)
        OperatingProfit = Column(Float)
        PeriodEndDate = Column(Float)
        PriceBookFQ = Column(Float)
        PriceEarningsFY = Column(Float)
        PriceEarningsTTM = Column(Float)
        QuickRatioFQ = Column(Float)
        QuickRatioFY = Column(Float)
        TotalAssets = Column(Float)
        TotalCurrentAssets = Column(Float)
        TotalCurrentLiabilities = Column(Float)
        TotalLiabilities= Column(Float)
        TotalReceivablesNet = Column(Float)
        TotalRevenue = Column(Float)
        TotalStockholdersEquity = Column(Float)
        fiscalqtr =  Column(String(255), primary_key=True)
        fiscalyr =  Column(String(255))
        date = Column(Date, primary_key=True)
        symbol =  Column(String(255), primary_key=True)
        
    engine = create_engine('sqlite:///stockAI.sqlite')
    Base.metadata.create_all(bind=engine)


# In[8]:

def connect_to_db():
    """start database connection

    Returns conn, Base, session
    """
    global engine, conn, db, Base, session, db
    db = 'stockAI.sqlite'
    engine = create_engine('sqlite:///'+db)

    conn = engine.connect()
    Base = automap_base()
    Base.prepare(engine=engine, reflect=True)
    session = Session(bind=engine)
    return conn, Base, session 

def urlhistprice_quandl(ticker, start_date=None):
    '''create url for historical price for quandl query
    '''
    try:
        assert start_date != None
        #using start date and relative 10years to ensure all symbols are within date range
        yyyy, mm, dd = start_date.split('-')
        end_date = (datetime(int(yyyy), int(mm), int(dd)) + relativedelta(years=10)).date().isoformat() 
    except AssertionError:
        #start_date = (datetime.now() + timedelta(-30)).date().isoformat()
        #if no start date is provided asumme 10 year financial history 
        start_date = (datetime.now() + relativedelta(years=-10)).date().isoformat()
        end_date = date.today().isoformat() #.strftime('%Y-%m-%d')
    end_date='2018-03-31'  #matching qtrly data
    start_date='2013-03-31' #matching qtrly data
    report_range = 'start_date=' + start_date + '&end_date=' + end_date

    collaspe = '&collapse=quarterly'
    tranform = '&transform=rdiff' 
    url = 'https://www.quandl.com/api/v3/datasets/WIKI/'+ticker+ \
                 '.json?start_date=' + report_range + collaspe + keychain.quandlkey()

    return start_date, url

def getfinancials_quandl(ticker, start_date=None):
    '''gets historical price from quandl
    '''
    with open(fn, 'a+') as f:
        ticker = ticker.lower()
        try:
            assert start_date != None
        except AssertionError:
            #start_date = (datetime.now() + timedelta(-30)).date().isoformat()
            #if no start date is provided asumme 10 year financial history 
            start_date = (datetime.now() + relativedelta(years=-10)).date().isoformat()

        start_date, query_url = urlhistprice_quandl(ticker , start_date)
        #Retrieve historical prices
        response = req.get(query_url)
        status_code = response.status_code
        now = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        if status_code != 200:
            f.write('{} FAILED with {} {} {}\nurl: {}\n'.format(ticker,status_code,response.reason,now,query_url))
        elif status_code == 200:
            try:
                res = response.json()
                column_names = ('date,open,high,low,close,volume,ex_dividend,split_ratio,adj_open,'+ \
                                                    'adj_high,adj_low,adj_close,adj_volume').lower().split(',')
                df_histprice = pd.DataFrame(data=res['dataset']['data'], columns=column_names)
                df_histprice['date'] = pd.to_datetime(df_histprice['date'])
                df_histprice['symbol'] = ticker
                #
                df_histprice['RMA_9'] = df_histprice.adj_close.rolling(9).mean().shift()
                df_histprice['RMA_21'] = df_histprice.adj_close.rolling(21).mean()    
                #connect to DB
                conn, _ , _ = connect_to_db()
                df_histprice.to_sql(name='historicalprice', con=conn, index=False, if_exists='append')
                conn.close()
                now = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
                f.write('{} SUCCEEDED with {} {}\nurl: {}\n'.format(ticker,status_code,now,query_url))
            except KeyError:
                f.write('{} FAILED with {} {} {}\nurl: {}\n'.format(ticker,status_code,response.reason,now,query_url))
    return df_histprice

def getperformance(ticker):
    '''save performance data, use a boolan function for this process 
        determine current portfolio
        if new portfolio create a new directory
        save save performance data
    '''
    sQTR = '2013q1'#only 5yr data availbale changed from 2008q1'
    eQTR = '2017q4'#only 5yr data availbale changed from 2018q1'
    url = 'https://datafied.api.edgar-online.com/v1/corefinancials?primarysymbols='+ticker+'&fiscalPeriod='+sQTR+'%7E'+eQTR+'&conceptGroups=LeverageRatiosMini&duration=QTR&fields=QuickRatioFQ%2CQuickRatioFY%2CCurrentRatioFQ%2CCurrentRatioFY%2CCurrentRatio%2CTotalReceivablesNet%2CTotalStockholdersEquity%2CTotalLiabilities%2CTotalCurrentLiabilities%2CTotalCurrentAssets%2CTotalAssets%2CCashandCashEquivalents%2CIntangibleAssets%2CGoodwill%2CCommonStock%2COperatingProfit%2CTotalRevenue%2CCostofRevenue%2CEBIT%2CGrossProfit%2CNetIncome%2CBasicEPSNetIncome%2CDilutedEPSNetIncome%2CMarketCapBasic%2CMarketCapTSO%2CPriceBookFQ%2CPriceEarningsFY%2CPriceEarningsTTM%2CGrossMarginPctFQ%2CGrossMarginPctFY%2CGrossMarginPctTTM%2COperatingMarginPctFQ%2COperatingMarginPctFY%2COperatingMarginPctTTM&debug=false&sortby=primarysymbol+asc&appkey=6s8qjfgagjjmaeu2tdrg9yed'
    stdQTR = {'Q1': '-03-31','Q2': '-06-30','Q3': '-09-30','Q4': '-12-31'}
    with open(fn, 'a+') as f:
        response = req.get(url)
        status_code = response.status_code
        now = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')

        if status_code != 200:
            f.write('{} FAILED with {} {} {}\nurl: {}\n'.format(ticker,status_code,response.reason,now,url))
        elif status_code == 200:
            try:
                res = response.json()
                perfData = []
                i_rows = res['result']['totalrows']
                for i in range(i_rows):
                    j_rows = len(res['result']['rowset'][i]['groups'])
                    perfParams = {}
                    for j in range(j_rows):
                        content = res['result']['rowset'][i]['groups'][j]
                        perfParams['fiscalyr'] = content['fiscalyear']
                        perfParams['fiscalqtr'] = 'Q'+ str(content['fiscalquarter'])
                        perfParams['date'] = str(content['fiscalyear']) + stdQTR[perfParams['fiscalqtr']]
                        perfParams['symbol'] = ticker
                        k_rows = len(content['rowset'])
                        for k in range(k_rows):
                            k_key = content['rowset'][k]['field']
                            if k_key not in ['duration', 'fiscalYear', 'FiscalQuarter']:
                                perfParams[k_key] = content['rowset'][k]['value']
                    perfData.append(perfParams)
                df_perfData = pd.DataFrame(perfData)
                df_perfData['date'] = pd.to_datetime(df_perfData['date'])
                #connect to DB
                conn, _ , _ = connect_to_db()
                df_perfData.to_sql(name='performancedata', con=conn, index=False, if_exists='append')
                conn.close()
                now = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
                f.write('{} SUCCEEDED with {} {}\nurl: {}\n'.format(ticker,status_code,now,url))
            except KeyError:
                f.write('{} FAILED with {} {} {}\nurl: {}\n'.format(ticker,status_code,response.reason,now,url))
    return df_perfData

def smoothData(timeSeries):
    timeSeries['date'] = pd.to_datetime(timeSeries['date'])
    smoothSeries = timeSeries.set_index('date')
    #smoothSeries['RMA_9'] = smoothSeries.adj_close.rolling(9).mean().shift()
    #smoothSeries['RMA_21'] = smoothSeries.adj_close.rolling(21).mean()
    plt.figure(figsize=(15,10))
    plt.grid(True)
    plt.plot(smoothSeries['adj_close'],label='VMC')
    #plt.plot(smoothSeries['RMA_9'], label='MA 9 day')
    #plt.plot(smoothSeries['RMA_21'], label='MA 21 day')
    plt.legend(loc=2)
    plt.show()
    #return smoothSeries

def escape_name(s):
    """Escape name to avoid SQL injection and keyword clashes.
    Doubles embedded backticks, surrounds the whole in backticks.
    Note: not security hardened, caveat emptor.
    """
    return "`{}`".format(s.replace('`', '``'))

def quote_name(s):
    """Quotes query items
    """
    return "'{}'".format(s)

def selectFromDict(table, dict_col, wherecol, dict_items):
    """Take dictionary object dict and produce sql for 
    selecting for named table"""
    adjunct =' OR '+ wherecol+' ='
    sql = 'SELECT '
    sql += ', '.join(map(escape_name, dict_col))
    sql += ' FROM ' + table
    sql += ' WHERE ' + wherecol + ' ='
    sql += adjunct.join(map(quote_name, dict_items))
    sql += ' ORDER BY'
    sql += ' symbol, date;'
    return sql

def sql_query_to_df(tablename, dict_col, wherecol, dict_items):
    """dict_col: table columns to return
       dict_item: items to query
       wherecol: column name with dict_items 
    """
    conn, _ , _ = connect_to_db()
    with conn:
        sql = selectFromDict(tablename, dict_col, wherecol, dict_items)
        df = pd.read_sql_query(sql, conn, index_col= 'date')
    return df 

def computeRatio(x):
    result = {'symbol': x['symbol'], 'date': x['date'], \
                'adj_openRatio': x['adj_open']/x['adj_open'].shift(1), \
                'adj_closeRatio': x['adj_close']/x['adj_close'].shift(1)
    }
    return pd.DataFrame(result)

def preprocess():
    conn, _ , _ = connect_to_db()
    with conn:
# Get target
        sql="Select date, adj_open, adj_close, symbol FROM historicalprice ORDER BY date"
        df_target_ = pd.read_sql_query(sql, conn, index_col='date')
        df_target_['symbol'] = df_target_['symbol'].str.upper()
        #df_target['adj_closeRatio'] = df_target['adj_close']/df_target['adj_close'].shift(1)
# Get features
        sql="Select date, fiscalqtr, MarketCapBasic, TotalRevenue, CommonStock, NetIncome, \
        TotalCurrentAssets, TotalCurrentLiabilities, TotalStockholdersEquity, symbol, EBIT, \
        DebttoEquity, GrossMarginPctTTM, OperatingMarginPctTTM, BasicEPSNetIncome, PriceEarningsTTM, \
        PriceEarningsFY, PriceBookFQ\
        FROM performancedata \
        ORDER BY date"
        df_features_ = pd.read_sql_query(sql, conn, index_col='date')
# compute quartly price ratio
    df_target_ = df_target_.reset_index()
    df_target_ = df_target_.groupby(['symbol']).apply(lambda x: computeRatio(x)).dropna()
# Compute key ratios
    #df_features['EPS']=df_features['CommonStock']/df_features['NetIncome']
    #df_features['PEG']=df_features['PE']/(df_features['EPS_PY']/df_features['EPS_CY']-1)
    #df_features['PBV'] = df_features['MarketCapBasic']/(df_features['TotalCurrentAssets']-df_features['TotalCurrentLiabilities'])
    df_features_['ROA'] = df_features_['NetIncome']/df_features_['TotalStockholdersEquity']

    df_features_ = df_features_.reset_index()
    data = df_target_.merge(df_features_, on=['symbol', 'date']).dropna().set_index('date')
    #features['PE'] = features['adj_close']/features['EPS']
    target = data.iloc[:,:3]
    features = data.iloc[:,2:]
    target.to_csv('target.csv')
    features.to_csv('features.csv')
    return target, features

# In[10]:
def main():
    #createDB()
    time.localtime()
    startTime = time.time()
    #for idx, sym in enumerate(symbols.tickersymbols()):
        #if idx > 501:
        #if sym == 'YUM':
            #res = getperformance(sym)
            #time.sleep(35)
    #for idx, sym in enumerate(symbols.tickersymbols()):
    #    if idx > 83:
    #        timeSeries = getfinancials_quandl(sym,start_date='2013-03-31')
    #        time.sleep(35)
    #smoothSeries =
    #smoothData(timeSeries)
    target, features = preprocess()
    print('Done after {}s'.format(time.time()-startTime))


# In[9]:
if __name__ == '__main__':
    fn = 'stockIA.log'
    main()



