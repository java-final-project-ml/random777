#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 28 12:42:47 2021

@author: madhavrai
"""
import sys
if sys.version_info[0] < 3: 
    from StringIO import StringIO
else:
    from io import StringIO
import sys
if sys.version_info[0] < 3: 
    from StringIO import StringIO
else:
    from io import StringIO


import io
    
import urllib.request

import pandas as pd

import json
from pydomo import Domo

  
from pydomo.datasets import DataSetRequest, Schema, Column, ColumnType, Policy
from pydomo.datasets import PolicyFilter, FilterOperator, PolicyType, Sorting



#Keys retrieved according to standard practice
domo = Domo('client-id','secret',api_host='api.domo.com')



domo.logger.info("\n**** Domo API - DataSet Examples ****\n")
datasets = domo.datasets

    # Define a DataSet Schema
dsr = DataSetRequest()
dsr.name = 'Bitcoinity Data'
dsr.description = 'Daily volatility statistics from various large crypto exchanges'

#Saving the dataset ids in json but probably some other way is better
key_mapping = {}
url = 'http://data.bitcoinity.org/export_data.csv?c=e&currency=USD&data_type=volatility&f=m10&g=15&r=day&st=log&t=l&timespan=2y'
with urllib.request.urlopen(url) as f:
    html = f.read().decode('utf-8')
    



TESTDATA = StringIO(html)

bitcoinity = pd.read_csv(TESTDATA, sep=",")

bitcoinity["Time"] = bitcoinity["Time"].apply(lambda x: x[:-13])


cols = []
for col in bitcoinity.columns:
    cols.append(Column(ColumnType.STRING, col))

dsr.schema = Schema(cols)

dataset = datasets.create(dsr)


key_mapping["bitcoinity"] = dataset['id']
domo.logger.info("Created DataSet for storing bitcoinity volatility " + dataset['id'])


bitcoinity = bitcoinity.set_index("Time")


bitcoinity.to_csv("bitcoinity_volatility.csv")


csv_file_path = './bitcoinity_volatility.csv'
datasets.data_import_from_file(dataset['id'], csv_file_path)
domo.logger.info("Uploaded bitcoinity daily volatility data from the major exchanges from a csv to DataSet {}".format(
                                                            dataset['id']))












import urllib.request

class AppURLopener(urllib.request.FancyURLopener):
    version = "Mozilla/5.0"
    
    
opener = AppURLopener()
with opener.open('https://www.lmaxdigital.com/dailyVolumeXLS.php') as f:
    html = io.BytesIO(f.read())
lmax = pd.io.excel.read_excel(html,skiprows=2)

lmax["Date"] = lmax["Date"].apply(lambda x: x.split("/")[-1] + "-" + x.split("/")[1] + "-" +  x.split("/")[0] )



dsr = DataSetRequest()
dsr.name = 'LMAX'
dsr.description = "Daily trading volume USD from LMAX's crypto exchange"

cols = []
for col in bitcoinity.columns:
    cols.append(Column(ColumnType.STRING, col))

dsr.schema = Schema(cols)

dataset = datasets.create(dsr)
domo.logger.info("Created DataSet for storing lmax daily trading volume at " + dataset['id'])


lmax = lmax.set_index("Date")

lmax.to_csv("lmax_trading_volume.csv")



csv_file_path = './lmax_trading_volume.csv'
datasets.data_import_from_file(dataset['id'], csv_file_path)

key_mapping["lmax"] = dataset["id"]
domo.logger.info("Uploaded lmax daily trading volume data from from a csv to DataSet {}".format(
                                                            dataset['id']))





with open("dataset_ids.json", "w") as outfile: 
    json.dump(key_mapping, outfile)




