#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 28 16:58:31 2021

@author: madhavrai
"""
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
from pydomo import Domo

  
from pydomo.datasets import DataSetRequest, Schema, Column, ColumnType, Policy
from pydomo.datasets import PolicyFilter, FilterOperator, PolicyType, Sorting

import json

#Retrieving dataset ids from json but probably needs to be changed for normal system
with open('sample.json') as json_file:
    data = json.load(json_file)

#Domo api Keys retrieved according to procedure
domo = Domo('client-id','secret',api_host='api.domo.com')



domo.logger.info("\n**** Domo API - DataSet Examples ****\n")
datasets = domo.datasets


dataset = datasets.get(data["bitcoinity"])
domo.logger.info("Retrieved DataSet " + dataset['id'])



url = 'http://data.bitcoinity.org/export_data.csv?c=e&currency=USD&data_type=volatility&f=m10&g=15&r=day&st=log&t=l&timespan=2y'
with urllib.request.urlopen(url) as f:
    html = f.read().decode('utf-8')
    



TESTDATA = StringIO(html)

bitcoinity = pd.read_csv(TESTDATA, sep=",")

bitcoinity["Time"] = bitcoinity["Time"].apply(lambda x: x[:-13])


cols = []
for col in bitcoinity.columns:
    cols.append(Column(ColumnType.STRING, col))




bitcoinity = bitcoinity.set_index("Time")


bitcoinity.to_csv("bitcoinity_volatility.csv")


csv_file_path = './bitcoinity_volatility.csv'
datasets.data_import_from_file(dataset['id'], csv_file_path)



class AppURLopener(urllib.request.FancyURLopener):
    version = "Mozilla/5.0"
    
    
opener = AppURLopener()
with opener.open('https://www.lmaxdigital.com/dailyVolumeXLS.php') as f:
    html = io.BytesIO(f.read())
lmax = pd.io.excel.read_excel(html,skiprows=2)

lmax["Date"] = lmax["Date"].apply(lambda x: x.split("/")[-1] + "-" + x.split("/")[1] + "-" +  x.split("/")[0] )

dataset = datasets.get(data["lmax"])
cols = []
for col in bitcoinity.columns:
    cols.append(Column(ColumnType.STRING, col))




lmax = lmax.set_index("Date")

lmax.to_csv("lmax_trading_volume.csv")



csv_file_path = './lmax_trading_volume.csv'
datasets.data_import_from_file(dataset['id'], csv_file_path)
domo.logger.info("Uploaded lmax daily trading volume data from from a csv to DataSet {}".format(
                                                            dataset['id']))


















