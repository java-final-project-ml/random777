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

url = 'http://data.bitcoinity.org/export_data.csv?c=e&currency=USD&data_type=volatility&f=m10&g=15&r=day&st=log&t=l&timespan=2y'
with urllib.request.urlopen(url) as f:
    html = f.read().decode('utf-8')


TESTDATA = StringIO(html)

bitcoinity = pd.read_csv(TESTDATA, sep=",")



import urllib.request

class AppURLopener(urllib.request.FancyURLopener):
    version = "Mozilla/5.0"
    
    
opener = AppURLopener()
with opener.open('https://www.lmaxdigital.com/dailyVolumeXLS.php') as f:
    html = io.BytesIO(f.read())
df = pd.io.excel.read_excel(html)

df["Date"] df[]
