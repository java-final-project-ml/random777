#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 28 10:18:45 2021

@author: madhavrai
"""
import pandas as pd
import json


import csv




times = []

rates = []

#Read csv file line by line
with open('historical_conversion_rates.csv') as csvfile:
    readCSV = csv.reader(csvfile, delimiter=',')
    for row in readCSV:
        #conversion rates column read line by line because it's does not fit json so output list of strings that contains price and token symbol in each string
        rates.append( row[2:])
        
        times.append(row[1])

        

tokens = []

price = []

csv_times = []

#Iterate through rows 1-n of list because row 0 is just columns headers
for i in range(1,len(times)):
    time = times[i][:-9]
    

    
    csv_times.append(time)

    
    #Specific string manipulation for the first element of each to get token
    tokens.append(rates[i][0].split('"')[0][1:])
        
    price.append(float(rates[i][0].split(':')[1]))
        
        
    #Middle rows 1-n-1 have the same format so they have same string manipulation
    
    for j in range(1,len(rates[i])-1):
        tokens.append(rates[i][j].split('"')[1])
        price.append(float((rates[i][j].split(':')[1])))
        csv_times.append(time)
        

    
    csv_times.append(time)
        
    #Last row needs special string manipulation to get the price
        
    tokens.append(rates[i][-1].split('"')[1])
        
    price.append(float(rates[i][-1].split(':')[1][:-2]))
    
#Store in dates, tokens, conversion rates into dataframe
df = pd.DataFrame({"date":csv_times ,"token":tokens, "conversion_rate":price })


#Set index date so there is no "Unamed:0" column in the csv when it's saved
df = df.set_index("date")


#Save csv file
df.to_csv("cleaned_historical_rates.csv")
    
    
    
    
        
        
    
        
        
        
    
    
    
        



        


        
        
        
        
        
        
        
        