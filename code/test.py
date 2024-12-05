import pandas as pd
import os
import requests

#read the testfile
data = pd.read_csv('data/online_retail.csv', sep = ',', encoding='ISO-8859-1')

for i in data.index:
    try:
        # convert the row to json
        export = data.loc[i].to_json()
        print(export)
    except:
        print(data.loc[i])
