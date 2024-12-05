import pandas as pd


#read the testfile
data = pd.read_csv('data/online_retail.csv', sep = ',')

for i in data.index:
    try:
        # convert the row to json
        export = data.loc[i].to_json()
        print(export)
    except:
        print(data.loc[i])