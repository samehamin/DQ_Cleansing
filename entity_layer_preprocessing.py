import pandas as pd
import re

def read_file(exten, file = None, column = None):
    df = None
    if exten == 'csv':
        df = pd.read_csv('data/' + file, encoding = 'utf-8')
    elif exten == 'excel':
        df = pd.read_excel('data/' + file, column, encoding = 'utf-8')
    return df

def sort_remove_duplicates(df):
    #df = df.sort_values("Establishment Name", inplace = True) 
    df.drop_duplicates(subset = "Establishment Name", inplace = True)
# =======================================================================


## DQ Sources preparation

data = read_file("excel", "34K_Cleansing_source.xlsx", "Sheet1")
data.describe()

# Removing duplicates
#sort_remove_duplicates(data)
data.describe()
# =======================================================================


## DQ Sources cleansing
data_clean = data.copy()

# =======================================================================