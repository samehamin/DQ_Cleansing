# -*- coding: utf-8 -*-
"""
Created on Tue Jan  8 18:33:01 2019

@author: sameamin
"""

import pandas as pd
import re

def pipeline_read_file(exten, file = None, column = None):
    df = None
    if exten == 'csv':
        df = pd.read_csv('data/' + file, encoding = 'utf-8')
    elif exten == 'excel':
        df = pd.read_excel('data/' + file, column, encoding = 'utf-8')
    return df

def pipeline_remove_duplicates_empty(tokens):
    tokens = list(filter(None, tokens))
    tokens = list(set(tokens))
    return tokens

def pipeline_remove_specials_spaces(tokens):
    tokens = [ re.sub("[^\u0621-\u06FF]+", ' ', str(token)) for  token in tokens]
    tokens = [re.sub("[ۭ]+", ' ', token) for  token in tokens]
    tokens = [token.strip() for  token in tokens if len(token.strip()) > 1]
    return tokens

def pipeline_remove_abbrev(tokens):
    # remove single chars
    for i in range(len(tokens)):
        token = tokens[i]
        token = ' '.join([w for w in str(token).split() if len(w) > 1])
        tokens[i] = token
    
    # remove abbreviations
    for i in range(len(tokens)):
        token = tokens[i]
        token = re.sub(" ذم ", ' ', token)
        token = re.sub(" م ", ' ', token)
        token = re.sub(" ار ", ' ', token)
        token = re.sub(" هه ", ' ', token)
        token = re.sub(" ش ذ م م ", ' ', token)
        token = re.sub(" ش م ذ ", ' ', token)
        token = re.sub(" ذ م م ", ' ', token)
        
        tokens[i] = token

    return tokens
    
def get_term_frequently(df):
    df = df['Entity Name'].str.split(expand=True).stack().value_counts()
    return df

def export_to_file(exten, df, file_name):
    if exten == 'excel':
        writer = pd.ExcelWriter(file_name)
        df.to_excel(writer,'Sheet1')
        writer.save()
    elif exten == 'csv':
        df.to_csv(file_name, encoding='utf-8', index=False)

#=====================================================================    
# pipeline Start
#=====================================================================
source_file_name = '34K_Cleansing_source.xlsx'

# read the file
data = pipeline_read_file('excel', source_file_name, column="data")
data.describe()

tokens = data['Establishment Name'].values

# special charachters and spaces
tokens = pipeline_remove_specials_spaces(tokens)

# remove abbreviations
tokens = pipeline_remove_abbrev(tokens)

# remove duplicates
tokens = pipeline_remove_duplicates_empty(tokens)


data = pd.DataFrame({'Entity Name': tokens})
data.head()

# check term frequently 
df_term_freq = get_term_frequently(data)

# write the file
export_to_file('csv', data, 'output/output_' + source_file_name)