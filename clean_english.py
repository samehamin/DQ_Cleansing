# -*- coding: utf-8 -*-
"""
Created on Tue Jan  8 18:33:01 2019

@author: sameamin
"""

import pandas as pd
import re
from num2words import num2words


def pipeline_read_file(file = None, column = None):
    #df = pd.read_excel(file, encoding = 'utf-8')
    df = pd.read_csv('data/' + file, encoding = 'utf-8')
    return df

def pipeline_remove_duplicates_empty(tokens):
    tokens = list(filter(None, tokens))
    #tokens = [ token.strip() for  token in tokens if len(token.strip()) > 0]
    tokens = list(set(tokens))
    return tokens

def pipeline_remove_specials_spaces(tokens):
    tokens = [ re.sub("[^A-Za-z0-9']+", ' ', str(token)) for  token in tokens]
    #tokens = [token.strip() for  token in tokens if len(token.strip()) > 1]
    return tokens

def pipeline_remove_abbrev(tokens):
    
    for i in range(len(tokens)):
        token = tokens[i]
        
        # remove single chars
        token = ' '.join([w for w in str(token).split() if len(w) > 1])
        
        # remove wrong abbreviations
        abbrevs_wrong = ['gen', 'gen\'s', 'llc', 'co', 'co.', 'tr', 'fze', 'est', 'nt', 'br', 'fzco',
                   'ltd', 'dmcc', 'fzc', 'fz', 'cont', 'mohd', 'ind',
                   'sh', 'dwc', 'limited', 'eng', 'rep', 'inc', 'difc', 'dsg', 'contg', 'lc',
                   'trd', 'int', 'emb', 'const', 'trd', 'll', 'px', 'adv', 'pvt', 'ind', 
                   'gmbh', 'con', 'ap', 'mea', 'llp', 'dx', 'pte', 'ag', 'sa', 'md', 'mrw',
                   'corp', 'pub', 'fzd', 'psc', 'es', 'contr', 'estb', 'eqpt', 'bu', 'hh',
                   'dsoa', 'ghq', 'lcc', 'ent', 'exhb', 'serv', 'ink', 'dist', 'ab', 'kg',
                   'hq', 'cons', 'bv', 'tra', 'wll', 'svc', 'nd', 'ad', 'lle', 'caf', 'comp',
                   'sal', 'slc', 'pdxb', 'trdg', 'trdgco', 'cowll', 'srl', 'coltd',
                   'zllcf', 'mfg', 'jv', 'pjsc', 'wwl', 'ser', 'pmdc', 'lda',
                   'dept', 'trllc', 'fzllc', 'collc', 'foodstuff', 'catering', 'indllc',
                   'trdcoltd', 'trdgest', 'fzoc', 'ltdco', 'lllc', 'col', 'tradcollc', 
                   'corpn', 'trdllc', 'trdest', 'indltd', 'llcc', 'equiptrest', 'contcollc', 
                   'servicesllc', 'llcbr', 'ltdd', 'contllc', 'eastfze', 'llco', 'lll',
                   'icd', 'tradg', 'fzer', 'sgj', 'llcons', 'bldconco', 'jlt',
                   'jafza', 'tradin', 'tradig','llllc']
        token = ' '.join([w for w in str(token).split() if w.lower() not in abbrevs_wrong])

        # capitalize token
        token = ' '.join( [word.capitalize() for word in token.lower().split()] )
        
        # Load the abbrev dict
        with open('data/abbrev_dict.csv', 'r') as document:
            dict = {}
            for line in document:
                line = line.split(',')
                if not line:  # empty line?
                    continue
                dict[line[0].lower()] = line[1]
    
            for word in token.split():
                if word.lower() in dict:
                    abbrev = (dict[word.lower()][:-1]).strip()
                    token = token.replace(word, abbrev)
    
        tokens[i] = token

    # remove Branches
    tokens = [re.sub(r"(Dubai Branch).*$", '', token) for token in tokens]
    tokens = [re.sub(r"(Owned By).*$", '', token) for token in tokens]
    tokens = [re.sub(r"(Fujairah Branch).*$", '', token) for token in tokens]
    tokens = [re.sub(r"(Ras Al Khaimah Branch).*$", '', token) for token in tokens]
    tokens = [re.sub(r"(Sharjah Branch).*$", '', token) for token in tokens]
    tokens = [re.sub(r"(Branch).*$", ' ', token) for token in tokens]
    tokens = [re.sub(r"\s+(Middle East)\s+$", '', token) for token in tokens]
    tokens = [re.sub(r"(General Trading).*$", '', token) for token in tokens]
    tokens = [re.sub(r"(Trading).*$", '', token) for token in tokens]
    tokens = [re.sub(r"(General Maintenance)\s*$", '', token) for token in tokens]
    tokens = [re.sub(r"(Engineering Consultancy)\s*$", '', token) for token in tokens]
    tokens = [re.sub(r"(Exhibition Organizing)\s*$", '', token) for token in tokens]
    tokens = [re.sub(r"(One Person Company)\s*$", '', token) for token in tokens]
    tokens = [re.sub(r"(Management Services)\s*$", '', token) for token in tokens]
    tokens = [re.sub(r"(And).*$", '', token) for token in tokens]
    tokens = [re.sub(r"(Dubai).*$", '', token) for token in tokens]
    tokens = [re.sub(r"(Abu Dhabi).*$", '', token) for token in tokens]
    tokens = [re.sub(r"(Sharjah).*$", '', token) for token in tokens]
    tokens = [re.sub(r"(Ajman).*$", '', token) for token in tokens]
    tokens = [re.sub(r"(DIB ATM).*$", '', token) for token in tokens]
    tokens = [re.sub(r"\bHayper Market\b", 'Hypermarket', token) for token in tokens]

    return tokens

def pipeline_convert_numbers(tokens):
    # TODO: convert numbers
    for i in range(len(tokens)):
        
        tmp_token = ''
        for w in tokens[i].split():
            if w.isnumeric():
                tmp_token += (num2words(int(w)) + ' ')
            else:
                tmp_token += (w + ' ')
        tokens[i] = tmp_token.strip()

    return tokens

def separate_al_char(tokens):
    exclude_al = ['ali', 'al', 'alarm', 'alia', 'alwan', 'aluminum', 'alaaeldin',
                  'alpha', 'allsa', 'almas', 'alif', 'almco', 'almamun', 'alam',
                  'almiya', 'alexandriah', 'alfiah', 'all', 'albany', 'alalamain']
    
    for i in range(len(tokens)):
        token = tokens[i]

        for word in token.split():
            if word.lower() not in exclude_al:
                word_temp = re.sub(r'^al+\w*', 'al ' + str(word[2:]), word.lower())
                token = token.replace(word, word_temp)

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
source_file_name = '16_01_19_source_en.csv'

# read the file
original_data = pipeline_read_file(source_file_name)

tokens = original_data['Entity Name'].values

# convert numbers to text
tokens = pipeline_convert_numbers(tokens)

# special charachters and spaces
tokens = pipeline_remove_specials_spaces(tokens)

# separate Al arabic char
tokens = separate_al_char(tokens)

# should be the last step - remove abbreviations 
tokens = pipeline_remove_abbrev(tokens)

# remove duplicates
tokens = pipeline_remove_duplicates_empty(tokens)


data = pd.DataFrame({'Entity Name': tokens})
data.head()

# check term frequently 
df_term_freq = get_term_frequently(data)

# write the file
export_to_file('csv', data, 'output/output_' + source_file_name)
