import pandas as pd
import numpy as np 


import Deduping_Files as dedup
from sklearn.feature_extraction.text import TfidfVectorizer 




def prep_duping_columns(df,deduping_cols,target_name = 'target'):
    print(deduping_cols)
    df['target'] =df[deduping_cols].apply(" ".join,axis =1 )
    df['target']= df['target'].str.upper()
    df['target']= df['target'].str.strip()
    return df['target']

import re 
import pandas as pd 
# from sklearn.feature_extraction.text import TfidfVectorizer 
from sparse_dot_topn import awesome_cossim_topn
def fuzzy_dedupe(df, percent_match = .9):
    #group hash table 
    group_lookup = {}

    #cleaning strings and return ngrames
    def ngrams_analyzer(string,number_of_grams=5):
        string = re.sub(r'[,-./]',r'',string)
        ngrams = zip(*[string[i:] for i in range(number_of_grams)])
        return [''.join(ngram) for ngram in ngrams]

    def find_group(row,col):
        if row in group_lookup:
            return group_lookup[row]
        elif col in group_lookup:
            return group_lookup[col]
        else:
            return None 
    def add_vals_to_lookup(group,row,col):
        group_lookup[row] = group 
        group_lookup[col] = group 

    def add_pair_to_lookup(row,col):
        group = find_group(row,col) #check if in group already 
        if group is not None:
            add_vals_to_lookup(group, row, col)
        else:
            add_vals_to_lookup(row,row,col)

    #create vecotirzer for matrix
    vectorizer = TfidfVectorizer(analyzer=ngrams_analyzer)
    #Target column 
    vals = df['target'].unique().astype('U')

    #build matrix 
    tfidf_matrix = vectorizer.fit_transform(vals)

    cosine_matrix = awesome_cossim_topn(tfidf_matrix, tfidf_matrix.transpose(), vals.size,percent_match) 

    #build cordinate matrix 
    coo_matrix = cosine_matrix.tocoo() 

    #creating pairs 

    for row, col in zip(coo_matrix.row, coo_matrix.col):
        if row != col: 
            add_pair_to_lookup(vals[row],vals[col])

    df['Group'] = df['target'].map(group_lookup).fillna(df['target'])

    return df

def fuzzy_dedupe_main(df,deduping_cols1,percent_match = .9):
    df['target'] = prep_duping_columns(df,deduping_cols1,target_name = 'target')
    df = fuzzy_dedupe(df, percent_match=percent_match)
    df = dedup.dedupe_dataframe(
    df, 
    deduping_columns = ['Group'], 
    output_deduped_df=False, 
    keep_dedupe_id_col=True, 
    add_rank_column=True,
    reset_blank_dedupe_combinations=True)
    return df





def fuzzy_compare_dataframes(df1,df2,deduping_cols1,deduping_cols2, fuzzy_percentage=.95, return_both_sources = False):
    df1 = df1.copy()
    df2 = df2.copy() 
    df1['target'] = prep_duping_columns(df1,deduping_cols1,target_name = 'target')
    df2['target'] = prep_duping_columns(df2,deduping_cols2,target_name = 'target')
    match1 = pd.concat([df1,df2],axis = 0 ).fillna("")
    match1 = fuzzy_dedupe(match1.copy(),fuzzy_percentage)
    match1 = dedup.dedupe_dataframe(
    match1, 
    deduping_columns = ['Group'], 
    output_deduped_df=False, 
    keep_dedupe_id_col=True, 
    add_rank_column=True,
    reset_blank_dedupe_combinations=True)
    #RUNNING HERE
    match1_df1 = match1[(match1['source'] =='DF2')]['Dedupe_ID'].tolist()
    match1['match'] = 0
    match1.loc[(match1['Dedupe_ID'].isin(match1_df1)) & (match1['source']=='DF1'),'match'] = 1
    match1 = match1.sort_values(by ='Dedupe_ID')
    fuzzy_matches1_all_df1 = match1[(match1['match']==1)& (match1['source']=='DF1')]
    fuzzy_matches1_all_df2 = match1[match1['source']=='DF2']
    if return_both_sources:
        return fuzzy_matches1_all_df1, fuzzy_matches1_all_df2
    else:
        return fuzzy_matches1_all_df1
