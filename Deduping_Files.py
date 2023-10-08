## Import Packages
import pandas as pd
import re

#########################################
### Main Functions
#########################################
def dedupe_dataframe(
    data, 
    deduping_columns, 
    sort_column = '',
    sort_ascending=True,
    additional_aggs=[], 
    columns_to_expand=[],
    output_deduped_df=True, 
    output_column_name='', 
    keep_dedupe_id_col=False, 
    add_rank_column=False,
    rank_column_name='', 
    columns_to_simplify=[],
    reset_blank_dedupe_combinations=True
):
    """
    ### Function: dedupe_dataframe
    - data: Data to use
    - deduping_columns: Columns to dedupe on. Is a `list`
    - additional_aggs: Needs to be in dict. Use the helper function `create_additional_aggregation_dict`
        - Format for additional aggregations
        - 'New_Column_Name':'Name you want to name a new column',
        - 'Agg_Column_Name':'Name of original data column',
        - 'Agg_Type':'Type to convert column to (Example: `str` -> `float`)',
        - 'Change_Dtype':'Type'  ##This to change the type of the columns that is being aggregated (Defaults to None)
            - If multiple, these are kept in a list
    - output_deduped_df: If false will return just the data not grouped by columns (just marked)
    - keep_dedupe_id_col: Include the Column Dedupe_ID in the output df
    - columns_to_expand: A `list` of columns that will be expanded out (ex Phone -> Phone_1, Phone_2, Phone_3, ect..)
        - These are taken as a set. So if multiple rows have the same exact info in cols provided, only one will be expanded
    - sort_column: Used in the rank column and the dedupe output. The column will be sorted on ascending (defualt) and the first will be taken
    - sort_ascending: If a sort column is provided, you can choose if you want to sort ascending or not (ascending is the defualt)
    - add_rank_column: The first item in group with be listed as 1, the next as 2, and so forth
        - This is useful if you need to mark one record as primary and the rest as dupes
    - rank_column_name: Output the name of the rank column. Default is 'Rank'
    - columns_to_simplify: A list of columns that will be concatinated and will have `simplify_text` function applied before the 
        dedupe column is added to the data. Useful for combining and simplifying addresses (ex addr1 + addr2)
    - reset_blank_dedupe_combinations: If a combination of dedupe columns results in a blank, Set the following:
        - Dedupe_Id = -1
        - Rank = 1
        - Dedupe_Count = 1
    """
    ### Work on a copy of the data
    data = data.copy()
    #### Standardize the data a bit before using it
    print('\rCleaning all data before any changes', end=" "*50)
    data = data.applymap(lambda x: " ".join(x.split()).upper().strip() if isinstance(x, str) else x)

    #### Combine and simplify columns. Remove old columns from dedupe list and add new column
    #### Run the dedupe id count and drop new column
    #### Else just run the dedupe count function
    print('\rAdding Dedupe Count and Dedupe Id', end=" "*50)
    if len(columns_to_simplify) > 0:
        ## All columns to simplify must be contained in the deduping_columns list
        assert sum([1 for col in columns_to_simplify if col in deduping_columns]) == len(columns_to_simplify), "The columns to simplify must be in the columns to dedupe"
        data['Simplified_Cols'] = data[columns_to_simplify].apply(lambda x: " ".join([simplify_text(x[col]) for col in columns_to_simplify]), axis=1)
        deduping_columns = [col for col in deduping_columns if col not in columns_to_simplify]
        deduping_columns = deduping_columns + ['Simplified_Cols']
        data = add_dedupe_count_column(data, deduping_columns, reset_blank_dedupe_combinations=reset_blank_dedupe_combinations)
        data = data.drop('Simplified_Cols', axis=1)
    else:
        data = add_dedupe_count_column(data, deduping_columns, reset_blank_dedupe_combinations=reset_blank_dedupe_combinations)

    #### If no sort column provided, just use the first column as the sort (basically random/order df provided)
    if sort_column=='':
        sort_column = data.columns[0]

    if len(additional_aggs) > 0:
        print('\rDoing Addtional Aggregations', end=" "*50)
        for agg in additional_aggs:
            #### We need to be able to change the dtype to something that can be summed if it is a text
            if agg['Change_Dtype']==float or agg['Change_Dtype']==int:
                data[agg['Agg_Column_Name']] = pd.to_numeric(data[agg['Agg_Column_Name']], errors='coerce').fillna('', downcast='infer')
            #### Error handling
            elif agg['Change_Dtype'] is not None:
                data[agg['Agg_Column_Name']] = data[agg['Agg_Column_Name']].astype(agg['Change_Dtype'], errors='ignore')
            #### Perform the aggregation
            data[agg['New_Column_Name']] = data.groupby(['Dedupe_ID'])[agg['Agg_Column_Name']].transform(agg['Agg_Type'])

    ### This section applies if there are choosen columns to expand
    if len(columns_to_expand) != 0:
        print('\rExapanding Chosen Columns', end=" "*50)
        throw_error_clause = False
        #### Group by columns desired so if multiple rows have the same exact info in cols provided, only one will be expanded
        temp = data[['Dedupe_ID'] + columns_to_expand].groupby(['Dedupe_ID'] + columns_to_expand).first().reset_index()
        #### Added on 10-13-22 if there is only one column being expanded, then lets not have any blanks
        if len(columns_to_expand) == 1:
            temp = temp[(temp[columns_to_expand[0]]!='') & (temp[columns_to_expand[0]].notnull())]
        temp['Key_ID'] = temp.groupby('Dedupe_ID').cumcount() + 1
        
        #### If there are too many columns and rows in the pivot, it will time out. Limit to 100,000,000 entries (timeout on example at 1.6 Billion)
        max_entries = len(temp['Dedupe_ID'].unique()) * temp['Key_ID'].max() * len(columns_to_expand)
        min_entries = len(temp['Dedupe_ID'].unique()) * len(columns_to_expand)
        limit_on_entries = 100000000
        ## Check if it hits the limit. If it does calculate max columns under limit. Prompt user to choose. They may use max,
        ## use their own number, or skip the expansion process
        if max_entries > limit_on_entries: 
            max_columns_possible_under_limit = int(limit_on_entries / min_entries)
            input_text = """The current expansion will result in {:,} cells. To avoid a timeout we need to limit that to under {:,}.
            \nYou will only be able to have {} column(s) per expansion to stay under the limit. If this is fine, please hit enter,
            otherwise enter a number to use. \nType 'X', 'EXIT', 'STOP' to skip expansion processes of deduping.
            """.format(max_entries, limit_on_entries, max_columns_possible_under_limit)
            while True:
                try:
                    user_choice = input(input_text)
                    if user_choice == '':
                        user_choice = max_columns_possible_under_limit
                    elif user_choice.upper() in ['X', 'EXIT', 'STOP']:
                        throw_error_clause = True
                    else:
                        user_choice = int(user_choice)
                    break
                except ValueError:
                    print('You entered a non integer value, try again.')
                    continue
            if not throw_error_clause:
                temp = temp[temp['Key_ID']<=user_choice]

        #### Need to throw an error if possible results are too large. Skip the whole thing       
        if not throw_error_clause:
            #### Pivot data on Dedupe_ID to get one row with as many columns as needed
            temp['Key_ID'] = temp['Key_ID'].astype(str)
            expanded_data = temp.pivot_table(index='Dedupe_ID',columns='Key_ID',values=columns_to_expand,aggfunc='first', fill_value='').sort_index(level=1,axis=1).reset_index()
            
            #### Clean up column names, order them from 1-whatever, and rename the id
            expanded_data.columns = expanded_data.columns.map('_'.join)
            cols_to_order = list(expanded_data.columns)
            cols_to_order.sort(key=lambda x: int(x.split('_')[-1]) if x.split('_')[-1] != '' else 0)
            expanded_data = expanded_data[cols_to_order]
            expanded_data = expanded_data.rename({'Dedupe_ID_':'Dedupe_ID'}, axis=1)
            
            #### We just add a check that it is grouped properly so it will merge correctly back with the data
            assert len(expanded_data) == len(expanded_data['Dedupe_ID'].unique()), "Expanding the Columns did not return a number equal to the unique number of dedupe ids"
            #### Merge back with the data and drop the original columns that were expanded
            data = data.merge(expanded_data, how='left', on='Dedupe_ID').fillna('')
            data = data.drop(columns_to_expand, axis=1)
    
    #### Add the Rank column. This just labels with the dedupe_id group 1-number of entries in group
    if add_rank_column:
        print('\rAdding rank column', end=" "*50)
        rank_column_name = "Rank" if rank_column_name=='' else rank_column_name
        data[rank_column_name] = data.groupby("Dedupe_ID")[sort_column].rank(method="first",ascending=sort_ascending).astype(int)
        if reset_blank_dedupe_combinations:
            data.loc[(data['Dedupe_ID']==-1), rank_column_name] = 1

    #### If true just take the first row within the group. All aggs were applied previously to every row so taking any row within group should give back correct agg for id
    if output_deduped_df:
        data = data.sort_values(sort_column, ascending=sort_ascending)
        data = data.groupby('Dedupe_ID').first().reset_index()            

    #### Rename the Dedupe_Count to something else if input provided
    if output_column_name != '':
        data = data.rename({'Dedupe_Count':output_column_name}, axis=1)

    #### Choose to keep the Dedupe_ID column. Default is to remove
    if not keep_dedupe_id_col:
        data = data.drop('Dedupe_ID', axis=1)

    return data

def compare_dataframes(data1, data2, columns_to_compare=[], columns_to_bring_over=None, new_column_names=None, simplify_columns=False):
    """
    - columns_to_compare: columns to use as key in match/join
        - If you have different column names in both: Make a `tuple`
        - [(`Column_From_data1`, `Column_From_data2`), ...]
    - rename_new_column: Needs to be the same len as data2_column_to_join
    - simplify_columns: Remove everything but letters and numbers from compared columns
    """
    data1, data2 = data1.copy(), data2.copy()

    #### Standardize the data a bit before using it
    data1 = data1.applymap(lambda x: " ".join(x.split()).upper().strip() if isinstance(x, str) else x)
    data2 = data2.applymap(lambda x: " ".join(x.split()).upper().strip() if isinstance(x, str) else x)

    #### Add in ids for the columns to compare
    if any([isinstance(i, tuple) for i in columns_to_compare]):
        #### If columns have tuple split the tuples (This way we can use different columns without haveing to change col names)
        data1['Combined_ID'] = data1[[k for (k,v) in columns_to_compare]].apply(lambda row: '_'.join(row.values.astype(str)), axis=1)
        data2['Combined_ID'] = data2[[v for (k,v) in columns_to_compare]].apply(lambda row: '_'.join(row.values.astype(str)), axis=1)
    else:
        data1['Combined_ID'] = data1[columns_to_compare].apply(lambda row: '_'.join(row.values.astype(str)), axis=1)
        data2['Combined_ID'] = data2[columns_to_compare].apply(lambda row: '_'.join(row.values.astype(str)), axis=1)

    if simplify_columns:
        data1['Combined_ID'] = data1['Combined_ID'].apply(lambda r: simplify_text(r))
        data2['Combined_ID'] = data2['Combined_ID'].apply(lambda r: simplify_text(r))

    #### Compare the IDs from both. Returns counts of matching from other df (0 means no matches, 1+ is number of matches from other df)    
    data1['Matches_From_Other_DF'] = data1['Combined_ID'].map(data2['Combined_ID'].value_counts()).fillna(0).astype(int)

    #### Get columns from one df to another (basically creating our own index to merge on)
    if columns_to_bring_over is not None:
        #### Convert single str to list 
        if type(columns_to_bring_over) != list:
            columns_to_bring_over = [columns_to_bring_over]            
        
        #### We can choose to rename the columns that we are pulling over (ie refnum -> old_refnum)
        if new_column_names is not None:
            #### Convert single str to list 
            if type(new_column_names) != list:
                new_column_names = [new_column_names]
            data2 = data2.rename({x:y for x,y in zip(columns_to_bring_over, new_column_names)}, axis=1)
            col_list = ['Combined_ID'] + new_column_names
        else:
            col_list = ['Combined_ID'] + columns_to_bring_over

        #### Groupby ID and get first record to pull over to data1
        data1 = data1.merge(
            data2[col_list].groupby('Combined_ID').first().reset_index(), 
            how='left', 
            on='Combined_ID'
        ).fillna('')

    #### We do not need the Combined_ID in output
    return data1[[i for i in data1.columns if i != 'Combined_ID']]

#########################################
### Helper Functions
#########################################
def add_dedupe_id_column(data, deduping_columns, reset_blank_dedupe_combinations):
    #### Found this on the internet. Only partly understand factorize. We combine the columns in deduping_columns
    #### then we factorize. This gives us a count of all the same combinations in a list that we pull out
    #### Returns a list of numbered groups corresponding to the dedupe col combinations that is assign to a new col in the df
    #### (11-18-22) Split this out to combine the dedupe columns, remove all spaces, and compare that way. This should make it so 123 Main Apt 3 == 123 Main | Apt 3
    #### If the combined string is empty set the id = -1 to filter out later. Blank is useless to us.
    data['Combined_deduping_columns'] = data[deduping_columns].apply(lambda row: "".join(''.join(row.values.astype(str)).split()), axis=1)
    data["Dedupe_ID"] = pd.factorize(data['Combined_deduping_columns'])[0]
    if reset_blank_dedupe_combinations:
        data.loc[(data['Combined_deduping_columns']==''), 'Dedupe_ID'] = -1
    data = data.drop('Combined_deduping_columns',axis=1)
    return data

def add_dedupe_count_column(data, deduping_columns, reset_blank_dedupe_combinations):
    #### We add the Dedupe_ID col and then get the counts associated with each ID assigned back to a new col
    data = add_dedupe_id_column(data, deduping_columns, reset_blank_dedupe_combinations=reset_blank_dedupe_combinations)
    data['Dedupe_Count'] = data.groupby(['Dedupe_ID'])['Dedupe_ID'].transform('count')
    if reset_blank_dedupe_combinations:
            data.loc[(data['Dedupe_ID']==-1), 'Dedupe_Count'] = 1
    return data

def create_additional_aggregation_dict(New_Column_Name, Agg_Column_Name, Agg_Type, Change_Dtype=None):
    """
    This is used in `Dedupe_Dataframe`. This function must be passed in a list to that function
    - New_Column_Name: The new name for the col. Can be the same as orginal to just replace the old column
    - Agg_Column_Name: The original col that we want to aggrigate
    - Agg_Type: Sum function to apply to Agg_Column_Name (Ex: sum, max, ' '.join)
    - Change_Dtype: You may change the dtype of the Agg_Column_Name (Ex: int, float, str)
    """
    return {
        'New_Column_Name':New_Column_Name,
        'Agg_Column_Name':Agg_Column_Name,
        'Agg_Type':Agg_Type,
        'Change_Dtype':Change_Dtype
    }

def simplify_text(text, exclude_extra_characters=False):
    """
    We need to clean up names of all non alphanumeric characters.
    """
    regex_pattern = r'[^A-Za-z0-9\- \.\,\#\/]+' if exclude_extra_characters else r'[^A-Za-z0-9\- ]+'
    return re.sub(regex_pattern, '', str(text))