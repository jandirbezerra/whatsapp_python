"""
Author............: Jandir Bezerra
Created Date......: 2019-08-31
Most recent update: 2022-04-18
Description.......: External library with specific functions for whatsapp exported files.
"""

from pathlib  import Path
import pandas as pd
import os
import re


# In[] Map all files inside the specified folder.
def fun_map_files(p_root_folder):
    v_list_files   = []
    for v_subfolder in p_root_folder:
        v_subfolder_path = Path(os.getcwd() + '/'+ v_subfolder +'/')
        for root, dirs, files in os.walk(v_subfolder_path):
            files = [ fi for fi in files if fi.endswith(".txt") ]
            for filename in files:
                v_list_files.append(str(root) +'\\'+ filename)
    return v_list_files


# In[] Load data from all files specified on the list passed by parameter
def load_data_file(p_list_files):
    
    # Reference to external variable
    v_tmp_repository = pd.DataFrame(columns=['date','sender','messages','source'])
    
    for v_file_path in p_list_files:
        
        v_source = v_file_path.split('\\')[-1:][0].replace('.txt','')
        v_source_type = v_file_path.split('\\')[-2:][0].replace('.txt','').upper()
        print('FILE BEING PROCESSED: {}'.format(v_source))
        
        # Open file
        v_file = open(v_file_path, 'r', encoding="utf8")
        
        # Extract from file 3 tuples accordingly with regular expressions already maped.
        # IMPORTANT -> The date format should be in YYYY-MM-DD hh (x) mm
        # E.g. -> 2020-07-30 11 h 21
        # For other date/time formats, the regular expression below must be adapted, otherwise it will fail.
        v_messages = re.findall('(\d+-\d+-\d+ \d+ \w \d+) - (.*:) (.*)', v_file.read())

        # Create a dataframe based on the tuples 
        v_data   = pd.DataFrame(v_messages,columns=['raw_date','raw_sender','raw_messages'])

        # On column sender, get everything after first semi-colon and add to the begining of column messages.
        v_data['sender_split'] = v_data['raw_sender'].str.split(':')
        v_data['sender']       = v_data['sender_split'].str[0]

        v_data['messages']         = ''
        v_data['messages_partial'] = ''
        for index, sender in v_data.iterrows():
            v_messages_partial = ''
            for sender_itens in sender['sender_split'][1:]:
                v_messages_partial = v_messages_partial + sender_itens +' '
            if len(v_messages_partial.strip()) > 0:
                v_data['messages_partial'][index] = v_messages_partial.strip()

        # If partial message is blank, just consider the raw_messages as the entire messages.
        v_data['messages'][(v_data.messages_partial.str.len()==0)] = v_data['raw_messages']
        
        # If partial message has something, attach it into the final messages column.
        v_data['messages'][(v_data.messages_partial.str.len()>0)]  = v_data['messages_partial'] +': '+ v_data['messages']

        #Extract only the raw date/time from the line
        v_data['date_str'] = v_data['raw_date'].str.slice(0, 10)


        #Build the final date column
        v_data['date'] = pd.to_datetime(v_data['date_str'], 
                                        dayfirst=False, 
                                        yearfirst=True,
                                        format='%Y-%m-%d')

        # Additional columns
        v_data['source']         = v_source
        v_data['source_type']    = v_source_type
        v_data['year']           = v_data['date'].dt.year
        v_data['month_number']   = v_data['date'].dt.month
        v_data['month_name']     = v_data['date'].dt.month_name()
        v_data['weekday_number'] = v_data['date'].dt.day
        v_data['weekday_name']   = v_data['date'].dt.day_name()

      
        # Insert transformed data into repository
        v_tmp_repository = v_tmp_repository.append(v_data, sort=True)
    
    # This column may be used to properly order by weekday, when having a numbering sequence in front of the name.
    v_tmp_repository['weekday_composite'] = v_tmp_repository['weekday_name'].replace({'Monday'   :'1-Monday',
                                                                                      'Tuesday'  :'2-Tuesday',
                                                                                      'Wednesday':'3-Wednesday',
                                                                                      'Thursday' :'4-Thursday',
                                                                                      'Friday'   :'5-Friday',
                                                                                      'Saturday' :'6-Saturday',
                                                                                      'Sunday'   :'7-Sunday'})

    # Reorder columns (when we reindex a dataframe, it automatically drops all unreferenced columns)
    v_tmp_repository = v_tmp_repository.reindex(columns=['date','year','month_number','month_name','weekday_number','weekday_name','weekday_composite','source','source_type','sender','messages'])

    # Provide the treated dataframe back to its caller.    
    return v_tmp_repository


# In[] This function provides some basic aggregated info - Must specify an option number on the calling.
def fnc_statistic(p_repository, p_option, p_source, p_date_from):
    
    # Observation
    # The command "reset_index" is being used at the end of most dataframe creation to properly distribute column names.

    # Debug
    # print(' p_option={}\n p_source={}\n p_date_from={}'.format(p_option, p_source, p_date_from))
    # return

    v_total_msg_dt_sc = p_repository.loc[(p_repository['date'].dt.date >= p_date_from) & (p_repository['source']==p_source)]['messages'].count()
    global v_date_display_from, v_date_display_to

    # Messages by sender filtered by source and period.
    if p_option == 1:
        v_pd_temp = pd.DataFrame(p_repository.loc[(p_repository['source']==p_source) & 
                                                  (p_repository['date'].dt.date>=p_date_from)].groupby('sender')['messages'].count()).sort_values('messages', ascending=False).reset_index()
        v_pd_temp['rate']  = round( (v_pd_temp['messages'] / v_total_msg_dt_sc)*100, 2)

    # Messages by week day filtered by source and period.
    elif p_option == 2:
        v_pd_temp = pd.DataFrame(p_repository[(p_repository['source']==p_source) & 
                                              (p_repository['date'].dt.date>=p_date_from)].groupby(['weekday_name','date'])['messages'].count()).sort_values(['date'], ascending=False).reset_index()
        v_pd_temp['rate']  = round( (v_pd_temp['messages'] / v_total_msg_dt_sc)*100, 2)

    # Messages by year/month filtered by source and period.
    elif p_option == 3:
        v_pd_temp = pd.DataFrame(p_repository[(p_repository['source']==p_source) & 
                                              (p_repository['date'].dt.date>=p_date_from)].groupby(['year','month_number','month_name'])['messages'].count()).sort_values(['messages'], ascending=False).reset_index()
        v_pd_temp['rate']  = round( (v_pd_temp['messages'] / v_total_msg_dt_sc)*100, 2)

    return v_pd_temp


# In[] This function returns a dataframe with occurency of a given list of words, breaking by group members. - AVAILABLE BUT NOT CURRENTLY IN USE
def fnc_words_frequency(p_repository, p_source, p_words, p_date_from):

    #Load distinct members who sent messagem containing one of the words in the list.
    v_members_distinct = p_repository[(p_repository['source'] == p_source) &
                                      (p_repository['date'].dt.date>=p_date_from) &
                                      (p_repository['messages'].str.contains('|'.join(p_words), case=False))]['sender'].unique().tolist()

    #For each member, load the number of messages containing one of the words in the list.
    v_words_count = []
    for v_member in v_members_distinct:
        v_words_count.append(p_repository[(p_repository['source'] == p_source) &
                                          (p_repository['date'].dt.date>=p_date_from) &
                                          (p_repository['messages'].str.contains('|'.join(p_words), case=False)) &
                                          (p_repository['sender'] == v_member)]['sender'].count()
                            )
    
    # Build the dictionary which will be used to return a dataframe.
    v_dic_words = {'sender'  : v_members_distinct,
                   'quantity': v_words_count}

    global v_date_display_from, v_date_display_to
    v_date_display_from = p_repository[(p_repository['source']==p_source) & (p_repository['date'].dt.date>=p_date_from)]['date'].min()
    v_date_display_to   = p_repository[(p_repository['source']==p_source) & (p_repository['date'].dt.date>=p_date_from)]['date'].max()

    return pd.DataFrame(v_dic_words).sort_values('quantity', ascending=False).reset_index(drop=True)


