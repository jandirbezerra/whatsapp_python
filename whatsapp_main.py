"""
Author............: Jandir Bezerra
Created Date......: 2019-08-31
Most recent update: 2022-04-18
Description.......: This code will read exported WhatsApp files and produce some aggregated info.
References........: https://docs.python.org/3/howto/regex.html#regex-howto
                    https://docs.python.org/3/library/re.html#re-syntax
                    https://docs.python.org/3/library/datetime.html#datetime-objects
                    https://plot.ly/python/basic-charts/
                    http://strftime.org/
"""

# Library used throughout the script
import pandas as pd
import datetime
import whatsapp_functions
import matplotlib.pyplot as plt
from wordcloud import WordCloud


# In[1] PRE-PROCESSING OF THE DATA

# 1.1) Set default columns for main dataframe
v_repository = pd.DataFrame(columns=['date','sender','messages','source'])

# 1.2) Load all files inside subfolders
v_list_files = whatsapp_functions.fun_map_files(['Groups','Individuals'])

# 1.3) Choose how far (in number of days) to do the analysis - 7(week), 30(month), 180(semester), 365(year) or a custom number.
v_dt_from   = datetime.date.today() + datetime.timedelta(-7)
v_dt_to     = datetime.date.today()
v_dt_days   = v_dt_to - v_dt_from

# Specific date range in variables below
#v_dt_from        = None
#v_dt_to          = None

# 1.4) Load data from files
v_repository = whatsapp_functions.load_data_file(v_list_files)

# 1.5) Check available sources and define which one should be used for statistics.
#for idx, val in enumerate(v_repository['source'].unique().tolist()):
#    print(idx, val)
v_source = v_repository['source'].unique()[0]


# In[2] CHOOSE AGGREAGATED INFO TO BE DISPLAYED - FOR A BETTER EXPERIENCE RUN EACH ONE INDIVIDUALLY 

# 2.1) Show amount of messages of the senders within a period - sorted from highest to the lowest.
v_pd = whatsapp_functions.fnc_statistic(v_repository, 1, v_source, v_dt_from)
print('Total messages by sender | ({} to {}) | past {} days'.format(v_dt_from, v_dt_to, v_dt_days.days))
print('--------------------------------------------------------------------')
for index, row in v_pd.iterrows():
    print('{} ({} %) | {}'.format(row['messages'], row['rate'], row['sender']))
print('\n')

# 2.2) Show amount of messages by weekday from specific source and period
v_dt_from_7 = datetime.date.today() + datetime.timedelta(-6)
v_pd = whatsapp_functions.fnc_statistic(v_repository, 2, v_source, v_dt_from_7)
print('Total messages by weekday | ({} to {}) | past 7 days'.format(v_dt_from_7, v_dt_to))
print('--------------------------------------------------------------------')
for index, row in v_pd.iterrows():
    print('{} ({} %) | {} ({})'.format(row['messages'], row['rate'], row['weekday_name'], row['date'].date()))
print('\n')

# 2.3) Show amount of messages by year/month from specific source on the last 365 days
v_dt_from_365 = datetime.date.today() + datetime.timedelta(-365)
v_pd = whatsapp_functions.fnc_statistic(v_repository, 3, v_source, v_dt_from_365)
v_pd = v_pd.sort_values(by=['year', 'month_number'])
print('Total messages by month | past 12 months')
print('----------------------------------------')
for index, row in v_pd.iterrows():
    print('{} ({}%) | {}/{}'.format(row['messages'], row['rate'], row['month_name'], int(row['year'])))


# In[3] GENERATE WORDCLOUD IMAGE WITH MOST WRITEN WORDS

# 3.1) Create a dataframe with the messages from the specified date range
#      Amount of days can be expanded by changing the number inside timedelta(-N)
v_dt_from_30 = datetime.date.today() + datetime.timedelta(-30)
v_pd_words = pd.DataFrame(v_repository.loc[(v_repository['source']==v_source) & 
                         (v_repository['date'].dt.date>=v_dt_from_30)]['messages']).reset_index()

# 3.2) Delete the rows making reference to media postage because they're useless
#      Atention! <Médias omis> is in french. You should replace this one by the language generated in your file.
v_pd_words = v_pd_words[v_pd_words.messages != '<Médias omis>']

# 3.3) Remove all emojis from the dataframe
filter_char = lambda c: ord(c) < 256
v_pd_words['messages'] = v_pd_words['messages'].apply(lambda s: ''.join(filter(filter_char, s)))

# 3.4) Remove all punctuation.
v_pd_words["messages"] = v_pd_words['messages'].str.replace('[^\w\s]','')

# 3.5) Set all words in lower case
v_pd_words["messages"] = v_pd_words["messages"].str.lower()

# 3.6) Remove all numbers since they won't be necessary
v_pd_words['messages'] = v_pd_words['messages'].str.replace('\d+', '')

# 3.7) Remove repeated character (at least 2 times) already known.
#      Another case where you should take into account the language.
#      The example below takes into account portuguese language.
v_pd_words["messages"] = v_pd_words["messages"].str.replace(u"k{2,}", "")
v_pd_words["messages"] = v_pd_words["messages"].str.replace(u"he{2,}", "")
v_pd_words["messages"] = v_pd_words["messages"].str.replace(u"ha{2,}", "")

# 3.8) Remove multiple blank spaces
v_pd_words.messages = v_pd_words.messages.replace(r'\s+', ' ', regex=True)

# 3.9) Delete empty messages because they're useless.
v_pd_words = v_pd_words[v_pd_words.messages != '']

# 3.10) Place all words into a list object.
v_list_general = v_pd_words['messages'].str.strip().tolist()
v_list_words = []
for x in v_list_general:
    v_list_words.extend(x.split())

# 3.11) Remove duplicated words from the list
v_list_words = list(dict.fromkeys(v_list_words))

# 3.12) Remove small words like article, pronoums, etc. Whatever you think is not relevant.
#      Another case where you should take into account the language.
#      The example below takes into account portuguese language.
v_words_to_remove = ['a','e','o',
                     'as','os',
                     'pra','para',
                     'que','se','te',
                     'da','de','do','das',
                     'em','um','uma',
                     'tu','vc','você','eu',
                     'este','estes','isso','isto',
                     'é','já','ja','tá','aí',
                     'me','mim','mas',
                     'não','nao',
                     'no','on',
                     'ele','ela','dele','dela',
                     'teu','tua',
                     'aquela','aquilo',
                     'com','tem']
for x in v_words_to_remove:
    v_list_words = list(filter((x).__ne__, v_list_words))

# 3.13) Generate a text based on the cleaned list of words
v_str_words = ''
for x in v_list_words:
    v_str_words = v_str_words +' '+ x

# 3.14) Generate a word cloud image.
#       You may change the parameters to match your desired visual.
wordcloud = WordCloud(background_color='white',
                      max_font_size=70,
                      width=1000,
                      height=600).generate(v_str_words)

# 3.15) Display the generated image:
plt.imshow(wordcloud, interpolation='bilinear')
plt.axis("off")
plt.show()

