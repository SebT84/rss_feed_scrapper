import os
import requests
import xmltodict
import json
import re

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from rss_scrapper.data import *
from rss_scrapper.params import *

def test():
    ### function to test ###
    print("✅ main file is working")

def get_latest_article():
    ### run the full package ###

    run_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')

    # get the list of RSS feed to ping
    df_rss_list = get_rss_feed_list()

    # ping RSS feed page, extract and format the data
    df_rss_data = get_rss_data(df_rss_list,run_timestamp)

    #generating the cut off date of historical data
    look_back_days = 7
    cut_off_date = datetime.today() - timedelta(days=look_back_days)
    cut_off_date_str = cut_off_date.strftime('%Y-%m-%d %H:%M:%S.%f')

    #filter latest articles
    df_rss_data_filtered = df_rss_data[df_rss_data['pubDate'] >= cut_off_date_str]

    #get latest articles from BQ
    df_latest_articles = get_latest_articles_from_bq(cut_off_date_str)

    #remove all articels that already exist
    df = pd.concat([df_rss_data_filtered,df_latest_articles]).drop_duplicates(keep=False)

    #load to BQ
    load_data_bq(df,True,'rss_feed_articles')

### function to ping and format rss data

def get_rss_data(df,run_timestamp):
    ### function to iterate over the RSS ping list, returns a dataframe ###
    df_article =[]
    log = []

    # loop iterating over each rss feed list row
    for u in range(len(df)):
        start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')

        try:
            df_u = get_rss_feed_data(df.iloc[u])

            if not isinstance(df_article, list):
                df_article = pd.concat([df_article,df_u], ignore_index=True, sort=False)
            else:
                df_article = df_u
            log.append(generate_requests_log(df.iloc[u],'success',run_timestamp,start_time))

        except:
            log.append(generate_requests_log(df.iloc[u],'failed',run_timestamp,start_time))

    load_data_bq(pd.DataFrame(log),False,'rss_feed_requests_log')

    return df_article


def get_rss_feed_data(df):

    # ping RSS feed
    response = requests.get(df['url'])
    data = xmltodict.parse(response.content)

    data_channel = data['rss']['channel']

    # extract and format articles data
    df_item = pd.DataFrame(extract_articles(data_channel['item']))

    #build the final dataframe
    df_item['rss_website'] = df['website']
    df_item['rss_topic'] = df['topic']
    df_item['rss_url'] =  df['url']
    df_item['rss_title'] = data_channel['title']
    df_item['rss_description'] = data_channel['description']
    df_item['rss_language'] = data_channel['language']
    df_item['rss_link'] = data_channel['link']

    return df_item

def extract_articles(data):
    ### function extracting and formating articles data ###
    article_list = []

    # loop to iterate over each articles, it extracts key information and formats them. return a list
    for i in range(len(data)):
        if 'title' in data[i].keys():
            title = data[i]['title']

        if 'description' in data[i].keys():
            description = remove_html_tags(data[i]['description'])
        else:
            description = ''

        if 'pubDate' in data[i].keys():
            pubdate = data[i]['pubDate']
            try:
                pubDate = datetime.strptime(pubdate, '%a, %d %b %Y %H:%M:%S %Z').strftime('%Y-%m-%d %H:%M:%S.%f')
            except:
                pubDate = datetime.strptime(pubdate, '%a, %d %b %Y %H:%M:%S %z').strftime('%Y-%m-%d %H:%M:%S.%f')

        if 'link' in data[i].keys():
            link = data[i]['link']
        else:
            link = ''

        if 'category' in data[i].keys():
            try:
                category = data[i]['category'][0]
            except:
                category = data[i]['category']
        else:
            category = {}

        dict_items = {'title':title
                      ,'description':description
                      ,'pubDate':pubDate
                      ,'link':link
                      ,'category':json.dumps(category)
                     }

        article_list.append(dict_items)

    return article_list

def generate_requests_log(df,status,run_timestamp,start_time):
    log = {'run_timestamp': run_timestamp
             ,'run_start_timestamp':start_time
             ,'run_end_timestamp':datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
             ,'run_status':status
             ,'website':df['website']
             ,'topic':df['topic']
             ,'url':df['url']
            }
    return log

def remove_html_tags(text):
    clean = re.compile('<.*?>')
    return re.sub(clean, '', text)
