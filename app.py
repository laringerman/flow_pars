import pandas as pd 
import requests
import json
import os
from dotenv import load_dotenv
import ast
import gspread
import re
import time
from datetime import datetime

#load env
load_dotenv()
#load google credentials
google_credentials = os.getenv('GOOGLE_CREDENTIALS')
credentials = ast.literal_eval(google_credentials)
gc = gspread.service_account_from_dict(credentials)
#load flowwow document
sh = gc.open('flowwow')

#create a list of sheets
worksheets_name = ['sem_tort', 'sem_b_tort', 'sem_hp_tort']
#сreate a list of search words
search_list = ['торт', 'бенто торт', 'торт на день рождение']
      
def clean_delivery_time(text):
    """
    function for clearing the text of delivery times from special characters
    
    Args:
        text: text with special caracters 
        
    Returns:
        text: clean text
    """

    if pd.isna(text):
        return text
    
    # Replace &nbsp; with space
    text = re.sub(r'&nbsp;', ' ', text)
    
    # Remove ≈ if present
    text = re.sub(r'≈\s*', '', text)
    
    # Remove <br> tags and replace with space
    text = re.sub(r'<br>\s*', ' ', text)
    
    # Remove extra spaces
    text = re.sub(r'\s+', ' ', text).strip()
    
    # Special case for time ranges (19:15 - 19:45)
    text = re.sub(r'(\d{1,2}:\d{2})\s*-\s*(\d{1,2}:\d{2})', r'\1-\2', text)
    return text

def get_page(search_name, page=1, coordinates = {"lat": 55.783514, "lng": 37.720232}):
    # Base URL
    url = "https://clientweb.flowwow.com/apiuser/products/search/"    


    # Headers
    headers = {
        "authority": "clientweb.flowwow.com",
        "accept": "application/json",
        "accept-language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
        "cache-control": "no-cache",
        "canonical-url": "https://clientweb.flowwow.com",
        "referer": "https://clientweb.flowwow.com/",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    property_dump = {                
        "range_group_ids": [],
        #search query
        "query": search_name,
        #city id - 1 is Moscow
        "city": 1     
    }
    property_dump.update(coordinates)

    # Parameters
    params = {
            "property": json.dumps(property_dump),
            "limit": 60,
            "filters": "{}",
            "currency": "RUB",
            "lang": "ru",
            "page": page
        }

    try:
            # Make the GET request
            response = requests.get(url, params=params, headers=headers)
            
            # Check if the request was successful
            if response.status_code == 200:
                # pass
                pass
            else:
                print(f"Request failed with status code {response.status_code}")
                print(response.text)

    except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")

    #make text as json
    j = response.json()
    return j
        
        
def get_df(j):
    """
    function for json to clean dataframe
    
    Args:
        j: json with data
        
    Returns:
        new_df: dataframe with data
    """
    #normalize json's data part
    df = pd.json_normalize(j['data']['items'])
    #create dataframe
    new_df = pd.DataFrame(df)
    #clear text in delivery time
    new_df['deliveryTime'] = new_df['deliveryTime'].apply(clean_delivery_time)
    #drop unused columns
    new_df = new_df.drop(columns=['scoreKm', 'scorePoints'])
    #fill empty cells with empty text
    new_df = new_df.fillna('')
    #add creation time
    new_df['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M')
    return new_df

def get_data(search_name):
    """
    function for get data from the flowwow use their hidden API and turn it to dataframe
    
    Args:
        search_name: the search query we will search for
        
    Returns:
        new_df: dataframe with data
    """

    j = get_page(search_name)

    # get df with first page
    new_df = get_df(j)

    if j['data']['total'] > 60:
        #get pages count
        pages_div = j['data']['total'] % 60

        #chech how pany pages
        if pages_div == 0:
            pages = int(j['data']['total'] / 60)
        else: 
            pages = int(j['data']['total'] / 60) + 1 
        
        #get data for all pages
        for page in range(2, pages+1):
                j = get_page(search_name, page)
                new_df_for_pages = get_df(j)
                new_df = pd.concat([new_df, new_df_for_pages])
                new_df = new_df.fillna('')
                #whait for 1 sec just in case
                time.sleep(1)
    else:
        pass
    return new_df

def load_new_data(new_df, sheet_name):
    """
    function to load dataframe to the specific sheet
    
    Args:
        new_df: dataframe to load
        sheet_name: name of an existing sheet
    """
    wks = sh.worksheet(sheet_name)
    wks.clear()
    wks.update([new_df.columns.values.tolist()] + new_df.values.tolist())

def get_querys_count(search_list, my_coordinates = {"lat": 55.783514, "lng": 37.720232}):
    data = []
    for search_query in range(0, len(search_list)):
        try:
            p = get_page(search_list[search_query],  coordinates = my_coordinates)
        except:
            p = 0
        data.append({
        'search_query' : search_list[search_query],
        'no adress': p['data']['total']
        })
        time.sleep(1)    
    return data

def get_querys_for_adress(list_name = 'search_list'):
    search_list = sh.worksheet(list_name).col_values(1)
    smol_data = get_querys_count(search_list)
    no_adress_data = get_querys_count(search_list, my_coordinates = {})
    smol_df = pd.DataFrame(smol_data)
    no_adr_df = pd.DataFrame(no_adress_data)
    final_df = no_adr_df.merge(smol_df, left_on='search_query', right_on='search_query')
    return final_df


#srart of the code
if __name__ == '__main__':
    #get data and load it to the specific sheet
    for sh_name, search_name in zip(worksheets_name,  search_list):
        search_df = get_data(search_name)
        load_new_data(search_df, sh_name)
    querys_count_df = get_querys_for_adress()
    load_new_data(querys_count_df, 'list_of_query')

