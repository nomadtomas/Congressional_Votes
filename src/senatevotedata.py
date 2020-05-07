import pandas as pd 
import numpy as np
import pymongo
import random
import time
import re
from IPython.display import clear_output
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

class GetVoteData(object):
    
    def __init__(self, headless=True):
        self.headless= headless
        self.url = 'https://www.senate.gov'
        
    def _mongo_vote_data(self):
        '''
        Queries senate_sessions database from mongo and returns a dataframe with all vote extensions since 1989
        '''
        #import data from mongodb
        client = pymongo.MongoClient()
        db = client.congress # database that has the collection with the data extracted from the senatescraper.py script
        collection = db.senate_sessions # collection that has the data extracted from the senatescraper.py script
        senate_df = pd.DataFrame(list(collection.find()))
        senate_df['year'] = senate_df['session'].apply(lambda x: re.sub(r"\D+", "", x[-7:])).astype(int)
        return senate_df
    
    def _mongo_senate_votes(self):
        '''
        Creates a mongodb connection and database. By default it will be called congress and the collections 
        will be called senate_sessions.   
        '''
        client = pymongo.MongoClient()
        # db name will be `congress` unless changed here to something else
        db = client.congress
        # table name will be `senate_votes` unless changed here to something else
        collection = db.senate_votes
        return collection

    def _session_ext_by_year(self, year):
        '''
        Calls mongo_vote_data function to build a dataframe, of which will be used to filter by year provided.
        '''
        df = self._mongo_vote_data()
        extensions = df[df['year'] == year]
        return extensions['extensions'].to_list()

    def load_votes_by_year(self, year):
        '''
        Scrapes voting data by year from the U.S. Senate website and loads it directly to a mongodb database.

        Parameters:
        -----------
        year : int
            Four-digit year (ex: 2020)  
        '''
        extensions = self._session_ext_by_year(year)
        collection = self._mongo_senate_votes()

        if self.headless == False:
            driver = webdriver.Chrome()
        else:
            options = Options()
            options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
            
        driver.implicitly_wait(60)

        count = 0
        # runs through all vote extensions on year provided
        for ext in extensions:

            clear_output(wait=True)
            driver.get(self.url + ext) 
            ps = driver.page_source
            collection.insert_one({'html': ps, 'time_scraped': time.ctime(), 'ext':ext})   
            time.sleep(random.randrange(3,10)) # added to simulate human interaction, prevents response failures.
            count += 1
            print("Current Progress: ", np.round(count/len(extensions) * 100, 2), "%")

        driver.close()
        return print("{} votes have been added to {}".format(year, collection))