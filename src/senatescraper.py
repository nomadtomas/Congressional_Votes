
import pandas as pd 
import numpy as np
from bs4 import BeautifulSoup as bs
import pymongo
from selenium import webdriver
from selenium.webdriver.support.select import Select
from selenium.webdriver.chrome.options import Options

class SenateScrape(object):
    
    def __init__(self, headless=False):
        self.headless = headless
        self.url = 'https://www.senate.gov'

    def _mongo_senate_sessions(self):
        '''
        Creates a mongodb connection and database. By default it will be called congress and the collections 
        will be called senate_sessions.   
        '''
        client = pymongo.MongoClient()
        # db name will be `congress` unless changed here to something else
        db = client.congress
        # table name will be `senate` unless changed here to something else
        pages = db.senate_sessions
        return pages

    def scrape_dataframe(self, ext, load_to_mongo=False):
        '''
        Scrapes data from the U.S. Senate website, and renders a dataframe with the appropriate senate
        session passed under the ext variable. 

        Parameters:
        -----------
        ext : str
            Extension to be added to the base url ('https://www.senate.gov').  
            Example ext: '/legislative/LIS/roll_call_lists/vote_menu_116_1.htm'
        
        load_to_mongo : bool (optional)
            By default will be False
            Will load data to a mongodb assigned by the mongo_senate_sessions function if True 
        
        Returns:
        --------
        Pandas dataframe of senate votes for any particular session. 
        '''
        # if true it will run selenium headless 
        if self.headless == False:
            driver = webdriver.Chrome()
        else:
            options = Options()
            options.add_argument("--headless")
            driver = webdriver.Chrome(options=options)
        
        driver.implicitly_wait(60)
        driver.get(self.url + ext) 
        # will select "All" rows to display all votes in the session   
        obj = Select(driver.find_element_by_name("listOfVotes_length"))
        obj.select_by_value("-1")
        curr_pg = driver.page_source
        curr_soup = bs(curr_pg,'lxml')
        data = curr_soup.find_all('td')
        # updates session name with scraped session name
        session = curr_soup.find(id="legislative_header").get_text(strip=True)
        # will generate a list of extentions
        urls = []
        for x in data:
            links = x.find_all('a')
            for a in links:
                urls.append(a['href'])
        # filters the url list to only containg legislative extentions
        series_urls = pd.Series(urls)[pd.Series(urls).str.contains('legislative')]

        df = pd.concat(pd.read_html(curr_pg))
        df['extensions'] = series_urls.to_list()
        df['session'] = session
        df['vote_num'] = df['Vote (Tally)'].apply(lambda x: x.split('(')[0].strip())
        df['vote_yea'] = df['Vote (Tally)'].apply(lambda x: x.split('(')[1].split('-')[0])
        df['vote_nay'] = df['Vote (Tally)'].apply(lambda x: x.split('(')[1].split('-')[1].replace(')',''))
        
        if load_to_mongo == True:
            pages = self._mongo_senate_sessions()
            pages.insert_many(df.to_dict('records'))
            
        driver.close()
        return df