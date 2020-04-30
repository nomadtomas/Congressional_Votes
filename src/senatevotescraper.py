import numpy as np
import pandas as pd
from bs4 import BeautifulSoup as bs
import requests
import pymongo
import time

class SenateVotes(object):
    
    def __init__(self):
        self.base_url = 'https://www.senate.gov'
        self.ext = '/legislative/votes_new.htm'
        self.alt_ext = None
        self.session = None
        self.curr_ext = True
        self.failed_links = []

    def mongo(self):
        '''
        Creates connection to mongodb database table
        '''
        client = pymongo.MongoClient()
        # db name will be `congress` unless changed here to something else
        db = client.congress
        #table name will be `senate` unless changed here to something else
        pages = db.senate
        return pages
        
    def soup(self, extension, parser='html.parser'):
        '''
        Parses data using beautiful soup.
        '''
        response = requests.get(self.base_url + extension)
        return bs(response.text, parser)
    
    def current_votes_ext(self):
        '''
        Gets current congressional session's extension
        '''
        if self.curr_ext == True:
            soup = self.soup(self.ext)
            return soup.select('p')[0].a['href']
        else:
            return self.alt_ext

    def current_votes_soup(self):
        '''
        Runs soup function to parse data 
        '''
        try:
            ext = self.current_votes_ext()
            current_soup = self.soup(ext)
            return current_soup
            
        except:
            print("failed to get current votes soup")
    
    def current_votes_extensions(self):
        '''
        Creates a list of extensions of all vote tallied on a congressional session
        '''
        soup = self.current_votes_soup()
        data = soup.find_all('td')
        # updates session name with scraped session name
        self.session = soup.find(id="legislative_header").get_text(strip=True)
        
        urls = []
        for x in data:
            links = x.find_all('a')
            for a in links:
                urls.append(a['href'])

        series_urls = pd.Series(urls)[pd.Series(urls).str.contains('legislative')] 
        return series_urls.to_list()
    
    def current_votes_df(self, alt_ext=None):
        '''
        Generates a dataframe of all roll call votes of session provided 
        '''
        if alt_ext != None:
            self.curr_ext = False
            self.alt_ext = alt_ext
            extensions = self.current_votes_extensions()
        else:
            extensions = self.current_votes_extensions()
            
        df = pd.concat(pd.read_html(self.base_url + self.current_votes_ext()))
        df['session'] = self.session
        df['vote_num'] = df['Vote (Tally)'].apply(lambda x: x.split('(')[0].strip())
        df['vote_yea'] = df['Vote (Tally)'].apply(lambda x: x.split('(')[1].split('-')[0])
        df['vote_nay'] = df['Vote (Tally)'].apply(lambda x: x.split('(')[1].split('-')[1].replace(')',''))
        df['extension'] = extensions
        return df

    def scrape_page(self, load_to_mongo=False):
        '''
        Scrapes senator vote data from the assinged current_votes_df 
        and loads it into a mogno database if load_to_mongo is equal to True
        '''

        if load_to_mongo ==True:
            df = self.current_votes_df()
            lst = df['extension'].to_list()

            for ext in lst:
                response = requests.get(self.base_url + ext)
                pages = self.mongo()
                pages.insert_one({'html': response.content, 'time_scraped': time.ctime()}) 
            
            return print("Finished Scraping: {}".format(self.session))
        else:
            return print("If you would like to load to a mongo database, set load_to_mongo to True")
 