import numpy as np
import pandas as pd
from bs4 import BeautifulSoup as bs
import requests

class SenateVotes(object):
    
    def __init__(self):
        self.base_url = 'https://www.senate.gov'
        self.ext = '/legislative/votes_new.htm'
        self.alt_ext = None
        self.failed_links = []
        self.session = None
        
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
        soup = self.soup(self.ext)
        return soup.select('p')[0].a['href']
        
    def current_votes_soup(self):
        '''
        Runs soup function to parse data 
        '''
        try:
            if self.alt_ext == None:
                ext = self.current_votes_ext()
            else:
                ext = self.alt_ext

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
            self.alt_ext = alt_ext

        extensions = self.current_votes_extensions()
        df = pd.concat(pd.read_html(self.base_url + self.current_votes_ext()))
        df['session'] = self.session
        df['vote_num'] = df['Vote (Tally)'].apply(lambda x: x.split('(')[0].strip())
        df['vote_yea'] = df['Vote (Tally)'].apply(lambda x: x.split('(')[1].split('-')[0])
        df['vote_nay'] = df['Vote (Tally)'].apply(lambda x: x.split('(')[1].split('-')[1].replace(')',''))
        df['extension'] = extensions
        return df
        
    def vote_summary_df(self, ext):
        '''
        Generates a dataframe of how senators voted on a particular issue
        '''
        votes = []
        
        try:
            soup = self.soup(ext)
            link = soup.find(class_='newspaperDisplay_3column')
            lst = link.get_text().split('\n')
            vote_list = [x for x in lst if len(x.strip()) > 1]

            card = soup.find(class_='contenttext')
            question = card.select("div:nth-of-type(1)")[0].get_text(strip=True).split(":")[1]
            vote_number = card.select("div:nth-of-type(2)")[0].get_text(strip=True).split(":")[1]
            vote_date = card.select("div:nth-of-type(3)")[0].get_text(strip=True).replace('Vote Date:', '')
            required_majority = card.select("div:nth-of-type(5)")[0].get_text(strip=True).split(":")[1]
            vote_result = card.select("div:nth-of-type(6)")[0].get_text(strip=True).split(":")[1]
            measure_number = card.select("div:nth-of-type(8)")[0].get_text(strip=True).split(":")[1]
            measure_title = card.select("div:nth-of-type(9)")[0].get_text(strip=True).split(":")[1]

            data = pd.DataFrame({'votes':vote_list})
            data['senator'] = data['votes'].apply(lambda x: x.split('(')[0])
            data['vote'] = data['votes'].apply(lambda x: x.split(',')[1])
            data['party'] = data['votes'].apply(lambda x: x.split('(')[1].split(',')[0].split('-')[0])
            data['state'] = data['votes'].apply(lambda x: x.split('(')[1].split(',')[0].split('-')[1].replace(')',''))
            data['question'] = question
            data['vote_number'] = vote_number
            data['vote_date'] = vote_date
            data['required_majority'] = required_majority
            data['vote_result'] = vote_result
            data['measure_number'] = measure_number
            data['measure_title'] = measure_title

            votes.append(data)

        except:
            print("Failed extension: {}".format(self.base_url + ext))
            self.failed_links.append(self.base_url + ext)
                
        df = pd.concat(votes)
        return df

    def votes_by_issue(self, issue):
        '''
        Generates a dataframe of how senators voted by any particual issue

        Parameters
        ----------
        issue : str
            Name of issue to be filted by.  

        Returns
        -------
        A dataframe with of all senators and vote information of filtered issue  
        '''
        df = self.current_votes_df()
        data = df[df['Issue']==str(issue)]
        lst = data['extension'].to_list()
        
        dt = []

        for x in lst:
            data = self.vote_summary_df(x)
            data['vote_attempts'] = len(lst)
            dt.append(data)
        
        df = pd.concat(dt)
        return df