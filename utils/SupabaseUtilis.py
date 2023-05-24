# General imports
import pandas as pd
import numpy as np
import json
import requests
from datetime import datetime
import postgrest
from postgrest.exceptions import APIError
from tqdm import tqdm
# Supabase
from supabase import create_client, Client


from dotenv import load_dotenv
# get path to the .env file
import os
from pathlib import Path
env_path = Path('.') / '.env'
load_dotenv(env_path)

url = os.getenv('URL')
key = os.getenv('API_KEY')
supabase: Client = create_client(url, key)


class DataInsertor:
    """
    TLDR: "Fancy INSERT INTO statement for the supabase" (Need to migrate over)

    Methods:
        * addDisruptionEvent(self, DisruptionEvent: dict)
        * addEventDataRelation
    """

    def __init__(self, supabase: Client):
        self.supabase = supabase

    def addDisruptionEvent(self, DisruptionEvent: dict):
        """
        Inserts a single row into the DisruptionEvent table
        Params:
            * DisruptionEvent: dict
                Example: {
                    'DisasterType': 'Flood',
                    'Location': 'Singapore',
                    'StartDate': '2021-08-01',
                    'EndDate': '2021-08-02',
                } 
                OR 
                E.g: {
                        "DisruptionEvent": "Earthquake",
                        "Location" : ["New York", "California"]
                        "Article_Published_Date" : "2021-05-12" 
                    }
        Error Handling:
            if event already exists in the database, it will not insert the event reuturn the event details as dictionary
            if unknown error occurs, it will return the None
        Success:
            returns the event details as a dictionary

        Returns:
            * DisruptionEvent: dict 
            with disruption_id key added
                Example: {
                    To be filled up
                }
        """
        # Check if Article_Published_Date key exists in the dictionary
        if 'Article_Published_Date' in DisruptionEvent:
            print(f'Article_Published_Date key exists in DisruptionEventDict When trying to insert {DisruptionEvent}')
            # Check if Location is a list
            assert type(DisruptionEvent['Location']) == list, 'Location must be a list when Article_Published_Date is present'
            # Process DisruptionEvent['Location'] to be a string seperated by comma
            DisruptionEvent['Location'] = ','.join(DisruptionEvent['Location'])
        
            # Re-format the DisruptionEvent dictionary, add the StartDate and EndDate keys 
            DisruptionEvent['StartDate'] = DisruptionEvent['Article_Published_Date']
            DisruptionEvent['EndDate'] = DisruptionEvent['Article_Published_Date']
            # Remove the Article_Published_Date key
            DisruptionEvent.pop('Article_Published_Date')



        # Check if DisruptionEvent has all the required keys
        required_keys = ['DisasterType', 'Location', 'StartDate', 'EndDate']
        for key in required_keys:
            if key not in DisruptionEvent:
                print(f'Error: {key} is missing from DisruptionEventDict')
                return None

        # Check if the event already exists in the database
        try:
            data, count = supabase.table('DisruptionEvents2').insert(
                DisruptionEvent).execute()
            print(f'data : {data}, data type: {type(data)} count : {count}')
            # Extract the id of the article that was just inserted
            disruption_id = data[1][0]['disruption_id']
            print(
                f'ID of the Event that was just inserted --> {disruption_id}')
            # Add id of the article to the article dictionary
            DisruptionEvent['disruption_id'] = disruption_id
            return DisruptionEvent

        except postgrest.exceptions.APIError as e:
            print(f'THIS IS THE DisruptionEvent after check already exists: {DisruptionEvent}')
            print(f'Error inserting into Supabase: {e}')
            print(e.code, e.message, e.details)
            if e.code == '23505':
                # If the article already exists in the database, return the article dictionary from the database
                print(
                    f'\nEvent with DisasterType {DisruptionEvent["DisasterType"]}, Location: {DisruptionEvent["Location"]}, Date: {DisruptionEvent["StartDate"]} already exists in the database.\n')
                existing_data = supabase.table('DisruptionEvents2').select('*').eq('DisasterType', DisruptionEvent["DisasterType"]).eq('StartDate', DisruptionEvent["StartDate"]).eq('Location',DisruptionEvent['Location'] ).execute()
                
                print(f'existing_data: {existing_data}')
                return existing_data.data[0]
            else:
                print(f'Unexpected error: {e}')
                return None

    def addEventDataRelation(self, id_data: int, id_event: int, similarity_score: dict, bertopic_score: dict=None, **kwargs):
        """
        Inserts a single row into the EventDataRelation table
        Params:
            * id_data: int
                id of the data article
            * id_event: int
                id of the event article
            * Similarity_score: dict
                Example: {
                    title: 0.5,
                    text: 0.6,
                    summary: 0.7
                }
            * bert_score: dict
                Example: {
                    topic_id : 2,
                    max_probability: 0.3412,
                    bertopic_model: "BERTopicGuided_spacy_titleV2"
                }
        Error Handling:
            if event already exists in the database, it will not insert the event reuturn the event details as dictionary
            if unknown error occurs, it will return the None
        Success:
            returns the event details as a dictionary
        """

        EventDataRelation = {
            'id_data': id_data, 'id_event': id_event, 'similarity_score': similarity_score,'bertopic_score': bertopic_score}
        print(f'Adding to EventDataRelation: {EventDataRelation}')
        try:
            data, count = supabase.table('EventDataRelation2').insert(
                EventDataRelation).execute()
            print(
                f'Successfully added to EventDataRelation: << id_data: {id_data}, id_event: {id_event} >>')
            return EventDataRelation

        except postgrest.exceptions.APIError as e:
            print(f'Error inserting into Supabase: {e}')
            print(e.code, e.message, e.details)
            if e.code == '23505':
                # If the article already exists in the database, return the article dictionary from the database
                print(
                    f'EventDataRelation with id_data: {EventDataRelation["id_data"]}, id_event: {EventDataRelation["id_event"]}, similarity_score: {EventDataRelation["similarity_score"]} already exists in the database.')
                existing_data = supabase.table('EventDataRelation2').select(
                    '*').eq('id_data', EventDataRelation["id_data"]).eq('id_event', EventDataRelation["id_event"]).execute()
                return existing_data.data[0]
            else:
                print(f'Unexpected error: {e}')
                return None

    def addArticleData(self, article_dict, **kwargs):
        """
        Inserts a single row into the Data table ( Data2 table )
        Vaildation for duplicates is done here

        Params:
            * article_dict: dict
                Keys are the column names and values are the values to be inserted into the table
            **kwargs: dict
                > table: str
                    Name of the data table to insert into
        """
        # Get the table name from the kwargs
        self.table = kwargs.get('table', 'Data')

        # Columns in the Data table
        columns_list_Data = ['Title','Article_Published_Date',"Text","Summary","Keywords_NER","Url","Sentiment","Subjectivity","Date_Scraped","DisruptionEvent","Data_Source","DisruptionEvent"]
        # Only insert the columns that are in the Data table
        article_dict = {k: v for k, v in article_dict.items() if k in columns_list_Data}

        # Insert the article information into the database, check duplicates by url
        try:
            data, count = supabase.table(
                self.table).insert(article_dict).execute()
            print(f'data : {data}, data type: {type(data)} count : {count}')
            # Extract the id of the article that was just inserted
            id = data[1][0]['id']
            print(f'ID of the article that was just inserted --> {id}')
            # Add id of the article to the article dictionary
            article_dict['id'] = id
            return article_dict

        except postgrest.exceptions.APIError as e:
            print(f'Error inserting into Supabase: {e}')
            print(e.code, e.message, e.details)
            if e.code == '23505':
                # If the article already exists in the database, return the article dictionary from the database
                print(
                    f'Article with URL {article_dict["Url"]} already exists in the database.')
                existing_data = supabase.table(self.table).select(
                    '*').eq('Url', article_dict['Url']).execute()
                return existing_data.data[0]
            else:
                print(f'Unexpected error: {e}')
                return None

class DataExtractor:
    """
    TLDR: Acts like a fancy "SELECT" Query for the Supabase database
    Class is used to format the data formats/ types after extraction from the database
    Params:
        * response: The response object from the database

    Public Methods:
        * formatData: Formats the data
            Returns: pandas dataframe 

    Private Methods:
        * convertDateTime: Converts datetime string to datetime object
            Returns: pandas dataframe
        * KeywordNer : Converts Json object to two separate columns
            Returns: pandas dataframe
        * industryCount: Counts the number of articles per industry
            Returns: pandas dataframe
        * checkDisruptionEvent: Checks if the DisruptionEvent exists in the database
            returns: None if not found, else returns the DisruptionEvent dict
    """

    def __init__(self, supabase: Client):
        self.supabase = supabase

    def _convertDateTime(self, df, column: str):
        df[column] = pd.to_datetime(df[column])
        return df

    def _keywordNer(self, df):
        """
        {"NER":["Myanmar"],"Keywords":["rare","homes","tornadoes","soe","large","thet","destroyed","200","fatalities","21","tornado","villages","myanmar","injured","houses","hits","near"]}
        """
        # Check if Keywords_NER column data is not null
        if df['Keywords_NER'] == None:
            # If null, return the df with keys and ner columns as empty
            df['Keywords'] = []
            df['NER'] = []
            return df
        
        df['Keywords'] = df['Keywords_NER'].apply(lambda x: x['Keywords'])
        df['NER'] = df['Keywords_NER'].apply(lambda x: x['NER'])
        return df

    def _checkDisruptionEvent(self, DisruptionEvent: dict):
        """
        Params:
            * DisruptionEvent: dict
                DisruptionEvent = {
                "DisasterType": "Flood",
                "Location": "China",
                "Article_Published_Date": "2000-06-17",   
                }
        Returns:
            * DisruptionEvent: dict
                DisruptionEvent = {'disruption_id': 3, 'created_at': '2023-04-12T02:52:21.125245+00:00', 'DisasterType': 'Flood', 'Location': 'China', 
                'StartDate': datetime.datetime(2000, 6, 17, 0, 0), 'EndDate': '2000-06-30', 'EventOccured': True}
            * None if DisruptionEvent not found
        """
        # Check if the DisruptionEvent exists in the database
        # if Article_Published_Date falls within 5 days of the StartDate, then it is the same DisruptionEvent
        response = self.supabase.table('DisruptionEvents2').select(
            "*").eq("DisasterType", DisruptionEvent['DisasterType']).eq('Location', DisruptionEvent['Location']).execute()
        # Check if the response is empty
        if len(response.data) == 0:
            return None

        # Check if the Article_Published_Date falls within 5 days of the StartDate
        for event in response.data:
            # Convert the StartDate to datetime object
            event['StartDate'] = datetime.strptime(
                event['StartDate'], '%Y-%m-%d')
            # Convert the Article_Published_Date to datetime object
            DisruptionEvent['Article_Published_Date'] = datetime.strptime(
                DisruptionEvent['Article_Published_Date'], '%Y-%m-%d')
            # Check if the Article_Published_Date falls within 5 days of the StartDate
            if abs((event['StartDate'] - DisruptionEvent['Article_Published_Date']).days) <= 5:
                print(f"DisruptionEvent found: {event}")
                return event
            else:
                print(
                    f'{DisruptionEvent["DisasterType"]}, {DisruptionEvent["Location"]} not found within 5 days of {DisruptionEvent["Article_Published_Date"]}')
                return None

    def getFormatedData(self, **kwargs):
        """
        Returns:
            dataframe of the data in the Data table, 
        Params:
            * kwargs:
                > columns: list of columns to select
                    if None, select all columns
                    If invalid columns, return None
                > nulls: bool
                    if False, remove all rows with null values

        """
        # Get kwargs
        columns = kwargs.get('columns', None)
        nulls = kwargs.get('nulls', True)

        if columns == None:
            # Select all data from data table
            response = self.supabase.table('Data').select("*").execute()
        else:
            # Select all data from data table
            try:
                columns_str = ','.join(columns)
                response = self.supabase.table(
                    'Data').select(columns_str).execute()
            except:
                print(f'Invalid columnsname passed in {columns}')
                return None
        # Check if the response is empty
        if len(response.data) == 0:
            return None
        
        df = pd.DataFrame(response.data)
        columns = df.columns

        # check if Article_Published_Date column exists
        if 'Article_Published_Date' in columns:
            df = self._convertDateTime(df, 'Article_Published_Date')

        # check if Keywords_NER column exists
        if 'Keywords_NER' in columns:
            df = self._keywordNer(df)

        # check if nulls argument is True
        if nulls == False:
            df = df.dropna()

        return df

    def getFormatedDataById(self, id: int):
        # Select all data from data table
        response = self.supabase.table(
            'Data').select("*").eq('id', id).execute()

        df = pd.DataFrame(response.data)
        columns = df.columns

        # check if Article_Published_Date column exists
        if 'Article_Published_Date' in columns:
            df = self._convertDateTime(df, 'Article_Published_Date')

        # check if Keywords_NER column exists
        if 'Keywords_NER' in columns:
            df = self._keywordNer(df)

        # check if Data_Source_id column exists
        if 'Data_Source_id' in columns:
            df = self._mapDataSources(df)
        return df


    def getTitleTextUrlFromId(self, id: int):
        """
        Returns the title of the article with the given id with the given id
        as a dictionary of the form {'Title':title, 'Text':text, 'id':id}
        """
        # Select Title from Data table where id = id
        response = self.supabase.table('Data').select(
            'Title,Text,Url').eq('id', id).execute()

        # Extract the title and text from the response return
        title = response.data[0]['Title']
        text = response.data[0]['Text']
        url = response.data[0]['Url']

        # Create a dictionary of the form { 'id':id, 'Title':title, 'Text':text}
        article_dict = {'id': id, 'Title': title, 'Text': text, 'Url': url}

        return article_dict

    def getIdTitleTextFromUrl(self, url: str):

        # Select Title from Data table where url is the url
        response = self.supabase.table('Data').select(
            'id,Title,Text,Url').eq('Url', url).execute()

        # Extract the title and text from the response return
        title = response.data[0]['Title']
        text = response.data[0]['Text']
        id = response.data[0]['id']

        # Create a dictionary of the form { 'id':id, 'Title':title, 'Text':text}
        article_dict = {'id': id, 'Title': title, 'Text': text, 'Url': url}

        return article_dict

    def getAllDataRelation(self):
        # Select all data from DataRelation table
        response = self.supabase.table('DataRelation').select('*').execute()

        print(f'response --> {response}')
        # get unique orign_id
        unique_orign_id = list(set([x['id_orign'] for x in response.data]))

        print(f'unique_orign_id --> {unique_orign_id}')
        # Create a dictionary of the form {orign_id: [related_id1, related_id2, ...]}
        orign_id_dict = {x: [] for x in unique_orign_id}

        # Loop through the response data and add the related_id to the orign_id_dict
        for x in response.data:
            orign_id_dict[x['id_orign']].append(x['id_related'])

        # Extract article attributes from the Data table
        df_list = []
        for orign_id in orign_id_dict:
            # get the Article information for orign_id article
            orign_article_df = self.getFormatedDataById(orign_id)

            # get the Article information for related_id articles
            for related_id in orign_id_dict[orign_id]:
                related_article_df = self.getFormatedDataById(related_id)

                # Add Similarity column to the related_article_df

                # Concatenate the orign_article_df and related_article_df
                orign_article_df = pd.concat(
                    [orign_article_df, related_article_df], axis=0)

            # Append the orign_article_df to the df_list
            df_list.append(orign_article_df)

        return df_list

    def getAllDataRelationById(self, id):
        # Select all data from DataRelation table
        response = self.supabase.table('DataRelation').select(
            '*').eq('id_orign', id).execute()

        print(f'response --> {response}')
        # get unique orign_id
        unique_orign_id = list(set([x['id_orign'] for x in response.data]))

        print(f'unique_orign_id --> {unique_orign_id}')
        # Create a dictionary of the form {orign_id: [related_id1, related_id2, ...]}
        orign_id_dict = {x: [] for x in unique_orign_id}

        # Loop through the response data and add the related_id to the orign_id_dict
        for x in response.data:
            orign_id_dict[x['id_orign']].append(x['id_related'])

        # Extract article attributes from the Data table
        df_list = []
        for orign_id in orign_id_dict:
            # get the Article information for orign_id article
            orign_article_df = self.getFormatedDataById(orign_id)

            # get the Article information for related_id articles
            for related_id in orign_id_dict[orign_id]:
                related_article_df = self.getFormatedDataById(related_id)

                # Add Similarity column to the related_article_df

                # Concatenate the orign_article_df and related_article_df
                orign_article_df = pd.concat(
                    [orign_article_df, related_article_df], axis=0)

            # Append the orign_article_df to the df_list
            df_list.append(orign_article_df)

        return df_list

    def getDisruptionEvent(self, **kwargs):
        """
        Method to get DisruptionEvents 

        Args:
            kwargs (dict): Dictionary of arguments to filter the data
            kwargs parameters
                * No_of_Events (int): Number of events to return
                * Date YYYY-MM-DD datetime format
                * DisasterType (string)
                * Location (string)
        """
        # Get the arguments from the kwargs dictionary
        no_of_events = kwargs.get('No_of_Events', 10)
        date = kwargs.get('Date', None)
        # Check if the date is in the correct format
        if date:
            try:
                date = datetime.strptime(date, '%Y-%m-%d')
            except ValueError:
                print('Date is not in the correct format YYYY-MM-DD')
                return None

        disaster_type = kwargs.get('DisasterType', None)
        location = kwargs.get('Location', None)
        disruption_id = kwargs.get('disruption_id', None)

        # Create logic to filter the data based on the arguments
        query = self.supabase.table('DisruptionEvents2').select('*')
        if date:
            # Filter date if in between the start and end date
            query = query.lte('StartDate', date).gte('EndDate', date)
        if disaster_type:
            query = query.eq('DisasterType', disaster_type)
        if location:
            query = query.eq('Location', location)

        if disruption_id:
            query = query.eq('disruption_id', disruption_id)

        # Sort by earliest date first
        query = query.order('StartDate', desc=True)

        # Extract the data from the DisruptionEvents table
        response = query.limit(no_of_events).execute()
        print(f'response --> {response}')
        return response

    def getLatestDisruptionEvents(self, no_of_events=10, skip=0, fake=False):
        """
        Method to get the latest disruption events
        returns a list of dictionaries
        """
        # Get the latest disruption events

        if fake:
            response = self.supabase.table('DisruptionEvents2').select(
                '*').eq('EventOccured', False).order('StartDate', desc=True).limit(no_of_events).execute()

        else:
            response = self.supabase.table('DisruptionEvents2').select(
                '*').order('StartDate', desc=True).limit(no_of_events).execute()

        # skip the first n events
        response.data = response.data[skip:]

        return response.data

    def getDisruptionEventResults(self, DisasterType, Location, StartDate, **kwargs):
        """
        kwargs parameters
            * score: bool
        Parameters:
            DisasterType (string)
            Location (string)
            StartDate (string) YYYY-MM-DD
        Returns:
            (similarity_score:int, related_articles:df)

        if Disruption event is not found returns None
        """
        # Get the arguments from the kwargs dictionary
        score = kwargs.get('score', False)

        # Get the latest disruption events
        response = self.supabase.table('DisruptionEvents2').select(
            '*').eq('DisasterType', DisasterType).eq('Location', Location).eq('StartDate', StartDate).execute()

        if len(response.data) == 0:
            return None

        # get disruption_id
        try:
            disruption_id = response.data[0].get('disruption_id', None)
        except:
            return None

        # get the related articles and their similarity score
        response2 = self.supabase.table('EventDataRelation2').select(
            '*,id_data, Data(id,*),DisruptionEvents(disruption_id,*)').eq('id_event', disruption_id).execute()
        # Convert the response to a Pandas DataFrame
        df = pd.DataFrame(response2.data)
        # Flatten the nested JSON columns
        df_data_normalize = pd.json_normalize(df['Data'])
        df_event_normalize = pd.json_normalize(df['DisruptionEvents'])
        # Merge the flattened columns with the original DataFrame
        df = pd.merge(df, df_data_normalize, left_index=True, right_index=True)
        df = pd.merge(df, df_event_normalize,
                      left_index=True, right_index=True)

        # Normalize thr similarity_score column
        df_similarity_normalize = pd.json_normalize(df['similarity_score'])
        df = pd.merge(df, df_similarity_normalize,
                      left_index=True, right_index=True)

        # Drop the original nested JSON columns
        df = df.drop(columns=['Data'])
        df = df.drop(columns=['DisruptionEvents'])
        df = df.drop(columns=['similarity_score'])

        # remove redundant columns
        df = df.drop(columns=['disruption_id', 'id'])

        # Rename the columns for similarity_score
        df.rename(columns={'text': 'text_similarity_score', 'title': 'title_similarity_score',
                  'summary': 'summary_similarity_score'}, inplace=True)

        if score:
            # get the median similarity score
            similarity_score = df['text_similarity_score'].median()

        return df if not score else (similarity_score, df)

    def getDisruptionEventResultsById(self, disruption_id, **kwargs):
        """
        kwargs parameters
            * score: bool
        Parameters:
            DisasterType (string)
            Location (string)
            StartDate (string) YYYY-MM-DD
        Returns:
            (similarity_score:int, related_articles:df)

        if Disruption event is not found returns None
        """
        # Get the arguments from the kwargs dictionary
        score = kwargs.get('score', False)

        # Get the latest disruption events
        # response = self.supabase.table('DisruptionEvents2').select('*').eq('DisasterType',DisasterType).eq('Location',Location).eq('StartDate',StartDate).execute()

        # # get disruption_id
        # disruption_id = response.data[0]['disruption_id']
        # if not disruption_id:
        #     return None

        # get the related articles and their similarity score
        response2 = self.supabase.table('EventDataRelation2').select(
            '*,id_data, Data(id,*),DisruptionEvents2(disruption_id,*)').eq('id_event', disruption_id).execute()
        # Check count of response
        if len(response2.data) == 0:
            return None
        
        # Convert the response to a Pandas DataFrame
        df = pd.DataFrame(response2.data)
        print('df: ', df.head())
        
        # Flatten the nested JSON columns
        df_data_normalize = pd.json_normalize(df['Data'])
        df_event_normalize = pd.json_normalize(df['DisruptionEvents2'])
        # Merge the flattened columns with the original DataFrame
        df = pd.merge(df, df_data_normalize, left_index=True, right_index=True)
        df = pd.merge(df, df_event_normalize,
                      left_index=True, right_index=True)

        # Normalize thr similarity_score column
        df_similarity_normalize = pd.json_normalize(df['similarity_score'])
        df = pd.merge(df, df_similarity_normalize,
                      left_index=True, right_index=True)

        # Drop the original nested JSON columns
        df = df.drop(columns=['Data'])
        df = df.drop(columns=['DisruptionEvents2'])
        df = df.drop(columns=['similarity_score'])

        # remove redundant columns
        df = df.drop(columns=['disruption_id', 'id'])

        # Rename the columns for similarity_score
        df.rename(columns={'text': 'text_similarity_score', 'title': 'title_similarity_score',
                  'summary': 'summary_similarity_score'}, inplace=True)

        if score:
            # get the median similarity score
            similarity_score = df['title_similarity_score'].median()

        return df if not score else (similarity_score, df)
