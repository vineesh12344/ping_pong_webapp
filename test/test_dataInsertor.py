
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.SupabaseUtilis import DataInsertor, DataExtractor
from supabase import create_client, Client
import unittest

url =  'https://ccvkooliuwesbkbmbdjo.supabase.co'
key = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImNjdmtvb2xpdXdlc2JrYm1iZGpvIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTY3OTk5Mjg1MiwiZXhwIjoxOTk1NTY4ODUyfQ.DnhmddHfA2i69hWWGBK8axI43d9hlEJsIzm95kxoMfY'
supabase: Client = create_client(url, key)

import unittest

class TestAddDisruptionEvent(unittest.TestCase):
    def setUp(self):
        # Instantiate the necessary classes and initialize any data needed for testing
        self.insertor = DataInsertor(supabase)

    def test_missing_keys(self):
        # Test case for when DisruptionEvent is missing required keys
        
        event = {'DisasterType': 'Flood'}
        DisruptionEvent = self.insertor.addDisruptionEvent(event)
        self.assertIsNone(DisruptionEvent)

    def test_existing_event(self):
        # Test case for when the event already exists in the database
        event = {
            'DisasterType': 'Flood',
            'Location': 'Malaysia',
            'StartDate': '2023-01-30',
            'EndDate': '2023-01-30',
        }
        # Insert the event into the database
        DisruptionEvent = self.insertor.addDisruptionEvent(event)
        print('DisruptionEvent: ', DisruptionEvent)

        # Check if returned event is the same as the inserted event
        self.assertEqual(DisruptionEvent['disruption_id'],1)

    def test_insert_event(self):
        # Test case for successfully inserting an event into the database
        event = {
            'DisasterType': 'Flood',
            'Location': 'Singapore',
            'StartDate': '2024-04-24',
            'EndDate': '2024-04-25',
        }
        DisruptionEvent = self.insertor.addDisruptionEvent(event)
        print('DisruptionEvent: ', DisruptionEvent)
        self.assertIsNotNone(DisruptionEvent)

        # Delete the event from the database
        data = supabase.table("DisruptionEvents2").delete().eq("disruption_id", DisruptionEvent['disruption_id']).execute()
    



if __name__ == '__main__':
    unittest.main()
