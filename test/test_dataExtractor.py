
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.FirebaseUtils import FirebaseInsertor, FirebaseExtractor
import unittest
from unittest.mock import patch
from datetime import datetime

class TestAddDisruptionEvent(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Instantiate the necessary classes and initialize any data needed for testing
        cls.extractor = FirebaseExtractor()
    
    def test_getGameData_byId_valid(self):
        # Prepare valid gameData
        game_id = "game_2023-05-24 23:31:32"

        # Add gameData to Firestore
        game_data = self.extractor.getGameData_byId(game_id=game_id)
        print('game_data: ', game_data)

        # Assert that the game_id is returned
        self.assertIsInstance(game_data, dict)

if __name__ == '__main__':
    unittest.main()
