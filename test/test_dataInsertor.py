
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.FirebaseUtils import FirebaseInsertor
import unittest
from unittest.mock import patch
from datetime import datetime

class TestAddDisruptionEvent(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Instantiate the necessary classes and initialize any data needed for testing
        cls.insertor = FirebaseInsertor()
    
    def test_addGameData_valid(self):
        # Prepare valid gameData
        gameData = {
            1: ['player1', 'player2', 'player1', 'player1', 'player1', 'player2', 'player1', 'player1', 'player1', 'player2', 'player1'],
            2: ['player1', 'player2', 'player1', 'player1', 'player1', 'player2', 'player1', 'player1', 'player1', 'player2', 'player1'],
            3: ['player1', 'player2', 'player1', 'player1', 'player1', 'player2', 'player1', 'player1', 'player1', 'player2', 'player1']
        }

        # Add gameData to Firestore
        game_id = self.insertor.addGameData(gameData)
        print('game_id: ', game_id)

        # Assert that the game_id is returned
        self.assertIsInstance(game_id, str)

    def test_addGameData_invalid_data_type(self):
        # Prepare invalid gameData (not a dictionary)
        gameData = 'invalid_data'
        
        # Assert that an exception is raised
        with self.assertRaises(Exception) as context:
            self.insertor.addGameData(gameData)

        # Assert the expected exception message
        self.assertEqual(str(context.exception), 'gameData is not a dictionary')

    def test_addGameData_invalid_keys(self):
        # Prepare invalid gameData (keys are not numeric)
        gameData = {
            'set1': ['player1', 'player2', 'player1', 'player1', 'player1', 'player2', 'player1', 'player1', 'player1', 'player2', 'player1'],
            2: ['player1', 'player2', 'player1', 'player1', 'player1', 'player2', 'player1', 'player1', 'player1', 'player2', 'player1'],
            3: ['player1', 'player2', 'player1', 'player1', 'player1', 'player2', 'player1', 'player1', 'player1', 'player2', 'player1']
        }

        # Assert that an exception is raised
        with self.assertRaises(Exception) as context:
            self.insertor.addGameData(gameData)

        # Assert the expected exception message
        self.assertEqual(str(context.exception), 'gameData keys (sets) must be numeric')




if __name__ == '__main__':
    unittest.main()
