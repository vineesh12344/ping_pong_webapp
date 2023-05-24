import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
import os
from datetime import datetime

# get path to secrets directory 
PATH_TO_SECRETS = os.path.abspath(os.path.join(os.path.join(os.path.dirname(__file__), '..'), 'secrets'))

class FirebaseInsertor:
    """
    Utility class for inserting data into firebase
    Initialization process handled 

    Methods:
    """

    def __init__(self):
        """
        Assumes that a singular .json api key file is present in the secrets directory

        creates attributes:
            self.db: firestore client

        """
        # open all files in the directory
        for filename in os.listdir(PATH_TO_SECRETS):
            # check if file is a json file
            if filename.endswith('.json'):
                # get path to file
                path_to_file = os.path.join(PATH_TO_SECRETS, filename)
                # initialize firebase admin sdk
                try:
                    cred = credentials.Certificate(path_to_file)
                    firebase_admin.initialize_app(cred)
                    # create a firestore client
                    self.db = firestore.client()
                    print('Firebase admin sdk initialized')
                    break
                except:
                    print('Error initializing firebase admin sdk, check if .json file is valid')
                    raise Exception('Error initializing firebase admin sdk , check if .json file is valid')
                
    
    def addGameData(self, gameData: dict):
        """
        Adds a gameData object to the database
            * includes datetime field to the gameData object

        Handles checking 
        Args:
            gameData: a dictionary containing the gameData object to be added to the database
            E.g: {
                '1': [player1,player2,player1,player1,...],
                '2' : [player1,player2,player1,player1,...],
                '3' : [player1,player2,player1,player1,... till the set is over],
            }
            keys:
                * numeric numbers: the set number
            values: 
                * a list of strings representing the player who won the point at that index

        Returns:
            gameData: the gameData object that was added to the database
        """

        # Convert integer keys to strings
        gameData = {str(key): value for key, value in gameData.items()}
        # Validate gameData object
        if not isinstance(gameData, dict):
            raise Exception('gameData is not a dictionary')
        
            
        # Check length of values in gameData object must be at least 11
        # # TODO add more robust validation logic here for valid game 
        # for value in gameData.values():
        #     if len(value) < 11:
        #         raise Exception('Each set values must contain at least 11 points')

        # Add the date to the gameData object
        current_datetime = datetime.now()
        gameData['datetime'] = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
        # convert to string for now 
        gameData['datetime'] = str(gameData['datetime'])
        print('gameData wpppp: ', gameData)

        # Add the data to Firestore
        # Set the id of the document to be the current datetime
        doc_ref = self.db.collection('PingPong').document(f'game_{current_datetime.strftime("%Y-%m-%d %H:%M:%S")}')
        doc_ref.set(gameData)

        # Print the auto-generated document ID
        print('Document ID:', doc_ref.id)
        return doc_ref.id

