import streamlit as st
import pandas as pd
import numpy as np
from utils.FirebaseUtils import FirebaseInsertor
from utils.GameLogic import gameEnd

@st.cache_resource
def initFirebase():
    inserter = FirebaseInsertor()

    return inserter
inserter = initFirebase()

def resetPage():
    del st.session_state.vin_score
    del st.session_state.ka_score
    del st.session_state.set_score_list
    del st.session_state.gameData
    st.experimental_rerun()

def endSet():
    last_score_copy = st.session_state.set_score_list.copy()
    # Reset set_score_list 
    st.session_state.set_score_list = []
    # Add new set key to gameData
    st.session_state.gameData[set_No+1] = dict(enumerate(last_score_copy,start = 1))
    st.session_state.vin_score = 0
    st.session_state.ka_score = 0

def endGame():
    output = {}
    for set in st.session_state.gameData.keys():
        output[set] = list(st.session_state.gameData[set].values())

    # Display game data
    st.markdown(f"## 🎯 Game Tracker")
    with st.expander("Game tracker.."):
        st.table({'sets' : output.keys(),'winners': output.values()})

    gameId = inserter.addGameData(output)

    st.info(f'Game with id {gameId} inserted into your ass', icon="ℹ️")

def populateGamedata():
    for set in st.session_state.gameData.keys():
        st.markdown(f"## 🎯 Set {set} Point Tracker")
        with st.container():
            with st.expander("Points tracker.."):
                st.table({"Points":st.session_state.gameData[set].keys(), "Winner" : st.session_state.gameData[set].values()})

st.title("🏓 Ping Pong Tracker")

# Set-up players 
n1, n2 = st.columns([1, 1])
with n1:
    PLAYER_1 = st.text_input("Player 1", "Vineesh")
with n2:
    PLAYER_2 = st.text_input("Player 2", "Kaleb")

# Set-up session state variables
if 'vin_score' not in st.session_state:
    st.session_state.vin_score = 0
    
if 'ka_score' not in st.session_state:
    st.session_state.ka_score = 0

if 'set_score_list' not in st.session_state:
    st.session_state.set_score_list = []

if "gameData" not in st.session_state:
    st.session_state.gameData = {}

b1, b2 = st.columns([1, 1])

with b1:
    player1_button = st.button(PLAYER_1, use_container_width = True, type = "primary")

with b2:
    player2_button = st.button(PLAYER_2, use_container_width = True, type = "primary")


if player1_button:
    st.session_state.vin_score += 1
    st.session_state.set_score_list.append(PLAYER_1)
elif player2_button:
    st.session_state.ka_score += 1
    st.session_state.set_score_list.append(PLAYER_2)

# Extract current set from gameData
try:
    set_No = max(st.session_state.gameData.keys())
except:
    set_No = 0

c1, c3 = st.columns([1, 1])

with c1:
    reset =  st.button("Reset", use_container_width = True)

with c3:
    end_game = st.button("End Game", use_container_width = True)

if reset:
    resetPage()

end, winner = gameEnd(st.session_state.vin_score, st.session_state.ka_score )

if end:
    print("Set ended")
    endSet()

# Display total score
st.table({'Person' : ["Vineesh","Kaleb"],"Score":[st.session_state.vin_score,st.session_state.ka_score]})

populateGamedata()


if end_game:
    endGame()