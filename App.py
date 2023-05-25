import streamlit as st
import pandas as pd
import numpy as np
from utils.FirebaseUtils import FirebaseInsertor

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
    st.session_state.gameData = {
        1: st.session_state.set_score_list,
    }

player1_button = st.sidebar.button(PLAYER_1, use_container_width = True)
player2_button = st.sidebar.button(PLAYER_2, use_container_width = True)

def updateSetPointTracker(set_score_list):
    return st.table({'Points':np.arange(1,len(st.session_state.set_score_list)+1),'Player Scored':st.session_state.set_score_list})
def updateGamePointTracker(gameData):
    return st.table({'Set':gameData.keys(),'Points':gameData.values()})

# Display total score
st.table({'Person' : ["Vineesh","Kaleb"],"Score":[st.session_state.vin_score,st.session_state.ka_score]})

if player1_button:
    st.session_state.vin_score += 1
    st.session_state.set_score_list.append(PLAYER_1)

elif player2_button:
    st.session_state.ka_score += 1
    st.session_state.set_score_list.append(PLAYER_2)

# Extract current set from gameData
set_No = max(st.session_state.gameData.keys())
# Game tracker
st.markdown(f"## 🎯 Set {set_No} Point Tracker")
with st.expander(f"🧾 Points Tracker..."):
    setPointTracker = updateSetPointTracker(st.session_state.set_score_list)


c1, c2,c3 = st.columns([1, 1,1])

with c1:
    reset =  st.button("Reset")
with c2:
    # Temporary button to end set ( later add logic to end set when one player reaches 11 points/duce)
    end_set = st.button("End Set")

with c3:
    end_game = st.button("End Game")

# display gameData
st.markdown(f"## 🧾 Total Game Data")
with st.expander(f" 🧾 Game Data..."):
    gamePointTracker = updateGamePointTracker(st.session_state.gameData)

if reset:
    del st.session_state.vin_score
    del st.session_state.ka_score
    del st.session_state.set_score_list
    st.experimental_rerun()

# Logic to end set
if end_set:
    # Make a copy of gameData
    gameData = st.session_state.gameData.copy()
    # Reset set_score_list 
    st.session_state.set_score_list = []
    # Add new set key to gameData
    gameData[set_No+1] = st.session_state.set_score_list

if end_game:
    pass