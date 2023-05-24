import streamlit as st
import pandas as pd
import numpy as np
from utils.FirebaseUtils import FirebaseInsertor

st.title("Ping Pong Tracker")

# Set-up players 
n1, n2 = st.columns([1, 1])
with n1:
    PLAYER_1 = st.text_input("Player 1", "Vineesh")
with n2:
    PLAYER_2 = st.text_input("Player 2", "Kaleb")

if 'vin_score' not in st.session_state:
    st.session_state.vin_score = 0

if 'ka_score' not in st.session_state:
    st.session_state.ka_score = 0

player1_button = st.sidebar.button(PLAYER_1, use_container_width = True)
player2_button = st.sidebar.button(PLAYER_2, use_container_width = True)

if 'player_scored_list' not in st.session_state:
    st.session_state.player_scored_list = []

def updateGamePointTracker(player_scored_list):
    return st.table({'Points':np.arange(1,len(st.session_state.player_scored_list)+1),'Player Scored':st.session_state.player_scored_list})

# Display total score
st.table({'Person' : ["Vineesh","Kaleb"],"Score":[st.session_state.vin_score,st.session_state.ka_score]})

if player1_button:
    st.session_state.vin_score += 1

    st.session_state.player_scored_list.append(PLAYER_1)

elif player2_button:
    st.session_state.ka_score += 1

    st.session_state.player_scored_list.append(PLAYER_2)


# Game tracker
st.markdown("## Set Point Tracker")
GamePointTracker = updateGamePointTracker(st.session_state.player_scored_list)


c1, c2 = st.columns([1, 1])
with c1:
    reset =  st.button("Reset")

with c2:
    end_game = st.button("End Game")

if reset:
    del st.session_state.vin_score
    del st.session_state.ka_score
    del st.session_state.player_scored_list
    st.experimental_rerun()

if end_game:
    pass