import streamlit as st
import pandas as pd
import numpy as np

st.title("Ping Pong Tracker")

if 'vin_score' not in st.session_state:
    st.session_state.vin_score = 0

if 'ka_score' not in st.session_state:
    st.session_state.ka_score = 0

vineesh = st.sidebar.button('Vineesh', use_container_width = True)

kaleb = st.sidebar.button('Kaleb', use_container_width = True)

if vineesh:
    st.session_state.vin_score += 1
elif kaleb:
    st.session_state.ka_score += 1

st.table({'Person' : ["Vineesh","Kaleb"],"Score":[st.session_state.vin_score,st.session_state.ka_score]})

c1, c2 = st.columns([1, 1])
with c1:
    reset =  st.button("Reset")

with c2:
    end_game = st.button("End Game")

if reset:
    del st.session_state.vin_score
    del st.session_state.ka_score
    st.experimental_rerun()



if end_game:
    pass