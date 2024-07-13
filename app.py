import pandas as pd
import numpy as np
import streamlit as st
from src.dbopen import DBOpen

db = DBOpen(True, 'RECENT', './src/config.yaml')

# OHLCV data
df_maria_db = db.select_from_table()

# USERNAME & PASSWORD
df_user_info = db.select_user_info()
USER_DATA = dict(zip(df_user_info['USERNAME'], df_user_info['PASSWORD']))

def main():
    # Initialize session state
    if 'logged_in' not in st.session_state:
        st.session_state.logged_in = False
    if 'username' not in st.session_state:
        st.session_state.username = ''

    # Login Form
    if not st.session_state.logged_in:
        st.title("Login Page")
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        if st.button("login"):
            if db.login(username, password, USER_DATA):
                st.session_state.logged_in = True
                st.session_state.username = username
                # st.success("Login Successful!")
                st.rerun() # st.session_state > Immediate application after update
            else:
                st.error("Wrong username or password!.")
    
    # Content to display after login
    else:
        st.title(f"Maria DB ({db.user}@{db.hostname})")
        st.write(f"Welcome, {st.session_state.username}!")
        st.dataframe(df_maria_db)

        # Show data
        if st.checkbox("Select from pyubit"):
            new_data = {
                "DAYS": st.text_input("DAYS"),
            }
            if new_data['DAYS'] != '':
                df_ohlcv = db.recent_ohlcv(int(new_data['DAYS']))
                st.dataframe(df_ohlcv)
                if st.button('Upload to mariadb'):
                    db.insert_into_table(df_ohlcv)
                    print('inserted!!')
                    st.rerun() # st.session_state > Immediate application after update

            else:
                pass

        if st.button("Log out"):
            st.session_state.logged_in = False
            st.session_state.username = ''
            st.rerun() # st.session_state > Immediate application after update



if __name__ == "__main__":
    main()


