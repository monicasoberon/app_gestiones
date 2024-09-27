import os
import streamlit as st
import pandas as pd
from snowflake.snowpark.functions import col
from streamlit_msal import Msal

auth_data = None

with st.sidebar:
    try:
        auth_data = Msal.initialize_ui(
            client_id=st.secrets["client_id"],  # Access client_id from st.secrets
            authority=st.secrets["authority"],
            scopes = ["User.Read", "User.ReadBasic.All"],
            connecting_label="Connecting...",
            disconnected_label="Disconnected",
            sign_in_label="Sign in",
            sign_out_label="Sign out"
        )
    except Exception as e:
        st.write(f"Error during MSAL initialization: {e}")

# Check if auth_data is returned
if auth_data:
    st.session_state["auth_data"] = auth_data
else:
    # If not authenticated yet, stop execution or show login required message
    if "auth_data" not in st.session_state:
        st.write("Please authenticate to access the application.")
        st.stop()

# If auth_data is available, proceed
if "auth_data" in st.session_state:
    account = st.session_state["auth_data"]["account"]
    name = account["name"]

    # Greet the authenticated user
    st.write(f"Bienvenid@, {name}!")
    st.write("Ya estas autenticado.")
    st.write("Puedes navegar las páginas de la aplicación con la barra lateral.")
else:
    st.write("Error: Could not retrieve user account data.")

cnx = st.connection("snowflake")
session = cnx.session()