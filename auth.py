import os
import streamlit as st
from snowflake.snowpark.functions import col
import pandas as pd
from streamlit_msal import Msal

# Authentication
def authenticate():
    auth_data = None

    with st.sidebar:
        try:
            auth_data = Msal.initialize_ui(
                client_id=st.secrets["client_id"],
                authority=st.secrets["authority"],
                scopes=["User.Read", "User.ReadBasic.All"],
                connecting_label="Connecting...",
                disconnected_label="Disconnected",
                sign_in_label="Sign in",
                sign_out_label="Sign out"
            )
        except Exception as e:
            st.write(f"Error during MSAL initialization: {e}")

    if auth_data:
        st.session_state["auth_data"] = auth_data
    else:
        if "auth_data" not in st.session_state:
            st.write("Please authenticate to access the application.")
            st.stop()

    return st.session_state.get("auth_data")

# Main function
def main():
    auth_data = authenticate()
    if auth_data:
        account = auth_data["account"]
        name = account["name"]

        # Greet the user
        st.write(f"Bienvenid@, {name}!")
        st.write("Ya estas autenticado.")
        
        # Page navigation
        pages = {
            "Gestión Clases": "pages/gestionClases.py",
            "Gestión Cursos": "pages/gestionCursos.py",
            "Gestión Sesión": "pages/gestionSesion.py",
            "Gestión Usuarios": "pages/gestionUsuarios.py",
        }

        # Sidebar for navigation
        selection = st.sidebar.selectbox("Ir a:", list(pages.keys()))
        page = pages[selection]

        # Import and run the selected page
        exec(open(page).read())

# Run the main function
if __name__ == "__main__":
    main()


cnx = st.connection("snowflake")
session = cnx.session()