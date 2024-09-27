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

        # Header and Subheader
        st.title("Gestión de Cursos")
        st.subheader("Bienvenido a la Plataforma de Gestión")

        # Greet the user
        st.markdown(f"**Bienvenid@, {name}!**")
        st.markdown("Ya estás autenticado. Navega las páginas de la aplicación usando los botones en la barra lateral.")

        # Page navigation with buttons in the sidebar
        st.sidebar.markdown("### Navegación:")
        pages = {
            "Gestión Clases": "pages/gestionClases.py",
            "Gestión Cursos": "pages/gestionCursos.py",
            "Gestión Sesión": "pages/gestionSesion.py",
            "Gestión Usuarios": "pages/gestionUsuarios.py",
        }

        # Create a button for each page
        for page_name, page_file in pages.items():
            if st.sidebar.button(page_name):
                # Clear the output before executing a new page
                st.experimental_rerun()
                exec(open(page_file).read())

# Run the main function
if __name__ == "__main__":
    main()

cnx = st.connection("snowflake")
session = cnx.session()
