import os
import streamlit as st
import pandas as pd
from snowflake.snowpark.functions import col
import seaborn as sns
import matplotlib.pyplot as plt

cnx = st.connection("snowflake")
session = cnx.session()

if "auth_data" not in st.session_state:
    st.write("Please authenticate to access this page.")
    st.stop()  # Stop the execution of this page

st.title("Reporte Actividad Comunidad")
st.write(
    """Este reporte brinda información sobre la participación en 
    sesiones, inscripción y finalización de cursos de los miembros 
    la comunidad.
    """
)

# Function to toggle visibility of dataframes
def toggle_dataframe_visibility(button_text, session_state_key, dataframe, key=None):
    if session_state_key not in st.session_state:
        st.session_state[session_state_key] = False

    if st.button(button_text, key=key):
        st.session_state[session_state_key] = not st.session_state[session_state_key]

    if st.session_state[session_state_key]:
        st.write(dataframe)


# Create Tabs for navigation
tabs = st.tabs(["Listado Comunidad", "Buscar Sesión", "Buscar Curso", "Visualizaciones"])

# Tab 1: Listado Comunidad
with tabs[0]:
    result = session.sql("SELECT NOMBRE, APELLIDO, CORREO, STATUS FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD;")
    df = result.to_pandas()
    st.write("Listado Completo Comunidad:")
    toggle_dataframe_visibility('Mostrar/Ocultar Listado Completo Comunidad', 'show_comunidad_df', df)

# Tab 2: Buscar Sesión
with tabs[1]:
    st.write('Buscar información de una sesión:')
    session_result = session.sql("SELECT NOMBRE_SESION FROM LABORATORIO.MONICA_SOBERON.SESION;")
    session_df = session_result.to_pandas()
    session_names = session_df['NOMBRE_SESION'].tolist()

    selected_session = st.selectbox('Selecciona una Sesión:', session_names)
    if selected_session:
        session_details_result = session.sql(f"SELECT NOMBRE_SESION, FECHA_SESION, LINK_SESION_INFORMATIVA FROM LABORATORIO.MONICA_SOBERON.SESION WHERE NOMBRE_SESION = '{selected_session}';")
        session_details_df = session_details_result.to_pandas()
        id_sesion = session.sql(f"select id_sesion FROM LABORATORIO.MONICA_SOBERON.SESION WHERE NOMBRE_SESION = '{selected_session}';").collect()[0][0]

        st.write("**Detalles de la Sesión:**")
        for index, row in session_details_df.iterrows():
            st.write(f"Nombre de la Sesión: {row['NOMBRE_SESION']}")
            st.write(f"Fecha de la Sesión: {row['FECHA_SESION']}")
            st.write(f"Link Informativa: {row['LINK_SESION_INFORMATIVA']}")

        # Fetch and display invited and attendance details
        invited_count = session.sql(f"""
            SELECT COUNT(*) 
            FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD AS C
            INNER JOIN LABORATORIO.MONICA_SOBERON.INVITACION_SESION AS I 
            ON C.ID_USUARIO = I.ID_USUARIO
            WHERE I.ID_SESION = {id_sesion};
        """).collect()[0][0]
        st.write(f"**Cantidad de Invitados:** {invited_count}")

        invited_df = session.sql(f"""
            SELECT C.NOMBRE, C.APELLIDO, C.CORREO
            FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD AS C
            INNER JOIN LABORATORIO.MONICA_SOBERON.INVITACION_SESION AS I 
            ON C.ID_USUARIO = I.ID_USUARIO
            WHERE I.ID_SESION = {id_sesion};
        """).to_pandas()
        toggle_dataframe_visibility('Mostrar/Ocultar Listado de Invitados', 'show_invited_df', invited_df)

        attended_count = session.sql(f"""
            SELECT COUNT(*)
            FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD AS C
            INNER JOIN LABORATORIO.MONICA_SOBERON.ASISTENCIA_SESION AS I 
            ON C.ID_USUARIO = I.ID_USUARIO
            WHERE I.ID_SESION = {id_sesion};
        """).collect()[0][0]
        st.write(f"**Cantidad de Usuarios que Asistieron:** {attended_count}")

        attended_df = session.sql(f"""
            SELECT C.NOMBRE, C.APELLIDO, C.CORREO
            FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD AS C
            INNER JOIN LABORATORIO.MONICA_SOBERON.ASISTENCIA_SESION AS I 
            ON C.ID_USUARIO = I.ID_USUARIO
            WHERE I.ID_SESION = {id_sesion};
        """).to_pandas()
        toggle_dataframe_visibility('Mostrar/Ocultar Usuarios que Asistieron', 'show_attended_df', attended_df)

        if invited_count != attended_count:
            no_attended_df = session.sql(f"""
                SELECT C.NOMBRE, C.APELLIDO, C.CORREO
                FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD AS C
                INNER JOIN LABORATORIO.MONICA_SOBERON.INVITACION_SESION AS I 
                ON C.ID_USUARIO = I.ID_USUARIO
                LEFT JOIN LABORATORIO.MONICA_SOBERON.ASISTENCIA_SESION AS A 
                ON I.ID_SESION = A.ID_SESION AND C.ID_USUARIO = A.ID_USUARIO
                WHERE A.ID_USUARIO IS NULL;
            """).to_pandas()
            st.write(f"**Cantidad de Invitados que No Asistieron:** {invited_count - attended_count}")
            toggle_dataframe_visibility('Mostrar/Ocultar Invitados que No Asistieron', 'show_no_attended_df', no_attended_df)

# Tab 3: Buscar Curso
with tabs[2]:
    st.write('Buscar información de un curso:')
    course_result = session.sql("SELECT NOMBRE_CURSO FROM LABORATORIO.MONICA_SOBERON.CURSO;")
    course_df = course_result.to_pandas()
    course_names = course_df['NOMBRE_CURSO'].tolist()

    selected_course = st.selectbox('Selecciona un Curso:', course_names)
    if selected_course:
        course_details_result = session.sql(f"""
            SELECT NOMBRE_CURSO, FECHA_INICIO, FECHA_FIN, PROVEEDOR, CORREO_CONTACTO, REQUIERE_CASO_USO 
            FROM LABORATORIO.MONICA_SOBERON.CURSO 
            WHERE NOMBRE_CURSO = '{selected_course}';
        """).to_pandas()

        st.write("**Detalles del Curso:**")
        for index, row in course_details_result.iterrows():
            st.write(f"Nombre del Curso: {row['NOMBRE_CURSO']}")
            st.write(f"Fecha de Inicio: {row['FECHA_INICIO']}")
            st.write(f"Fecha de Fin: {row['FECHA_FIN']}")
            st.write(f"Proveedor: {row['PROVEEDOR']}")
            st.write(f"Correo Contacto: {row['CORREO_CONTACTO']}")
            st.write(f"Requiere Caso de Uso: {'Si' if row['REQUIERE_CASO_USO'] else 'No'}")

        # Fetch and display course invited, registered, and non-registered details
        invited_count_course = session.sql(f"""
            SELECT COUNT(*)
            FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD AS C
            INNER JOIN LABORATORIO.MONICA_SOBERON.INVITACION_CURSO AS I 
            ON C.ID_USUARIO = I.ID_USUARIO
            WHERE I.ID_CURSO = (SELECT ID_CURSO FROM LABORATORIO.MONICA_SOBERON.CURSO WHERE NOMBRE_CURSO = '{selected_course}');
        """).collect()[0][0]
        st.write(f"**Cantidad de Invitados:** {invited_count_course}")

        invited_df_course = session.sql(f"""
            SELECT C.NOMBRE, C.APELLIDO, C.CORREO
            FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD AS C
            INNER JOIN LABORATORIO.MONICA_SOBERON.INVITACION_CURSO AS I 
            ON C.ID_USUARIO = I.ID_USUARIO
            WHERE I.ID_CURSO = (SELECT ID_CURSO FROM LABORATORIO.MONICA_SOBERON.CURSO WHERE NOMBRE_CURSO = '{selected_course}');
        """).to_pandas()
        toggle_dataframe_visibility('Mostrar/Ocultar Listado Invitados', 'show_invited_course_df', invited_df_course)

        registered_count = session.sql(f"""
            SELECT COUNT(*)
            FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD AS C
            INNER JOIN LABORATORIO.MONICA_SOBERON.REGISTRADOS_CURSO AS R 
            ON C.ID_USUARIO = R.ID_USUARIO
            WHERE R.ID_CURSO = (SELECT ID_CURSO FROM LABORATORIO.MONICA_SOBERON.CURSO WHERE NOMBRE_CURSO = '{selected_course}');
        """).collect()[0][0]
        st.write(f"**Cantidad de Usuarios Registrados:** {registered_count}")

        registered_df = session.sql(f"""
            SELECT C.NOMBRE, C.APELLIDO, C.CORREO
            FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD AS C
            INNER JOIN LABORATORIO.MONICA_SOBERON.REGISTRADOS_CURSO AS R 
            ON C.ID_USUARIO = R.ID_USUARIO
            WHERE R.ID_CURSO = (SELECT ID_CURSO FROM LABORATORIO.MONICA_SOBERON.CURSO WHERE NOMBRE_CURSO = '{selected_course}');
        """).to_pandas()
        toggle_dataframe_visibility('Mostrar/Ocultar Usuarios Registrados', 'show_registered_df', registered_df)

        if invited_count_course != registered_count:
            not_registered_df = session.sql(f"""
                SELECT C.NOMBRE, C.APELLIDO, C.CORREO
                FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD AS C
                INNER JOIN LABORATORIO.MONICA_SOBERON.INVITACION_CURSO AS I 
                ON C.ID_USUARIO = I.ID_USUARIO
                LEFT JOIN LABORATORIO.MONICA_SOBERON.REGISTRADOS_CURSO AS R 
                ON I.ID_CURSO = R.ID_CURSO AND C.ID_USUARIO = R.ID_USUARIO
                WHERE R.ID_USUARIO IS NULL;
            """).to_pandas()
            st.write(f"**Cantidad de Invitados que No se Registraron:** {invited_count_course - registered_count}")
            toggle_dataframe_visibility('Mostrar/Ocultar Invitados que No Registraron', 'show_no_registered_df', not_registered_df)

# Tab 4: Visualizaciones
with tabs[3]:
    st.write('Visualizaciones:')
    st.write('Cantidad de Sesiones por Mes:')
    sessions_per_month = session.sql("""
        SELECT MONTH(FECHA_SESION) AS MES, COUNT(*) AS CANTIDAD
        FROM LABORATORIO.MONICA_SOBERON.SESION
        GROUP BY MES
        ORDER BY MES;
    """).to_pandas()
    fig, ax = plt.subplots(figsize=(1, 1))
    sns.barplot(data=sessions_per_month, x='MES', y='CANTIDAD', ax=ax)
    ax.set_xlabel('Mes', fontsize=6)  # Change the font size of the x-axis label
    ax.set_ylabel('Cantidad', fontsize=6)  # Change the font size of the y-axis label
    ax.set_title('Cantidad de Sesiones por Mes', fontsize=8)  # Change the font size of the title

    st.pyplot(fig)

    st.write('Cantidad de Invitados vs. Asistentes por Sesión:')
    invited_vs_attended = session.sql("""
        SELECT S.NOMBRE_SESION, COUNT(I.ID_USUARIO) AS INVITADOS, COUNT(A.ID_USUARIO) AS ASISTENTES
        FROM LABORATORIO.MONICA_SOBERON.SESION AS S
        LEFT JOIN LABORATORIO.MONICA_SOBERON.INVITACION_SESION AS I
        ON S.ID_SESION = I.ID_SESION
        LEFT JOIN LABORATORIO.MONICA_SOBERON.ASISTENCIA_SESION AS A
        ON S.ID_SESION = A.ID_SESION
        GROUP BY S.NOMBRE_SESION
        ORDER BY INVITADOS DESC;
    """).to_pandas()
    print(invited_vs_attended)
    #sns.histplot(data=invited_vs_attended, x='NOMBRE_SESION', y='Cantidad', hue='Tipo', multiple='stack')
    #st.pyplot()
    
    st.write('Cantidad de Usuarios Registrados por Curso:')
    registered_per_course = session.sql("""
        SELECT C.NOMBRE_CURSO, COUNT(R.ID_USUARIO) AS CANTIDAD
        FROM LABORATORIO.MONICA_SOBERON.CURSO AS C
        INNER JOIN LABORATORIO.MONICA_SOBERON.REGISTRADOS_CURSO AS R
        ON C.ID_CURSO = R.ID_CURSO
        GROUP BY C.NOMBRE_CURSO
        ORDER BY CANTIDAD DESC;
    """).to_pandas()
    fig2, ax = plt.subplots(figsize=(2, 3))
    sns.barplot(data=registered_per_course, x='CANTIDAD', y='NOMBRE_CURSO', ax=ax)
    st.pyplot(fig2)

    st.write('Cantidad de Invitados vs. Registrados por Curso:')
    invited_vs_registered = session.sql("""
        SELECT C.NOMBRE_CURSO, COUNT(I.ID_USUARIO) AS INVITADOS, COUNT(R.ID_USUARIO) AS REGISTRADOS
        FROM LABORATORIO.MONICA_SOBERON.CURSO AS C
        LEFT JOIN LABORATORIO.MONICA_SOBERON.INVITACION_CURSO AS I
        ON C.ID_CURSO = I.ID_CURSO
        LEFT JOIN LABORATORIO.MONICA_SOBERON.REGISTRADOS_CURSO AS R
        ON C.ID_CURSO = R.ID_CURSO
        GROUP BY C.NOMBRE_CURSO
        ORDER BY INVITADOS DESC;
    """).to_pandas()

    sns.barplot(data=invited_vs_registered, x='INVITADOS', y='NOMBRE_CURSO', color='blue', label='Invitados')
    sns.barplot(data=invited_vs_registered, x='REGISTRADOS', y='NOMBRE_CURSO', color='green', label='Registrados')



    