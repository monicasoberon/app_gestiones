import os
import streamlit as st
import pandas as pd
from snowflake.snowpark.functions import col

cnx = st.connection("snowflake")
session = cnx.session()

if "auth_data" not in st.session_state:
    st.write("Please authenticate to access this page.")
    st.stop()  # Stop the execution of this page
    
st.title("Datos de Invitados y Asistencias de Sesiones")
st.write(
"""Esta pantalla ayuda a registrar datos de las sesiones al igual que los invitados y los participantes.
"""
)

st.title("Reporte Actividad Comunidad")
st.write(
    """Este reporte brinda información sobre la participación en 
    sesiones, inscripción y finalización de cursos de los miembros 
    la comunidad.
    """
)

def toggle_dataframe_visibility(button_text, session_state_key, dataframe, key=None):
    if session_state_key not in st.session_state:
        st.session_state[session_state_key] = False

    if st.button(button_text, key=key):
        st.session_state[session_state_key] = not st.session_state[session_state_key]

    if st.session_state[session_state_key]:
        st.write(dataframe)


# Execute SQL query and get results
result = session.sql("SELECT NOMBRE, APELLIDO, CORREO, STATUS FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD;")

# Convert Snowpark DataFrame to pandas DataFrame
df = result.to_pandas()

toggle_dataframe_visibility('Mostrar/Ocultar Listado Completo Comunidad', 'show_comunidad_df', df, key='comunidad_button')

# Option to search session or course information
st.write('¿Deseas buscar información de una sesión o curso?')
option = st.selectbox('Selecciona una opción:', ['Sesión', 'Curso', ''])

# Fetch session or course information based on the selected option
if option == 'Sesión':
    # Query for session information
    session_result = session.sql("SELECT NOMBRE_SESION FROM LABORATORIO.MONICA_SOBERON.SESION;")
    session_df = session_result.to_pandas()
    session_names = session_df['NOMBRE_SESION'].tolist()

    # Display session select box
    selected_session = st.selectbox('Selecciona una Sesión:', session_names)
    if selected_session:
        # Query for session details based on the selected session
        session_details_result = session.sql(f"SELECT NOMBRE_SESION, FECHA_SESION, LINK_SESION_INFORMATIVA FROM LABORATORIO.MONICA_SOBERON.SESION WHERE NOMBRE_SESION = '{selected_session}';")
        id_sesion = session.sql(f"select id_sesion FROM LABORATORIO.MONICA_SOBERON.SESION WHERE NOMBRE_SESION = '{selected_session}';")
        # Convert to pandas DataFrame
        session_details_df = session_details_result.to_pandas()

        session_id_result = session.sql(f"""
        SELECT ID_SESION
        FROM LABORATORIO.MONICA_SOBERON.SESION
        WHERE NOMBRE_SESION = '{selected_session}';
            """)
        session_id_df = session_id_result.to_pandas()
        id_sesion = session_id_df['ID_SESION'].iloc[0]
        
        # Display the session details as a list
        st.write("**Detalles de la Sesión:**")
        for index, row in session_details_df.iterrows():
            st.write(f" Nombre de la Sesión: {row['NOMBRE_SESION']}")
            st.write(f" Fecha de la Sesión: {row['FECHA_SESION']}")
            st.write(f" Link Informativa: {row['LINK_SESION_INFORMATIVA']}")

        invitados_counts = session.sql(f"""
    SELECT COUNT(*) 
    FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD AS C
    INNER JOIN LABORATORIO.MONICA_SOBERON.INVITACION_SESION AS I 
    ON C.ID_USUARIO = I.ID_USUARIO
    WHERE I.ID_SESION = {id_sesion};
        """)
        invitado_count = invitados_counts.collect()[0][0]
        st.write(f"**Cantidad de Invitados:** {invitado_count}")

        invitadosSesion = session.sql(f"""
    SELECT C.NOMBRE, C.APELLIDO, C.CORREO
    FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD AS C
    INNER JOIN LABORATORIO.MONICA_SOBERON.INVITACION_SESION AS I 
    ON C.ID_USUARIO = I.ID_USUARIO
    WHERE I.ID_SESION = {id_sesion};
        """)
        
        invitados_sesion_df = invitadosSesion.to_pandas()
        toggle_dataframe_visibility('Mostrar/Ocultar Listado Invitados', 'show_invitados_df', invitados_sesion_df, key='invitados_button')

        asistios_counts = session.sql(f"""SELECT COUNT(*) FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD AS C INNER JOIN LABORATORIO.MONICA_SOBERON.ASISTENCIA_SESION AS I ON C.ID_USUARIO = I.ID_USUARIO WHERE I.ID_SESION = {id_sesion};""")
        asistio_count = asistios_counts.collect()[0][0]
        st.write(f" **Cantidad de Usuarios que Asistieron:** {asistio_count}")

        asistioSesion = session.sql("SELECT NOMBRE, APELLIDO, CORREO FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD AS C INNER JOIN LABORATORIO.MONICA_SOBERON.ASISTENCIA_SESION AS I ON C.ID_USUARIO = I.ID_USUARIO INNER JOIN LABORATORIO.MONICA_SOBERON.SESION AS S ON I.ID_SESION = S.ID_SESION;")
        asistio_sesion_df = asistioSesion.to_pandas()
        toggle_dataframe_visibility('Mostrar/Ocultar Usuarios que Asistieron', 'show_asistio_df', asistio_sesion_df, key='asistio_button')

        if invitado_count != 0:
            st.write(f" **Cantidad de Invitados que no Asistieron:** {invitado_count - asistio_count}")
            no_asistieron = session.sql("""
        SELECT C.NOMBRE, C.APELLIDO, C.CORREO 
        FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD AS C
        INNER JOIN LABORATORIO.MONICA_SOBERON.INVITACION_SESION AS I 
        ON C.ID_USUARIO = I.ID_USUARIO 
        LEFT JOIN LABORATORIO.MONICA_SOBERON.ASISTENCIA_SESION AS A 
        ON C.ID_USUARIO = A.ID_USUARIO AND I.ID_SESION = A.ID_SESION
        WHERE A.ID_USUARIO IS NULL;
        """)
            no_asistieron_df = no_asistieron.to_pandas()
            toggle_dataframe_visibility('Mostrar/Ocultar Invitados que No Asistieron', 'show_no_asistieron_df', no_asistieron_df, key='no_asistieron_button')

            data = {'Status': ['Invited', 'Attended'], 'Count': [invitado_count, asistio_count]}
            df = pd.DataFrame(data)

elif option == 'Curso':
    # Query for course information
    course_result = session.sql("SELECT NOMBRE_CURSO FROM LABORATORIO.MONICA_SOBERON.CURSO;")
    course_df = course_result.to_pandas()
    course_names = course_df['NOMBRE_CURSO'].tolist()

    # Display course select box
    selected_course = st.selectbox('Selecciona un Curso:', course_names)
    if selected_course:
        
        course_id_result = session.sql(f"""
        SELECT id_curso
        FROM LABORATORIO.MONICA_SOBERON.curso
        WHERE NOMBRE_curso = '{selected_course}';
            """)
        course_id_df = course_id_result.to_pandas()
        id_curso = course_id_df['ID_CURSO'].iloc[0]
        
        # Query for course details based on the selected course
        course_details_result = session.sql(f"SELECT NOMBRE_CURSO, FECHA_INICIO, FECHA_FIN, PROVEEDOR, CORREO_CONTACTO, REQUIERE_CASO_USO FROM LABORATORIO.MONICA_SOBERON.CURSO WHERE NOMBRE_CURSO = '{selected_course}';")

        # Convert to pandas DataFrame
        course_details_df = course_details_result.to_pandas()

        # Display the course details as a list
        st.write("**Detalles del Curso:**")
        for index, row in course_details_df.iterrows():
            st.write(f" Nombre del Curso: {row['NOMBRE_CURSO']}")
            st.write(f" Fecha de Inicio: {row['FECHA_INICIO']}")
            st.write(f" Fecha de Fin: {row['FECHA_FIN']}")
            st.write(f" Proveedor: {row['PROVEEDOR']}")
            st.write(f" Correo Contacto: {row['CORREO_CONTACTO']}")
            if row['REQUIERE_CASO_USO']:
                caso = 'Si'
            else:
                caso = 'No'
            st.write(f" Requiere Caso de Uso: {caso}")

        invitados_c = session.sql(f"""SELECT COUNT(*) FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD AS C INNER JOIN LABORATORIO.MONICA_SOBERON.INVITACION_CURSO AS I ON C.ID_USUARIO = I.ID_USUARIO where I.ID_CURSO = {id_curso};""")
        invitado_c = invitados_c.collect()[0][0]
        st.write(f"**Cantidad de Invitados:** {invitado_c}")

        invitadosSesionC = session.sql(f"""SELECT NOMBRE, APELLIDO, CORREO FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD AS C INNER JOIN LABORATORIO.MONICA_SOBERON.INVITACION_CURSO AS I ON C.ID_USUARIO = I.ID_USUARIO where I.ID_CURSO = {id_curso};""")
        invitados_sesionC_df = invitadosSesionC.to_pandas()
        toggle_dataframe_visibility('Mostrar/Ocultar Listado Invitados', 'show_invitados_df', invitados_sesionC_df, key='invitados_button')

        registros_c = session.sql(f"""SELECT COUNT(*) FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD AS C INNER JOIN LABORATORIO.MONICA_SOBERON.REGISTRADOS_CURSO AS I ON C.ID_USUARIO = I.ID_USUARIO where I.ID_CURSO = {id_curso};""")
        registro_c = registros_c.collect()[0][0]
        st.write(f"**Cantidad de Usuarios Registrados:** {registro_c}")

        registroSesionC = session.sql(f"""SELECT NOMBRE, APELLIDO, CORREO FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD AS C INNER JOIN LABORATORIO.MONICA_SOBERON.REGISTRADOS_CURSO AS I ON C.ID_USUARIO = I.ID_USUARIO where I.ID_CURSO = {id_curso};""")
        registro_sesionC_df = registroSesionC.to_pandas()
        toggle_dataframe_visibility('Mostrar/Ocultar Usuarios que Asistieron', 'show_asistio_df', registro_sesionC_df, key='asistio_button')

        st.write(f" **Cantidad de Invitados que No se Registraron:** {invitado_c - registro_c}")
    # Obtener la lista de invitados que no se registraron
        no_registrados_c = session.sql("""
SELECT C.NOMBRE, C.APELLIDO, C.CORREO 
FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD AS C
INNER JOIN LABORATORIO.MONICA_SOBERON.INVITACION_CURSO AS I 
ON C.ID_USUARIO = I.ID_USUARIO 
LEFT JOIN LABORATORIO.MONICA_SOBERON.REGISTRADOS_CURSO AS R 
ON C.ID_USUARIO = R.ID_USUARIO AND I.ID_CURSO = R.ID_CURSO
WHERE R.ID_USUARIO IS NULL;
""")
        no_registrados_c_df = no_registrados_c.to_pandas()
        toggle_dataframe_visibility('Mostrar/Ocultar Invitados que no se Registraron', 'show_no_registrados_df', no_registrados_c_df, key='no_registrados_button')

