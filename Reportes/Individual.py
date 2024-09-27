import os
import streamlit as st
import pandas as pd
from snowflake.snowpark.functions import col

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

# Create Tabs for navigation
tabs = st.tabs(["Listado Comunidad", "Buscar Sesión", "Buscar Curso"])

# Tab 1: Listado Comunidad
with tabs[0]:
    result = session.sql("SELECT NOMBRE, APELLIDO, CORREO, STATUS FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD;")
    df = result.to_pandas()
    st.write("Listado Completo Comunidad:")
    st.dataframe(df)

# Tab 2: Buscar Sesión
with tabs[1]:
    st.write('Buscar información de una sesión:')
    session_result = session.sql("SELECT NOMBRE_SESION FROM LABORATORIO.MONICA_SOBERON.SESION;")
    session_df = session_result.to_pandas()
    session_names = session_df['NOMBRE_SESION'].tolist()

    selected_session = st.selectbox('Selecciona una Sesión:', session_names)
    if selected_session:
        # Create nested tabs
        sub_tabs = st.tabs(["Detalles de Sesión", "Invitados", "Asistencia", "No Asistieron"])

        with sub_tabs[0]:  # Detalles de Sesión
            session_details_result = session.sql(f"SELECT NOMBRE_SESION, FECHA_SESION, LINK_SESION_INFORMATIVA FROM LABORATORIO.MONICA_SOBERON.SESION WHERE NOMBRE_SESION = '{selected_session}';")
            session_details_df = session_details_result.to_pandas()
            id_sesion = session.sql(f"select id_sesion FROM LABORATORIO.MONICA_SOBERON.SESION WHERE NOMBRE_SESION = '{selected_session}';").collect()[0][0]

            st.write("**Detalles de la Sesión:**")
            for index, row in session_details_df.iterrows():
                st.write(f"Nombre de la Sesión: {row['NOMBRE_SESION']}")
                st.write(f"Fecha de la Sesión: {row['FECHA_SESION']}")
                st.write(f"Link Informativa: {row['LINK_SESION_INFORMATIVA']}")

        with sub_tabs[1]:  # Invitados
            invited_df = session.sql(f"""
                SELECT C.NOMBRE, C.APELLIDO, C.CORREO
                FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD AS C
                INNER JOIN LABORATORIO.MONICA_SOBERON.INVITACION_SESION AS I 
                ON C.ID_USUARIO = I.ID_USUARIO
                WHERE I.ID_SESION = {id_sesion};
            """).to_pandas()
            st.write("**Lista de Invitados:**")
            st.dataframe(invited_df)

        with sub_tabs[2]:  # Asistencia
            attended_df = session.sql(f"""
                SELECT C.NOMBRE, C.APELLIDO, C.CORREO
                FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD AS C
                INNER JOIN LABORATORIO.MONICA_SOBERON.ASISTENCIA_SESION AS I 
                ON C.ID_USUARIO = I.ID_USUARIO
                WHERE I.ID_SESION = {id_sesion};
            """).to_pandas()
            st.write("**Lista de Asistentes:**")
            st.dataframe(attended_df)

        with sub_tabs[3]:  # No Asistieron
            no_attended_df = session.sql(f"""
                SELECT C.NOMBRE, C.APELLIDO, C.CORREO
                FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD AS C
                INNER JOIN LABORATORIO.MONICA_SOBERON.INVITACION_SESION AS I 
                ON C.ID_USUARIO = I.ID_USUARIO
                LEFT JOIN LABORATORIO.MONICA_SOBERON.ASISTENCIA_SESION AS A 
                ON I.ID_SESION = A.ID_SESION AND C.ID_USUARIO = A.ID_USUARIO
                WHERE A.ID_USUARIO IS NULL;
            """).to_pandas()
            st.write("**Invitados que No Asistieron:**")
            st.dataframe(no_attended_df)

# Tab 3: Buscar Curso
with tabs[2]:
    st.write('Buscar información de un curso:')
    course_result = session.sql("SELECT NOMBRE_CURSO FROM LABORATORIO.MONICA_SOBERON.CURSO;")
    course_df = course_result.to_pandas()
    course_names = course_df['NOMBRE_CURSO'].tolist()

    selected_course = st.selectbox('Selecciona un Curso:', course_names)
    if selected_course:
        # Create nested tabs
        sub_tabs = st.tabs(["Detalles del Curso", "Invitados", "Registrados", "No Registrados"])

        with sub_tabs[0]:  # Detalles del Curso
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

        with sub_tabs[1]:  # Invitados
            invited_df_course = session.sql(f"""
                SELECT C.NOMBRE, C.APELLIDO, C.CORREO
                FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD AS C
                INNER JOIN LABORATORIO.MONICA_SOBERON.INVITACION_CURSO AS I 
                ON C.ID_USUARIO = I.ID_USUARIO
                WHERE I.ID_CURSO = (SELECT ID_CURSO FROM LABORATORIO.MONICA_SOBERON.CURSO WHERE NOMBRE_CURSO = '{selected_course}');
            """).to_pandas()
            st.write("**Lista de Invitados:**")
            st.dataframe(invited_df_course)

        with sub_tabs[2]:  # Registrados
            registered_df = session.sql(f"""
                SELECT C.NOMBRE, C.APELLIDO, C.CORREO
                FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD AS C
                INNER JOIN LABORATORIO.MONICA_SOBERON.REGISTRADOS_CURSO AS R 
                ON C.ID_USUARIO = R.ID_USUARIO
                WHERE R.ID_CURSO = (SELECT ID_CURSO FROM LABORATORIO.MONICA_SOBERON.CURSO WHERE NOMBRE_CURSO = '{selected_course}');
            """).to_pandas()
            st.write("**Lista de Registrados:**")
            st.dataframe(registered_df)

        with sub_tabs[3]:  # No Registrados
            not_registered_df = session.sql(f"""
                SELECT C.NOMBRE, C.APELLIDO, C.CORREO
                FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD AS C
                INNER JOIN LABORATORIO.MONICA_SOBERON.INVITACION_CURSO AS I 
                ON C.ID_USUARIO = I.ID_USUARIO
                LEFT JOIN LABORATORIO.MONICA_SOBERON.REGISTRADOS_CURSO AS R 
                ON I.ID_CURSO = R.ID_CURSO AND C.ID_USUARIO = R.ID_USUARIO
                WHERE R.ID_USUARIO IS NULL;
            """).to_pandas()
            st.write("**Invitados que No Registraron:**")
            st.dataframe(not_registered_df)
