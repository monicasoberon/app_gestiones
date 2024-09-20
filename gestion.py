import streamlit as st
import pandas as pd
from snowflake.snowpark.functions import col
from streamlit_msal import Msal


client_id="674d8292-6dc4-4f8f-a4d0-575f1e0837c8"
authority="https://login.microsoftonline.com/876969de-3b40-4648-872a-0ebecb3489e6"
redirect_uri="https://appgestiones-monicasoberon.streamlit.app/"


with st.sidebar:
    auth_data = Msal.initialize_ui(
        client_id=client_id,
        authority=authority,
        scopes=["User.Read"], 
        connecting_label="Connecting",
        disconnected_label="Disconnected",
        sign_in_label="Sign in",
        sign_out_label="Sign out"
    )

if auth_data:
    st.session_state["auth_data"] = auth_data
else:
    if "auth_data" not in st.session_state:
        st.write("Please authenticate to access the application.")
        st.stop()

# Retrieve user account info from session state
account = st.session_state["auth_data"]["account"]
name = account["name"]

# Greet the authenticated user
st.write(f"Hello {name}!")
st.write("Protected content available.")

cnx = st.connection("snowflake")
session = cnx.session()

tabsm = st.tabs(["Gestion Sesiones", "Gestion Cursos", "Gestion Usuarios"])
# Set up the Streamlit app
with tabsm[0]:
    st.title("Datos de Invitados y Asistencias de Sesiones")
    st.write(
    """Esta pantalla ayuda a registrar datos de las sesiones al igual que los invitados y los participantes.
    """
)


# Create tabs
    tabs = st.tabs(["Crear Sesión", "Lista de Invitados", "Lista de Asistentes"])

    with tabs[0]:
        st.header("Crear Nueva Sesión")
        with st.form(key='new_session_form'):
            session_name = st.text_input("Nombre de la Sesión")
            session_date = st.date_input("Fecha de la Sesión")
            link_session_info = st.text_input("Enlace de Información de la Sesión (opcional)")
        
            submit_button = st.form_submit_button(label='Crear Sesión')
        
            if submit_button:
                if session_name and session_date:
                # Insert new session into the database
                    query = f"""
                INSERT INTO SESION (NOMBRE_SESION, FECHA_SESION, LINK_SESION_INFORMATIVA)
                VALUES ('{session_name}', '{session_date}', '{link_session_info}');
                    """
                    session.sql(query).collect()
                    st.success(f"Sesión '{session_name}' creada con éxito.")
                else:
                    st.error("Por favor, ingrese el nombre y la fecha de la sesión.")

    with tabs[1]:
        st.header("Lista de Invitados")

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
                    
            email_input = st.text_area(
                "Pega la lista de correos electrónicos aquí (uno por línea):",
                height=300
            )       

            if st.button("Procesar Correos"):
            # Process the input emails
                assistant_email_list = [email.replace(chr(10), '').replace(chr(13), '').strip().lower() for email in assistant_email_input.split('\n') if email.strip()]
            # Remove duplicates
                assistant_email_list = list(set(assistant_email_list))
                
                df_assistants = pd.DataFrame(assistant_email_list, columns=['Correo'])

                # Display the processed emails
                st.write("Correos electrónicos de asistentes procesados:")
                st.dataframe(df_assistants)
                
            # Insert into the database
                insert_query = f"""
                INSERT INTO invitados_sesion (id_sesion, id_usuario)
                SELECT {id_sesion}, c.id_usuario
                FROM comunidad c
                WHERE c.correo IN ({', '.join(f"'{email}'" for email in assistant_email_list)});
                """

                # Execute the query
                session.sql(insert_query).collect()
                st.success("Correos electrónicos procesados y listos para ser subidos a la base de datos.")

    with tabs[2]:
        st.header("Lista de Usuarios que Asistieron")
        
        # Query for session information
        session_result = session.sql("SELECT NOMBRE_SESION FROM LABORATORIO.MONICA_SOBERON.SESION;")
        session_df = session_result.to_pandas()
        session_names = session_df['NOMBRE_SESION'].tolist()

        # Display session select box
        selected_session = st.selectbox('Selecciona una Sesión: ', session_names)
        if selected_session:
            # Query for session details based on the selected session
            session_details_result = session.sql(f"SELECT NOMBRE_SESION, FECHA_SESION, LINK_SESION_INFORMATIVA FROM LABORATORIO.MONICA_SOBERON.SESION WHERE NOMBRE_SESION = '{selected_session}';")
            session_id_result = session.sql(f"SELECT ID_SESION FROM LABORATORIO.MONICA_SOBERON.SESION WHERE NOMBRE_SESION = '{selected_session}';")
            
            session_details_df = session_details_result.to_pandas()
            session_id_df = session_id_result.to_pandas()
            id_sesion = session_id_df['ID_SESION'].iloc[0]
                
            # Display the session details as a list
            st.write("**Detalles de la Sesión:**")
            for index, row in session_details_df.iterrows():
                st.write(f" Nombre de la Sesión: {row['NOMBRE_SESION']}")
                st.write(f" Fecha de la Sesión: {row['FECHA_SESION']}")
                
            # Assistant email input
            assistant_email_input = st.text_area(
                "Pega la lista de correos electrónicos de los usuarios que asistieron aquí (uno por línea):",
                height=300
            )

            if st.button("Procesar Correos Asistentes"):
                # Process the input emails
                assistant_email_list = [email.replace(chr(10), '').replace(chr(13), '').strip().lower() for email in assistant_email_input.split('\n') if email.strip()]

                # Remove duplicates
                assistant_email_list = list(set(assistant_email_list))
                
                df_assistants = pd.DataFrame(assistant_email_list, columns=['Correo'])

                # Display the processed emails
                st.write("Correos electrónicos de asistentes procesados:")
                st.dataframe(df_assistants)
                
            # Insert into the database
                insert_query = f"""
                INSERT INTO asistencia_sesion (id_sesion, id_usuario)
                SELECT {id_sesion}, c.id_usuario
                FROM comunidad c
                WHERE c.correo IN ({', '.join(f"'{email}'" for email in assistant_email_list)});
                """

                # Execute the query
                session.sql(insert_query).collect()
                st.success("Asistencias registradas con éxito.")
        
with tabsm[1]:
    # Set up the Streamlit app
    st.title("Gestión de Cursos")
    st.write("Esta aplicación te ayuda a gestionar cursos, invitados y sesiones.")

    # Create tabs
    tabs = st.tabs(["Crear Curso", "Editar Curso", "Registrar Instructor", "Lista de Invitados", "Lista de Registrados"])

    with tabs[0]:
        st.header("Crear Nuevo Curso")
        with st.form(key='new_course_form'):
            course_name = st.text_input("Nombre del Curso")
            course_start_date = st.date_input("Fecha de Inicio")
            course_end_date = st.date_input("Fecha de Fin")
            course_provider = st.text_input("Proveedor")
            requires_case = st.checkbox("¿Requiere Caso de Uso?")
            course_contact_email = st.text_input("Correo de Contacto")

            
            # Select multiple sessions for the course
            session_result = session.sql("SELECT ID_SESION, NOMBRE_SESION FROM LABORATORIO.MONICA_SOBERON.SESION;")
            session_df = session_result.to_pandas()
            session_names = session_df['NOMBRE_SESION'].tolist()
            selected_sessions = st.multiselect('Selecciona las Sesiones:', session_names)
            
            # Instructor dropdown
            instructor_result = session.sql("SELECT ID_INSTRUCTOR, NOMBRE_INSTRUCTOR, APELLIDO_INSTRUCTOR FROM LABORATORIO.MONICA_SOBERON.INSTRUCTOR;")
            instructor_df = instructor_result.to_pandas()
            instructor_names = [f"{row['NOMBRE_INSTRUCTOR']} {row['APELLIDO_INSTRUCTOR']}" for index, row in instructor_df.iterrows()]
            selected_instructor = st.selectbox("Selecciona el Instructor del Curso:", instructor_names)
            
            submit_button = st.form_submit_button(label='Crear Curso')
            
            if submit_button:
                if course_name and course_start_date and course_end_date:
                    # Insert new course into the CURSO table
                    insert_course_query = f"""
                    INSERT INTO LABORATORIO.MONICA_SOBERON.CURSO (NOMBRE_CURSO, FECHA_INICIO, FECHA_FIN, PROVEEDOR, REQUIERE_CASO_USO, CORREO_CONTACTO)
                    VALUES ('{course_name}', '{course_start_date}', '{course_end_date}', '{course_provider}', {requires_case}, '{course_contact_email}');
                    """
                    session.sql(insert_course_query).collect()

                    # Get the ID of the newly inserted course
                    course_id_result = session.sql(f"SELECT ID_CURSO FROM LABORATORIO.MONICA_SOBERON.CURSO WHERE NOMBRE_CURSO = '{course_name}';")
                    course_id_df = course_id_result.to_pandas()
                    course_id = course_id_df['ID_CURSO'].iloc[0]
                    
                    # Insert selected sessions into the TIENE_SESION table
                    for session_name in selected_sessions:
                        session_id_result = session.sql(f"SELECT ID_SESION FROM LABORATORIO.MONICA_SOBERON.SESION WHERE NOMBRE_SESION = '{session_name}';")
                        session_id_df = session_id_result.to_pandas()
                        session_id = session_id_df['ID_SESION'].iloc[0]  
                        insert_session_query = f"""
                        INSERT INTO LABORATORIO.MONICA_SOBERON.TIENE_SESION (ID_CURSO, ID_SESION)
                        VALUES ({course_id}, {session_id});
                        """
                        session.sql(insert_session_query).collect()

                    # Insert the instructor-course relationship into the IMPARTE table
                    instructor_id_result = session.sql(f"""
                    SELECT ID_INSTRUCTOR FROM LABORATORIO.MONICA_SOBERON.INSTRUCTOR
                    WHERE CONCAT(NOMBRE_INSTRUCTOR, ' ', APELLIDO_INSTRUCTOR) = '{selected_instructor}';
                    """)
                    instructor_id_df = instructor_id_result.to_pandas()
                    instructor_id = instructor_id_df['ID_INSTRUCTOR'].iloc[0]
                    
                    insert_instructor_query = f"""
                    INSERT INTO LABORATORIO.MONICA_SOBERON.IMPARTE (ID_INSTRUCTOR, ID_CURSO)
                    VALUES ({instructor_id}, {course_id});
                    """
                    session.sql(insert_instructor_query).collect()

                    st.success(f"Curso '{course_name}' creado con éxito y asociado a las sesiones seleccionadas.")
                else:
                    st.error("Por favor, completa toda la información del curso.")

    # Sección para editar un curso existente
    with tabs[1]:
        st.header("Editar Curso Existente")
        
        # Get list of existing courses for selection
        course_result = session.sql("SELECT ID_CURSO, NOMBRE_CURSO FROM LABORATORIO.MONICA_SOBERON.CURSO;")
        course_df = course_result.to_pandas()
        course_names = course_df['NOMBRE_CURSO'].tolist()
        selected_course_name = st.selectbox("Selecciona el Curso a Editar:", course_names)
        
        if selected_course_name:
            # Get the details of the selected course
            course_id_result = session.sql(f"SELECT * FROM LABORATORIO.MONICA_SOBERON.CURSO WHERE NOMBRE_CURSO = '{selected_course_name}';")
            course_details_df = course_id_result.to_pandas()
            course_details = course_details_df.iloc[0]
            
            st.write("**Actualización de Datos del Curso:**")
            with st.form(key='edit_course_form'):
                new_course_name = st.text_input("Nombre del Curso", value=course_details['NOMBRE_CURSO'])
                new_course_start_date = st.date_input("Fecha de Inicio", value=course_details['FECHA_INICIO'])
                new_course_end_date = st.date_input("Fecha de Fin", value=course_details['FECHA_FIN'])
                new_course_provider = st.text_input("Proveedor", value=course_details['PROVEEDOR'])
                new_requires_case = st.checkbox("¿Requiere Caso de Uso?", value=course_details['REQUIERE_CASO_USO'])
                new_course_contact_email = st.text_input("Correo de Contacto", value=course_details['CORREO_CONTACTO'])
                
                # Select multiple sessions for the course
                session_result = session.sql("SELECT ID_SESION, NOMBRE_SESION FROM LABORATORIO.MONICA_SOBERON.SESION;")
                session_df = session_result.to_pandas()
                session_names = session_df['NOMBRE_SESION'].tolist()
                
                # Fetch current sessions for the selected course
                course_sessions_result = session.sql(f"""
                SELECT s.NOMBRE_SESION
                FROM LABORATORIO.MONICA_SOBERON.TIENE_SESION ts
                JOIN LABORATORIO.MONICA_SOBERON.SESION s ON ts.ID_SESION = s.ID_SESION
                WHERE ts.ID_CURSO = {course_details['ID_CURSO']};
                """)
                course_sessions_df = course_sessions_result.to_pandas()
                current_sessions = course_sessions_df['NOMBRE_SESION'].tolist()
                
                selected_sessions = st.multiselect('Selecciona las Sesiones:', session_names, default=current_sessions)
                
                # Instructor dropdown
                instructor_result = session.sql("SELECT ID_INSTRUCTOR, NOMBRE_INSTRUCTOR, APELLIDO_INSTRUCTOR FROM LABORATORIO.MONICA_SOBERON.INSTRUCTOR;")
                instructor_df = instructor_result.to_pandas()
                instructor_names = [f"{row['NOMBRE_INSTRUCTOR']} {row['APELLIDO_INSTRUCTOR']}" for index, row in instructor_df.iterrows()]
                selected_instructor = st.selectbox("Selecciona el Instructor del Curso:", instructor_names, index=instructor_names.index(f"{course_details['INSTRUCTOR_NOMBRE']} {course_details['INSTRUCTOR_APELLIDO']}") if 'INSTRUCTOR_NOMBRE' in course_details and 'INSTRUCTOR_APELLIDO' in course_details else 0)
                
                update_button = st.form_submit_button(label='Actualizar Curso')
                
                if update_button:
                    if new_course_name and new_course_start_date and new_course_end_date:
                        # Update course details
                        update_course_query = f"""
                        UPDATE LABORATORIO.MONICA_SOBERON.CURSO
                        SET NOMBRE_CURSO = '{new_course_name}',
                            FECHA_INICIO = '{new_course_start_date}',
                            FECHA_FIN = '{new_course_end_date}',
                            PROVEEDOR = '{new_course_provider}',
                            REQUIERE_CASO_USO = {new_requires_case},
                            CORREO_CONTACTO = '{new_course_contact_email}'
                        WHERE ID_CURSO = {course_details['ID_CURSO']};
                        """
                        session.sql(update_course_query).collect()

                        # Update the TIENE_SESION table
                        session_id_result = session.sql(f"""
                        SELECT ID_SESION FROM LABORATORIO.MONICA_SOBERON.SESION WHERE NOMBRE_SESION IN ({', '.join(f"'{s}'" for s in selected_sessions)});""")
                        session_id_df = session_id_result.to_pandas()
                        session_ids = session_id_df['ID_SESION'].tolist()
                        
                        # Remove existing session links
                        delete_sessions_query = f"DELETE FROM LABORATORIO.MONICA_SOBERON.TIENE_SESION WHERE ID_CURSO = {course_details['ID_CURSO']};"
                        session.sql(delete_sessions_query).collect()
                        
                        # Insert new session links
                        for session_id in session_ids:
                            insert_session_query = f"""
                            INSERT INTO LABORATORIO.MONICA_SOBERON.TIENE_SESION (ID_CURSO, ID_SESION)
                            VALUES ({course_details['ID_CURSO']}, {session_id});
                            """
                            session.sql(insert_session_query).collect()

                        # Update instructor
                        instructor_id_result = session.sql(f"""
                        SELECT ID_INSTRUCTOR FROM LABORATORIO.MONICA_SOBERON.INSTRUCTOR
                        WHERE CONCAT(NOMBRE_INSTRUCTOR, ' ', APELLIDO_INSTRUCTOR) = '{selected_instructor}';
                        """)
                        instructor_id_df = instructor_id_result.to_pandas()
                        instructor_id = instructor_id_df['ID_INSTRUCTOR'].iloc[0]
                        
                        update_instructor_query = f"""
                        UPDATE LABORATORIO.MONICA_SOBERON.IMPARTE
                        SET ID_INSTRUCTOR = {instructor_id}
                        WHERE ID_CURSO = {course_details['ID_CURSO']};
                        """
                        session.sql(update_instructor_query).collect()

                        st.success(f"Curso '{new_course_name}' actualizado con éxito.")
                    else:
                        st.error("Por favor, completa toda la información del curso.")
        else:
            st.info("Selecciona un curso para editar.")

            
    with tabs[2]:
        st.header("Crear Nuevo Instructor")
        
        with st.form(key='new_instructor_form'):
            first_name = st.text_input("Nombre del Instructor")
            last_name = st.text_input("Apellido del Instructor")
            instructor_email = st.text_input("Correo Electrónico del Instructor")
            
            submit_instructor_button = st.form_submit_button(label='Crear Instructor')
            
            if submit_instructor_button:
                if first_name and last_name and instructor_email:
                    # Insert new instructor into the database
                    query = f"""
                    INSERT INTO LABORATORIO.MONICA_SOBERON.INSTRUCTOR (NOMBRE_INSTRUCTOR, APELLIDO_INSTRUCTOR, CORREO_INSTRUCTOR)
                    VALUES ('{first_name}', '{last_name}', '{instructor_email}');
                    """
                    session.sql(query).collect()
                    st.success(f"Instructor '{first_name} {last_name}' creado con éxito.")
                else:
                    st.error("Por favor, ingrese el nombre, apellido y correo del instructor.")
        
    with tabs[3]:
        st.header("Lista de Invitados")

        # Query for course information
        course_result = session.sql("SELECT NOMBRE_CURSO FROM LABORATORIO.MONICA_SOBERON.CURSO;")
        course_df = course_result.to_pandas()
        course_names = course_df['NOMBRE_CURSO'].tolist()

        # Display course select box
        selected_course = st.selectbox('Selecciona un Curso:', course_names)
        if selected_course:
            # Query for course details based on the selected course
            course_details_result = session.sql(f"""
                SELECT NOMBRE_CURSO, FECHA_INICIO, FECHA_FIN, PROVEEDOR 
                FROM LABORATORIO.MONICA_SOBERON.CURSO 
                WHERE NOMBRE_CURSO = '{selected_course}';
            """)
            id_curso_result = session.sql(f"""
                SELECT ID_CURSO 
                FROM LABORATORIO.MONICA_SOBERON.CURSO 
                WHERE NOMBRE_CURSO = '{selected_course}';
            """)
            
            course_details_df = course_details_result.to_pandas()
            id_curso_df = id_curso_result.to_pandas()
            id_curso = id_curso_df['ID_CURSO'].iloc[0]
            
            # Display the course details
            st.write("**Detalles del Curso:**")
            for index, row in course_details_df.iterrows():
                st.write(f"Nombre del Curso: {row['NOMBRE_CURSO']}")
                st.write(f"Fecha de Inicio: {row['FECHA_INICIO']}")
                st.write(f"Fecha de Fin: {row['FECHA_FIN']}")
                st.write(f"Proveedor: {row['PROVEEDOR']}")

            # Query for registered users for the selected course
            invitados_result = session.sql(f"""
                SELECT c.NOMBRE, c.APELLIDO, c.CORREO 
                FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD c
                JOIN LABORATORIO.MONICA_SOBERON.INVITACION_CURSO r
                ON c.ID_USUARIO = r.ID_USUARIO
                WHERE r.ID_CURSO = (
                    SELECT ID_CURSO 
                    FROM LABORATORIO.MONICA_SOBERON.CURSO 
                    WHERE NOMBRE_CURSO = '{selected_course}'
                );
            """)
            
            inv_df = invitados_result.to_pandas()
            
            # Display the registered users
            st.write("**Usuarios Invitados:**")
            st.dataframe(inv_df)

            st.write("**Agregar Usuarios:**")
                
            email_input = st.text_area(
                "Pega la lista de correos electrónicos aquí (uno por línea):",
                height=300,
                key="text4"
            )
            
            if st.button("Procesar Correos de Invitados"):
                # Process the input emails
                email_list = [email.strip().lower() for email in email_input.split('\n') if email.strip()]
                email_list = list(set(email_list))  # Remove duplicates
                
                df_invitados = pd.DataFrame(email_list, columns=['Correo'])
                
                # Display the processed emails
                st.write("Correos electrónicos de invitados procesados:")
                st.dataframe(df_invitados)

                course_id_result = session.sql(f"""
                SELECT ID_CURSO 
                FROM LABORATORIO.MONICA_SOBERON.CURSO 
                WHERE NOMBRE_CURSO = '{selected_course}';
                """)
                course_id_df = course_id_result.to_pandas()
                course_id = course_id_df['ID_CURSO'].iloc[0]

                for email in email_list:
                # Get the user ID for the email
                    user_id_result = session.sql(f"""
                    SELECT ID_USUARIO 
                    FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD 
                    WHERE CORREO = '{email}';
                """)
                    user_id_df = user_id_result.to_pandas()

                    if not user_id_df.empty:
                        user_id = user_id_df['ID_USUARIO'].iloc[0]

                        insert_query = f"""
                    INSERT INTO LABORATORIO.MONICA_SOBERON.INVITACION_CURSO (ID_CURSO, ID_USUARIO)
                    VALUES ({course_id}, {user_id});
                    """
                        session.sql(insert_query).collect()

                    st.success("Usuarios registrados agregados con éxito.")

    with tabs[4]:
        st.header("Lista de Usuarios Registrados")

        # Query for course information
        course_result = session.sql("SELECT NOMBRE_CURSO FROM LABORATORIO.MONICA_SOBERON.CURSO;")
        course_df = course_result.to_pandas()
        course_names = course_df['NOMBRE_CURSO'].tolist()

        # Display course select box
        selected_course = st.selectbox('Selecciona un Curso: ', course_names)
        if selected_course:
            # Query for course details based on the selected course
            course_details_result = session.sql(f"""
                SELECT NOMBRE_CURSO, FECHA_INICIO, FECHA_FIN, PROVEEDOR 
                FROM LABORATORIO.MONICA_SOBERON.CURSO 
                WHERE NOMBRE_CURSO = '{selected_course}';
            """)
            
            course_details_df = course_details_result.to_pandas()
            
            # Display the course details
            st.write("**Detalles del Curso:**")
            for index, row in course_details_df.iterrows():
                st.write(f"Nombre del Curso: {row['NOMBRE_CURSO']}")
                st.write(f"Fecha de Inicio: {row['FECHA_INICIO']}")
                st.write(f"Fecha de Fin: {row['FECHA_FIN']}")
                st.write(f"Proveedor: {row['PROVEEDOR']}")
            
            # Query for registered users for the selected course
            registered_result = session.sql(f"""
                SELECT c.NOMBRE, c.APELLIDO, c.CORREO 
                FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD c
                JOIN LABORATORIO.MONICA_SOBERON.REGISTRADOS_CURSO r
                ON c.ID_USUARIO = r.ID_USUARIO
                WHERE r.ID_CURSO = (
                    SELECT ID_CURSO 
                    FROM LABORATORIO.MONICA_SOBERON.CURSO 
                    WHERE NOMBRE_CURSO = '{selected_course}'
                );
            """)
            
            registered_df = registered_result.to_pandas()
            
            # Display the registered users
            st.write("**Usuarios Registrados:**")
            st.dataframe(registered_df)

            st.write("**Agregar Usuarios:**")
            email_input2 = st.text_area(
        "Pega la lista de correos electrónicos aquí (uno por línea):",
        height=300,  key='email_input_key'  
    )

            if st.button("Procesar Correos de Registrados"):
        # Process the input emails
                email_list = [email.strip().lower() for email in email_input2.split('\n') if email.strip()]
                email_list = list(set(email_list))  # Remove duplicates

                if email_list:
                    df_registrados = pd.DataFrame(email_list, columns=['Correo'])

            # Display the processed emails
                    st.write("Correos electrónicos de registrados procesados:")
                    st.dataframe(df_registrados)

                    course_id_result = session.sql(f"""
                SELECT ID_CURSO 
                FROM LABORATORIO.MONICA_SOBERON.CURSO 
                WHERE NOMBRE_CURSO = '{selected_course}';
            """)
                    course_id_df = course_id_result.to_pandas()
                    course_id = course_id_df['ID_CURSO'].iloc[0]

            # Insert into the REGISTRADOS_CURSO table for each email
                    for email in email_list:
                # Get the user ID for the email
                        user_id_result = session.sql(f"""
                    SELECT ID_USUARIO 
                    FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD 
                    WHERE CORREO = '{email}';
                """)
                        user_id_df = user_id_result.to_pandas()

                        if not user_id_df.empty:
                            user_id = user_id_df['ID_USUARIO'].iloc[0]

                    # Insert the user and course relationship into REGISTRADOS_CURSO
                            insert_query = f"""
                    INSERT INTO LABORATORIO.MONICA_SOBERON.REGISTRADOS_CURSO (ID_CURSO, ID_USUARIO)
                    VALUES ({course_id}, {user_id});
                    """
                            session.sql(insert_query).collect()

                    st.success("Usuarios registrados agregados con éxito.")

with tabsm[2]:

        # Write directly to the app
    st.title("Gestión de Usuarios")
    st.write(
            """Esta pantalla permite la gestión de los datos personales de los usuarios,
            al igual que su estatus y detalles sobre sus cursos.
            """
        )
    tab1, tab2, tab3 = st.tabs(["Usuario Existente", "Nuevo Usuario", "Eliminar Usuario"])

    with tab1:
        st.header("Editar Usuarios")
        # Query to get a list of community members
        comunidad_result = session.sql("SELECT CORREO FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD;")
        comunidad_df = comunidad_result.to_pandas()
        comunidad_correos = comunidad_df['CORREO'].tolist()

        # Display selectbox for individual member selection
        miembro = st.selectbox('Selecciona un miembro:', comunidad_correos)
        if miembro:
            # Query to get individual member details
            miembro_sql = session.sql(f"SELECT NOMBRE, APELLIDO, CORREO, STATUS FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD WHERE CORREO = '{miembro}';")
            miembro_df = miembro_sql.to_pandas()

            # Display member details
            st.write("**Detalles del Miembro:**")
            for index, row in miembro_df.iterrows():
                st.write(f" Nombre: {row['NOMBRE']}")
                st.write(f" Apellido: {row['APELLIDO']}")
                st.write(f" Correo: {row['CORREO']}")
                st.write(f" Estatus: {row['STATUS']}")

            st.write("**Actualización de Datos Personales:**")
            # Form to update user details
            with st.form(key='update_form'):
                nombre_nuevo = st.text_input('Nombre Nuevo:', value=row['NOMBRE'])
                apellido_nuevo = st.text_input('Apellido Nuevo:', value=row['APELLIDO'])
                correo_nuevo = st.text_input('Correo Nuevo:', value=row['CORREO'])
                estatus_nuevo = st.checkbox('Estatus (Activo = True, Inactivo = False)', value=row['STATUS'])

                submit_button = st.form_submit_button(label='Actualizar Detalles')

                if submit_button:
                    # Update the user details in the database
                    session.sql(f"""
                        UPDATE LABORATORIO.MONICA_SOBERON.COMUNIDAD
                        SET NOMBRE = '{nombre_nuevo}', APELLIDO = '{apellido_nuevo}', STATUS = '{estatus_nuevo}', CORREO = '{correo_nuevo}'
                        WHERE CORREO = '{miembro}';
                    """).collect()
                    
                    st.success("Detalles actualizados exitosamente.")

            registrados = session.sql(f""" 
                SELECT C.NOMBRE_CURSO, C.FECHA_INICIO, C.FECHA_FIN, R.SOLICITUD_APROBADA, R.CURSO_APROBADO
                FROM LABORATORIO.MONICA_SOBERON.REGISTRADOS_CURSO AS R 
                INNER JOIN LABORATORIO.MONICA_SOBERON.CURSO AS C 
                ON R.ID_CURSO = C.ID_CURSO 
                WHERE R.ID_USUARIO = (SELECT ID_USUARIO FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD WHERE CORREO = '{miembro}');
            """)

            registrados_df = registrados.to_pandas()
            st.write("**Cursos Registrados:**")
            st.write(registrados_df)


            updated_rows = []

    # Loop through the DataFrame and create checkboxes for each course
            for index, row in registrados_df.iterrows():
                st.write(f"**Curso:** {row['NOMBRE_CURSO']}")

        
        # Create checkboxes for 'Solicitud Aprobada' and 'Curso Aprobado' and store their state
                solicitud_aprobada = st.checkbox(
                    'Solicitud Aprobada', 
                    value=bool(row['SOLICITUD_APROBADA']), 
                    key=f'solicitud_{index}'
                )
                curso_aprobado = st.checkbox(
                    'Curso Aprobado', 
                    value=bool(row['CURSO_APROBADO']), 
                    key=f'curso_{index}'
            )
        
        # Append the updated data to the list
                updated_rows.append({
                    'NOMBRE_CURSO': row['NOMBRE_CURSO'],
                    'SOLICITUD_APROBADA': solicitud_aprobada,
                    'CURSO_APROBADO': curso_aprobado
                })
                

    # When the user clicks the button, apply all the updates
        if st.button('Guardar Cambios'):
            for updated_row in updated_rows:
                query = f"""
                UPDATE LABORATORIO.MONICA_SOBERON.REGISTRADOS_CURSO
                SET SOLICITUD_APROBADA = {1 if updated_row['SOLICITUD_APROBADA'] else 0},
                    CURSO_APROBADO = {1 if updated_row['CURSO_APROBADO'] else 0}
                WHERE ID_USUARIO = (SELECT ID_USUARIO FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD WHERE CORREO = '{miembro}')
                AND ID_CURSO = (SELECT ID_CURSO 
                    FROM LABORATORIO.MONICA_SOBERON.CURSO 
                    WHERE NOMBRE_CURSO'{updated_row['NOMBRE_CURSO']}');
                """
                session.sql(query)
        
            st.success("Cambios guardados exitosamente.")
            
    with tab2:
        # Write directly to the app
        st.header("Creación de Usuarios")
        st.write("**Ingresa los Datos Personales:**")

        # Form to create a new user
        with st.form(key='create_form'):
            nombre_nuevo = st.text_input('Nombre:', value='')
            apellido_nuevo = st.text_input('Apellido:', value='')
            correo_nuevo = st.text_input('Correo:', value='')  # Add an input for the email
            estatus_nuevo = st.checkbox('Estatus (Activo = True, Inactivo = False)', value=False)

            submit_button = st.form_submit_button(label='Crear Usuario')

            if submit_button:
                # Convert the checkbox value (True/False) to 1/0 for the database
                estatus_value = 1 if estatus_nuevo else 0

                # Insert the new user into the database
                session.sql(f"""
                    INSERT INTO LABORATORIO.MONICA_SOBERON.COMUNIDAD (NOMBRE, APELLIDO, CORREO, STATUS)
                    VALUES ('{nombre_nuevo}', '{apellido_nuevo}', '{correo_nuevo}', {estatus_value});
                """).collect()
                    
                st.success("Usuario creado exitosamente.")

    with tab3:
        st.header("Eliminar Usuario")
        st.write(
            """Eliminar un usuario es algo definitivo. Se borrarán todos sus datos."""
        )

        # Query to get a list of community members
        comunidad_result_del = session.sql("SELECT CORREO FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD;")
        comunidad_df_del = comunidad_result_del.to_pandas()
        comunidad_correos_del = comunidad_df_del['CORREO'].tolist()

        # Display selectbox for individual member selection
        miembro_del = st.selectbox('Selecciona un miembro:', comunidad_correos_del, key='del')
        
        if miembro_del:
            # Query to get individual member details
            miembro_sql_del = session.sql(f"SELECT NOMBRE, APELLIDO, CORREO, STATUS FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD WHERE CORREO = '{miembro_del}';")
            miembro_df_del = miembro_sql_del.to_pandas()

            # Display member details
            st.write("**Estas seguro que deseas eliminar al usuario:**")
            for index, row in miembro_df_del.iterrows():
                st.write(f"Nombre: {row['NOMBRE']}")
                st.write(f"Apellido: {row['APELLIDO']}")
                st.write(f"Correo: {row['CORREO']}")
                st.write(f"Estatus: {row['STATUS']}")

            # Confirmation checkbox
            seguro = st.checkbox("Estoy seguro de que quiero eliminar este usuario.")

            # Deletion button, only enabled if checkbox is checked
            if seguro:
                if st.button('Eliminar Usuario'):
                    # Delete the user from the database
                    session.sql(f"DELETE FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD WHERE CORREO = '{miembro_del}';").collect()
                    st.success(f"El usuario con correo {miembro_del} ha sido eliminado exitosamente.")

        
