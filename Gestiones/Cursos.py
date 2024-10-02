import os
import streamlit as st
import pandas as pd
from snowflake.snowpark.functions import col

cnx = st.connection("snowflake")
session = cnx.session()

if "auth_data" not in st.session_state:
    st.write("Please authenticate to access this page.")
    st.stop()  # Stop the execution of this page
    
# Set up the Streamlit app
st.title("Gestión de Cursos")
st.write("Esta aplicación te ayuda a gestionar cursos, invitados y sesiones.")

# Create tabs
tabs = st.tabs(["Crear Curso", "Editar Curso", "Lista de Invitados", "Lista de Registrados"])

with tabs[0]:
    st.header("Crear Nuevo Curso")

    with st.form(key='new_course_form'):

        nombres = session.sql("""SELECT NOMBRE_CURSO FROM LABORATORIO.MONICA_SOBERON.NOMBRE_CURSO;""")
        course_name = st.selectbox("Nombre del Curso", nombres.to_pandas()['NOMBRE_CURSO'].tolist())
        course_name_id = session.sql(f"SELECT ID_NOMBRE FROM LABORATORIO.MONICA_SOBERON.NOMBRE_CURSO WHERE NOMBRE_CURSO = '{course_name}';").to_pandas()['ID_NOMBRE'].iloc[0]
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
        
        submit_button2 = st.form_submit_button(label='Crear Curso')
        
        if submit_button2:
            if course_name and course_start_date and course_end_date:
                # Insert new course into the CURSO table
                insert_course_query = f"""
                INSERT INTO LABORATORIO.MONICA_SOBERON.CURSO (ID_NOMBRE, FECHA_INICIO, FECHA_FIN, PROVEEDOR, REQUIERE_CASO_USO, CORREO_CONTACTO)
                VALUES ('{course_name_id}', '{course_start_date}', '{course_end_date}', '{course_provider}', {requires_case}, '{course_contact_email}');
                """
                session.sql(insert_course_query).collect()

                # Get the ID of the newly inserted course
                course_id_result = session.sql(f"""SELECT ID_CURSO FROM LABORATORIO.MONICA_SOBERON.CURSO as c
                                               INNER JOIN LABORATORIO.MONICA_SOBERON.NOMBRE_CURSO as n
                                               ON n.id_nombre = c.id_nombre
                                               WHERE n.NOMBRE_CURSO = '{course_name}';""")
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

    with st.form(key='new_course'):
        st.write("Si el curso no esta listado como opción en los nombres, añadelo aquí.")
        course_name = st.text_input("Nombre del Curso Nuevo a Añadir a la Base de Datos")

        submit_button = st.form_submit_button(label='Crear Curso')
        
        if submit_button:
            if course_name:
                insert_course_name_query = f"""
                INSERT INTO LABORATORIO.MONICA_SOBERON.NOMBRE_CURSO (NOMBRE_CURSO)
                VALUES ('{course_name}');
                """
                session.sql(insert_course_name_query).collect()

# Sección para editar un curso existente
with tabs[1]:
    st.header("Editar Curso Existente")
    
    # Fetch course names, IDs, and dates along with the `id_nombre` to link them
    nombres_result = session.sql("""
        SELECT n.NOMBRE_CURSO, c.ID_CURSO, c.FECHA_INICIO, c.FECHA_FIN
        FROM LABORATORIO.MONICA_SOBERON.NOMBRE_CURSO AS n 
        INNER JOIN LABORATORIO.MONICA_SOBERON.CURSO AS c ON n.ID_NOMBRE = c.ID_NOMBRE;
    """)
    nombres_df = nombres_result.to_pandas()

    # Check if the DataFrame is empty before accessing it
    if nombres_df.empty:
        st.error("No se encontraron cursos.")
    else:
        # Combine course name with start and end dates for display
        nombres_df['course_name_with_dates'] = nombres_df.apply(
            lambda row: f"{row['NOMBRE_CURSO']} ({row['FECHA_INICIO'].strftime('%d/%m/%Y')} - {row['FECHA_FIN'].strftime('%d/%m/%Y')})", axis=1)

        # Use the selectbox to display the combined name and dates
        selected_course_name_with_dates = st.selectbox("Selecciona el Curso:", nombres_df['course_name_with_dates'])

        # Get the ID_CURSO for the selected course
        selected_course_id = nombres_df.loc[nombres_df['course_name_with_dates'] == selected_course_name_with_dates, 'ID_CURSO'].values[0]

        # Fetch course details and allow editing if a course is selected
        if selected_course_id:
            # Get the details of the selected course
            course_details_result = session.sql(f"SELECT * FROM LABORATORIO.MONICA_SOBERON.CURSO WHERE ID_CURSO = {selected_course_id};")
            course_details_df = course_details_result.to_pandas()
            course_details = course_details_df.iloc[0]
        
        st.write("**Actualización de Datos del Curso:**")
        with st.form(key='edit_course_form'):

            course_name_result = session.sql(f"""SELECT NOMBRE_CURSO FROM LABORATORIO.MONICA_SOBERON.NOMBRE_CURSO n inner 
                                             join LABORATORIO.MONICA_SOBERON.CURSO c on n.id_nombre = c.id_nombre WHERE c.id_curso = {selected_course_id};""")
            course_name_df = course_name_result.to_pandas()
            current_course_name = course_name_df['NOMBRE_CURSO'].iloc[0]
            
            new_course_name = st.text_input("Nombre del Curso", value=current_course_name)
            new_course_start_date = st.date_input("Fecha de Inicio", value=course_details['FECHA_INICIO'])
            new_course_end_date = st.date_input("Fecha de Fin", value=course_details['FECHA_FIN'])
            new_course_provider = st.text_input("Proveedor", value=course_details['PROVEEDOR'])
            new_requires_case = st.checkbox("¿Requiere Caso de Uso?", value=course_details['REQUIERE_CASO_USO'])  # Boolean remains as is
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
                WHERE ts.ID_CURSO = {selected_course_id};
            """)
            course_sessions_df = course_sessions_result.to_pandas()
            current_sessions = course_sessions_df['NOMBRE_SESION'].tolist()
            
            selected_sessions = st.multiselect('Selecciona las Sesiones:', session_names, default=current_sessions)
            
            # Instructor dropdown
            instructor_result = session.sql("SELECT ID_INSTRUCTOR, NOMBRE_INSTRUCTOR, APELLIDO_INSTRUCTOR FROM LABORATORIO.MONICA_SOBERON.INSTRUCTOR;")
            instructor_df = instructor_result.to_pandas()
            instructor_names = [f"{row['NOMBRE_INSTRUCTOR']} {row['APELLIDO_INSTRUCTOR']}" for index, row in instructor_df.iterrows()]
            selected_instructor = st.selectbox("Selecciona el Instructor del Curso:", instructor_names)
            
            update_button = st.form_submit_button(label='Actualizar Curso')
            
            if update_button:
                if new_course_name and new_course_start_date and new_course_end_date:
                    # Update course details
                    update_course_query = f"""
                    UPDATE LABORATORIO.MONICA_SOBERON.CURSO
                    SET ID_NOMBRE = (SELECT ID_NOMBRE FROM LABORATORIO.MONICA_SOBERON.NOMBRE_CURSO WHERE NOMBRE_CURSO = '{new_course_name}'),
                        FECHA_INICIO = '{new_course_start_date}',
                        FECHA_FIN = '{new_course_end_date}',
                        PROVEEDOR = '{new_course_provider}',
                        REQUIERE_CASO_USO = {new_requires_case},  -- Boolean passed directly
                        CORREO_CONTACTO = '{new_course_contact_email}'
                    WHERE ID_CURSO = {selected_course_id};
                    """
                    session.sql(update_course_query).collect()

                    # Update the TIENE_SESION table
                    session_id_result = session.sql(f"""
                    SELECT ID_SESION 
                    FROM LABORATORIO.MONICA_SOBERON.SESION 
                    WHERE NOMBRE_SESION IN ({', '.join([f"'{s}'" for s in selected_sessions])});
                    """)
                    session_id_df = session_id_result.to_pandas()
                    session_ids = session_id_df['ID_SESION'].tolist()
                    
                    # Remove existing session links
                    delete_sessions_query = f"DELETE FROM LABORATORIO.MONICA_SOBERON.TIENE_SESION WHERE ID_CURSO = {selected_course_id};"
                    session.sql(delete_sessions_query).collect()
                    
                    # Insert new session links
                    for session_id in session_ids:
                        insert_session_query = f"""
                        INSERT INTO LABORATORIO.MONICA_SOBERON.TIENE_SESION (ID_CURSO, ID_SESION)
                        VALUES ({selected_course_id}, {session_id});
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
                    WHERE ID_CURSO = {selected_course_id};
                    """
                    session.sql(update_instructor_query).collect()

                    st.success(f"Curso '{new_course_name}' actualizado con éxito.")
                else:
                    st.error("Por favor, completa toda la información del curso.")


with tabs[2]:
    st.header("Registrar Invitados")
    # Check if the DataFrame is empty before accessing it
    if nombres_df.empty:
        st.error("No se encontraron cursos.")
    else:
        # Combine course name with start and end dates for display
        nombres_df['course_name_with_dates'] = nombres_df.apply(lambda row: f"{row['NOMBRE_CURSO']} ({row['FECHA_INICIO'].strftime('%d/%m/%Y')} - {row['FECHA_FIN'].strftime('%d/%m/%Y')})", axis=1)

        # Use the selectbox to display the combined name and dates
        selected_course_name_with_dates = st.selectbox("Selecciona el Curso:", nombres_df['course_name_with_dates'])

        # Get the ID_CURSO for the selected course
        selected_course_id = nombres_df.loc[nombres_df['course_name_with_dates'] == selected_course_name_with_dates, 'ID_CURSO'].values[0]

        # Query for course details based on the selected course
        course_details_result = session.sql(f"""
            SELECT n.NOMBRE_CURSO, c.FECHA_INICIO, c.FECHA_FIN, c.PROVEEDOR 
            FROM LABORATORIO.MONICA_SOBERON.CURSO c inner join
            LABORATORIO.MONICA_SOBERON.NOMBRE_CURSO n 
            ON c.id_nombre = n.id_nombre
            WHERE c.ID_CURSO = '{selected_course_id}';
        """)
        id_curso = selected_course_id

        course_details_df = course_details_result.to_pandas()
    
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
            WHERE r.ID_CURSO = '{id_curso}'
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
        
        if st.button("Procesar Correos de Invitados", key = "process4"):
            # Process the input emails
            email_list = [email.strip().lower() for email in email_input.split('\n') if email.strip()]
            email_list = list(set(email_list))  # Remove duplicates
            
            df_invitados = pd.DataFrame(email_list, columns=['Correo'])
            
            # Display the processed emails
            st.write("Correos electrónicos de invitados procesados:")
            st.dataframe(df_invitados)

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
                        SELECT {id_curso}, {user_id}
                        WHERE NOT EXISTS (
                        SELECT 1 
                        FROM LABORATORIO.MONICA_SOBERON.INVITACION_CURSO 
                        WHERE ID_CURSO = {id_curso} 
                        AND ID_USUARIO = {user_id}
                    );
                    """
                    session.sql(insert_query).collect()

                st.success("Usuarios invitados nuevos agregados con éxito.")

with tabs[3]:
    selected_course = st.selectbox('Selecciona un Curso: ', nombres_df['course_name_with_dates'])
    selected_course_id = nombres_df.loc[nombres_df['course_name_with_dates'] == selected_course_name_with_dates, 'ID_CURSO'].values[0]

    if selected_course:
        # Query for course details based on the selected course
        course_details_result = session.sql(f"""
            SELECT n.NOMBRE_CURSO, c.FECHA_INICIO, c.FECHA_FIN, c.PROVEEDOR 
            FROM LABORATORIO.MONICA_SOBERON.CURSO as c inner join
            LABORATORIO.MONICA_SOBERON.NOMBRE_CURSO as n
            ON n.id_nombre = c.id_nombre
            WHERE c.ID_CURSO = '{selected_course_id}';
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
                WHERE ID_NOMBRE = (
                        SELECT ID_NOMBRE 
                        FROM LABORATORIO.MONICA_SOBERON.NOMBRE_CURSO 
                        WHERE NOMBRE_CURSO ='{selected_course}'
            ));
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

        if st.button("Procesar Correos de Registrados", key = "process5"):
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
                            SELECT {course_id}, {user_id}
                            WHERE NOT EXISTS (
                                SELECT 1
                                FROM LABORATORIO.MONICA_SOBERON.REGISTRADOS_CURSO r
                                WHERE r.ID_CURSO = {course_id} AND r.ID_USUARIO = {user_id}
                            );
                        """

                        session.sql(insert_query).collect()

                st.success("Usuarios registrados agregados con éxito.")
