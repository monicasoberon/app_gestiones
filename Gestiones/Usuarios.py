import os
import streamlit as st
import pandas as pd
from snowflake.snowpark.functions import col

cnx = st.connection("snowflake")
session = cnx.session()

if "auth_data" not in st.session_state:
    st.write("Please authenticate to access this page.")
    st.stop()  # Stop the execution of this page

st.title("Gestión de Usuarios")
st.write(
        """Esta pantalla permite la gestión de los datos personales de los usuarios,
        al igual que su estatus y detalles sobre sus cursos.
        """
    )
tab1, tab2, tab5, tab3, tab4 = st.tabs(["Crear Usuario", "Editar Usuario", "Pegar correos Outlook", "Registrar Instructor", "Eliminar Usuario"])

with tab1:
# Write directly to the app
    st.header("Creación de Usuarios")
    st.write("**Ingresa los Datos Personales:**")

    with st.form(key='create_form'):
        nombre_nuevo = st.text_input('Nombre:', value='')
        apellido_nuevo = st.text_input('Apellido:', value='')
        correo_nuevo = st.text_input('Correo:', value='')  # Add an input for the email
        estatus_nuevo = st.checkbox('Estatus (Activo = True, Inactivo = False)', value=False)
        negocio_nuevo = st.text_input('Negocio (opcional):', value='')  # New optional field
        area_nueva = st.text_input('Área (opcional):', value='')  # New optional field
        pais_nuevo = st.text_input('País (opcional):', value='')  # New optional field

        submit_button = st.form_submit_button(label='Crear Usuario')

        if submit_button:
            # Convert the checkbox value (True/False) to 1/0 for the database
            estatus_value = 1 if estatus_nuevo else 0

            # Insert the new user into the database
            session.sql(f"""
                INSERT INTO LABORATORIO.MONICA_SOBERON.COMUNIDAD (NOMBRE, APELLIDO, CORREO, STATUS, NEGOCIO, AREA, PAIS)
                VALUES ('{nombre_nuevo}', '{apellido_nuevo}', '{correo_nuevo}', {estatus_value}, 
                        '{negocio_nuevo}', '{area_nueva}', '{pais_nuevo}');
            """).collect()
                
            st.success("Usuario creado exitosamente.")

with tab5:
    st.header("Añadir Usuarios Faltantes")
    st.write("""Esta sección sirve para pegar los correos copiados al seleccionar reply all en outlook. 
                Aquí se formatean los correos y se añaden a la comunidad los que aún no se encuentran en ella.""")

    # Input for emails
    correos_input = st.text_area("Pega aquí los correos:")

    if st.button("Añadir Usuarios", key="usuario"):
        st.write("Botón de añadir usuarios fue presionado.")  # Debug point
        if correos_input:
            # Process the input emails
            correos = correos_input.split(";")  # Split by semicolon
            correos_formateados = []

            for correo in correos:
                # Clean and extract email from the format
                correo_limpio = correo.split("<")[-1].strip().rstrip(">")
                correo_final = correo_limpio.replace(chr(10), '').replace(chr(13), '').strip().lower()
                correos_formateados.append(correo_final)

            st.write(f"Correos procesados: {correos_formateados}")  # Debug point

            # Get the existing community emails from the database
            try:
                comunidad_result = session.sql("SELECT CORREO FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD;")
                comunidad_df = comunidad_result.to_pandas()
                comunidad_correos = set(comunidad_df['CORREO'].tolist())
            except Exception as e:
                st.error(f"Error al consultar la base de datos: {e}")
                st.stop()

            # Filter new emails that are not in the community
            nuevos_correos = set(correos_formateados) - comunidad_correos
            st.write(f"Nuevos correos que no están en la comunidad: {nuevos_correos}")  # Debug point

            # Display the filtered new emails
            if nuevos_correos:
                st.write(f"Los siguientes correos no están registrados en la comunidad:")
                st.write(", ".join(nuevos_correos))
                st.write("Complete los datos de los siguientes usuarios:")

                # Create a form to submit all data at once
                with st.form("user_info_form"):
                    st.write("**Registrar Información de Usuarios Faltantes**")  # Header for the form

                    user_data = []
                    for correo in nuevos_correos:
                        # Display each email and create input fields for the remaining attributes
                        st.markdown(f"##### Correo: {correo}")  # Display email prominently
                        cols = st.columns(6)  # Create columns for attributes except email

                        # Editable fields for each attribute
                        nombre = cols[0].text_input(f"Nombre para {correo}", placeholder="Nombre", key=f"nombre_{correo}")
                        apellido = cols[1].text_input(f"Apellido para {correo}", placeholder="Apellido", key=f"apellido_{correo}")
                        negocio = cols[2].text_input(f"Negocio para {correo}", placeholder="Negocio", key=f"negocio_{correo}")
                        area = cols[3].text_input(f"Área para {correo}", placeholder="Área", key=f"area_{correo}")
                        pais = cols[4].text_input(f"País para {correo}", placeholder="País", key=f"pais_{correo}")
                        status = cols[5].checkbox(f"Activo para {correo}", value=True, key=f"status_{correo}")

                        # Collect data for this user
                        user_data.append({
                            "Correo": correo,
                            "Nombre": nombre if nombre else None,
                            "Apellido": apellido if apellido else None,
                            "Negocio": negocio if negocio else None,
                            "Área": area if area else None,
                            "País": pais if pais else None,
                            "Estatus": status
                        })

                    # Submit button for the form
                    submit_button = st.form_submit_button("Registrar Usuarios")

                    if submit_button:
                        st.write("Formulario enviado. Procesando usuarios...")  # Debug point

                        # Once the form is submitted, process the user_data list
                        for user in user_data:
                            st.write(f"Registrando usuario: {user['Correo']}")  # Debug point for each user

                            # Insert query with direct handling of NULL values
                            insert_query = f"""
                            INSERT INTO LABORATORIO.MONICA_SOBERON.COMUNIDAD 
                            (NOMBRE, APELLIDO, CORREO, STATUS, NEGOCIO, AREA, PAIS)
                            VALUES (
                                {f"'{user['Nombre']}'" if user['Nombre'] is not None else 'NULL'}, 
                                {f"'{user['Apellido']}'" if user['Apellido'] is not None else 'NULL'}, 
                                '{user['Correo']}', 
                                {user['Estatus']}, 
                                {f"'{user['Negocio']}'" if user['Negocio'] is not None else 'NULL'}, 
                                {f"'{user['Área']}'" if user['Área'] is not None else 'NULL'}, 
                                {f"'{user['País']}'" if user['País'] is not None else 'NULL'}
                            );
                            """
                            st.write(f"SQL Query: {insert_query}")  # Debugging the SQL query

                            try:
                                # Execute the SQL query to insert new user
                                st.write("Executing SQL query...")  # Debug point before execution
                                session.sql(insert_query).collect()
                                st.success(f"Usuario {user['Nombre'] or user['Correo']} registrado con éxito.")
                            except Exception as e:
                                st.error(f"Error al registrar {user['Correo']}: {e}")

            else:
                st.success("Todos los correos ya están registrados en la comunidad.")

    
with tab2:

    st.header("Editar Usuarios")
    # Query to get a list of community members
    comunidad_result = session.sql("SELECT CORREO FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD;")
    comunidad_df = comunidad_result.to_pandas()
    comunidad_correos = comunidad_df['CORREO'].tolist()

    # Display selectbox for individual member selection
    miembro = st.selectbox('Selecciona un miembro:', comunidad_correos)
    if miembro:
        # Query to get individual member details
        miembro_sql = session.sql(f"""
            SELECT NOMBRE, APELLIDO, CORREO, STATUS, NEGOCIO, AREA, PAIS 
            FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD 
            WHERE CORREO = '{miembro}';
        """)
        miembro_df = miembro_sql.to_pandas()

        # Display member details
        st.write("**Detalles del Miembro:**")
        for index, row in miembro_df.iterrows():
            st.write(f" Nombre: {row['NOMBRE']}")
            st.write(f" Apellido: {row['APELLIDO']}")
            st.write(f" Correo: {row['CORREO']}")
            st.write(f" Estatus: {row['STATUS']}")
            st.write(f" Negocio: {row['NEGOCIO']}")
            st.write(f" Área: {row['AREA']}")
            st.write(f" País: {row['PAIS']}")

        st.write("**Actualización de Datos Personales:**")
        # Form to update user details
        with st.form(key='update_form'):
            nombre_nuevo = st.text_input('Nombre Nuevo:', value=row['NOMBRE'])
            apellido_nuevo = st.text_input('Apellido Nuevo:', value=row['APELLIDO'])
            correo_nuevo = st.text_input('Correo Nuevo:', value=row['CORREO'])
            estatus_nuevo = st.checkbox('Estatus (Activo = True, Inactivo = False)', value=row['STATUS'])
            negocio_nuevo = st.text_input('Negocio Nuevo (opcional):', value=row['NEGOCIO'])  # New optional field
            area_nueva = st.text_input('Área Nueva (opcional):', value=row['AREA'])  # New optional field
            pais_nuevo = st.text_input('País Nuevo (opcional):', value=row['PAIS'])  # New optional field

            submit_button = st.form_submit_button(label='Actualizar Detalles')

            if submit_button:
                # Update the user details in the database
                session.sql(f"""
                    UPDATE LABORATORIO.MONICA_SOBERON.COMUNIDAD
                    SET NOMBRE = '{nombre_nuevo}', APELLIDO = '{apellido_nuevo}', 
                        STATUS = {1 if estatus_nuevo else 0}, CORREO = '{correo_nuevo}',
                        NEGOCIO = '{negocio_nuevo}', AREA = '{area_nueva}', PAIS = '{pais_nuevo}'
                    WHERE CORREO = '{miembro}';
                """).collect()
                
                st.success("Detalles actualizados exitosamente.")

        # Fetch registered courses for the member
        registrados = session.sql(f""" 
            SELECT N.NOMBRE_CURSO, C.FECHA_INICIO, C.FECHA_FIN, R.SOLICITUD_APROBADA, R.CURSO_APROBADO
            FROM LABORATORIO.MONICA_SOBERON.REGISTRADOS_CURSO AS R 
            INNER JOIN LABORATORIO.MONICA_SOBERON.CURSO AS C 
            ON R.ID_CURSO = C.ID_CURSO 
            INNER JOIN LABORATORIO.MONICA_SOBERON.CATALOGO_CURSOS AS N
            ON C.ID_CATALOGO = N.ID_CATALOGO
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
        if st.button('Guardar Cambios', key = "process7"):
            for updated_row in updated_rows:
                query = f"""
                UPDATE LABORATORIO.MONICA_SOBERON.REGISTRADOS_CURSO
                SET SOLICITUD_APROBADA = {1 if updated_row['SOLICITUD_APROBADA'] else 0},
                    CURSO_APROBADO = {1 if updated_row['CURSO_APROBADO'] else 0}
                WHERE ID_USUARIO = (SELECT ID_USUARIO FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD WHERE CORREO = '{miembro}')
                AND ID_CURSO = (SELECT ID_CURSO 
                    FROM LABORATORIO.MONICA_SOBERON.CURSO AS C
                    INNER JOIN LABORATORIO.MONICA_SOBERON.CATALOGO_CURSOS AS N
                    ON C.ID_CATALOGO = N.ID_CATALOGO 
                    WHERE N.NOMBRE_CURSO = '{updated_row['NOMBRE_CURSO']}');
                """
                session.sql(query)

            st.success("Cambios guardados exitosamente.")        

with tab3:
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

with tab4:
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
            borrar = st.button('Eliminar Usuario', key = "process8")
            if borrar:
                session.sql(f"DELETE FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD WHERE CORREO = '{miembro_del}';").collect()
                st.success(f"El usuario con correo {miembro_del} ha sido eliminado exitosamente.")

