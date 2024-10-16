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

@st.cache_data
def get_user_names():
    users_result = session.sql("""
        SELECT correo, nombre, apellido FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD;
    """)
    users_df = users_result.to_pandas()

    # Create a formatted column 'USUARIOS' combining 'correo', 'nombre', and 'apellido'
    users_df['USUARIOS'] = users_df.apply(
        lambda row: f"{row['NOMBRE']} {row['APELLIDO']} : {row['CORREO']}", axis=1)
    return users_df

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

        # Dropdown for Negocio with "No Registrar" option
        negocio_nuevo = st.selectbox('Negocio:', ['Ventas', 'Mercadotecnia', 'Operaciones', 'Finanzas', 'No Registrar'])

        # Dropdown for Área with "No Registrar" option
        area_nueva = st.selectbox('Área:', ['IT', 'Recursos Humanos', 'Logística', 'Administración', 'Comercial', 'No Registrar'])

        # Dropdown for País with "No Registrar" option
        pais_nuevo = st.selectbox('País:', ['México', 'Estados Unidos', 'Colombia', 'Argentina', 'No Registrar'])

        submit_button = st.form_submit_button(label='Crear Usuario')

        if submit_button:
            # Convert the checkbox value (True/False) to 1/0 for the database
            estatus_value = 1 if estatus_nuevo else 0

            # Handle "No Registrar" option for Negocio, Área, and País by setting them to None in the database
            negocio_value = None if negocio_nuevo == 'No Registrar' else negocio_nuevo
            area_value = None if area_nueva == 'No Registrar' else area_nueva
            pais_value = None if pais_nuevo == 'No Registrar' else pais_nuevo

            # Use parameterized queries instead of string interpolation
            insert_query = """
            INSERT INTO LABORATORIO.MONICA_SOBERON.COMUNIDAD (NOMBRE, APELLIDO, CORREO, STATUS, NEGOCIO, AREA, PAIS)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """

            try:
                session.sql(insert_query, (nombre_nuevo, apellido_nuevo, correo_nuevo, estatus_value, negocio_value, area_value, pais_value)).collect()
                st.success("Usuario creado exitosamente.")
            except Exception as e:
                st.error(f"Error al crear el usuario: {e}")


with tab5:
    st.header("Añadir Usuarios Faltantes")
    st.write("""Esta sección sirve para pegar los correos copiados al seleccionar reply all en Outlook. 
                Aquí se formatean los correos y se añaden a la comunidad los que aún no se encuentran en ella.""")
    
    # Input for emails
    correos_input = st.text_area("Pega aquí los correos:")

    # Add a button for submitting the emails
    runbtn = st.button("Añadir Usuarios", key="usuario")

    # Initialize session state for handling the button state
    if "runbtn_state" not in st.session_state:
        st.session_state.runbtn_state = False

    # If the button is pressed or state is true
    if runbtn or st.session_state.runbtn_state:
        st.session_state.runbtn_state = True

        # Ensure the text area has input
        if correos_input:
            # Process the input emails
            correos = correos_input.split(";")  # Split by semicolon
            correos_formateados = [correo.split("<")[-1].strip().rstrip(">").replace(chr(10), '').replace(chr(13), '').strip().lower() for correo in correos]
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

            if nuevos_correos:
                st.write("Complete los datos de los siguientes usuarios:")

                # Create a dataframe with columns for user data input
                user_data = pd.DataFrame(
                    [{"Correo": correo, "Nombre": "", "Apellido": "", "Negocio": "", "Área": "", "País": "", "Estatus": True}
                    for correo in nuevos_correos]
                )

                # Use st.data_editor with editable columns, except for 'Correo'
                edited_user_data = st.data_editor(
                    user_data,
                    use_container_width=True  # Allow better UI scaling
                )

                # Submit button for saving the changes
                if st.button("Registrar Usuarios", key="submit_users"):
                    st.write("Formulario enviado. Procesando usuarios...")  # Debug point

                    # Process the user_data list and insert into the database
                    for index, row in edited_user_data.iterrows():
                        st.write(f"Registrando usuario: {row['Correo']}")  # Debug point

                        # Insert query with direct handling of NULL values
                        insert_query = f"""
                        INSERT INTO LABORATORIO.MONICA_SOBERON.COMUNIDAD 
                        (NOMBRE, APELLIDO, CORREO, STATUS, NEGOCIO, AREA, PAIS)
                        VALUES (
                            {f"'{row['Nombre']}'" if row['Nombre'] else 'NULL'}, 
                            {f"'{row['Apellido']}'" if row['Apellido'] else 'NULL'}, 
                            '{row['Correo']}', 
                            {row['Estatus']}, 
                            {f"'{row['Negocio']}'" if row['Negocio'] else 'NULL'}, 
                            {f"'{row['Área']}'" if row['Área'] else 'NULL'}, 
                            {f"'{row['País']}'" if row['País'] else 'NULL'}
                        );
                        """

                        try:
                            session.sql(insert_query).collect()  
                            st.success(f"Usuario {row['Nombre'] or row['Correo']} registrado con éxito.")
                        except Exception as e:
                            st.error(f"Error al registrar {row['Correo']}: {e}")
            else:
                st.success("Todos los correos ya están registrados en la comunidad.")
    
with tab2:

    st.header("Editar Usuarios") 
    # Display selectbox for individual member selection
    users_df = get_user_names()
    miembro = st.selectbox('Selecciona un miembro:', users_df['USUARIOS'])
    if miembro:
        # Query to get individual member details
        miembro = miembro.split(' : ')[0]
        miembro_sql = session.sql(f"""
            SELECT NOMBRE, APELLIDO, CORREO, STATUS, NEGOCIO, AREA, PAIS, BAJA_EMPRESA
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
            st.write(f" Baja Empresa: {row['BAJA_EMPRESA']}")

        st.write("**Actualización de Datos Personales:**")
        # Form to update user details
        with st.form(key='update_form'):
            nombre_nuevo = st.text_input('Nombre Nuevo:', value=row['NOMBRE'])
            apellido_nuevo = st.text_input('Apellido Nuevo:', value=row['APELLIDO'])
            correo_nuevo = st.text_input('Correo Nuevo:', value=row['CORREO'])
            estatus_nuevo = st.checkbox('Estatus (Activo = True, Inactivo = False)', value=bool(row['STATUS']))

            # Dropdown for Negocio with "No Registrar" option
            negocio_nuevo = st.selectbox('Negocio Nuevo:', ['Ventas', 'Mercadotecnia', 'Operaciones', 'Finanzas', 'No Registrar'], 
                                        index=4 if row['NEGOCIO'] is None else ['Ventas', 'Mercadotecnia', 'Operaciones', 'Finanzas'].index(row['NEGOCIO']))

            # Dropdown for Área with "No Registrar" option
            area_nueva = st.selectbox('Área Nueva:', ['IT', 'Recursos Humanos', 'Logística', 'Administración', 'Comercial', 'No Registrar'], 
                                    index=5 if row['AREA'] is None else ['IT', 'Recursos Humanos', 'Logística', 'Administración', 'Comercial'].index(row['AREA']))

            # Dropdown for País with "No Registrar" option
            pais_nuevo = st.selectbox('País Nuevo:', ['México', 'Estados Unidos', 'Colombia', 'Argentina', 'No Registrar'], 
                                    index=4 if row['PAIS'] is None else ['México', 'Estados Unidos', 'Colombia', 'Argentina'].index(row['PAIS']))

            baja_nuevo = st.checkbox('Baja Empresa (Baja de la Empresa = True, En la Empresa = False)', value=bool(row['BAJA_EMPRESA']))

            submit_button = st.form_submit_button(label='Actualizar Detalles')

            if submit_button:
                # Handle "No Registrar" for Negocio, Área, and País by setting them to None in the database
                negocio_value = None if negocio_nuevo == 'No Registrar' else negocio_nuevo
                area_value = None if area_nueva == 'No Registrar' else area_nueva
                pais_value = None if pais_nuevo == 'No Registrar' else pais_nuevo

                # Use parameterized queries to update user details in the database
                update_query = """
                UPDATE LABORATORIO.MONICA_SOBERON.COMUNIDAD
                SET NOMBRE = ?, APELLIDO = ?, STATUS = ?, CORREO = ?, NEGOCIO = ?, AREA = ?, PAIS = ?, BAJA_EMPRESA = ?
                WHERE CORREO = ?
                """

                try:
                    session.sql(update_query, (nombre_nuevo, apellido_nuevo, estatus_value, correo_nuevo, negocio_value, area_value, pais_value, 1 if baja_nuevo else 0, miembro)).collect()
                    st.success("Detalles actualizados exitosamente.")
                except Exception as e:
                    st.error(f"Error al actualizar el usuario: {e}")

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
        """Eliminar un usuario es algo definitivo. Se borrarán todos sus datos. Solo se pueden borrar usuarios que no se han invitado, asistido, o registrado a un curso o sesión."""
    )

    users_df = get_user_names()
    miembro = st.selectbox('Selecciona un miembro:', users_df['USUARIOS'], key='del')
    if miembro:
        # Query to get individual member details
        miembro_del = miembro.split(' : ')[0]
    
    if miembro_del:
        # Query to get individual member details
        miembro_sql_del = session.sql(f"SELECT NOMBRE, APELLIDO, CORREO, STATUS FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD WHERE CORREO = '{miembro_del}';")
        miembro_df_del = miembro_sql_del.to_pandas()

        # Get the user ID
        miembro_id_result = session.sql(f"SELECT id_usuario FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD WHERE CORREO = '{miembro_del}';")
        miembro_id = miembro_id_result.to_pandas().iloc[0, 0]  # Extract the id_usuario value

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
            check_data = session.sql(f"""
            SELECT 
                (SELECT COUNT(*) FROM LABORATORIO.MONICA_SOBERON.INVITACION_CURSO WHERE ID_USUARIO = {miembro_id}) AS INVITADOS_COUNT,
                (SELECT COUNT(*) FROM LABORATORIO.MONICA_SOBERON.REGISTRADOS_CURSO WHERE ID_USUARIO = {miembro_id}) AS REGISTRADOS_COUNT,
                (SELECT COUNT(*) FROM LABORATORIO.MONICA_SOBERON.INVITACION_SESION WHERE ID_USUARIO = {miembro_id}) AS INVITADO_COUNT,
                (SELECT COUNT(*) FROM LABORATORIO.MONICA_SOBERON.ASISTENCIA_SESION WHERE ID_USUARIO = {miembro_id}) AS ASISTENTES_COUNT
            """).to_pandas()

            if check_data.iloc[0]['INVITADOS_COUNT'] > 0 or check_data.iloc[0]['REGISTRADOS_COUNT'] > 0 or check_data.iloc[0]['INVITADO_COUNT'] > 0 or check_data.iloc[0]['ASISTENTES_COUNT'] > 0:
                st.error("Este usuario no se puede eliminar porque ha participado en cursos o sesiones.")
                st.write(f"Invitaciones a Cursos: {check_data.iloc[0]['INVITADOS_COUNT']}")
                st.write(f"Inscripciones a Cursos: {check_data.iloc[0]['REGISTRADOS_COUNT']}")
                st.write(f"Invitaciones a Sesiones: {check_data.iloc[0]['INVITADO_COUNT']}")
                st.write(f"Asistencia a Sesiones: {check_data.iloc[0]['ASISTENTES_COUNT']}")


            if check_data.iloc[0]['INVITADOS_COUNT'] == 0 and check_data.iloc[0]['REGISTRADOS_COUNT'] == 0 and check_data.iloc[0]['INVITADO_COUNT'] == 0 and check_data.iloc[0]['ASISTENTES_COUNT'] == 0:
                borrar = st.button('Eliminar Usuario', key="processU")
                if borrar:
                    session.sql(f"DELETE FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD WHERE ID_USUARIO = {miembro_id};").collect()
                    st.success(f"El usuario ha sido eliminado exitosamente.")
            else:
                st.error("Este usuario no se puede eliminar porque ha sido invitado, asistido, o registrado a un curso o sesión.")
