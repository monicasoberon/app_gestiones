import os
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from snowflake.snowpark.functions import col

# Connect to Snowflake
cnx = st.connection("snowflake")
session = cnx.session()

# Authentication check
if "auth_data" not in st.session_state:
    st.write("Please authenticate to access this page.")
    st.stop()

# Set a general Seaborn style for all visualizations
sns.set_style("whitegrid")
color_palette = sns.color_palette("muted")

st.title("Estadísticas del Centro de Transformación Digital")

### Section 1: Top 10 Most Involved Users ###
st.write("### Usuarios Más Involucrados")
st.write(
    """
    Esta gráfica muestra los 10 usuarios que han estado más involucrados 
    en el Centro de Transformación Digital, con base en las sesiones a las 
    que han asistido y los cursos en los que se han inscrito.
    """
)

# SQL query for top involved users
top_users_result = session.sql("""
    SELECT 
        C.CORREO,
        COUNT(DISTINCT A.ID_SESION) AS sesiones_asistidas, 
        COUNT(DISTINCT R.ID_CURSO) AS cursos_inscritos, 
        COUNT(DISTINCT A.ID_SESION) + COUNT(DISTINCT R.ID_CURSO) AS participacion_total
    FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD AS C
    LEFT JOIN LABORATORIO.MONICA_SOBERON.ASISTENCIA_SESION AS A
    ON C.ID_USUARIO = A.ID_USUARIO
    LEFT JOIN LABORATORIO.MONICA_SOBERON.REGISTRADOS_CURSO AS R
    ON C.ID_USUARIO = R.ID_USUARIO
    GROUP BY C.CORREO
    ORDER BY participacion_total DESC
    LIMIT 10;
""")
top_users_df = top_users_result.to_pandas()

# Create a figure for top users
fig, ax = plt.subplots(figsize=(10, 6))
sns.barplot(data=top_users_df, x='PARTICIPACION_TOTAL', y='CORREO', ax=ax, palette=color_palette)
ax.set_title('Top 10 Usuarios Más Involucrados', fontsize=16, weight='bold')
ax.set_xlabel('Total de Participaciones (Sesiones + Cursos)', fontsize=12)
ax.set_ylabel('Correo del Usuario', fontsize=12)
ax.set_xticks(ax.get_xticks())
plt.xticks(fontsize=10)
plt.yticks(fontsize=10)
sns.despine()

# Display the figure in Streamlit
st.pyplot(fig)

### Section 2: Most Popular Courses ###
st.write("### Cursos Más Populares")
st.write(
    """
    Aquí puedes ver los cursos más populares basados en la cantidad de 
    inscripciones de los usuarios. Esta métrica ayuda a identificar qué 
    cursos están generando mayor interés entre los miembros del Centro de 
    Transformación Digital.
    """
)

# SQL query for popular courses
popular_courses_result = session.sql("""
    SELECT N.NOMBRE_CURSO, COUNT(R.ID_USUARIO) AS inscripciones
    FROM LABORATORIO.MONICA_SOBERON.CATALOGO_CURSOS AS N
    INNER JOIN LABORATORIO.MONICA_SOBERON.CURSO AS C
    ON N.ID_CATALOGO = C.ID_CATALOGO
    INNER JOIN LABORATORIO.MONICA_SOBERON.REGISTRADOS_CURSO AS R
    ON C.ID_CURSO = R.ID_CURSO
    GROUP BY N.NOMBRE_CURSO
    ORDER BY inscripciones DESC
    LIMIT 10;
""")
popular_courses_df = popular_courses_result.to_pandas()

# Create a figure for popular courses
fig, ax = plt.subplots(figsize=(10, 6))
sns.barplot(data=popular_courses_df, x='INSCRIPCIONES', y='NOMBRE_CURSO', ax=ax, palette="Blues_d")
ax.set_title('Top 10 Cursos Más Populares', fontsize=16, weight='bold')
ax.set_xlabel('Número de Inscripciones', fontsize=12)
ax.set_ylabel('Nombre del Curso', fontsize=12)
plt.xticks(fontsize=10)
plt.yticks(fontsize=10)
sns.despine()

# Display the figure in Streamlit
st.pyplot(fig)

### Section 3: Course Completion Rates ###
st.write("### Tasas de Finalización de Cursos")
st.write(
    """
    Esta gráfica compara el número de inscripciones con el número de 
    finalizaciones para cada curso, permitiendo ver qué tan efectivos son 
    los cursos en mantener a los participantes hasta el final.
    """
)

# SQL query for completion rates
completion_rates_result = session.sql("""
    SELECT N.NOMBRE_CURSO, COUNT(R.ID_USUARIO) AS inscritos, 
           SUM(CASE WHEN R.CURSO_APROBADO = 'True' THEN 1 ELSE 0 END) AS completados
    FROM LABORATORIO.MONICA_SOBERON.CURSO AS C
    INNER JOIN LABORATORIO.MONICA_SOBERON.REGISTRADOS_CURSO AS R
    ON C.ID_CURSO = R.ID_CURSO
    INNER JOIN LABORATORIO.MONICA_SOBERON.CATALOGO_CURSOS AS N
    ON C.ID_CATALOGO = N.ID_CATALOGO
    GROUP BY N.NOMBRE_CURSO
    ORDER BY inscritos DESC;
""")
completion_rates_df = completion_rates_result.to_pandas()

# Calculate completion rate
completion_rates_df['COMPLETION_RATE'] = (completion_rates_df['COMPLETADOS'] / completion_rates_df['INSCRITOS']) * 100

# Create a figure for course completion rates
fig, ax = plt.subplots(figsize=(10, 6))
sns.barplot(data=completion_rates_df, x='COMPLETION_RATE', y='NOMBRE_CURSO', ax=ax, palette="Greens_d")
ax.set_title('Tasas de Finalización de Cursos', fontsize=16, weight='bold')
ax.set_xlabel('Porcentaje de Finalización (%)', fontsize=12)
ax.set_ylabel('Nombre del Curso', fontsize=12)
plt.xticks(fontsize=10)
plt.yticks(fontsize=10)
sns.despine()

# Display the figure in Streamlit
st.pyplot(fig)

### Section 4: User Invitation Recommendations ###
st.write("### Recomendaciones de Invitación a Cursos")
st.write(
    """
    Esta tabla muestra a los usuarios que han asistido a sesiones pero 
    aún no se han inscrito en ningún curso.
    """
)

# SQL query for users to invite to courses
invite_recommendations_result = session.sql("""
    SELECT C.NOMBRE, C.APELLIDO, C.CORREO, COUNT(A.ID_USUARIO) AS sesiones_asistidas
    FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD AS C
    LEFT JOIN LABORATORIO.MONICA_SOBERON.ASISTENCIA_SESION AS A
    ON C.ID_USUARIO = A.ID_USUARIO
    LEFT JOIN LABORATORIO.MONICA_SOBERON.REGISTRADOS_CURSO AS R
    ON C.ID_USUARIO = R.ID_USUARIO
    WHERE R.ID_USUARIO IS NULL
    GROUP BY C.NOMBRE, C.APELLIDO, C.CORREO
    HAVING COUNT(A.ID_USUARIO) > 0
    ORDER BY sesiones_asistidas DESC;
""")
invite_recommendations_df = invite_recommendations_result.to_pandas()

st.write("**Usuarios a Invitar a Cursos**")
st.write(invite_recommendations_df)

### Section 5: Dynamic Course Attendance Information ###
st.write("### Información Dinámica de Asistencia por Curso")
# Function to get course names
@st.cache_data
def get_course_names():
    nombres_result = session.sql("""
        SELECT n.NOMBRE_CURSO, c.ID_CURSO, c.FECHA_INICIO, c.FECHA_FIN
        FROM LABORATORIO.MONICA_SOBERON.CATALOGO_CURSOS AS n 
        INNER JOIN LABORATORIO.MONICA_SOBERON.CURSO AS c ON n.ID_CATALOGO = c.ID_CATALOGO;
    """)
    nombres_df = nombres_result.to_pandas()
    nombres_df['FECHA_INICIO'] = pd.to_datetime(nombres_df['FECHA_INICIO'], errors='coerce').dt.strftime('%d/%m/%Y')
    nombres_df['FECHA_FIN'] = pd.to_datetime(nombres_df['FECHA_FIN'], errors='coerce').dt.strftime('%d/%m/%Y')
    nombres_df['course_name_with_dates'] = nombres_df.apply(
        lambda row: f"{row['NOMBRE_CURSO']} ({row['FECHA_INICIO']} - {row['FECHA_FIN']})" 
        if pd.notnull(row['FECHA_INICIO']) and pd.notnull(row['FECHA_FIN']) 
        else f"{row['NOMBRE_CURSO']} (Fecha no disponible)", axis=1
    )
    return nombres_df

# Function to get class dates for a specific course
@st.cache_data
def get_class_dates(course_id):
    clases_result = session.sql(f"""
        SELECT clase.id_clase, clase.fecha 
        FROM LABORATORIO.MONICA_SOBERON.CLASE clase
        INNER JOIN LABORATORIO.MONICA_SOBERON.CURSO curso 
        ON clase.id_curso = curso.id_curso
        WHERE curso.id_curso = {course_id};
    """).to_pandas()
    return clases_result

# Function to get attendance data for a specific class
@st.cache_data
def get_class_attendance(class_id):
    students_result = session.sql(f"""
        SELECT id_usuario 
        FROM LABORATORIO.MONICA_SOBERON.ASISTENCIA_CLASE 
        WHERE id_clase = {class_id};
    """).to_pandas()
    return students_result

# Load course names and create a selectbox for course selection
courses_df = get_course_names()
selected_course_name_with_dates = st.selectbox("Selecciona un Curso:", courses_df['course_name_with_dates'])
selected_course_id = courses_df.loc[courses_df['course_name_with_dates'] == selected_course_name_with_dates, 'ID_CURSO'].values[0]

# Fetch class dates for the selected course
class_dates_df = get_class_dates(selected_course_id)

if not class_dates_df.empty:
    st.write(f"### Fechas de las Clases para el Curso: {selected_course_name_with_dates}")
    selected_class_date = st.selectbox("Selecciona una Fecha de Clase:", class_dates_df['FECHA'])
    
    # Get the class ID for the selected class date
    selected_class_id = class_dates_df.loc[class_dates_df['FECHA'] == selected_class_date, 'ID_CLASE'].values[0]
    
    # Fetch and display attendance for the selected class
    attendance_df = get_class_attendance(selected_class_id)

    if not attendance_df.empty:
        st.write("### Asistencia para la Clase Seleccionada")
        st.table(attendance_df)

        # Display attendance count for visualization
        attendance_count = attendance_df['ID_USUARIO'].nunique()
        st.write(f"**Total de Asistentes:** {attendance_count}")

    else:
        st.write("No hay información de asistencia para esta clase.")
else:
    st.write(f"No hay clases disponibles para el curso: {selected_course_name_with_dates}")