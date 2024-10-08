import os
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from snowflake.snowpark.functions import col

cnx = st.connection("snowflake")
session = cnx.session()

if "auth_data" not in st.session_state:
    st.write("Please authenticate to access this page.")
    st.stop()  # Stop the execution of this page

st.title("Estadísticas del Centro de Transformación Digital")

# Top 10 most involved users (most sessions + courses attended)
st.write("### Usuarios Más Involucrados")
st.write(
    """
    Esta gráfica muestra los 10 usuarios que han estado más involucrados 
    en el Centro de Transformación Digital, con base en las sesiones a las 
    que han asistido y los cursos en los que se han inscrito. La participación 
    total se calcula combinando asistencia a sesiones y registro en cursos, 
    sin contar varias asistencias a la misma sesión o curso más de una vez.
    """
)

# Corrected SQL query for top users (distinct participation)
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
    GROUP BY C.NOMBRE, C.APELLIDO
    ORDER BY participacion_total DESC
    LIMIT 10;
""")
top_users_df = top_users_result.to_pandas()

# Create figure for top users
fig, ax = plt.subplots(figsize=(10, 6))
sns.barplot(data=top_users_df, x='PARTICIPACION_TOTAL', y='CORREO', ax=ax)
ax.set_title('Top 10 Usuarios Más Involucrados')

# Display the figure in Streamlit
st.pyplot(fig)

# Most popular courses (highest enrollments)
st.write("### Cursos Más Populares")
st.write(
    """
    Aquí puedes ver los cursos más populares basados en la cantidad de 
    inscripciones de los usuarios. Esta métrica ayuda a identificar qué 
    cursos están generando mayor interés entre los miembros del Centro de 
    Transformación Digital.
    """
)

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

# Create figure for popular courses
fig, ax = plt.subplots(figsize=(10, 6))
sns.barplot(data=popular_courses_df, x='INSCRIPCIONES', y='NOMBRE_CURSO', ax=ax)
ax.set_title('Top 10 Cursos Más Populares')

# Display the figure in Streamlit
st.pyplot(fig)

# Course completion rates
st.write("### Tasas de Finalización de Cursos")
st.write(
    """
    Esta gráfica compara el número de inscripciones con el número de 
    finalizaciones para cada curso, permitiendo ver qué tan efectivos son 
    los cursos en mantener a los participantes hasta el final. Un curso con 
    una alta tasa de finalización indica un buen nivel de compromiso de los 
    usuarios.
    """
)

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

# Create figure for course completion rates
fig, ax = plt.subplots(figsize=(10, 6))
sns.barplot(data=completion_rates_df, x='COMPLETION_RATE', y='NOMBRE_CURSO', ax=ax)
ax.set_title('Tasas de Finalización de Cursos')
ax.set_xlabel('Porcentaje de Finalización (%)')

# Display the figure in Streamlit
st.pyplot(fig)

# User invitation recommendations (users who attended sessions but haven't enrolled in courses)
st.write("### Recomendaciones de Invitación a Cursos")
st.write(
    """
    Esta tabla muestra a los usuarios que han asistido a sesiones pero 
    aún no se han inscrito en ningún curso. Este grupo es ideal para 
    recibir invitaciones a cursos, ya que ya han demostrado interés 
    al participar en las sesiones.
    """
)

invite_recommendations_result = session.sql("""
    SELECT C.NOMBRE, C.APELLIDO, C.CORREO, COUNT(A.ID_USUARIO) AS sesiones_asistidas
    FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD AS C
    LEFT JOIN LABORATORIO.MONICA_SOBERON.ASISTENCIA_SESION AS A
    ON C.ID_USUARIO = A.ID_USUARIO
    LEFT JOIN LABORATORIO.MONICA_SOBERON.REGISTRADOS_CURSO AS R
    ON C.ID_USUARIO = R.ID_USUARIO
    WHERE R.ID_USUARIO IS NULL  -- No course registration
    GROUP BY C.NOMBRE, C.APELLIDO, C.CORREO
    HAVING COUNT(A.ID_USUARIO) > 0
    ORDER BY sesiones_asistidas DESC;
""")
invite_recommendations_df = invite_recommendations_result.to_pandas()

st.write("**Usuarios a Invitar a Cursos**")
st.write(invite_recommendations_df)
