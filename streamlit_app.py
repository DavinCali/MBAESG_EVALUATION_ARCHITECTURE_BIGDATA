# Import python packages
import streamlit as st
import pandas as pd
import snowflake.connector
import altair as alt
from snowflake.snowpark.context import get_active_session

# Write directly to the app
st.title(f"Linkedin")

# Get the current credentials
session = get_active_session()

# 1. Top 10 des titres les plus publiés par industrie
st.header("Top 10 des titres les plus publiés par industrie")
query1 = """
    SELECT industry_id, title, COUNT(*) AS nb_postes
    FROM job_postings jp
    JOIN job_industries ji ON jp.job_id = ji.job_id
    GROUP BY industry_id, title
    ORDER BY nb_postes DESC
    LIMIT 10;
"""
# Exécuter la requête avec Snowpark
df1 = session.sql(query1).to_pandas()

st.bar_chart(df1.set_index('TITLE')['NB_POSTES'])


# 2. Top 10 des postes les mieux rémunérés
st.header("Top 10 des postes les mieux rémunérés par industrie")
query2 = """
    SELECT industry_id, title, MAX(TRY_CAST(max_salary AS FLOAT)) AS max_salary
    FROM job_postings jp
    JOIN job_industries ji ON jp.job_id = ji.job_id
    WHERE max_salary IS NOT NULL
    GROUP BY industry_id, title
    ORDER BY max_salary DESC
    LIMIT 10;
"""
df2 = session.sql(query2).to_pandas()
st.bar_chart(df2.set_index('TITLE')['MAX_SALARY'])


# 3. Camembert taille entreprise
st.header("Répartition des offres par taille d’entreprise")
query3 = """
    SELECT c.company_size, COUNT(*) AS nb_offres
    FROM job_postings jp
    JOIN companies c ON TRY_CAST(jp.company_name AS INT) = TRY_CAST(c.company_id AS INT)
    GROUP BY c.company_size
    ORDER BY c.company_size;
"""
df3 = session.sql(query3).to_pandas()
df3.columns = [col.lower() for col in df3.columns]

pie_chart_3 = alt.Chart(df3).mark_arc().encode(
    theta=alt.Theta(field="nb_offres", type="quantitative"),
    color=alt.Color(field="company_size", type="nominal"),
    tooltip=["company_size", "nb_offres"]
).properties(title="Répartition des offres par taille d’entreprise")

st.altair_chart(pie_chart_3, use_container_width=True)


# 4. Répartition par secteur d’activité
st.header("4️⃣ Répartition des offres par secteur d’activité")
query4 = """
    SELECT industry_id, COUNT(*) AS nb_offres
    FROM job_postings jp
    JOIN job_industries ji ON jp.job_id = ji.job_id
    GROUP BY industry_id
    ORDER BY nb_offres DESC;
"""
df4 = session.sql(query4).to_pandas()
st.bar_chart(df4.set_index('INDUSTRY_ID')['NB_OFFRES'])


# 5. Répartition des offres par type d'emploi
st.header("Répartition par type d’emploi")
query5 = """
    SELECT formatted_work_type, COUNT(*) AS nb_offres
    FROM job_postings
    GROUP BY formatted_work_type
    ORDER BY nb_offres DESC;
"""
df5 = session.sql(query5).to_pandas()

# Renommer colonnes en minuscules pour éviter KeyError
df5.columns = [col.lower() for col in df5.columns]

# Création du camembert avec Altair
pie_chart_5 = alt.Chart(df5).mark_arc().encode(
    theta=alt.Theta(field="nb_offres", type="quantitative"),
    color=alt.Color(field="formatted_work_type", type="nominal"),
    tooltip=["formatted_work_type", "nb_offres"]
).properties(title="Répartition par type d’emploi")

st.altair_chart(pie_chart_5, use_container_width=True)