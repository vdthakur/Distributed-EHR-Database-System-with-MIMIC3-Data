import pymysql
import mysql.connector
from sqlalchemy import create_engine, text

from tableone import TableOne, load_dataset
from scipy.stats import chi2_contingency
from scipy.stats import ttest_ind
from pandas.plotting import table 
from scipy.stats import f_oneway, ttest_ind, chi2_contingency
from scipy import stats

import streamlit as st
import plotly.express as  px
import warnings
import os
import pandas as pd
import plotly.graph_objects as go
import io
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
warnings.filterwarnings("ignore")


st.set_option('deprecation.showPyplotGlobalUse', False)
import plotly.graph_objects as go

pymysql.install_as_MySQLdb()




################## Connecting to Database on Subject ID and Querying Information ###################    
    

# Function to connect to the database corresponding to the hash of the subject_id
def connect_to_database(subject_id):
    db_index = int(str(subject_id)[-1])  # hash function identifies last digit of subject_id
    uri = f'mysql+pymysql://root:Dsci-551@localhost/DB_{db_index}'
    engine = create_engine(uri)
    return engine




# Function to query data for a specific subject_id from the database
def query_data_by_subject_id(engine, subject_id):
    with engine.connect() as conn:
        query = f"""SELECT 
                        ce.subject_id,  ce.charttime, ce.label, 
                        ce.category, ce.unitname, p.gender, p.dob, 
                        p.dod,p.expire_flag, a.diagnosis, a.hospital_expire_flag, 
                        d.icd9_code, d.short_title,
                        
                        CASE 
                            WHEN p.expire_flag = 0 THEN 'Survivor'
                            WHEN p.expire_flag = 1 THEN 'Non-Survivor'
                            ELSE NULL
                        END AS status
                        
                        
                        FROM chartevents ce 
                        JOIN patients p ON ce.subject_id = p.subject_id 
                        JOIN admissions a ON ce.subject_id = a.subject_id 
                        JOIN diagnosis d ON ce.subject_id = d.subject_id 
                        JOIN labevents le ON ce.subject_id = le.subject_id WHERE ce.subject_id = {subject_id}"""
        
        result = conn.execute(text(query)).fetchall()
        
        
    return pd.DataFrame(result, columns=['ce_subject_id',  'ce_charttime', 'ce_label', 'ce_category', 'ce_unitname', 'p_gender', 'p_dob', 'p_dod', 'p_expire_flag', 'a_diagnosis', 'a_hospital_expire_flag','d_icd9_code', 'd_short_title', 'status'])




# Function to query patient profile data
def query_patient_profile(engine, subject_id):
    with engine.connect() as conn:
        query = f"""SELECT 
                        gender, dob, dod, expire_flag,
                        CASE 
                            WHEN expire_flag = 0 THEN 'Survivor'
                            WHEN expire_flag = 1 THEN 'Non-Survivor'
                            ELSE NULL
                        END AS status                            
                        FROM patients 
                        WHERE subject_id = {subject_id}"""
        result = conn.execute(text(query)).fetchall()
    return pd.DataFrame(result, columns=['gender', 'dob', 'dod', 'expire_flag','status'])




# Function to query admission history data
def query_admission_history(engine, subject_id):
    with engine.connect() as conn:
        query = f"""SELECT admittime, dischtime, deathtime, admission_location, diagnosis, hospital_expire_flag,
                        CASE 
                            WHEN hospital_expire_flag = 0 THEN 'Survivor'
                            WHEN hospital_expire_flag = 1 THEN 'Non-Survivor'
                            ELSE NULL
                        END AS status                     
                    
                    FROM admissions 
                    WHERE subject_id = {subject_id}"""
        result = conn.execute(text(query)).fetchall()
    return pd.DataFrame(result, columns=['Admit time', 'Disch time', 'Death time', 'Admission Location','Diagnosis', 'Hospital expire flag','status'])



# Function to query diagnosis data
def query_diagnosis(engine, subject_id):
    with engine.connect() as conn:
        query = f"""SELECT icd9_code, short_title, long_title FROM diagnosis WHERE subject_id = {subject_id}"""
        result = conn.execute(text(query)).fetchall()
    return pd.DataFrame(result, columns=['Disease icd9 code', 'Diagnosis', 'Description'])



# Function to query lab events data
def query_lab_events(engine, subject_id):
    with engine.connect() as conn:
        query = f"""SELECT charttime, label, valuenum, valueuom, category  FROM labevents WHERE subject_id = {subject_id}"""
        result = conn.execute(text(query)).fetchall()
    return pd.DataFrame(result, columns=['Event chart time', 'Test Name', 'Reading', 'Unit', 'Category'])



# Function to query vitals data
def query_vitals(engine, subject_id):
    with engine.connect() as conn:
        query = f"""SELECT charttime, label, valuenum, valueuom, category  FROM chartevents WHERE subject_id = {subject_id}"""
        result = conn.execute(text(query)).fetchall()
    return pd.DataFrame(result, columns=['Event chart time', 'Test Name', 'Reading', 'Unit', 'Category'])






################ Functions for MIMIC Data Statsistics and Analytics ###################

def chisq_test(bp_data1, variable, group):
    bp_data1 = bp_data1.dropna(subset=[variable, group])

    contingency_table = pd.crosstab(bp_data1[group], bp_data1[variable])
    chi2, p_val, _, _ = chi2_contingency(contingency_table)

    result = {'Variable': variable, 'Group': [group], 'Test': 'Chi-square test', 'Statistic': chi2, 'P-value': p_val}

    # Plot using Plotly
    fig = px.histogram(bp_data1, x=variable, color=group, barmode='group', category_orders={variable: sorted(bp_data1[variable].unique())})
    fig.update_layout(title=f"Counts of {variable} by {group}", xaxis_title= variable, yaxis_title="Count")
    fig.update_traces(marker_line_color='black', marker_line_width=1.5)
    fig.update_layout(margin=dict(r=100))

    fig.update_layout(width=500)
    
    # Display results and plot side by side
    col1, col3 = st.columns(2)
    with col1:
        st.plotly_chart(fig)
    with col3:
        st.markdown("<h5>Summary Statistics</h5>", unsafe_allow_html=True)
        st.write(pd.DataFrame(result))
        
    return result


def pp(database_uris):
    
    diag_rows = set()
    for uri in database_uris:
        engine = create_engine(uri)
        with engine.connect() as conn:
    
            query = """    
                    SELECT
                        pat.gender,
                        ROUND(DATEDIFF(adm.admittime, pat.dob) / 365.25, 4) AS 'Age (Years)',
                        ROUND(DATEDIFF(adm.dischtime, adm.admittime) / 7, 4) AS 'Admit Duration (Weeks)',
                        pat.expire_flag,
                        
                        CASE 
                            WHEN pat.expire_flag = 0 THEN 'Survivor'
                            WHEN pat.expire_flag = 1 THEN 'Non-Survivor'
                            ELSE NULL
                        END AS status
                        
                    FROM
                        admissions adm
                    JOIN
                        patients pat
                    ON
                        pat.subject_id = adm.subject_id;
                        """
            result = conn.execute(text(query)).fetchall()
            diag_rows.update(row for row in result)
            
    return diag_rows    



def get_num_diagnoses(database_uris):
    
    diag_rows = set()
    for uri in database_uris:
        engine = create_engine(uri)
        with engine.connect() as conn:
            query = """
                SELECT p.subject_id , count(d.icd9_code) as num_diagnoses, p.expire_flag,
                       CASE 
                            WHEN p.expire_flag = 0 THEN 'Survivor'
                            WHEN p.expire_flag = 1 THEN 'Non-Survivor'
                            ELSE NULL
                        END AS status
                
                FROM patients p
                JOIN diagnosis d ON d.subject_id = p.subject_id
                group by p.subject_id
            """

            result = conn.execute(text(query)).fetchall()
            diag_rows.update(row for row in result)
    return diag_rows



def get_resp_rate(database_uris):
    
    diag_rows = set()
    for uri in database_uris:
        engine = create_engine(uri)
        with engine.connect() as conn:
            query = """
                        select chartevents.subject_id, 
                            max(gender) as Gender, 
                            max(value) as 'Max Respiratory Rate', 
                            'BPM' AS 'Value Unit',
                            chartevents.itemid,
                            max(expire_flag) as 'Expiry Flag',
                            
                            CASE 
                                WHEN MAX(expire_flag) = 0 THEN 'Survivor'
                                WHEN MAX(expire_flag) = 1 THEN 'Non-Survivor'
                                ELSE NULL
                            END AS status
                            
                            
                            from chartevents
                            join  patients on patients.subject_id = chartevents.subject_id
                            where chartevents.itemid in (618, 220210, 224689, 224690) and valueuom like '%insp%'
                            group by chartevents.subject_id, chartevents.itemid 
                            
                            
                            
                            
                            """
    
            result = conn.execute(text(query)).fetchall()
            diag_rows.update(row for row in result)  
            
    return diag_rows            
    

    
    
def get_temp(database_uris):
    
    diag_rows = set()
    for uri in database_uris:
        engine = create_engine(uri)
        with engine.connect() as conn:
            query = """
                        select chartevents.subject_id,  
                            max(value) as 'Max Temperature', 
                            'Deg C' AS 'Value Unit',
                            max(expire_flag) as 'Expiry Flag',
                            
                            CASE 
                                WHEN MAX(expire_flag) = 0 THEN 'Survivor'
                                WHEN MAX(expire_flag) = 1 THEN 'Non-Survivor'
                                ELSE NULL
                            END AS status
                            
                            from chartevents
                            join  patients on patients.subject_id = chartevents.subject_id
                            where chartevents.itemid in (677)
                            group by chartevents.subject_id, chartevents.itemid """
    
            result = conn.execute(text(query)).fetchall()
            diag_rows.update(row for row in result)  
            
    return diag_rows  
 
    
    

def get_wbc(database_uris):
    
    diag_rows = set()
    for uri in database_uris:
        engine = create_engine(uri)
        with engine.connect() as conn:
            query = """
                      select max(value) as 'Max WBC count', 
                            'K/uL' AS 'Value Unit',
                            max(expire_flag) as 'ExpFlag',
                            
                            CASE 
                                WHEN MAX(expire_flag) = 0 THEN 'Survivor'
                                WHEN MAX(expire_flag) = 1 THEN 'Non-Survivor'
                                ELSE NULL
                            END AS status
                            
                            
                            from labevents
                            join  patients on patients.subject_id = labevents.subject_id
                            where labevents.label like '%white blood cell%'
                            group by labevents.subject_id, labevents.itemid"""
    
            result = conn.execute(text(query)).fetchall()
            diag_rows.update(row for row in result)  
            
    return diag_rows    




def min_hemo(database_uris):

    diag_rows = set()
    for uri in database_uris:
        engine = create_engine(uri)
        with engine.connect() as conn:
            
            query = """
                    SELECT
                        MIN(CAST(value AS DECIMAL)) AS 'Min Hemoglobin',
                        MAX(expire_flag) AS 'ExpFlag',
                        
                        CASE 
                            WHEN MAX(expire_flag) = 0 THEN 'Survivor'
                            WHEN MAX(expire_flag) = 1 THEN 'Non-Survivor'
                            ELSE NULL
                        END AS status
                        
                        
                    FROM
                        labevents
                    JOIN
                        patients ON patients.subject_id = labevents.subject_id
                    WHERE
                        labevents.label LIKE '%hemoglobin%'
                        AND value REGEXP '^-?[0-9]+\.?[0-9]*$' -- Filter condition for numerical values
                    GROUP BY
                        labevents.subject_id, labevents.itemid;

            """
            result = conn.execute(text(query)).fetchall()
            diag_rows.update(row for row in result)  
            
    return diag_rows                

    
def boxplot_fn(bp_data1, group, variable, description=None, conclusion=None):
    
    # Plotly box plot on left
    fig = go.Figure()
    for groups, data in bp_data1.groupby(group):
        fig.add_trace(go.Box(y=data[variable], name=groups))

    fig.update_layout(
        title= variable + ' across ' + group,
        xaxis=dict(title = group),
        yaxis=dict(title =  variable),
        width=400, 
        height=400,
        margin=dict(t=50, b=50, l=50, r=50)
    )

    # Perform t-test between all group pairs
    results = []
    groups = bp_data1[group].unique()
    for i in range(len(groups)):
        for j in range(i+1, len(groups)):
            group1 = bp_data1[bp_data1[group] == groups[i]][variable].astype(float)
            group2 = bp_data1[bp_data1[group] == groups[j]][variable].astype(float)
            t_stat, p_val = ttest_ind(group1, group2)
            results.append({'Group 1': groups[i], 'Group 2': groups[j], 'Variable': variable,
                            'Test': 't-test', 'Statistic': t_stat, 'P-value': p_val})

            
    # Create a DataFrame of t-test results
    results_df = pd.DataFrame(results)

    # Display Plotly figure and t-test results on right
    col1, col2 = st.columns(2)
    with col1:
        st.plotly_chart(fig)
    with col2:
        with st.expander("Description", expanded=True):
            st.write(description)
        st.markdown("---")
        with st.expander("Conclusion", expanded=True):
            st.write(conclusion)
        st.markdown("---")
        st.write(results_df)



def plot_top_diagnoses(database_uris):
    diagnoses_count = {}
    top_n=5
    for uri in database_uris:
        engine = create_engine(uri)
        with engine.connect() as conn:
            query = """
                SELECT d.short_title, COUNT(*) AS count
                FROM diagnosis d
                GROUP BY d.short_title
                ORDER BY count DESC
                LIMIT 5
            """

            result = conn.execute(text(query)).fetchall()

            for row in result:
                short_title, count = row
                diagnoses_count[short_title] = diagnoses_count.get(short_title, 0) + count

    # Convert the dictionary into a DataFrame for plotting
    diagnoses_df = pd.DataFrame(diagnoses_count.items(), columns=['Diagnosis', 'Count'])

    # Plot using Plotly
    fig = px.bar(diagnoses_df, x='Diagnosis', y='Count', title=f'Most Common Diagnoses')
    st.plotly_chart(fig)

    
    
##################### STORING NEW INFO OF USERS ######################


# Function to connect to the database
def connect_to_database_insertion(dbn):
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="Dsci-551",
        database=dbn
    )



# Function to store patient information in the database
def store_patient_information(conn, subject_id, gender, dob, dod, expire_flag):
    cursor = conn.cursor()
    query = "INSERT INTO user_patients (subject_id, gender, dob, dod, expire_flag) VALUES (%s, %s, %s, %s, %s)"
    cursor.execute(query, (subject_id, gender, dob, dod, expire_flag))
    conn.commit()
    cursor.close()
    
    

# Function to store patient information in the database
def store_admit_information(conn, subject_id, admittime, dischtime, hosp_adm_id, diagnosis, expire_flag):
    cursor = conn.cursor()
    query = "INSERT INTO user_admissions (subject_id, hadm_id, admittime, dischtime, diagnosis, hospital_expire_flag) VALUES (%s, %s, %s, %s, %s, %s)"
    cursor.execute(query, (subject_id, hosp_adm_id, admittime, dischtime, diagnosis, expire_flag))
    conn.commit()
    cursor.close()
    
    
    
# Function to store patient information in the database
def store_diag_information(conn, subject_id, idc9_code, hosp_adm_id, diagnosis, descr):
    cursor = conn.cursor()
    query = "INSERT INTO user_diagnosis (icd9_code, short_title, long_title, subject_id, hadm_id, seq_num) VALUES (%s, %s, %s, %s, %s, %s)"
    cursor.execute(query, ( idc9_code, diagnosis, descr ,subject_id, hosp_adm_id, hosp_adm_id))
    conn.commit()
    cursor.close()
    
    

# Function to store patient information in the database
def store_lab_information(conn, subject_id, hadm_id, lcharttime, wbc_count, hemo, itemid1,itemid2):
    cursor = conn.cursor()
    query = "INSERT INTO user_labevents (subject_id, hadm_id, itemid, charttime, value, label) VALUES (%s, %s, %s, %s, %s, %s)"
    cursor.execute(query, (subject_id, hadm_id, itemid1, lcharttime, wbc_count, 'White Blood Cells'))
    
    query = "INSERT INTO user_labevents (subject_id, hadm_id, itemid, charttime, value, label) VALUES (%s, %s, %s, %s, %s, %s)"
    cursor.execute(query, (subject_id,  hadm_id, itemid2, lcharttime, hemo, 'Hemoglobin'))
    
    conn.commit()
    cursor.close()
    
    
    
# Function to store patient information in the database
def store_vital_information(conn, subject_id, hadm_id, itemid, ceventtime, resprate, tempp):
    cursor = conn.cursor()
    query = "INSERT INTO user_chartevents (subject_id, hadm_id, itemid, charttime, value, label) VALUES (%s,%s,%s, %s, %s, %s)"
    cursor.execute(query, (subject_id, hadm_id,  itemid,ceventtime, tempp, 'Temperature'))
    
    query = "INSERT INTO user_chartevents (subject_id, hadm_id, itemid, charttime, value, label) VALUES (%s,%s,%s, %s, %s, %s)"
    cursor.execute(query, (subject_id, hadm_id,  itemid,ceventtime, resprate, 'Respiratory Rate'))
    
    conn.commit()
    cursor.close() 
    
            
# Function to delete all information in all the user databases
def delete_all_user_data(database_uris):
    for uri in database_uris:
        engine = create_engine(uri)
        with engine.connect() as conn:
            query = text("DELETE FROM user_labevents")
            conn.execute(query)
            query = text("DELETE FROM user_diagnosis")
            conn.execute(query)
            query = text("DELETE FROM user_chartevents")
            conn.execute(query)
            query = text("DELETE FROM user_admissions")
            conn.execute(query)
            query = text("DELETE FROM user_patients")
            conn.execute(query)
            
            conn.commit()

    
    
    
    
################ Functions for User Data Statsistics and Analytics ###################


def pp_user(database_uris):
    
    diag_rows = set()
    for uri in database_uris:
        engine = create_engine(uri)
        with engine.connect() as conn:
    
            query = """    
                    SELECT
                        pat.gender,
                        ROUND(DATEDIFF(adm.admittime, pat.dob) / 365.25, 4) AS 'Age (Years)',
                        ROUND(DATEDIFF(adm.dischtime, adm.admittime) / 7, 4) AS 'Admit Duration (Weeks)',
                        pat.expire_flag,
                        
                        CASE 
                            WHEN pat.expire_flag = 0 THEN 'Survivor'
                            WHEN pat.expire_flag = 1 THEN 'Non-Survivor'
                            ELSE NULL
                        END AS status
                        
                    FROM
                        user_admissions adm
                    JOIN
                        user_patients pat
                    ON
                        pat.subject_id = adm.subject_id;
                        """
            result = conn.execute(text(query)).fetchall()
            diag_rows.update(row for row in result)
            
    return diag_rows    



def get_num_diagnoses_user(database_uris):
    
    diag_rows = set()
    for uri in database_uris:
        engine = create_engine(uri)
        with engine.connect() as conn:
            query = """
                SELECT p.subject_id , count(d.icd9_code) as num_diagnoses, p.expire_flag,
                       CASE 
                            WHEN p.expire_flag = 0 THEN 'Survivor'
                            WHEN p.expire_flag = 1 THEN 'Non-Survivor'
                            ELSE NULL
                        END AS status
                
                FROM user_patients p
                JOIN user_diagnosis d ON d.subject_id = p.subject_id
                group by p.subject_id
            """

            result = conn.execute(text(query)).fetchall()
            diag_rows.update(row for row in result)
    return diag_rows



def get_resp_rate_user(database_uris):
    
    diag_rows = set()
    for uri in database_uris:
        engine = create_engine(uri)
        with engine.connect() as conn:
            query = """
                        select user_chartevents.subject_id,  
                            max(value) as 'Max Respiratory Rate', 
                            'Deg C' AS 'Value Unit',
                            max(expire_flag) as 'Expiry Flag',

                            CASE 
                                WHEN MAX(expire_flag) = 0 THEN 'Survivor'
                                WHEN MAX(expire_flag) = 1 THEN 'Non-Survivor'
                                ELSE NULL
                            END AS status

                            from user_chartevents
                            join  user_patients on user_patients.subject_id = user_chartevents.subject_id
                            where user_chartevents.label = "Respiratory Rate"
                            group by user_chartevents.subject_id, user_chartevents.itemid;
                            
                            """
    
            result = conn.execute(text(query)).fetchall()
            diag_rows.update(row for row in result)  
            
    return diag_rows            
    

    
    
def get_temp_user(database_uris):
    
    diag_rows = set()
    for uri in database_uris:
        engine = create_engine(uri)
        with engine.connect() as conn:
            query = """
                        select user_chartevents.subject_id,  
                        max(value) as 'Max Temperature', 
                        'Deg C' AS 'Value Unit',
                        max(expire_flag) as 'Expiry Flag',

                        CASE 
                            WHEN MAX(expire_flag) = 0 THEN 'Survivor'
                            WHEN MAX(expire_flag) = 1 THEN 'Non-Survivor'
                            ELSE NULL
                        END AS status

                        from user_chartevents
                        join  user_patients on user_patients.subject_id = user_chartevents.subject_id
                        where user_chartevents.label = "Temperature"
                        group by user_chartevents.subject_id, user_chartevents.itemid; """
    
            result = conn.execute(text(query)).fetchall()
            diag_rows.update(row for row in result)  
            
    return diag_rows  
 
    
    

def get_wbc_user(database_uris):
    
    diag_rows = set()
    for uri in database_uris:
        engine = create_engine(uri)
        with engine.connect() as conn:
            query = """
                      select max(value) as 'Max WBC count', 
                            'K/uL' AS 'Value Unit',
                            max(expire_flag) as 'ExpFlag',
                            
                            CASE 
                                WHEN MAX(expire_flag) = 0 THEN 'Survivor'
                                WHEN MAX(expire_flag) = 1 THEN 'Non-Survivor'
                                ELSE NULL
                            END AS status
                            
                            
                            from user_labevents
                            join  user_patients on user_patients.subject_id = user_labevents.subject_id
                            where user_labevents.label like '%white blood cell%'
                            group by user_labevents.subject_id, user_labevents.itemid"""
    
            result = conn.execute(text(query)).fetchall()
            diag_rows.update(row for row in result)  
            
    return diag_rows    




def min_hemo_user(database_uris):

    diag_rows = set()
    for uri in database_uris:
        engine = create_engine(uri)
        with engine.connect() as conn:
            
            query = """
                    SELECT
                        MIN(CAST(value AS DECIMAL)) AS 'Min Hemoglobin',
                        MAX(expire_flag) AS 'ExpFlag',
                        
                        CASE 
                            WHEN MAX(expire_flag) = 0 THEN 'Survivor'
                            WHEN MAX(expire_flag) = 1 THEN 'Non-Survivor'
                            ELSE NULL
                        END AS status
                        
                        
                    FROM
                        user_labevents
                    JOIN
                        user_patients ON user_patients.subject_id = user_labevents.subject_id
                    WHERE
                        user_labevents.label LIKE '%hemoglobin%'
                        AND value REGEXP '^-?[0-9]+\.?[0-9]*$' -- Filter condition for numerical values
                    GROUP BY
                        user_labevents.subject_id, user_labevents.itemid;

            """
            result = conn.execute(text(query)).fetchall()
            diag_rows.update(row for row in result)  
            
    return diag_rows                

    



def plot_top_diagnoses_user(database_uris):
    diagnoses_count = {}
    top_n=5
    for uri in database_uris:
        engine = create_engine(uri)
        with engine.connect() as conn:
            query = """
                SELECT d.short_title, COUNT(*) AS count
                FROM user_diagnosis d
                GROUP BY d.short_title
                ORDER BY count DESC
                LIMIT 5
            """

            result = conn.execute(text(query)).fetchall()

            for row in result:
                short_title, count = row
                diagnoses_count[short_title] = diagnoses_count.get(short_title, 0) + count

    # Convert the dictionary into a DataFrame for plotting
    diagnoses_df = pd.DataFrame(diagnoses_count.items(), columns=['Diagnosis', 'Count'])

    # Plot using Plotly
    fig = px.bar(diagnoses_df, x='Diagnosis', y='Count', title=f'Most Common Diagnoses')
    st.plotly_chart(fig)

  






###################### MAIN Streamlit web application #########################
def main():
    
    
    st.set_page_config(page_title="Intensive Care", page_icon=":stethoscope:", layout="wide")
    st.title(":stethoscope: Intensive Care")
    st.write("<p style='font-size: 22px;'> A biostatistics tool for ICU patient risk assessment based on MIMIC3 cohort </p>", unsafe_allow_html=True)

    st.markdown('<style>div.block-container{padding-top:1rem;}</style>', unsafe_allow_html=True)
    
    st.markdown('---')
    
    database_uris = [f'mysql+pymysql://root:Dsci-551@localhost/DB_{i}' for i in range(10)]
    

    st.subheader("Search by Subject ID (Authorized)")
    subject_id = st.number_input("Enter Subject ID:", min_value=1)
    
    
    # Connecting to the database corresponding to the hash of the subject_id
    engine = connect_to_database(subject_id)
    

    col1, col2, col3, col4, col5 = st.columns(5)
    

    if col1.button("Patient Profile"):
        patient_profile = query_patient_profile(engine, subject_id)
        st.subheader("Patient Profile")
        st.write(patient_profile)
    

    if col2.button("Admission History"):
        admission_history = query_admission_history(engine, subject_id)
        st.subheader("Admission History")
        st.write(admission_history)
    

    if col3.button("Diagnosis"):
        diagnosis_data = query_diagnosis(engine, subject_id)
        st.subheader("Diagnosis")
        st.write(diagnosis_data)
    

    if col4.button("Lab Events"):
        lab_events = query_lab_events(engine, subject_id)
        st.subheader("Lab Events")
        st.write(lab_events)
    

    if col5.button("Vitals"):
        vitals_data = query_vitals(engine, subject_id)
        st.subheader("Vitals")
        st.write(vitals_data)
        
        
    st.markdown('---')
        
    st.subheader("Statistical Overview of MIMIC3 cohort")


    ### PATIENT PROFILE ###
    if st.button("Patient Profile and Admission Analytics"):
        
        pdata = pp(database_uris)
        pdata = pd.DataFrame(pdata)
        
        
        st.markdown("---")
        
        variable = 'gender'
        group = 'status'
        res1 = chisq_test(pdata, variable, group)
        
        st.markdown("---")
        
        group= 'status'
        variable = 'Age (Years)'

        group1 = pdata[pdata['status'] == 'Survivor']['Age (Years)'].astype(float)
        group2 = pdata[pdata['status'] == 'Non-Survivor']['Age (Years)'].astype(float)

        t_stat_icv, p_val_icv = ttest_ind(group1, group2)

        description_aget = f"T-test results for Age (grouped by Status):"
        conclusion_aget = f"  - Test: t-test\n  - Statistic: {t_stat_icv}\n  - P-value: {p_val_icv}"

        boxplot_fn(pdata, 'status', 'Age (Years)', description_aget, conclusion_aget)
        st.markdown("---")
        
        group= 'status'
        variable = 'Admit Duration (Weeks)'

        group1 = pdata[pdata['status'] == 'Survivor']['Admit Duration (Weeks)'].astype(float)
        group2 = pdata[pdata['status'] == 'Non-Survivor']['Admit Duration (Weeks)'].astype(float)

        t_stat_icv, p_val_icv = ttest_ind(group1, group2)

        description_aget = f"T-test results for Admit Duration (Weeks) (grouped by Status):"
        conclusion_aget = f"  - Test: t-test\n  - Statistic: {t_stat_icv}\n  - P-value: {p_val_icv}"

        boxplot_fn(pdata, 'status', 'Admit Duration (Weeks)', description_aget, conclusion_aget)
        
        st.markdown("---")

    ### DIAGNOSIS ###
    
    if st.button("Diagnosis Data Analytics"):
        
        subj_diag_counts = get_num_diagnoses(database_uris)
        df_num_diag = pd.DataFrame(subj_diag_counts, columns=['subject_id' , 'num_diagnoses', 'expire_flag','status'])

        
        ### NUM DIAGNOSIS T-TEST ###
        group= 'status'
        variable = 'num_diagnoses'

        group1 = df_num_diag[df_num_diag['status'] == 'Survivor']['num_diagnoses'].astype(float)
        group2 = df_num_diag[df_num_diag['status'] == 'Non-Survivor']['num_diagnoses'].astype(float)
        
        
        t_stat_icv, p_val_icv = ttest_ind(group1, group2)

        description_aget = f"T-test results for num_diagnoses (grouped by Status):"
        conclusion_aget = f"  - Test: t-test\n  - Statistic: {t_stat_icv}\n  - P-value: {p_val_icv}"

        boxplot_fn(df_num_diag, 'status', 'num_diagnoses', description_aget, conclusion_aget)

        st.markdown("---")
        
        plot_top_diagnoses(database_uris)
        
        
        
    ### VITAL DATA ####
    
    if st.button("Vital Data Analytics"):
        
        resp_data = get_resp_rate(database_uris)
        resp_data = pd.DataFrame(resp_data)
        
        group= 'status'
        variable = 'Max Respiratory Rate'

        group1 = resp_data[resp_data['status'] == 'Survivor']['Max Respiratory Rate']
        group2 = resp_data[resp_data['status'] == 'Non-Survivor']['Max Respiratory Rate']


        boxplot_fn(resp_data, 'status', 'Max Respiratory Rate')
        
        st.markdown("---")
        
        ### TEMPERATURE ###
        tmpd = get_temp(database_uris)

        tmpd = pd.DataFrame(tmpd)
        tmpd['status'] = tmpd['Expiry Flag'].map({0: 'Survivor', 1: 'Non-Survivor'})
        
        group= 'status'
        variable = 'Max Temperature'

        group1 = tmpd[tmpd['status'] == 'Survivor']['Max Temperature']
        group2 = tmpd[tmpd['status'] == 'Non-Survivor']['Max Temperature']


        boxplot_fn(tmpd, 'status', 'Max Temperature')
        st.markdown("---")

        


    ##################### LAB EVENTS ##########################
    
    if st.button("Lab Report Data Analytics"):
        
        ### MAX RESPIRATORY RATE ###
        
        wbc_data = get_wbc(database_uris)
        wbc_data = pd.DataFrame(wbc_data)
        
        group= 'status'
        variable = 'Max Respiratory Rate'

        group1 = wbc_data[wbc_data['status'] == 'Survivor']['Max WBC count']
        group2 = wbc_data[wbc_data['status'] == 'Non-Survivor']['Max WBC count']


        boxplot_fn(wbc_data, 'status', 'Max WBC count')
        
        st.markdown("---")
        
        ### MIN HEMOGLOBIN ###
        minhemo = min_hemo(database_uris)

        minhemo = pd.DataFrame(minhemo)
        minhemo['status'] = minhemo['ExpFlag'].map({0: 'Survivor', 1: 'Non-Survivor'})

        
        group= 'status'
        variable = 'Min Hemoglobin'

        group1 = minhemo[minhemo['status'] == 'Survivor']['Min Hemoglobin']
        group2 = minhemo[minhemo['status'] == 'Non-Survivor']['Min Hemoglobin']

        boxplot_fn(minhemo, 'status', 'Min Hemoglobin')


    st.markdown('---')
    
    
    
    
    ######################################## Section for storing patient information #########################################
    st.subheader("Add Patient Data")
    

    subject_id = st.number_input("Enter Subject ID (unique identifier for your patient):", value=0, key="subject_id_input")
    if subject_id:
        db_index = int(str(subject_id)[-1])  # hash function identifies last digit of subject_id
        dbn = f"DB_{db_index}"
        conn = connect_to_database_insertion(dbn)
        cursor = conn.cursor()
        

        st.subheader("User Patient Profile")

        gender = st.radio("Gender", ["Male", "Female", "Null"])
        dob = st.text_input("Datetime of Birth", "0000-00-00 00:00:00")
        dod = st.text_input("Datetime of Death", "0000-00-00 00:00:00")
        expire_flag = st.checkbox("Expire Flag")

        st.subheader("User Admission Data")

        admittime = st.text_input("Datetime of Admission", "0000-00-00 00:00:00")
        dischtime = st.text_input("Datetime of Discharge", "0000-00-00 00:00:00")
        hosp_adm_id = st.number_input("Hospital Admission ID", value=None, step=1)

        st.subheader("User Diagnosis")
        
        idc9_code = st.number_input("idc9_code", value=None, step=1)
        
        diagnosis = st.text_input('Diagnosis')
        descr = st.text_input('Description', None)
        if descr == None:
            descr = diagnosis
        st.subheader("User Lab report Data")
        
        
        lcharttime = st.text_input("Datetime of LabEvent", "0000-00-00 00:00:00")
        itemid1 = st.number_input("WBC Count Lab test code", None)
        itemid2 = st.number_input("Hemoglobin Lab test code", None)
        wbc_count = st.number_input('WBC Count (K/uL)', value=None)
        hemo = st.number_input('Hemoglobin (K/uL)', value=None)
        

        st.subheader("User Vital Signs Data")
        itmid = st.number_input("Event code (unique identifier)", None)
        ceventtime = st.text_input("Datetime of ChartEvent", "0000-00-00 00:00:00")
        resprate = st.number_input('Respiratory Rate', value=None)
        tempp = st.number_input("Temperature", value=None)
        
        
    if st.button("Upload"):
        
        if gender == "Male":
            gender_value = "M"
        elif gender == "Female":
            gender_value = "F"
        else:
            gender_value = None  # Null value

        try:
            store_patient_information(conn, subject_id, gender_value, dob, dod, expire_flag)
            st.success("Patient information stored successfully!")
        except Exception as e:
            st.error(f"Patient information : An error occurred: {str(e)}")
            
        try:
            store_admit_information(conn, subject_id, admittime, dischtime,hosp_adm_id, diagnosis, expire_flag)
            st.success("Admission information stored successfully!")
        except Exception as e:
            st.error(f"Admission information : An error occurred: {str(e)}")

        try:
            store_diag_information(conn, subject_id, idc9_code, hosp_adm_id, diagnosis, descr)
            
            st.success("Diagnosis information stored successfully!")
        except Exception as e:
            st.error(f"Diagnosis information : An error occurred: {str(e)}")
            
        try:

            store_lab_information(conn, subject_id, hosp_adm_id, lcharttime, wbc_count, hemo, itemid1,itemid2)
            
            st.success("Lab Data information stored successfully!")
        except Exception as e:
            st.error(f"Lab Data information : An error occurred: {str(e)}")
            
        try:
            
            store_vital_information(conn, subject_id, hosp_adm_id, itmid, ceventtime, resprate, tempp)
            
            st.success("Vitals information stored successfully!")
            
        except Exception as e:
            st.error(f"Vitals information : An error occurred: {str(e)}")


        
    st.subheader("Statistical Overview of User's Input Data")


    ### PATIENT PROFILE ###
    
    if st.button("User Patient Profile and Admission Analytics"):
        
        try:

            pdata = pp_user(database_uris)
            pdata = pd.DataFrame(pdata)

            st.markdown("---")

            variable = 'gender'
            group = 'status'
            res1 = chisq_test(pdata, variable, group)

            st.markdown("---")

            group= 'status'
            variable = 'Age (Years)'

            group1 = pdata[pdata['status'] == 'Survivor']['Age (Years)'].astype(float)
            group2 = pdata[pdata['status'] == 'Non-Survivor']['Age (Years)'].astype(float)

            t_stat_icv, p_val_icv = ttest_ind(group1, group2)

            description_aget = f"T-test results for Age (grouped by Status):"
            conclusion_aget = f"  - Test: t-test\n  - Statistic: {t_stat_icv}\n  - P-value: {p_val_icv}"

            boxplot_fn(pdata, 'status', 'Age (Years)', description_aget, conclusion_aget)
            st.markdown("---")

            group= 'status'
            variable = 'Admit Duration (Weeks)'

            group1 = pdata[pdata['status'] == 'Survivor']['Admit Duration (Weeks)'].astype(float)
            group2 = pdata[pdata['status'] == 'Non-Survivor']['Admit Duration (Weeks)'].astype(float)

            t_stat_icv, p_val_icv = ttest_ind(group1, group2)

            description_aget = f"T-test results for Admit Duration (Weeks) (grouped by Status):"
            conclusion_aget = f"  - Test: t-test\n  - Statistic: {t_stat_icv}\n  - P-value: {p_val_icv}"

            boxplot_fn(pdata, 'status', 'Admit Duration (Weeks)', description_aget, conclusion_aget)

            st.markdown("---")
            
        except:
            print("User Database Empty")

    ### DIAGNOSIS ###

    if st.button("User Diagnosis Data Analytics"):
        
        try:

            subj_diag_counts = get_num_diagnoses_user(database_uris)
            df_num_diag = pd.DataFrame(subj_diag_counts, columns=['subject_id' , 'num_diagnoses', 'expire_flag', 'status'])


            # NUM DIAGNOSIS T-TEST
            group= 'status'
            variable = 'num_diagnoses'

            group1 = df_num_diag[df_num_diag['status'] == 'Survivor']['num_diagnoses'].astype(float)
            group2 = df_num_diag[df_num_diag['status'] == 'Non-Survivor']['num_diagnoses'].astype(float)


            t_stat_icv, p_val_icv = ttest_ind(group1, group2)

            description_aget = f"T-test results for num_diagnoses (grouped by Status):"
            conclusion_aget = f"  - Test: t-test\n  - Statistic: {t_stat_icv}\n  - P-value: {p_val_icv}"

            boxplot_fn(df_num_diag, 'status', 'num_diagnoses', description_aget, conclusion_aget)

            st.markdown("---")

            plot_top_diagnoses_user(database_uris)

        except:
            
            print("User Database Empty")

            
    ### VITAL DATA ####
    
    if st.button("User Vital Data Analytics"):
        
        try:

            resp_data = get_resp_rate_user(database_uris)
            resp_data = pd.DataFrame(resp_data)

            if 'status' in resp_data.columns:
                group1 = resp_data.loc[resp_data['status'] == 'Survivor', 'Max Respiratory Rate']
                group2 = resp_data.loc[resp_data['status'] == 'Non-Survivor', 'Max Respiratory Rate']

                boxplot_fn(resp_data, 'status', 'Max Respiratory Rate')
            else:
                st.error("The 'status' column is missing in the DataFrame.")



            st.markdown("---")
            


            
            #### TEMP ####
            tmpd = get_temp_user(database_uris)
            tmpd = pd.DataFrame(tmpd)

            group= 'status'
            variable = 'Max Temperature'

            group1 = tmpd[tmpd['status'] == 'Survivor']['Max Temperature']
            group2 = tmpd[tmpd['status'] == 'Non-Survivor']['Max Temperature']


            boxplot_fn(tmpd, 'status', 'Max Temperature')
            st.markdown("---")

        except:
            print("User Database Empty")


    ### LAB EVENTS ####

    if st.button("User Lab Report Data Analytics"):

        ## RESP RATE ###
        
        try:

            wbc_data = get_wbc_user(database_uris)
            wbc_data = pd.DataFrame(wbc_data)

            group= 'status'
            variable = 'Max Respiratory Rate'

            group1 = wbc_data[wbc_data['status'] == 'Survivor']['Max WBC count']
            group2 = wbc_data[wbc_data['status'] == 'Non-Survivor']['Max WBC count']


            boxplot_fn(wbc_data, 'status', 'Max WBC count')

            st.markdown("---")

            ### MIN HEMO ###

            minhemo = min_hemo_user(database_uris)
            minhemo = pd.DataFrame(minhemo)

            group= 'status'
            variable = 'Min Hemoglobin'

            group1 = minhemo[minhemo['status'] == 'Survivor']['Min Hemoglobin']
            group2 = minhemo[minhemo['status'] == 'Non-Survivor']['Min Hemoglobin']

            boxplot_fn(minhemo, 'status', 'Min Hemoglobin')
            
        except:
            print("User Database Empty")


    st.markdown('---')     
    
            
    if st.button("Delete All Data"):
        delete_all_user_data(database_uris)
        st.success("All Data Deleted")
        
#     delete_all_user_data(database_uris)


if __name__ == "__main__":
    main()
