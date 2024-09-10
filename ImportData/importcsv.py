import pandas as pd
from sqlalchemy import create_engine
import pymysql

# install pymysql as the MySQLdb interface 
pymysql.install_as_MySQLdb()

# generate database URIs for 10 separate databases, each with its own connection details
database_uris = [f'mysql+mysqldb://root:Dsci-551@localhost/DB_{i}' for i in range(10)]

# calculate which database to use based on the last digit of the subject_id
def calculate_database(subject_id):
    return int(str(subject_id)[-1])

# connect to all specified databases and return a dictionary of engine connections
def connect_to_databases(uris):
    engines = {}
    for i, uri in enumerate(uris):
        db_key = f'DB_{i}'
        engines[db_key] = create_engine(uri)
    return engines

# insert data into the appropriate database based on the hash of subject_id
def insert_data_based_on_hash(engines, csv_file_path, table_name, key):
    # read the CSV file in chunks for efficient memory management
    for chunk in pd.read_csv(csv_file_path, chunksize=10000):
        # assign each row to a target database based on the subject_id
        chunk['target_db'] = chunk[key].apply(lambda x: f'DB_{calculate_database(x)}')
        for db_key in engines.keys():
            # filter rows for each database
            chunk_to_insert = chunk[chunk['target_db'] == db_key].drop('target_db', axis=1)
            if not chunk_to_insert.empty:
                # insert the data into the correct table and database
                chunk_to_insert.to_sql(name=table_name, con=engines[db_key], if_exists='append', index=False)
                print(f"Inserted {len(chunk_to_insert)} rows into {db_key}.")
    print(f"Insertion for {table_name} completed\n")

# list of CSV file paths to be processed
csv_file_path = ['ADMISSIONS.csv', 'CHARTEVENTS.csv', 'D_ICD_DIAGNOSES.csv', 'LABEVENTS.csv', 'PATIENTS.csv']

# process each CSV file and insert data into the corresponding database
for file in csv_file_path:
    if file == "ADMISSIONS.csv":
        table_name = 'admissions' 
        key = 'subject_id'
        engines = connect_to_databases(database_uris)
        insert_data_based_on_hash(engines, file, table_name, key)
    elif file == "CHARTEVENTS.csv":
        table_name = 'chartevents'
        key = 'subject_id' 
        engines = connect_to_databases(database_uris)
        insert_data_based_on_hash(engines, file, table_name, key)
    elif file == "D_ICD_DIAGNOSES.csv":
        table_name = 'diagnoses'
        key = 'subject_id' 
        engines = connect_to_databases(database_uris)
        insert_data_based_on_hash(engines, file, table_name, key)
    elif file == "LABEVENTS.csv":
        table_name = 'labevents'
        key = 'subject_id' 
        engines = connect_to_databases(database_uris)
        insert_data_based_on_hash(engines, file, table_name, key)
    elif file == "PATIENTS.csv":
        table_name = 'patients' 
        key = 'SUBJECT_ID'
        engines = connect_to_databases(database_uris)
        insert_data_based_on_hash(engines, file, table_name, key)
