import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text
import pymysql
import json
import warnings
from datetime import datetime

warnings.filterwarnings("ignore", category=DeprecationWarning, message=".*pyarrow.*")

database_uris = [f'mysql+pymysql://root:Dsci-551@localhost/DB_{i}' for i in range(10)]

# Maps to correct database

def calculate_database(subject_id):
    return int(str(subject_id)[-1])

# Connects to databases

def connect_to_databases(uris):
    engines = {}
    for i, uri in enumerate(uris):
        db_key = f'DB_{i}'
        engines[db_key] = create_engine(uri)
    return engines

def clean_datetime(dt_str):
    dt_str = dt_str.strip()  # Trim whitespace
    if dt_str == 'NULL':
        return None  
    try:
        return datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')
    except ValueError:
        return dt_str

def prepare_record(record, datetime_fields):
    for field in record:
        if field in datetime_fields:
            record[field] = clean_datetime(record[field])
        elif isinstance(record[field], str):  # Only trim non-datetime string fields
            record[field] = record[field].strip()
    return record


### ADMISSION FNCTIONS ###

def insert_admission(engine_dict, record):
    target_db = f"DB_{calculate_database(record['subject_id'])}"
    engine = engine_dict[target_db]
    datetime_fields = ['admittime', 'dischtime', 'deathtime', 'edregtime', 'edouttime']
    record = prepare_record(record, datetime_fields) 
    df = pd.DataFrame([record])
    df.to_sql(name='admissions', con=engine, if_exists='append', index=False)
    print(f"Record inserted into {target_db}.")

def bulk_insert_admissions(engine_dict, records):
    # Assuming records is a list of dictionaries
    for record in records:
        target_db = f"DB_{calculate_database(record['subject_id'])}"
        engine = engine_dict[target_db]
        datetime_fields = ['admittime', 'dischtime', 'deathtime', 'edregtime', 'edouttime']
        record = prepare_record(record, datetime_fields)
        df = pd.DataFrame([record])
        df.to_sql(name='admissions', con=engine, if_exists='append', index=False)
        print(f"Record inserted into {target_db}.")


def update_admission(engine_dict, subject_id, updates):
    target_db = f'DB_{calculate_database(subject_id)}'
    engine = engine_dict[target_db]
    # Prepare datetime fields in updates for correct SQL formatting
    datetime_fields = ['admittime', 'dischtime', 'deathtime', 'edregtime', 'edouttime']
    updates = prepare_record(updates, datetime_fields)
    with engine.connect() as conn:
        # Check if the record exists
        existing = conn.execute(text("SELECT * FROM admissions WHERE subject_id = :subject_id"), {'subject_id': subject_id}).fetchone()
        if existing is None:
            print(f"No admission record found for subject_id {subject_id}.")
            return False  # Indicate that update did not proceed  
        # If a record is found, proceed with update
        update_str = ', '.join([f"{k} = :{k}" for k in updates.keys()])
        sql_command = text(f"UPDATE admissions SET {update_str} WHERE subject_id = :subject_id")
        updates.update({'subject_id': subject_id})
        result = conn.execute(sql_command, updates)
        conn.commit() 
        # Feedback on the outcome of the update operation
        if result.rowcount == 0:
            print("No records updated; check if the updates have valid values and differ from current values.")
        else:
            print(f"{result.rowcount} record(s) updated in {target_db}.")  
        return True  # Indicate successful update

def delete_admission(engine_dict, subject_id):
    target_db = f'DB_{calculate_database(subject_id)}'
    engine = engine_dict[target_db]
    with engine.connect() as conn:
        existing = conn.execute(text("SELECT * FROM admissions WHERE subject_id = :subject_id"), {'subject_id': subject_id}).fetchone()
        if existing is None:
            print(f"No admission record found for subject_id {subject_id}.")
            return
        sql_command = text("DELETE FROM admissions WHERE subject_id = :subject_id")
        result = conn.execute(sql_command, {'subject_id': subject_id})
        conn.commit()
        print(f"{result.rowcount} record(s) deleted from {target_db}.")

def get_admission(engine_dict, subject_id):
    target_db = f"DB_{calculate_database(subject_id)}"
    engine = engine_dict[target_db]
    with engine.connect() as conn:
        sql_command = text(f"SELECT * FROM admissions WHERE subject_id = {subject_id};")
        result = conn.execute(sql_command)
        record = result.fetchone()
        if record:
            print("Admission Record Found:")
            print()
            for column, value in zip(result.keys(), record):
                print(f"{column}: {value}")
            print()
            return True
        else:
            print("No admission record found.")
            return False

### PATIENT FUNCTIONS ###

def insert_patient(engine_dict, record):
    subject_id = record['subject_id']
    target_db = f'DB_{calculate_database(subject_id)}'
    engine = engine_dict[target_db]
    datetime_fields = ['dob', 'dod', 'dod_hosp', 'dod_ssn']
    record = prepare_record(record, datetime_fields) 
    df = pd.DataFrame([record])
    df.to_sql(name='patients', con=engine, if_exists='append', index=False)
    print(f"Patient record inserted into {target_db}.")
    
def bulk_insert_patients(engine_dict, records):
    for record in records:
        target_db = f"DB_{calculate_database(record['subject_id'])}"
        engine = engine_dict[target_db]
        datetime_fields = ['dob', 'dod', 'dod_hosp', 'dod_ssn']
        record = prepare_record(record, datetime_fields)
        df = pd.DataFrame([record])
        df.to_sql(name='patients', con=engine, if_exists='append', index=False)
        print(f"Record inserted into {target_db}.")

def update_patient(engine_dict, subject_id, updates):
    target_db = f'DB_{calculate_database(subject_id)}'
    engine = engine_dict[target_db]
    datetime_fields = ['dob', 'dod', 'dod_hosp', 'dod_ssn']
    updates = prepare_record(updates, datetime_fields)
    with engine.connect() as conn:
        # Check if the record exists
        existing = conn.execute(text("SELECT * FROM patients WHERE subject_id = :subject_id"), {'subject_id': subject_id}).fetchone()
        if existing is None:
            print(f"No patient record found for subject_id {subject_id}.")
            return False  
        update_str = ', '.join([f"{k} = :{k}" for k in updates.keys()])
        sql_command = text(f"UPDATE patients SET {update_str} WHERE subject_id = :subject_id")
        updates.update({'subject_id': subject_id})
        result = conn.execute(sql_command, updates)
        conn.commit()        
        if result.rowcount == 0:
            print("No records updated; check if the updates have valid values and differ from current values.")
        else:
            print(f"{result.rowcount} record(s) updated in {target_db}.")
        return True 


def delete_patient(engine_dict, subject_id):
    target_db = f'DB_{calculate_database(subject_id)}'
    engine = engine_dict[target_db]
    with engine.connect() as conn:
        existing = conn.execute(text("SELECT * FROM patients WHERE subject_id = :subject_id"), {'subject_id': subject_id}).fetchone()
        if existing is None:
            print(f"No patient record found for subject_id {subject_id}.")
            return False  # Indicate that no record was deleted
        sql_command = text("DELETE FROM patients WHERE subject_id = :subject_id")
        result = conn.execute(sql_command, {'subject_id': subject_id})
        conn.commit()
        print(f"{result.rowcount} record(s) deleted from {target_db}.")
        return True  # Indicate successful deletion


def get_patient(engine_dict, subject_id):
    target_db = f"DB_{calculate_database(subject_id)}"
    engine = engine_dict[target_db]
    with engine.connect() as conn:
        # Ensure the SQL command correctly includes the parameter placeholder
        sql_command = text("SELECT * FROM patients WHERE subject_id = :subject_id")
        # Pass the parameter as part of the execute function call
        result = conn.execute(sql_command, {'subject_id': subject_id})
        record = result.fetchone()
        if record:
            print("Patient Record Found:")
            print()
            for column, value in zip(result.keys(), record):
                print(f"{column}: {value}")
            print()
            return True  # Indicate the record exists
        else:
            print("No patient record found.")
            return False


### CHARTEVENT FUNCTIONS ###

def insert_chartevent(engine_dict, record):
    subject_id = record['subject_id']
    target_db = f"DB_{calculate_database(subject_id)}"
    engine = engine_dict[target_db]
    datetime_fields = ['charttime', 'storetime']
    record = prepare_record(record, datetime_fields)
    df = pd.DataFrame([record])
    with engine.connect() as conn:
        df.to_sql(name='chartevents', con=conn, if_exists='append', index=False)
        conn.commit()  # Explicit commit
        last_id = conn.execute(text('SELECT LAST_INSERT_ID();')).scalar()
        print(f"Inserted record ID: {last_id} into {target_db}")  # Debugging output
    return last_id

def bulk_insert_chartevents(engine_dict, records):
    for record in records:
        subject_id = record['subject_id']
        target_db = f"DB_{calculate_database(subject_id)}"
        engine = engine_dict[target_db]
        datetime_fields = ['charttime', 'storetime']  # Adjust based on your table's datetime fields
        record = prepare_record(record, datetime_fields)
        df = pd.DataFrame([record])
        df.to_sql(name='chartevents', con=engine, if_exists='append', index=False)
        print(f"Record for subject_id {subject_id} inserted into {target_db}.")

def update_chartevent(engine_dict):
    subject_id = int(input("Enter the subject_id for which you want to update a chartevent: "))
    target_db = f"DB_{calculate_database(subject_id)}"
    engine = engine_dict[target_db]
    with engine.connect() as conn:
        result = conn.execute(text("SELECT id, label, charttime FROM chartevents WHERE subject_id = :subject_id"), {'subject_id': subject_id})
        records = result.mappings().all()  # Use mappings() to get a list of dictionaries
        if not records:
            print(f"No chartevents found for subject_id {subject_id}.")
            return False
        print("Available records:")
        for record in records:
            print(f"ID: {record['id']}, Label: {record['label']}, Charttime: {record['charttime']}")

        record_id = int(input("Enter the ID of the chartevent to update: "))
        existing = conn.execute(text("SELECT * FROM chartevents WHERE id = :id"), {'id': record_id}).fetchone()
        if not existing:
            print(f"No chartevent record found for id {record_id}.")
            return False

        update_input = input("Enter the updates as a JSON string (e.g., {'field':'value'}) - use double quotes: ")
        updates = json.loads(update_input)
        datetime_fields = ['charttime', 'storetime']
        updates = prepare_record(updates, datetime_fields)
        update_str = ', '.join([f"{k} = :{k}" for k in updates.keys()])
        sql_command = text(f"UPDATE chartevents SET {update_str} WHERE id = :id")
        updates.update({'id': record_id})
        result = conn.execute(sql_command, updates)
        conn.commit()

        if result.rowcount == 0:
            print("No records updated; check if the updates have valid values and differ from current values.")
        else:
            print(f"\n{result.rowcount} record(s) updated in {target_db}.\n")
            # Prompt to view the updated record
            view = input("Would you like to view the updated record? (yes/no): ")
            if view.lower() == 'yes':
                result = conn.execute(text("SELECT * FROM chartevents WHERE id = :id"), {'id': record_id})
                updated_record = result.fetchone()
                columns = result.keys()  # Get column names from the result
                print("\nUpdated Chart Event Record:")
                for column, value in zip(columns, updated_record):
                    print(f"{column}: {value}")
                print()
            return True


def delete_chartevent(engine_dict):
    subject_id = int(input("Enter the subject_id for which you want to delete a chartevent: "))
    target_db = f"DB_{calculate_database(subject_id)}"
    engine = engine_dict[target_db]
    with engine.connect() as conn:
        result = conn.execute(text("SELECT id, label, charttime FROM chartevents WHERE subject_id = :subject_id"), {'subject_id': subject_id})
        records = result.fetchall()
        if not records:
            print(f"No chartevents found for subject_id {subject_id}.")
            return False
        print("Available records:")
        for record in records:
            print(f"ID: {record.id}, Label: {record.label}, Charttime: {record.charttime}")
        record_id = int(input("Enter the ID of the chartevent to delete: "))
        existing = conn.execute(text("SELECT * FROM chartevents WHERE id = :id"), {'id': record_id}).fetchone()
        if not existing:
            print(f"No chartevent record found for id {record_id}.")
            return False
        sql_command = text("DELETE FROM chartevents WHERE id = :id")
        result = conn.execute(sql_command, {'id': record_id})
        conn.commit()
        if result.rowcount > 0:
            print(f"Record with ID {record_id} successfully deleted.")
            return True
        else:
            print("No records were deleted. Please check the conditions.")
            return False

def get_chartevent(engine_dict, subject_id):
    # Determine the correct database based on the subject_id
    target_db = f"DB_{calculate_database(subject_id)}"
    engine = engine_dict[target_db]
    with engine.connect() as conn:
        # Query to fetch the most recent record for the given subject_id
        sql_command = text("""
            SELECT * FROM chartevents 
            WHERE subject_id = :subject_id 
            ORDER BY id DESC 
            LIMIT 1
        """)
        result = conn.execute(sql_command, {'subject_id': subject_id})
        record = result.fetchone()
        if record:
            print("\nMost Recent Chart Event Record Found:")
            for column, value in zip(result.keys(), record):
                print(f"{column}: {value}")
            return True
        else:
            print("\nNo chart event record found for subject_id {subject_id}.\n")
            return False



# DIAGNOSIS FUNCTIONS

def insert_diagnosis(engine_dict, record):
    subject_id = record['subject_id']
    target_db = f'DB_{calculate_database(subject_id)}'
    engine = engine_dict[target_db]
    df = pd.DataFrame([record])
    df.to_sql(name='diagnosis', con=engine, if_exists='append', index=False)
    print(f"Diagnosis record inserted into {target_db}.")
    
def bulk_insert_diagnosis(engine_dict, records):
    for record in records:
        target_db = f"DB_{calculate_database(record['subject_id'])}"
        engine = engine_dict[target_db]
        df = pd.DataFrame([record])
        df.to_sql(name='diagnosis', con=engine, if_exists='append', index=False)
        print(f"Record inserted into {target_db}.")

def delete_diagnosis(engine_dict, subject_id):
    target_db = f'DB_{calculate_database(subject_id)}'
    engine = engine_dict[target_db]
    # Check if the record exists
    with engine.connect() as conn:
        existing = conn.execute(text("SELECT * FROM diagnosis WHERE subject_id = :subject_id"), 
                                {'subject_id': subject_id}).fetchone()
        if existing is None:
            print(f"No diagnosis record found for subject_id {subject_id}.")
            return False
        with engine.connect() as conn:
            sql_command = text("DELETE FROM diagnosis WHERE subject_id = :subject_id")
            result = conn.execute(sql_command, {'subject_id': subject_id})
            conn.commit()
            print(f"{result.rowcount} record(s) deleted from {target_db}.")
            return True

def update_diagnosis(engine_dict, subject_id, updates):
    target_db = f'DB_{calculate_database(subject_id)}'
    engine = engine_dict[target_db]
    with engine.connect() as conn:
        # Verify existing record
        existing = conn.execute(
            text("SELECT * FROM diagnosis WHERE subject_id = :subject_id"),
            {'subject_id': subject_id}
        ).fetchone()
        if not existing:
            print(f"No diagnosis record found for subject_id {subject_id}.")
            return False

        print(f"Existing data for subject_id {subject_id}: {existing}")
        # Prepare and execute update
        update_str = ', '.join([f"{k} = :{k}" for k in updates.keys()])
        sql_command = text(f"UPDATE diagnosis SET {update_str} WHERE subject_id = :subject_id")
        updates.update({'subject_id': subject_id})
        result = conn.execute(sql_command, updates)
        conn.commit()
        print(f"Attempted to update {result.rowcount} record(s).")
        if result.rowcount == 0:
            print("No records updated; check if the updates have valid values and differ from current values.")
        else:
            print(f"{result.rowcount} record(s) updated in {target_db}.")
        return True

def get_diagnosis(engine_dict, subject_id):
    target_db = f"DB_{calculate_database(subject_id)}"
    engine = engine_dict[target_db]
    with engine.connect() as conn:
        sql_command = text(f"SELECT * FROM diagnosis WHERE subject_id = :subject_id;")
        result = conn.execute(sql_command, {'subject_id': subject_id})
        record = result.fetchone()
        if record:
            print("Diagnosis Record Found:")
            print()
            for column, value in zip(result.keys(), record):
                print(f"{column}: {value}")
            print()
            return True
        else:
            print("No diagnosis record found.")
            return False

# LABEVENT Functions

def insert_labevent(engine_dict, record):
    subject_id = record['subject_id']
    target_db = f"DB_{calculate_database(subject_id)}"
    engine = engine_dict[target_db]
    with engine.connect() as conn:
        df = pd.DataFrame([record])
        df.to_sql(name='labevents', con=conn, if_exists='append', index=False)
        conn.execute(text('SELECT 1'))  # Dummy query to ensure transaction is not empty
        conn.commit()  # Committing the transaction to ensure LAST_INSERT_ID() works as expected
        # Fetch the last inserted ID within the same connection immediately after commit
        last_id = conn.execute(text('SELECT LAST_INSERT_ID();')).scalar()
        print(f"Inserted record ID: {last_id} into {target_db}")  # Debugging output
        return last_id
    
def bulk_insert_labevents(engine_dict, records):
    for record in records:
        subject_id = record['subject_id']
        target_db = f"DB_{calculate_database(subject_id)}"
        engine = engine_dict[target_db]
        datetime_fields = ['charttime']  # Assume only 'charttime' needs formatting
        record = prepare_record(record, datetime_fields)
        df = pd.DataFrame([record])
        df.to_sql(name='labevents', con=engine, if_exists='append', index=False)
        print(f"Record for subject_id {subject_id} inserted into {target_db}.")
        
def delete_labevent(engine_dict):
    try:
        subject_id = int(input("Enter the subject_id for which you want to delete a labevent: "))
    except ValueError:
        print("Invalid input: subject_id must be a number.")
        return False
    target_db = f"DB_{calculate_database(subject_id)}"
    engine = engine_dict[target_db]
    with engine.connect() as conn:
        result = conn.execute(text("SELECT id, label, charttime FROM labevents WHERE subject_id = :subject_id"), {'subject_id': subject_id})
        records = result.fetchall()
        if not records:
            print(f"No labevents found for subject_id {subject_id}.")
            return False
        print("Available records:")
        for record in records:
            print(f"ID: {record.id}, Label: {record.label}, Charttime: {record.charttime}")
        try:
            record_id = int(input("Enter the ID of the labevent to delete: "))
        except ValueError:
            print("Invalid input: ID must be a number.")
            return False
        existing = conn.execute(text("SELECT * FROM labevents WHERE id = :id"), {'id': record_id}).fetchone()
        if not existing:
            print(f"No labevent record found for id {record_id}.")
            return False
        sql_command = text("DELETE FROM labevents WHERE id = :id")
        result = conn.execute(sql_command, {'id': record_id})
        conn.commit()
        if result.rowcount > 0:
            print(f"Record with ID {record_id} successfully deleted.")
            return True
        else:
            print("No records were deleted. Please check the conditions.")
            return False


def update_labevent(engine_dict):
    subject_id = int(input("Enter the subject_id for which you want to update a labevent: "))
    target_db = f"DB_{calculate_database(subject_id)}"
    engine = engine_dict[target_db]
    with engine.connect() as conn:
        result = conn.execute(text("SELECT id, label, charttime FROM labevents WHERE subject_id = :subject_id"), {'subject_id': subject_id})
        records = result.mappings().all()  # Use mappings() to get a list of dictionaries
        if not records:
            print(f"No labevents found for subject_id {subject_id}.")
            return False
        print("Available records:")
        for record in records:
            print(f"ID: {record['id']}, Label: {record['label']}, Charttime: {record['charttime']}")

        record_id = int(input("Enter the ID of the labevent to update: "))
        existing = conn.execute(text("SELECT * FROM labevents WHERE id = :id"), {'id': record_id}).fetchone()
        if not existing:
            print(f"No labevent record found for id {record_id}.")
            return False
        update_input = input("Enter the updates as a JSON string (e.g., {'field':'value'}) - use double quotes: ")
        updates = json.loads(update_input)
        datetime_fields = ['charttime']  # Ensure this list contains all datetime fields in your labevents table
        updates = prepare_record(updates, datetime_fields)
        update_str = ', '.join([f"{k} = :{k}" for k in updates.keys()])
        sql_command = text(f"UPDATE labevents SET {update_str} WHERE id = :id")
        updates.update({'id': record_id})
        result = conn.execute(sql_command, updates)
        conn.commit()
        if result.rowcount == 0:
            print("No records updated; check if the updates have valid values and differ from current values.")
        else:
            print(f"\n{result.rowcount} record(s) updated in {target_db}.\n")
            # Prompt to view the updated record
            view = input("Would you like to view the updated record? (yes/no): ")
            if view.lower() == 'yes':
                result = conn.execute(text("SELECT * FROM labevents WHERE id = :id"), {'id': record_id})
                updated_record = result.fetchone()
                columns = result.keys()  # Get column names from the result
                print("\nUpdated Lab Event Record:")
                for column, value in zip(columns, updated_record):
                    print(f"{column}: {value}")
                print()
            return True



def get_labevent(engine_dict, subject_id):
    target_db = f"DB_{calculate_database(subject_id)}"
    engine = engine_dict[target_db]
    with engine.connect() as conn:
        # Fetch the latest record for this subject_id
        sql_command = text("""
            SELECT * FROM labevents 
            WHERE subject_id = :subject_id 
            ORDER BY id DESC 
            LIMIT 1
        """)
        result = conn.execute(sql_command, {'subject_id': subject_id})
        record = result.fetchone()
        if record:
            print("\nMost Recent Lab Event Record Found:")
            for column, value in zip(result.keys(), record):
                print(f"{column}: {value}")
            print()
            return True
        else:
            print(f"\nNo lab event record found for subject_id {subject_id}.\n")
            return False


# Headers for each input
TABLE_COLUMNS = {
    'admissions': ['subject_id', 'hadm_id', 'admittime', 'dischtime', 'deathtime', 'admission_type', 'admission_location', 'discharge_location', 'insurance', 'language', 'religion', 'marital_status', 'ethnicity', 'edregtime', 'edouttime', 'diagnosis', 'hospital_expire_flag', 'has_chartevents_data'],
    'chartevents': ['subject_id', 'hadm_id', 'icustay_id', 'itemid', 'charttime', 'storetime', 'cgid', 'value', 'valuenum', 'valueuom', 'warning', 'error', 'resultstatus', 'stopped', 'label', 'abbreviation', 'dbsource', 'category', 'unitname', 'param_type', 'conceptid'],
    'patients': ['subject_id', 'gender', 'dob', 'dod', 'dod_hosp', 'dod_ssn', 'expire_flag'],
    'diagnosis': ['icd9_code', 'short_title', 'long_title', 'subject_id', 'hadm_id', 'seq_num'],
    'labevents': ['subject_id', 'hadm_id', 'itemid', 'charttime', 'value', 'valuenum', 'valueuom', 'flag', 'label', 'fluid', 'category', 'loinc_code']
}

import json

def main():
    # Create list of engines for each database
    engines = connect_to_databases(database_uris)
    while True:
        # Menu
        print("\nWelcome to the Database Manager!")
        print("1. Insert a record")
        print("2. Bulk Insertions")
        print("3. Update a record")
        print("4. Delete a record")
        print("5. Exit")
        
        choice = input("Enter your choice (1-5): ")

        # Invalid choice
        if choice not in ['1', '2', '3', '4','5']:
            print("Invalid choice. Please enter a number between 1 and 5.")
            continue  # This will skip the rest of the loop and start over

        # Breaks loop and ends program if ended
        if choice == '5':
            print("Exiting the Database Manager.")
            break

        # Menu for which table you want to interact with
        print("\nChoose a table to interact with:")
        print("a. Admissions")
        print("b. Chart Events")
        print("c. Lab Events")
        print("d. Patients")
        print("e. Diagnosis")
        table_choice = input("Enter your choice (a-e): ").lower()

        # Maps inputs to actual table name
        table_mapping = {
            'a': 'admissions',
            'b': 'chartevents',
            'c': 'labevents',
            'd': 'patients',
            'e': 'diagnosis'
        }

        # Creates a map of a list of functions for each table - insert, bulk, update, delete, and get
        if table_choice in table_mapping:
            table_name = table_mapping[table_choice]
            record_functions = {
                'admissions': (insert_admission, bulk_insert_admissions, update_admission, delete_admission, get_admission),
                'chartevents': (insert_chartevent, bulk_insert_chartevents, update_chartevent, delete_chartevent, get_chartevent),
                'labevents': (insert_labevent, bulk_insert_labevents, update_labevent, delete_labevent, get_labevent),
                'patients': (insert_patient, bulk_insert_patients, update_patient, delete_patient, get_patient),
                'diagnosis': (insert_diagnosis, bulk_insert_diagnosis, update_diagnosis, delete_diagnosis, get_diagnosis)
            }[table_name]
        # Invalid table choice
        else:
            print("Invalid table choice.")
            continue

        # Shows you the required fields and takes the input
        if choice == '1':
            print(f"\nSelected table: {table_name.capitalize()}")
            print("Required fields for insertion:")
            headers = TABLE_COLUMNS[table_name]
            print(", ".join(headers))
            record_input = input(f"Enter the new record for {table_name} as comma-separated values matching the above fields:\n")
            record_values = record_input.split(',')
            if len(record_values) != len(headers):
                print("Error: The number of values entered does not match the number of fields.")
                continue
            # Prepares the record and uses the 0th function for the table - insert
            record_dict = dict(zip(headers, record_values))
            inserted_id = record_functions[0](engines, record_dict) 
            # If the user wants to view the function, it'll perform the 4th index function for the table which is the get function
            view = input("Would you like to view the most recent record for this subject? (yes/no): ")
            if view.lower() == 'yes':
                print()
                record_functions[4](engines, record_dict['subject_id'])  # Assuming get_latest_chartevent_by_subject is indexed as 3 in functions
                print()
        # Allows you to enter multiple records separated by a newline
        elif choice == '2':
            print(f"\nSelected table for bulk insertion: {table_name.capitalize()}")
            print("Enter multiple records separated by line breaks. Each record should match the required fields.")
            print("Example input for two records (end each record with a newline):")
            print("1234,5678,...\n2345,6789,...")  # Adjust example based on table structure
            # Empty list for the records and prepares them
            bulk_records = []
            while True:
                record_input = input()
                if record_input == "":
                    break
                record_values = record_input.split(',')
                if len(record_values) != len(TABLE_COLUMNS[table_name]):
                    print("Error: The number of values entered does not match the number of fields. Start again.")
                    break
                bulk_records.append(dict(zip(TABLE_COLUMNS[table_name], record_values)))
            if bulk_records:
                # Performs the 1st index function for the table which is bulk insert
                record_functions[1](engines, bulk_records)
        # Update function functions differently for chartevents and labevents vs. the other tables
        elif choice == '3':
            if table_name in ['chartevents', 'labevents']:  # Use the new update process for these tables
                # This will involve auto-generated IDs for events instead of just the subject_id
                success = record_functions[2](engines)  
                if success:
                    # Indicates success
                    print("Update successful.")
            else:
                # Normal update function is called for the rest of the tables, 2nd index function for each table
                item_id = int(input(f"Enter the subject_id of the {table_name} record to update: "))
                # It will perform the get function (4th index function) to show you the record and its fields
                record_exists = record_functions[4](engines, item_id)
                if record_exists:
                    # Enter the update as a JSON string: {"field", "value"}
                    update_input = input("Enter the updates as a JSON string (e.g., {'field':'value'}) - use double quotes  ")
                   # Load the json and perform the update using the 2nd index function
                    updates = json.loads(update_input)
                    success = record_functions[2](engines, item_id, updates)
                    if success:
                        # Perform 4th index function (get) if you want to see newly inserted record
                        view = input("Would you like to view the updated record? (yes/no): ")
                        if view.lower() == 'yes':
                            print()
                            record_functions[4](engines, item_id)
                            print()
        elif choice == '4':
            # For chartevents and labevents, we will be deleting based on auto-generated id rather than subject_id
            # Remember, we query based on subject_id and we choose which ID under the subject_id we want to delete
            if table_name in ['chartevents', 'labevents']:
                delete_function = delete_chartevent if table_name == 'chartevents' else delete_labevent
                delete_function(engines) 
            # Otherwise, delete based on subject_id for other tables
            else:
                item_id = int(input(f"Enter the subject_id of the {table_name} record to delete: "))
                # We call the 3rd index function for the table which is delete and indicate success
                success = record_functions[3](engines, item_id)
                if success:
                    print(f"Record with ID {item_id} successfully deleted.")
                else:
                    print(f"Failed to delete record with ID {item_id}.")

if __name__ == "__main__":
    main()
