import pymysql
from sqlalchemy import create_engine, text

pymysql.install_as_MySQLdb()

base_uri = 'mysql+mysqldb://root:Dsci-551@localhost/'

database_names = [f'DB_{i}' for i in range(10)]

def create_database_and_tables(engine, db_name):
    with engine.connect() as conn:
        conn.execute(text(f"DROP DATABASE IF EXISTS {db_name};"))
        # create database
        conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {db_name};"))
        conn.execute(text(f"USE {db_name};"))
        
        # drop existing tables
        conn.execute(text("DROP TABLE IF EXISTS admissions;"))
        conn.execute(text("DROP TABLE IF EXISTS chartevents;"))
        conn.execute(text("DROP TABLE IF EXISTS labevents;"))
        conn.execute(text("DROP TABLE IF EXISTS diagnosis;"))
        conn.execute(text("DROP TABLE IF EXISTS patients;"))

        conn.execute(text("DROP TABLE IF EXISTS user_admissions;"))
        conn.execute(text("DROP TABLE IF EXISTS user_chartevents;"))
        conn.execute(text("DROP TABLE IF EXISTS user_labevents;"))
        conn.execute(text("DROP TABLE IF EXISTS user_diagnosis;"))
        conn.execute(text("DROP TABLE IF EXISTS user_patients;"))

        
        # create the 'admissions' table with 'subject_id' and 'hadm_id' as a composite primary key
        conn.execute(text("""
            CREATE TABLE admissions(
                subject_id INT NOT NULL,
                hadm_id INT NOT NULL,
                admittime DATETIME,
                dischtime DATETIME,
                deathtime DATETIME,
                admission_type VARCHAR(200),
                admission_location VARCHAR(200),
                discharge_location VARCHAR(200),
                insurance VARCHAR(200),
                language VARCHAR(200),
                religion VARCHAR(200),
                marital_status VARCHAR(200),
                ethnicity VARCHAR(200),
                edregtime DATETIME,
                edouttime DATETIME,
                diagnosis TEXT,
                hospital_expire_flag TINYINT(1),
                has_chartevents_data TINYINT(1),
                PRIMARY KEY (subject_id, hadm_id)
            );
        """))

        # create the 'chartevents' table with 'id' as the primary key
        conn.execute(text("""
            CREATE TABLE chartevents(
                id INT NOT NULL AUTO_INCREMENT,
                subject_id INT NOT NULL,
                hadm_id INT NOT NULL,
                icustay_id INT,
                itemid INT NOT NULL,
                charttime DATETIME,
                storetime DATETIME,
                cgid INT,
                value VARCHAR(255),
                valuenum FLOAT,
                valueuom VARCHAR(20),
                warning TINYINT(1),
                error TINYINT(1),
                resultstatus VARCHAR(50),
                stopped VARCHAR(50),
                label VARCHAR(255),
                abbreviation VARCHAR(50),
                dbsource VARCHAR(50),
                category VARCHAR(50),
                unitname VARCHAR(50),
                param_type VARCHAR(50),
                conceptid INT,
                PRIMARY KEY (id)
            );
        """))

        # create the 'patients' table with 'subject_id' as the primary key.
        conn.execute(text("""
            CREATE TABLE patients(
                subject_id INT PRIMARY KEY,
                gender CHAR(1),
                dob DATETIME,
                dod DATETIME,
                dod_hosp DATETIME,
                dod_ssn DATETIME,
                expire_flag INT
            );
        """))
        

        # create the 'diagnosis' table with a composite primary key consisting of 'icd9_code', 'subject_id', and 'hadm_id'
        conn.execute(text("""
            CREATE TABLE diagnosis(
                icd9_code VARCHAR(10) NOT NULL,
                short_title VARCHAR(100) NOT NULL, 
                long_title VARCHAR(255) NOT NULL, 
                subject_id INT NOT NULL,
                hadm_id INT NOT NULL,
                seq_num INT NOT NULL,
                PRIMARY KEY (icd9_code, subject_id, hadm_id)
            );
        """))

        # create the 'labevents' table with 'id' as the primary key
        conn.execute(text("""
            CREATE TABLE labevents(
                id INT NOT NULL AUTO_INCREMENT,
                subject_id INT NOT NULL,
                hadm_id INT,
                itemid INT NOT NULL,
                charttime DATETIME NOT NULL,
                value VARCHAR(55),
                valuenum DECIMAL(9, 4),
                valueuom VARCHAR(7),
                flag VARCHAR(8),
                label VARCHAR(255), 
                fluid VARCHAR(50), 
                category VARCHAR(50), 
                loinc_code VARCHAR(10), 
                PRIMARY KEY (id)
            );
        """))
        
        # temporary user-specific tables are created, mirrors the structure of its corresponding general table but is meant for temporary use by front end application 
        conn.execute(text("""
            CREATE TABLE user_admissions(
                subject_id INT NOT NULL,
                hadm_id INT NOT NULL,
                admittime DATETIME,
                dischtime DATETIME,
                deathtime DATETIME,
                admission_type VARCHAR(200),
                admission_location VARCHAR(200),
                discharge_location VARCHAR(200),
                insurance VARCHAR(200),
                language VARCHAR(200),
                religion VARCHAR(200),
                marital_status VARCHAR(200),
                ethnicity VARCHAR(200),
                edregtime DATETIME,
                edouttime DATETIME,
                diagnosis TEXT,
                hospital_expire_flag TINYINT(1),
                has_chartevents_data TINYINT(1),
                PRIMARY KEY (subject_id, hadm_id)
            );
        """))


        conn.execute(text("""
            CREATE TABLE user_chartevents(
                id INT NOT NULL AUTO_INCREMENT,
                subject_id INT NOT NULL,
                hadm_id INT NOT NULL,
                icustay_id INT,
                itemid INT NOT NULL,
                charttime DATETIME,
                storetime DATETIME,
                cgid INT,
                value VARCHAR(255),
                valuenum FLOAT,
                valueuom VARCHAR(20),
                warning TINYINT(1),
                error TINYINT(1),
                resultstatus VARCHAR(50),
                stopped VARCHAR(50),
                label VARCHAR(255),
                abbreviation VARCHAR(50),
                dbsource VARCHAR(50),
                category VARCHAR(50),
                unitname VARCHAR(50),
                param_type VARCHAR(50),
                conceptid INT,
                PRIMARY KEY (id)
            );
        """))

        conn.execute(text("""
            CREATE TABLE user_patients(
                subject_id INT PRIMARY KEY,
                gender CHAR(1),
                dob DATETIME,
                dod DATETIME,
                dod_hosp DATETIME,
                dod_ssn DATETIME,
                expire_flag INT
            );
        """))
        
        conn.execute(text("""
            CREATE TABLE user_diagnosis(
                icd9_code VARCHAR(10) NOT NULL,
                short_title VARCHAR(100) NOT NULL, 
                long_title VARCHAR(255) NOT NULL, 
                subject_id INT NOT NULL,
                hadm_id INT NOT NULL,
                seq_num INT NOT NULL,
                PRIMARY KEY (icd9_code, subject_id, hadm_id)
            );
        """))

        conn.execute(text("""
            CREATE TABLE user_labevents(
                id INT NOT NULL AUTO_INCREMENT,
                subject_id INT NOT NULL,
                hadm_id INT,
                itemid INT NOT NULL,
                charttime DATETIME NOT NULL,
                value VARCHAR(55),
                valuenum DECIMAL(9, 4),
                valueuom VARCHAR(7),
                flag VARCHAR(8),
                label VARCHAR(255), 
                fluid VARCHAR(50), 
                category VARCHAR(50), 
                loinc_code VARCHAR(10), 
                PRIMARY KEY (id)
            );
        """))


        
        print(f"Database {db_name} and tables created successfully.")

def main():
    # create an SQLAlchemy engine that will connect to the MySQL server using the base URI
    engine = create_engine(base_uri)
    # iterate over each database name in the database_names list
    for db_name in database_names:
        # call the function to create a database and its tables for each database name
        create_database_and_tables(engine, db_name)

if __name__ == "__main__":
    main()
