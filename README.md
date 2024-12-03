# A Distributed Electronic Health Record System and Web-Application Based on MIMIC-3 Data

Please Note: The data was accessed in compliance with the PhysioNet Credentialed Data Use Agreement, therefore the CSV files have not been uploaded.

In this project, we’re handling Electronic Health Record (EHR) data, which is typically too large to be stored in a single database. Effective management of EHR data requires scalability and careful organization of sensitive and comprehensive patient information. Our Project aims to leverage MySQL to build a distributed database system for the MIMIC III dataset. Furthermore, we aim to develop an efficient Database manager that allows for the execution of create, read, update, and delete functions on the data within the databases. Lastly, we aim to utilize this data to develop a Intensive Care tool that provides a biostatistical overview of ICU patients’ medical data to analyze and draw conclusions about new ICU admitted patients.

For this project, we utilized the MIMIC III dataset [1], an open source dataset of de-identified health-related data associated with over 40000 patients who stayed in critical care units between 2001 and 2012. The database includes information such as demographics, vital sign measurements made at the bedside (~1 data point per hour), laboratory test results, procedures, medications, caregiver notes, imaging reports, and mortality (including post-hospital discharge). The implementation will begin with identifying the most relevant CSV files for our end goal. The implementation will begin with understanding the tables and relations of this database, such as -

Patients (Admission_ids, Name, Age, gender, admission date etc)
Admissions (Admission_id, Diagnosis, Period etc.)
LabResults (Admission_id, Blood work, Date etc)
Medications (Admission_id, Medication, Dosage, Frequency, Period etc)
Since this data is highly structured and relational, MySQL has been utilized.


## Architecture Design (Flow Diagram and Description)
![340779197-0de70311-eb24-4243-b44c-cda400aa5aef](https://github.com/user-attachments/assets/d5d91084-188a-43a1-979b-c8a0039c38f7)

In the above flow diagram, the data was initially imported from our five CSV files. In order to do so the subject id for each row in each of the CSV files was read and stripped for its last digit. This digit (0-9) determines which of the 10 databases (yellow triangles) that subject ID would be sent to. Within each of these Databases are pre-created tables - admissions, labevents, chartevents, patients, and diagnoses - which correspond to the five CSV files. The tables prefixed with “user_” are temporary data store files utilized by the Front End Application as can be seen in the bottom right of the diagram. The data is then further utilized by the Database Manager and User Application to perform their various functionalities as seen in our Implementation video. Each of these functionalities is dependent on the Subject ID for efficient data retrieval as this was utilized in our initial hash function.

The implementation begins with identifying the most relevant CSV files for our end goal. While we previously identified a few files in our proposal as seen in section 2, we conducted further analysis of what would be needed to achieve our goals. Our team pivoted our decision in the files as we found the following files and their structures to be of utmost importance.

* PATIENTS (Subject_ID, gender, dob, dod, dod_hosp, dod_ssn, expire_flag)

* ADMISSIONS (Subject_ID, hadm_id, admittime, dischtime, deathtime, admission_type, admissions_location, discharge_location, insurance, language, religion, marital_status, ethnicity, edregtime, diagnosis, hospital_expire_flag, has_chartevents_data)

* D_ICD_DIAGNOSES (icd9_code, short_title, long_title, subject_id, hadm_id, seq_num)

* LABEVENTS (Subject_ID, hadm_id, itemid, charttime, value, valuenum, valueuom, flag, label, fluid, category, Ionic_code)

* CHARTEVENTS (Subject_ID, hadm_id, icustay_id, itemid, charrtime, storettime, cgid, value, valuenum, valueuom, warning, error, resultstatus, stopped, label, abbreviation, dbscource, category, unitname, param_type, conceptid)

Since these CSV files follow a highly structured format, our team felt it would be best to utilize MySQL as our database. In order to efficiently store this data in a distributed manner, our next step was to determine a hash key that would partition the above tables across several databases. As seen above, one common attribute found in all of the tables was subject_id which is a five digit unique identifier for each subject. Due to the large amount of data per subject, our team determined it was best to utilize the last digit of the subject id as the hashing method. With this method, query performance is improved by localizing related data, thereby enhancing overall database management and access. As a result of this method, our project will be utilizing 10 databases following the naming convention - DB_# (DB_0, DB_1…DB_9). We developed two Python scripts in order to create the databases and the tables as well as insert the data efficiently. In the first script, “sqlscript.py”, we set up a MySQL database environment by creating 10 databases and the relevant tables using SQLAlchemy and PyMySQL libraries to connect to the server. A base URI for the MySQL connection as well as a list of database names was defined and further used to iterate over each database name, connecting to the MySQL server and creating the specified database if it doesn’t already exist.

For each database, the tables ‘admissions’, ‘chartevents’, ‘labevents’, ‘patients’, and ‘diagnosis’ are created. These tables are in line with the original CSV files from which the data originates from. Each table’s structure is defined with data types and constraints as can be seen in the implementation video. For the tables, labevents and chartevents, an auto incremented integer denoted as “id” was used as the primary key as no other attribute, or combination of attributes, provided a unique identifier for each row. For the other tables, primary keys and composite primary keys were developed from the existing attributes to develop a unique identifier for each data row as seen in the code file as well as in the video. The specifics of what was determined to be the primary key were not particularly relevant to achieving our goals within the DB manager and front end applications. Additionally, the script creates duplicate sets of tables with ‘user_’ prefixes, which are used as temporary datastores for the front end application..

The second script we created, “importcsv.py”, efficiently distributes and inserts data from multiple CSV files into ten different MySQL databases using the hash function based on the subject ID. We utilized the Pandas library for data manipulation and the SQLAlchemy library to manage database connections. The calculate database function is used to calculate the target database for each record by using the last digit of the subject ID. The insert function reads the data in chunks of 10,000 rows to efficiently handle memory and processes each chunk by applying the hash function. This determines the target database for each row, after which the data is inserted into the corresponding database. This is applied to the five CSV data files, each associated with specific tables and key columns defined by the sqlscript.py. By distributing the data across multiple databases using this hash-based approach, we are optimizing main memory usage while hashing and inserting the data.

## Database Manager

The database manager effectively uses PyMySQL to connect to the created databases that we can interact with. When the manager is running, it will prompt the user to take one of five different actions:

* Insertions
* Bulk Insertions
* Updates
* Deletions
* Exit the database manager

Once the user chooses one of the four options, they have the option to interact with one of the following tables: admissions, diagnoses, chartevents, labevents, and patients.

When the user attempts to insert a record into the database, they are prompted to enter each record as a set of comma separated values, which are then prepared, cleaned, and inserted via an SQL query through pymysql. The records will be sent to the proper database based on hashing on the last number of the subject_id field, with the insertion to the proper database being indicated. Bulk insertions effectively take this functionality and convert it into a for loop, where the insert function is essentially performed on each record. For chartevents and labevents in particular, this will generate a unique ID for each inserted record as one subject_id can have multiple chart and lab events under their name. For insertions, the user has the option to view the newly inserted record after the operation is performed, where the dbmanager effectively executes a SELECT * FROM tablename WHERE subject_id = …

For deletions and updates however, functionalities differ between chart/lab events and admissions, diagnoses, and patients. When the user attempts to update or delete a chartevent or a labevent, they first query based on the subject_id and are then presented with all the IDs of the events under the subject_id. They then have the option to select which event ID they want to update or delete, rather than updating/deleting the subject_id as a whole.

In general, however, when attempting to update, the dbmanager will retrieve and display the record with all of its fields and associated values if it exists, allowing the user to see what potential fields they can work with. The user can then enter their desired updates as a JSON string following the template {“field”: “value”, “field2”: “value2”}. This json is then loaded and prepared and the update operation occurs.

Finally, for delete operations, this will essentially execute a DELETE query on the id or the subject_id (depending on the table), and it will indicate that the record has been deleted. The user can interact with the DB Manager as much as they desire until they want to exit.

## User Application

MIMIC is a database of ICU patients from a hospital in Massachusetts, where data was collected for all those patients in great detail. The tool, called ‘Intensive Care’ aims to provide a Biostatistical overview of ICU patients’ medical data, so that any ICU practitioner, medical personnel, or even medical researcher can use the analysis to draw conclusions about new ICU admitted patients. If they have the vital signs data of a new batch of patients, they can cross-check with the statistical summary of the vital signs data of the MIMIC cohort patients and evaluate the chances of survival of the patients in the new batch.

The current version of the data that we have used, is a demo version provided by the PhysioNet organization [2], which is open to public access. Every attribute of the patients’ data in this version can be accessed, including their subject ID, Date of Birth, age, etc. No confidentiality of data is being violated here when we access the patients’ data by the Subject ID of our Intensive Care Tool.

## Functionalities

### Establishing Python-MySQL connection Functionality

We have used MySQL for the database, PyMySQL has been used to connect to this database in the Python application. We have also used MySQL connector and SQLAlchemy for various functions. We have used the Streamlit and Plotly packages to build this application’s front-end. On the back end, we have used the Pandas, NumPy, Matplotlib, and Seaborn libraries. For statistical analysis we have used the table one SciPy, Pandas and some other libraries.

### Search by Subject ID Functionality

We can search by subject ID of the MIMIC cohort itself. We can enter the subject ID, and see the patient profile corresponding to that ID, the admission history, the diagnosis, the lab events data, and the vitals data. We input the subject ID that we want to query. That Subject ID is hashed and the hash key gives us the specific URL of the database that would contain this particular Subject ID. Using the URL we obtain an engine using the SQLAlchemy library [3], which connects us to that database.

Once we have established the connection, we can query the data for that Subject ID. We input the Engine, the Subject ID into the function with and a specific SQL query which produces the relevant data, and it results in all the data using this con.execute(text(query)).fetchall() command.

Note that the data is being retrieved and processed entirely using MySQL queries only, the use Pandas DataFrame is only for displaying on the frontend application.

Similarly, we can access other tables as well; chartevents, patients, admissions, diagnosis, labevents using this pipeline. Statistical Tests and Analytics Functionality Statistical Analysis is performed on the entire data, which is collected using a for loop which goes over all the ten databases one by one, creating the engine for each database, and then querying out the information using SQL query from all the databases.

Chi-square test [4] is a very popular biostatistics test which is used for comparing the significance of one categorical variable with another categorical variable like the age and the Survival Status. And then finally, we use that collected information for statistical analysis. So here the first one is patient profile and admission analytics. So we can see this chart. That is the count of gender by status. So the status is the survivor or non-survivor status, whether the patient survived in ICU or did not. And here we also have the chi-square test for this. And we also have the age across status. So what is the age group of people in general or the average age who survived and did not survive? We can see that. T-test [5] is another very popular biostatistics test which is used for comparison of two numerical variables like the age or the duration and the duration of stay in ICU. Is the age significant or not in determining the survivor or non-survivor status? That is what the T-test is. The p-value tells us the significance level, i.e. if the hypothesis lies in the 95% confidence or not.

Example - Density graph and Box Plot representing the distribution of populations for the control group (in blue) and the treated group (in orange). The vertical dashed line (in red) shows the initial cholesterol and inclusion criteria for the study.

We also analyze the admit duration across status using t-test since the admission duration is also a numerical variable. Frequency Count Plots have been used for the Diagnoses Data analytics. For this, we have specific functions which query information from the diagnosis table. A patient can have multiple diagnoses, so we collect all the different diagnoses. We rank the diagnosis in terms of frequency, like which diagnosis is the most recurring amongst patients, which is the second most recurring diagnosis, third most, and so on. We produce a frequency count plot for the most common diagnosis. Other functions perform above tests for other important parametric data available in MIMIC, such as Respiratory Rate and Temperature which are the most important vital data. For those also we have used t-test here for determining whether the maximum temperature and minimum Respiratory Rate are statistically significant in determining whether a person will survive or not survive.

Here also we have used specific SQL queries to retrieve the required data from the databases. And then we have performed statistical tests on that on the retrieved data. Similarly for is the labevents Data analytics. In the lab report we have the WBC that is a white blood cell count and the hemoglobin count measures for the patients. Here also we have produced a t-test which tells us whether the maximum white blood cell count and the minimum hemoglobin count are significant or not significant in determining the status of a patient. This way, all the analytics on the entire MIMIC cohort database were done. User Data Collection Functionality The other functionality that our application has is that it can take the information of new users and store it for some time and allow the users to see the analytics report on that data so they can use the analytics report on their data, compare it with the MIMIC data and draw conclusions. Here also we connect to the databases using MySQL connector. Then we take that connection and we obtain the data from the user, the subject ID, gender or date of birth, date of death, and the expiry flag. And we insert into the relevant table that is the user_patient, which stores all this demographic data. We can take a Subject ID from the user, which is different from the MIMIC Subject IDs because this is unique for whatever cohort the user themselves have. Once the Subject ID is input, we get all the most relevant data for statistical analysis;

* Gender
* Date of birth
* Date of death
* Expiry flag
* Date of admission
* Date of discharge
* Hospital ID
* User diagnosis
* Description of that diagnosis
* White blood cell count
* Hemoglobin count
* Respiratory rate
* Temperature
  
Finally, once we upload the data we can see a message whether the patient information, Admission, diagnosis, lab data, and vitals information was stored successfully or not. For whatever number of patients the user has uploaded the data, they can see the entire analytics for all those patients, similar to the MIMIC cohort. They can see for their own cohort the user data diagnosis analytics, the corresponding chi-squared test, t-test, the frequency plots, the vital data analytics, then the lab report, data analytics and so on. And finally, they can delete all the data that they have just uploaded in order to maintain the security of their data. We have SQL queries that query out particular user tables’ data. And when they exit this tool, these user tables will be wiped off for any other new user. For this task, we have created another function called delete_all_user_data containing MySQL queries . When they exit this wipes off all the databases or all, the temporary tables from the databases.

## Tech Stack Used

* MySQL -> MySQL Workbench 8.0, MySQL Shell
* Python -> PyMySQL, SQLAlchemy, Streamlit, Plotly, SciPy, Seaborn
* Others -> CSV, JSON, AWS EC2

## Learning Outcomes

This project helped us become proficient in MySQL for data management and retrieval. We also gained a good understanding of the entire pipeline of creating databases and tables to integrate them into frontend applications. Although we learned a lot, we encountered some challenges in the process. For example, understanding the data structure and selecting the most relevant CSV files for our Intensive Care tool proved to be difficult as we had to carefully look through numerous CSV files to identify the most relevant data. In addition, establishing a hashing key for data retrieval in the DB Manager and Front End Application provided some difficulty. After trial and error, we decided that using the last digit of the subject_id as a hash key was the most efficient method as both of our applications operate based on the subject_id.

# Implementation Steps

## MySQL Setup

The existing MySQL URI assumes the following credentials:
- **Host**: `localhost`
- **User**: `root`
- **Password**: `Dsci-551`

Please configure your MySQL with these credentials or update the relevant parts of the URI in the provided code files to match your own credentials.

---

## Import Data

The project is built using **local MySQL**. Please ensure local MySQL is used for setup. All files should be executed from their downloaded directories—do not move files to other locations.

### Steps:

1. **Download the `ImportData` Directory**:
   - Obtain the `ImportData` directory from Google Drive. This directory contains:
     - CSV files
     - Python scripts (`importcsv.py` and `sqlscript.py`)

2. **File Path Configuration**:
   - The `importcsv.py` script relies on the CSV files being in the same directory as the scripts (`ImportData`). Ensure this structure is maintained.

3. **Table Definitions and Database Creation**:
   - The file `sqlscript.py` contains table definitions and functions to create the necessary databases.

4. **Update MySQL Credentials**:
   - If needed, replace the default MySQL URI with your own credentials in both `sqlscript.py` and `importcsv.py`.
     - **Default Credentials**:
       - Username: `root`
       - Password: `Dsci-551`

5. **Create Databases and Tables**:
   - Run `sqlscript.py` within the `ImportData` directory to create the databases and tables.

6. **Import Data**:
   - Execute `importcsv.py` in the same directory to read and process the CSV files.
   - The script:
     - Processes each CSV file in chunks.
     - Hashes and distributes the data into 10 separate databases based on the last digit of the `subject_id` (0-9).

7. **Completion**:
   - Once complete, the data will be correctly hashed and inserted into the respective databases.

---

## Database Manager

### Steps:

1. **Download the `DBManager` Directory**:
   - Obtain the `DBManager` directory from Google Drive.

2. **Update MySQL Credentials**:
   - Modify the MySQL URI in `dbmanager.py` if needed, using your credentials.
     - **Default Credentials**:
       - Username: `root`
       - Password: `Dsci-551`

3. **Run DB Manager**:
   - Execute the `dbmanager.py` script within the `DBManager` directory.

4. **Outcome**:
   - The database manager will be ready for use.

---

## Front-End Application

### Steps:

1. **Download the `FrontEndApplication` Directory**:
   - Obtain the `FrontEndApplication` directory from Google Drive.

2. **Update MySQL Credentials**:
   - Replace the username and password in `IC_Tool.py` with your own credentials if necessary.
     - **Default Credentials**:
       - Username: `root`
       - Password: `Dsci-551`

3. **Install Dependencies**:
   - Run the following command to install required packages:
     ```bash
     pip install -r requirements.txt
     ```

4. **Launch the Application**:
   - Execute the following command to start the front-end application:
     ```bash
     streamlit run IC_Tool.py
     ```
   - The application will automatically open in your default browser.

5. **Recommended Environment**:
   - Use **Anaconda Prompt** with Jupyter Notebook installed, or ensure the `streamlit` package is properly configured in your Python path.

---

## Summary

Following these steps will ensure:
- Successful setup of the MySQL database.
- Proper import and distribution of data across databases.
- Implementation of the database manager.
- Deployment of the front-end application for user interaction.

---

## References

[1] Johnson AEW, Pollard TJ, Shen L, et al. MIMIC-III, a freely accessible critical care database. Sci Data. 2016;3:160035. doi:10.1038/sdata.2016.35

[2] MIT Laboratory for Computational Physiology. MIMIC-III Clinical Database Documentation. https://mimic.mit.edu/docs/iii/tables/. Accessed May 3, 2024.

[3] SQLAlchemy. Engines - SQLALCHEMY Documentation. https://docs.sqlalchemy.org/en/20/core/engines.html. Accessed May 3, 2024.

[4] Valeri L, VanderWeele TJ. SAS Macros for Testing Statistical Mediation in Data with Binary Mediators or Outcomes. Epidemiology. 2013;24(6):878-885. doi:10.1097/EDE.0b013e31829d524e

[5] Biau DJ, Kerneis S, Porcher R. Statistics in Brief: The Importance of Sample Size in the Planning and Interpretation of Medical Research. Clin Orthop Relat Res. 2008;466(9):2282-2288. doi:10.1007/s11999-008-0346-9
