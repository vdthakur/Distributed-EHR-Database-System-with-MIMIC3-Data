# Distributed-Electronic-Health-Record-System-and-Web-Application-Based-on-MIMIC-111-Data
A Distributed Electronic Health Record System and Web-Application Based on MIMIC-3 Data

In this project, we’re handling Electronic Health Record (EHR) data, which is typically too large to be stored in a single database. Effective management of EHR data requires scalability and careful organization of sensitive and comprehensive patient information. Our Project aims to leverage MySQL to build a distributed database system for the MIMIC III dataset. Furthermore, we aim to develop an efficient Database manager that allows for the execution of create, read, update, and delete functions on the data within the databases. Lastly, we aim to utilize this data to develop a Intensive Care tool that provides a biostatistical overview of ICU patients’ medical data to analyze and draw conclusions about new ICU admitted patients.

For this project, we utilized the MIMIC III dataset [1], an open source dataset of de-identified health-related data associated with over 40000 patients who stayed in critical care units between 2001 and 2012. The database includes information such as demographics, vital sign measurements made at the bedside (~1 data point per hour), laboratory test results, procedures, medications, caregiver notes, imaging reports, and mortality (including post-hospital discharge). The implementation will begin with identifying the most relevant CSV files for our end goal. The implementation will begin with understanding the tables and relations of this database, such as -

Patients (Admission_ids, Name, Age, gender, admission date etc)
Admissions (Admission_id, Diagnosis, Period etc.)
LabResults (Admission_id, Blood work, Date etc)
Medications (Admission_id, Medication, Dosage, Frequency, Period etc)
Since this data is highly structured and relational, MySQL has been utilized.
