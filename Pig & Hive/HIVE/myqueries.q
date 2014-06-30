ADD JAR /home/cloudera/Class5/myudfs.jar;

CREATE DATABASE healthDB;
USE healthDB;

CREATE TABLE healthCareSampleDS (PatientID INT, Name STRING, DOB STRING, PhoneNumber STRING, EmailAddress STRING, SSN STRING, Gender STRING, Disease STRING, weight FLOAT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '/home/cloudera/Class5/healthcare_Sample_dataset1.csv' INTO table healthCareSampleDS;

CREATE TEMPORARY FUNCTION deIdentify AS 'myudfs.Deidentify';

CREATE TABLE healthCareSampleDSDeidentified AS SELECT PatientID, deIdentify(Name), deIdentify(DOB), deIdentify(PhoneNumber), deIdentify(EmailAddress), deIdentify(SSN), deIdentify(Gender), deIdentify(Disease), deIdentify(weight) FROM healthCareSampleDS;
