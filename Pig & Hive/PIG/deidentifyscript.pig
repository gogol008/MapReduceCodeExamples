REGISTER myudfs.jar;

A = LOAD '/healthcare_Sample_dataset2.csv' using PigStorage(',') AS (PatientID: int, Name: chararray, DOB: chararray, PhoneNumber: chararray, EmailAddress: chararray, SSN: chararray, Gender: chararray, Disease: chararray, weight: float);

B = LOAD '/healthcare_Sample_dataset1.csv' using PigStorage(',') AS (PatientID: int, Name: chararray, DOB: chararray, PhoneNumber: chararray, EmailAddress: chararray, SSN: chararray, Gender: chararray, Disease: chararray, weight: float);

C = UNION A, B;

D = FOREACH C GENERATE PatientID, com.test.deidentify.DeIdentifyUDF(Name,'12345678abcdefgh'), com.test.deidentify.DeIdentifyUDF(DOB,'12345678abcdefgh'), com.test.deidentify.DeIdentifyUDF(PhoneNumber,'12345678abcdefgh'), com.test.deidentify.DeIdentifyUDF(EmailAddress,'12345678abcdefgh'),com.test.deidentify.DeIdentifyUDF(SSN,'12345678abcdefgh'), com.test.deidentify.DeIdentifyUDF(Disease,'12345678abcdefgh'),weight;

STORE D into '/deidentifiedDir';
