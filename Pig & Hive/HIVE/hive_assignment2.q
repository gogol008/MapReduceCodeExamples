Hive Assignment 2:
=====================================
Question 1: Create an external table for Patent data set so that it can be used efficiently for queries which require looking into patents granted for given year. 
---------------------------------
CREATE EXTERNAL TABLE patents ( PATENT STRING , year String, GDATE STRING ,APPYEAR STRING ,COUNTRY STRING ,POSTATE STRING ,ASSIGNEE STRING ,ASSCODE STRING ,CLAIMS STRING ,NCLASS STRING ,CAT STRING ,SUBCAT STRING ,CMADE STRING ,CRECEIVE STRING ,RATIOCIT STRING ,GENERAL STRING ,ORIGINAL STRING ,FWDAPLAG STRING ,BCKGTLAG STRING ,SELFCTUB STRING ,SELFCTLB STRING ,SECDUPBD STRING ,SECDLWBD STRING ) COMMENT 'This is the patents table' PARTITIONED BY(GYEAR STRING) CLUSTERED BY(NCLASS) SORTED BY(NCLASS) INTO 32 BUCKETS ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' STORED AS TEXTFILE LOCATION '/input/patents/data';

For creating data in different partition:
-------------------------------------------
hadoop dfs -put patent_1963.txt /input/patents/data/gyear=1963/
hadoop dfs -put patent_1964.txt /input/patents/data/gyear=1964/
hadoop dfs -put patent_1965.txt /input/patents/data/gyear=1965/
hadoop dfs -put patent_1999.txt /input/patents/data/gyear=1999/

For creating partitions in table
---------------------------------
ALTER TABLE patents ADD PARTITION(gyear=1963);
ALTER TABLE patents ADD PARTITION(gyear=1964);
ALTER TABLE patents ADD PARTITION(gyear=1965);
ALTER TABLE patents ADD PARTITION(gyear=1999);


You can also use directly this:
===================================
LOAD DATA LOCAL INPATH "patent_1963.txt" INTO TABLE patents PARTITION(gyear=1963);
LOAD DATA LOCAL INPATH "patent_1964.txt" INTO TABLE patents PARTITION(gyear=1964);
LOAD DATA LOCAL INPATH "patent_1965.txt" INTO TABLE patents PARTITION(gyear=1965);
LOAD DATA LOCAL INPATH "patent_1999.txt" INTO TABLE patents PARTITION(gyear=1999);



Question 2: Find out number of patents granted in year 1963.
--------------------------------
select count(*) from patents where GYEAR=1963;

Question 3: Find out number of patents granted in each country in year 1999.
--------------------------------
select COUNTRY, count(*) from patents where GYEAR=1999 group by COUNTRY;
