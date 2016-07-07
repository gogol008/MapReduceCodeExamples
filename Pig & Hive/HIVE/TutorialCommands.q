
Hive Command:

1. Create Table: 
========================
CREATE EXTERNAL TABLE player_runs(player_id INT, year_of_play STRING, runs_scored INT)
COMMENT 'This is the player_run table'
STORED AS TEXTFILE;


2. See all tables in a database:
=========================
Show tables;


3. See Schema of a table 
=========================
describe player_runs


4. Alter table commands :
=========================
ALTER TABLE player_runs RENAME TO runs_of_player;
ALTER TABLE runs_of_player ADD COLUMNS (balls_played INT COMMENT 'a new int column');


5. Drop Table
=========================
drop table runs_of_player;

CREATE TABLE player_runs(player_id INT, year_of_play STRING, runs_scored INT, balls_played INT, country STRING)
COMMENT 'This is the player_run table'
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH "data/runs.csv" INTO TABLE player_runs; --LocalFS

LOAD DATA INPATH "/user/debadyuti.roychowdhury_gmail/HiveData/runs.csv" INTO TABLE player_runs;  --HDFS

select * from player_runs limit 10;


6. Create external table 
=========================

CREATE EXTERNAL TABLE player_runs_p(player_id INT, year_of_play STRING, runs_scored INT, balls_played INT)
COMMENT 'This is the staging player_runs table'  PARTITIONED BY(country STRING)  
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS TEXTFILE;
-- LOCATION '/input/runs';

hadoop fs -mkdir /input/runs/country=India
hadoop fs -put data/runs_India.csv /input/runs/country=India/

ALTER TABLE player_runs_p ADD PARTITION(country='India');


7. Loading data into Table
=============================

LOAD DATA INPATH '/user/debadyuti.roychowdhury_gmail/HiveData/runs_us.csv' INTO TABLE player_runs_p PARTITION(country='US');
LOAD DATA INPATH '/user/debadyuti.roychowdhury_gmail/HiveData/runs_india.csv' INTO TABLE player_runs_p PARTITION(country='India');


FROM player_runs_p
INSERT OVERWRITE TABLE player_runs 
SELECT player_id, year_of_play, runs_scored,balls_played;
--where player_id=10;


8. Simple Select Query:
==================
Select player_id, runs_scored from player_runs;
select * from player_runs where player_id=10;

9. Group BY Query:
====================
select player_id, sum(runs_scored) from player_runs group by player_id;

10 Join : 
==================

CREATE EXTERNAL TABLE players(player_id INT, name STRING)
COMMENT 'This is the staging player table'  
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS TEXTFILE;
--LOCATION '/input/player';

LOAD DATA INPATH "/user/debadyuti.roychowdhury_gmail/HiveData/player.csv" INTO TABLE players;

select * from player_runs join players on player_runs.player_id=players.player_id;

select * from players full outer join player_runs on players.player_id=player_runs.player_id;

select * from players left outer join player_runs on players.player_id=player_runs.player_id;




11 Multi Insert:
===========================

CREATE TABLE player_total_runs(player_id INT, runs_scored INT)
COMMENT 'This is the player_total_run table'
STORED AS TEXTFILE;

CREATE TABLE yearly_runs(year_of_play STRING, runs_scored INT)
COMMENT 'This is the yearly_run table'
STORED AS TEXTFILE;


//Insert into two tables
FROM player_runs
INSERT OVERWRITE TABLE player_total_runs
    select player_id, sum(runs_scored) 
    group by player_id 
INSERT OVERWRITE TABLE yearly_runs
    select year_of_play, sum(runs_scored) 
    group by year_of_play;


//Insert into one table and one HDFS file
FROM player_runs
INSERT OVERWRITE TABLE player_total_runs
    select player_id, sum(runs_scored) 
    group by player_id 
INSERT OVERWRITE DIRECTORY '/output/yearly_runs'
    select year_of_play, sum(runs_scored) 
    group by year_of_play;



12 Partioning + Distribute:
============================


CREATE EXTERNAL TABLE player_runs_distribute(player_id INT, year_of_play STRING, runs_scored INT, balls_played INT)
COMMENT 'This is the staging player_runs table'  PARTITIONED BY(country STRING)  
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS TEXTFILE;
--LOCATION '/input/runs_distribute/';

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

FROM player_runs 
INSERT OVERWRITE TABLE player_runs_distribute PARTITION(country)
SELECT player_id,  year_of_play , runs_scored , balls_played, country 
DISTRIBUTE BY  country;


select * from player_runs_distribute where country='India' limit 10;

13 Partioning + Bucketing:
============================


CREATE EXTERNAL TABLE player_runs_clustered(player_id INT, year_of_play STRING, runs_scored INT, balls_played INT)
COMMENT 'This is the  player_runs table'  PARTITIONED BY(country STRING) 
clustered by (player_id) INTO 10 buckets 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;
--LOCATION '/input/runs_clustered/';

LOAD DATA INPATH "/user/debadyuti.roychowdhury_gmail/HiveData/runs_extra.csv" OVERWRITE INTO TABLE player_runs;

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.enforce.bucketing=true;

FROM player_runs 
INSERT OVERWRITE TABLE player_runs_clustered PARTITION(country)
SELECT player_id,  year_of_play , runs_scored , balls_played, country 
DISTRIBUTE BY  country;


select avg(runs_scored) from player_runs_clustered TABLESAMPLE(BUCKET 1 OUT OF 10);


14. MapSide Join
=======================
select /*+ MAPJOIN(players)*/ * from players join player_runs on players.player_id=player_runs.player_id;

select * from players join player_runs on players.player_id=player_runs.player_id;








