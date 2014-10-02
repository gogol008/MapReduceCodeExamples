
----loading and parsing data-----

A = load '/weatherPIG' using TextLoader as (data:chararray);
AF = foreach A generate TRIM(SUBSTRING(data, 6, 14)), TRIM(SUBSTRING(data, 46, 53)), TRIM(SUBSTRING(data, 38, 45));
store AF into '/data6' using PigStorage(',');
S = load '/data6/part-m-00000' using PigStorage(',') as (date:chararray, min:double, max:double);

-------Hot Days------

X = filter S by max > 25;
dump X;

-------Cold Days------

X = filter S by min < 0;
dump X;

-------Hottest Day-----

/* puts S's data in H1's Tuple */

H1 = group S all; 	
I = foreach H1 generate MAX(S.max) as maximum;
X = filter S by max == I.maximum;

-------Coldest Day------

H2 = group S all;
J = foreach H2 generate MIN(S.min) as minimum;
X = filter S by min == J.minimum;


-----UDF-----
register PIGUdfCorrupt.jar;

A = load '/weatherPIG' using TextLoader as (data:chararray);
AF = foreach A generate TRIM(SUBSTRING(data, 6, 14)), IfCorrupted(TRIM(SUBSTRING(data, 46, 53))), IfCorrupted(TRIM(SUBSTRING(data, 38, 45)));
store AF into '/data2' using PigStorage(',');
S = load '/data2/part-m-00000' using PigStorage(',') as (date:chararray, min:double, max:double);


------------------

A = load '/data1' as (a1:int, a2:int);
B = load '/data2' as (b1:int, b2:int);
X = UNION A, B;
dump X;
//onschema


----------------------------------

A = LOAD '/j1' as (a1:int, a2:int, a3:int);
B = LOAD '/j2' as (b1:int, b2:int);
X = JOIN A BY a1, B BY b1;
dump X;

------------------------------------

A = load '/student' as (name:chararray, age:int, gpa:float);
B = load '/studentRoll' as (name:chararray, rollno:int);

X = group A by name;
dump X;

X = cogroup Aby name, B by name;
dump X;

register myudf.jar;
X = filter A by IsOfAge(age);
