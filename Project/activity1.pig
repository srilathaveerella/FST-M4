-- Load input file from HDFS
inputFile1 = LOAD 'hdfs:///user/sri/Activities/episode*' USING PigStorage('\t') AS (name:chararray, speech:chararray);
-- Combine the names from the above stage
grpd = GROUP inputFile1 BY name;
-- Count the number of names
cntd = FOREACH grpd GENERATE group, COUNT(inputFile1.name) AS cnt;
-- Order by Desc
orderDesc = ORDER cntd BY cnt DESC;
-- Store the result in HDFS
STORE orderDesc INTO 'hdfs:///user/sri/Activity/activity3Output';
