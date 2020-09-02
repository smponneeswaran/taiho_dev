/*
dimvisitrequiredfields

Note : PLO having details of counts of primaryendpoint, secondaryendpoint and sdvfields derived from  metadata objects

*/

CREATE TABLE dimvisitrequiredfields AS
SELECT v.studyid, 
       v.visitnum, 
       v.visit, 
       v.formid,
       sum(CASE WHEN fd.isprimaryendpoint THEN 1 ELSE 0 END) * 1.0 AS primaryendpointfields,
       sum(CASE WHEN fd.issecondaryendpoint THEN 1 ELSE 0 END) * 1.0 AS secondaryendpointfields,
       sum(CASE WHEN fd.issdv THEN 1 ELSE 0 END) * 1.0 AS sdvfields,
       count(fd.objectuniquekey) * 1.0 AS fields, 
       v.objectuniquekey,
       now()::timestamp as comprehend_update_time
FROM visitform v 
JOIN fielddef fd ON (v.studyid = fd.studyid AND v.formid = fd.formid AND fd.isrequired = true)
GROUP BY v.studyid, v.visitnum, v.visit, v.formid, v.objectuniquekey;
