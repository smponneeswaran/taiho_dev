CREATE TABLE rpt_studymilestone AS (
WITH studymilestonedata as ( 
SELECT 
       comprehendid, 
       studyid, 
       milestoneseq, 
       milestonelabel, 
       CAST(MIN(CASE WHEN LOWER(milestonetype)='baseline' THEN expecteddate END) AS VARCHAR) AS curbaselinedate, 
       CAST(MIN(CASE WHEN LOWER(milestonetype)='original' THEN expecteddate END) AS VARCHAR) AS orgbaselinedate, 
       CAST(MIN(CASE WHEN LOWER(milestonetype)='planned'  THEN expecteddate END) AS VARCHAR) AS planneddate, 
       CAST(MIN(CASE WHEN LOWER(milestonetype)='actual'   THEN expecteddate END) AS VARCHAR) AS actualdate, 
       ismandatory, 
       iscriticalpath 
FROM studymilestone 
GROUP BY comprehendid, studyid, milestoneseq, milestonelabel, ismandatory, iscriticalpath),

studydata AS (
SELECT 
       comprehendid, 
       studyid, 
       studyname, 
       studydescription, 
       studystatus, 
       studyphase, 
       studysponsor, 
       therapeuticarea, 
       "program", 
       medicalindication, 
       studystartdate, 
       studycompletiondate, 
       comprehend_update_time, 
       objectuniquekey, 
       studystatusdate, 
       isarchived, 
       istsdv 
FROM study)  

SELECT 
       a.comprehendid, 
       a.studyid, 
       a.studyname, 
       a.studydescription, 
       a.studystatus, 
       a.studyphase, 
       a.studysponsor, 
       a.therapeuticarea, 
       a.program, 
       a.medicalindication, 
       a.studystartdate, 
       a.studycompletiondate, 
       b.milestoneseq, 
       b.milestonelabel, 
       TO_DATE(b.curbaselinedate,'yyyy-mm-dd') AS curbaselinedate, 
       TO_DATE(b.orgbaselinedate,'yyyy-mm-dd') AS orgbaselinedate, 
       TO_DATE(b.planneddate,'yyyy-mm-dd') AS planneddate, 
       TO_DATE(b.actualdate,'yyyy-mm-dd') AS actualdate, 
       (TO_DATE(b.actualdate,'yyyy-mm-dd') - COALESCE(TO_DATE(b.planneddate,'yyyy-mm-dd'),TO_DATE(b.curbaselinedate,'yyyy-mm-dd'),TO_DATE(b.orgbaselinedate,'yyyy-mm-dd'))) AS datediff, 
       b.ismandatory, 
       b.iscriticalpath, 
       a.studystatusdate, 
       a.isarchived, 
       a.istsdv, 
       a.objectuniquekey, 
       now()::TIMESTAMP AS comprehend_update_time  
FROM studydata a 
JOIN studymilestonedata b ON (a.comprehendid = b.comprehendid) 
ORDER by a.studyid,b.milestoneseq) ;
