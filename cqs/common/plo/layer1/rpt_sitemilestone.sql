CREATE TABLE rpt_sitemilestone AS (
WITH sitemilestonedata as ( 
SELECT 
       comprehendid, 
       studyid, 
       siteid,
       milestoneseq, 
       milestonelabel,
       CAST(MIN(CASE WHEN LOWER(milestonetype)='baseline' THEN expecteddate END) AS VARCHAR) AS curbaselinedate, 
       CAST(MIN(CASE WHEN LOWER(milestonetype)='original' THEN expecteddate END) AS VARCHAR) AS orgbaselinedate, 
       CAST(MIN(CASE WHEN LOWER(milestonetype)='planned'  THEN expecteddate END) AS VARCHAR) AS planneddate,
       CAST(MIN(CASE WHEN LOWER(milestonetype)='actual'   THEN expecteddate END) AS VARCHAR) AS actualdate,
       ismandatory 
FROM sitemilestone 
GROUP BY comprehendid, studyid, siteid, milestoneseq, milestonelabel, ismandatory),

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
FROM study),  

sitedata AS (
SELECT 
       comprehendid, 
       studyid, 
       siteid, 
       sitename, 
       croid, 
       sitecro, 
       sitecountry, 
       siteregion, 
       sitecreationdate, 
       siteactivationdate, 
       sitedeactivationdate, 
       siteinvestigatorname, 
       sitecraname, 
       siteaddress1, 
       siteaddress2, 
       sitecity, 
       sitestate, 
       sitepostal, 
       objectuniquekey, 
       comprehend_update_time, 
       sitestatus, 
       sitestatusdate 
FROM site )  

SELECT  
       a.comprehendid,
       a.studyid,
       c.studyname,
       c.studydescription,
       c.studystatus,
       c.studyphase,
       c.studysponsor,
       c.therapeuticarea,
       c.program,
       c.medicalindication,
       c.studystartdate,
       c.studycompletiondate,
       a.siteid,
       a.sitename,
       a.croid,
       a.sitecro,
       a.sitecountry,
       a.siteregion, 
       a.sitecreationdate, 
       a.siteactivationdate, 
       a.sitedeactivationdate,
       a.sitestatus,
       a.sitestatusdate,
       b.milestoneseq,
       b.milestonelabel,
       TO_DATE(b.curbaselinedate,'yyyy-mm-dd') AS curbaselinedate,
       TO_DATE(b.orgbaselinedate,'yyyy-mm-dd') AS orgbaselinedate,
       TO_DATE(b.planneddate,'yyyy-mm-dd') AS planneddate,
       TO_DATE(b.actualdate,'yyyy-mm-dd') AS actualdate,
       (TO_DATE(b.actualdate,'yyyy-mm-dd') - COALESCE(TO_DATE(b.planneddate,'yyyy-mm-dd'),TO_DATE(b.curbaselinedate,'yyyy-mm-dd'),TO_DATE(b.orgbaselinedate,'yyyy-mm-dd'))) AS datediff, 
       b.ismandatory,
       c.studystatusdate,
       c.isarchived,
       c.istsdv,
       a.objectuniquekey,
       now()::TIMESTAMP AS comprehend_update_time  
FROM sitedata a 
JOIN sitemilestonedata b ON (a.comprehendid=b.comprehendid) 
JOIN studydata c ON (a.studyid=c.studyid) 
ORDER by a.studyid,a.siteid,b.milestoneseq) ;
