/*
CCDM DS mapping
Notes: Standard mapping to CCDM DS table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject ),

ds_data AS (

----Disposition Event: All Subjects----

SELECT  dm."project"::TEXT AS studyid,
right(dm."SiteNumber",3)::TEXT AS siteid,
right(dm."Subject",7)::TEXT AS usubjid,
1.0::NUMERIC AS dsseq, --deprecated
'All Subject'::TEXT AS dscat,
'All Subject'::TEXT AS dsterm,
null::DATE AS dsstdtc,
null::TEXT AS dsscat 
from tas3681_101."DM" dm

union all 

--Disposition Event: Consented

SELECT  dm."project"::TEXT AS studyid,
right(dm."SiteNumber",3)::TEXT AS siteid,
right(dm."Subject",7)::TEXT AS usubjid,
2.0::NUMERIC AS dsseq, --deprecated
'Consent'::TEXT AS dscat,
'Consented'::TEXT AS dsterm,
dm."DMICDAT"::DATE AS dsstdtc,
null::TEXT AS dsscat 
from tas3681_101."DM" dm

union all 

--Disposition Event: Failed Screen

SELECT  dm."project"::TEXT AS studyid,
right(dm."SiteNumber",3)::TEXT AS siteid,
right(dm."Subject",7)::TEXT AS usubjid,
2.1::NUMERIC AS dsseq, --deprecated
'Enrollement'::TEXT AS dscat,
'Failed Screen'::TEXT AS dsterm,
COALESCE(dm."MinCreated" ,dm."RecordDate")::DATE AS dsstdtc,
null::TEXT AS dsscat  
from tas3681_101."DM" dm
  
union all 

--Disposition Event: Enrollment

SELECT  dm."project"::TEXT AS studyid,
right(dm."SiteNumber",3)::TEXT AS siteid,
right(dm."Subject",7)::TEXT AS usubjid,
2.1::NUMERIC AS dsseq, --deprecated
'Enrollement'::TEXT AS dscat,
'Enrolled'::TEXT AS dsterm,
ex."EXOSTDAT"::DATE AS dsstdtc,
null::TEXT AS dsscat  
from tas3681_101."DM" dm
inner join tas3681_101."EXO" ex
on dm."project"=ex."project" and dm."SiteNumber"=ex."SiteNumber" and dm."Subject"=ex."Subject"

union all 

--Disposition Event: Withdrawn


SELECT  dm."project"::TEXT AS studyid,
right(dm."SiteNumber",3)::TEXT AS siteid,
right(dm."Subject",7)::TEXT AS usubjid,
4.1::NUMERIC AS dsseq, --deprecated
'Completion'::TEXT AS dscat,
'Withdrawn'::TEXT AS dsterm,
ds."DSDAT"::DATE AS dsstdtc,
null::TEXT AS dsscat  
from tas3681_101."DM" dm
inner join tas3681_101."DS" ds
on dm."project"=ds."project" and dm."SiteNumber"=ds."SiteNumber" and dm."Subject"=ds."Subject"


union all 

--Disposition Event: Study Completion

SELECT  dm."project"::TEXT AS studyid,
right(dm."SiteNumber",3)::TEXT AS siteid,
right(dm."Subject",7)::TEXT AS usubjid,
5.0::NUMERIC AS dsseq, --deprecated
'Completion'::TEXT AS dscat,
'Completed'::TEXT AS dsterm,
es."EOSDAT"::DATE AS dsstdtc,
null::TEXT AS dsscat  
from tas3681_101."DM" dm
inner join tas3681_101."EOS" es
on dm."project"=es."project" and dm."SiteNumber"=es."SiteNumber" and dm."Subject"=es."Subject"



)

SELECT
        /*KEY (ds.studyid || '~' || ds.siteid || '~' || ds.usubjid)::TEXT AS comprehendid, KEY*/
        ds.studyid::TEXT AS studyid,
        ds.siteid::TEXT AS siteid,
        ds.usubjid::TEXT AS usubjid,
        ds.dsseq::NUMERIC AS dsseq, --deprecated
        ds.dscat::TEXT AS dscat,
        ds.dsscat::TEXT AS dsscat,
        ds.dsterm::TEXT AS dsterm,
        ds.dsstdtc::DATE AS dsstdtc
        /*KEY , (ds.studyid || '~' || ds.siteid || '~' || ds.usubjid || '~' || ds.dsseq)::TEXT  AS objectuniquekey KEY*/
        /*KEY , now()::TIMESTAMP WITH TIME ZONE AS comprehend_update_time KEY*/
FROM ds_data ds
JOIN included_subjects s ON (ds.studyid = s.studyid AND ds.siteid = s.siteid AND ds.usubjid = s.usubjid);  