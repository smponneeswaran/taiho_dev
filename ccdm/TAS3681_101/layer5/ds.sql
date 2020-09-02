/*
CCDM DS mapping
Notes: Standard mapping to CCDM DS table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject ),

ds_data AS (

----Disposition Event: All Subjects----

SELECT  dm."project"::text AS studyid,
dm."SiteNumber"::text AS siteid,
dm."Subject"::text AS usubjid,
1.0::NUMERIC AS dsseq, 
'All Subjects'::text AS dscat,
'All Subjects'::text AS dsterm,
null::DATE AS dsstdtc,
null::text AS dsscat 
from tas3681_101."DM" dm

union all 

--Disposition Event: Consented

SELECT  dm."project"::text AS studyid,
dm."SiteNumber"::text AS siteid,
dm."Subject"::text AS usubjid,
2.0::NUMERIC AS dsseq, 
'Consent'::text AS dscat,
'Consented'::text AS dsterm,
dm."DMICDAT"::DATE AS dsstdtc,
null::text AS dsscat 
from tas3681_101."DM" dm

union all 

--Disposition Event: Failed Screen

SELECT  dm."project"::text AS studyid,
dm."SiteNumber"::text AS siteid,
dm."Subject"::text AS usubjid,
2.1::NUMERIC AS dsseq, 
'Enrollement'::text AS dscat,
'Failed Screen'::text AS dsterm,
COALESCE(dm."MinCreated" ,dm."RecordDate")::DATE AS dsstdtc,
null::text AS dsscat
from tas3681_101."DM" as dm
left join 
(
	select * from tas3681_101."IE"
	where ("project","SiteNumber", "Subject", "serial_id")
	in (
	
	select "project","SiteNumber", "Subject", max(serial_id)  as serial_id
	from tas3681_101."IE"
	group by 1,2,3
	)

) ie


on dm."project" = ie."project" AND dm."SiteNumber"= ie."SiteNumber" AND dm."Subject" = ie."Subject"
where ie."IEYN" = 'No'
  
union all 

--Disposition Event: Enrollment

SELECT  dm."project"::text AS studyid,
dm."SiteNumber"::text AS siteid,
dm."Subject"::text AS usubjid,
3.0::NUMERIC AS dsseq,
'Enrollement'::text AS dscat,
'Enrolled'::text AS dsterm,
min(ex."EXOSTDAT")::DATE AS dsstdtc,
null::text AS dsscat  
from tas3681_101."DM" dm
left join tas3681_101."EXO" ex
on dm."project"=ex."project" and dm."SiteNumber"=ex."SiteNumber" and dm."Subject"=ex."Subject"
group by  dm."project", dm."SiteNumber", dm."Subject"

union all 

--Disposition Event: Withdrawn


SELECT  dm."project"::text AS studyid,
dm."SiteNumber"::text AS siteid,
dm."Subject"::text AS usubjid,
4.1::NUMERIC AS dsseq, 
'Completion'::text AS dscat,
'Withdrawn'::text AS dsterm,
ds."DSDAT"::DATE AS dsstdtc,
null::text AS dsscat  
from tas3681_101."DM" dm
left join 

(
	select * from tas3681_101."DS"
	where ("project","SiteNumber", "Subject", "serial_id")
	in (
	
	select "project","SiteNumber", "Subject", max(serial_id)  as serial_id
	from tas3681_101."DS"
	group by 1,2,3
	)

) ds
on dm."project"=ds."project" and dm."SiteNumber"=ds."SiteNumber" and dm."Subject"=ds."Subject"
where ds."DSREAS" <> 'End of study per 2 protocol'


union all 

--Disposition Event: Study Completion

SELECT  dm."project"::text AS studyid,
dm."SiteNumber"::text AS siteid,
dm."Subject"::text AS usubjid,
5.0::NUMERIC AS dsseq, 
'Completion'::text AS dscat,
'Completed'::text AS dsterm,
es."EOSDAT"::DATE AS dsstdtc,
null::text AS dsscat
from tas3681_101."DM" dm
left join tas3681_101."EOS" es
on dm."project"=es."project" and dm."SiteNumber"=es."SiteNumber" and dm."Subject"=es."Subject"
where es."EOSREAS" = 'Study Completion'



)

SELECT
        /*KEY (ds.studyid || '~' || ds.siteid || '~' || ds.usubjid)::text AS comprehendid, KEY*/
        ds.studyid::text AS studyid,
        ds.siteid::text AS siteid,
        ds.usubjid::text AS usubjid,
        ds.dsseq::NUMERIC AS dsseq,
        ds.dscat::text AS dscat,
        ds.dsscat::text AS dsscat,
        ds.dsterm::text AS dsterm,
        ds.dsstdtc::DATE AS dsstdtc
        /*KEY , (ds.studyid || '~' || ds.siteid || '~' || ds.usubjid || '~' || ds.dsseq)::text  AS objectuniquekey KEY*/
        /*KEY , now()::TIMESTAMP WITH TIME ZONE AS comprehend_update_time KEY*/
FROM ds_data ds
JOIN included_subjects s ON (ds.studyid = s.studyid AND ds.siteid = s.siteid AND ds.usubjid = s.usubjid);  