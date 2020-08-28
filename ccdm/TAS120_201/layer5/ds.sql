/*
CCDM DS mapping
Notes: Standard mapping to CCDM DS table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject ),

ds_data AS (

----Disposition Event: All Subjects----

SELECT  'TAS120-201'::TEXT AS studyid,
dm."SiteNumber"::TEXT AS siteid,
dm."Subject"::TEXT AS usubjid,
1.0::NUMERIC AS dsseq, --deprecated
'All Subject'::TEXT AS dscat,
null::TEXT AS dsscat,
'All Subject'::TEXT AS dsterm,
null::DATE AS dsstdtc,  
null::TEXT AS dsgrpid,
null::TEXT AS dsrefid,
null::TEXT AS dsspid,
null::TEXT AS dsdecod,
null::TEXT AS visit,
null::NUMERIC AS visitnum,
null::INTEGER AS visitdy,
null::TEXT AS epoch,
null::TIMESTAMP WITHOUT TIME ZONE AS dsdtc,
null::INTEGER AS dsstdy
from tas120_201."DM" dm

union all 

--Disposition Event: Consented

SELECT  'TAS120-201'::TEXT AS studyid,
enr."SiteNumber"::TEXT AS siteid,
enr."Subject"::TEXT AS usubjid,
2.0::NUMERIC AS dsseq, --deprecated
'Consent'::TEXT AS dscat,
null::TEXT AS dsscat,
'Consented'::TEXT AS dsterm,
enr."DMICDAT"::DATE AS dsstdtc,  
null::TEXT AS dsgrpid,
null::TEXT AS dsrefid,
null::TEXT AS dsspid,
null::TEXT AS dsdecod,
null::TEXT AS visit,
null::NUMERIC AS visitnum,
null::INTEGER AS visitdy,
null::TEXT AS epoch,
null::TIMESTAMP WITHOUT TIME ZONE AS dsdtc,
null::INTEGER AS dsstdy
from tas120_201."ENR" enr

union all 

--Disposition Event: Failed Screen

SELECT  'TAS120-201'::TEXT AS studyid,
enr."SiteNumber"::TEXT AS siteid,
enr."Subject"::TEXT AS usubjid,
2.1::NUMERIC AS dsseq, --deprecated
'Enrollement'::TEXT AS dscat,
null::TEXT AS dsscat,
'Failed Screen'::TEXT AS dsterm,
COALESCE(enr."MinCreated" ,enr."RecordDate")::DATE AS dsstdtc,  
null::TEXT AS dsgrpid,
null::TEXT AS dsrefid,
null::TEXT AS dsspid,
null::TEXT AS dsdecod,
null::TEXT AS visit,
null::NUMERIC AS visitnum,
null::INTEGER AS visitdy,
null::TEXT AS epoch,
null::TIMESTAMP WITHOUT TIME ZONE AS dsdtc,
null::INTEGER AS dsstdy
from tas120_201."ENR" enr

union all 

--Disposition Event: Enrollment

SELECT  'TAS120-201'::TEXT AS studyid,
enr."SiteNumber"::TEXT AS siteid,
enr."Subject"::TEXT AS usubjid,
3.0::NUMERIC AS dsseq, --deprecated
'Enrollement'::TEXT AS dscat,
null::TEXT AS dsscat,
'Enrolled'::TEXT AS dsterm,
enr."ENRDAT"::DATE AS dsstdtc,  
null::TEXT AS dsgrpid,
null::TEXT AS dsrefid,
null::TEXT AS dsspid,
null::TEXT AS dsdecod,
null::TEXT AS visit,
null::NUMERIC AS visitnum,
null::INTEGER AS visitdy,
null::TEXT AS epoch,
null::TIMESTAMP WITHOUT TIME ZONE AS dsdtc,
null::INTEGER AS dsstdy
from tas120_201."ENR" enr

union all 

--Disposition Event: Withdrawn

SELECT  'TAS120-201'::TEXT AS studyid,
ds."SiteNumber"::TEXT AS siteid,
ds."Subject"::TEXT AS usubjid,
4.1::NUMERIC AS dsseq, --deprecated
'Completion'::TEXT AS dscat,
ds."DSREAS"::TEXT AS dsscat,
'Withdrawn'::TEXT AS dsterm,
ds."DSDAT"::DATE AS dsstdtc,  
null::TEXT AS dsgrpid,
null::TEXT AS dsrefid,
null::TEXT AS dsspid,
null::TEXT AS dsdecod,
null::TEXT AS visit,
null::NUMERIC AS visitnum,
null::INTEGER AS visitdy,
null::TEXT AS epoch,
null::TIMESTAMP WITHOUT TIME ZONE AS dsdtc,
null::INTEGER AS dsstdy
from tas120_201."DS" ds

union all 
--Disposition Event: Withdrawn_2

SELECT  'TAS120-201'::TEXT AS studyid,
eos."SiteNumber"::TEXT AS siteid,
eos."Subject"::TEXT AS usubjid,
4.1::NUMERIC AS dsseq, --deprecated
'Completion'::TEXT AS dscat,
null::TEXT AS dsscat,
'Withdrawn'::TEXT AS dsterm,
eos."EOS_DAT"::DATE AS dsstdtc,  
null::TEXT AS dsgrpid,
null::TEXT AS dsrefid,
null::TEXT AS dsspid,
null::TEXT AS dsdecod,
null::TEXT AS visit,
null::NUMERIC AS visitnum,
null::INTEGER AS visitdy,
null::TEXT AS epoch,
null::TIMESTAMP WITHOUT TIME ZONE AS dsdtc,
null::INTEGER AS dsstdy
from tas120_201."EOS" eos

union all 

--Disposition Event: Study Completion

SELECT  'TAS120-201'::TEXT AS studyid,
eos."SiteNumber"::TEXT AS siteid,
eos."Subject"::TEXT AS usubjid,
5.0::NUMERIC AS dsseq, --deprecated
'Completion'::TEXT AS dscat,
null::TEXT AS dsscat,
'completed'::TEXT AS dsterm,
eos."EOS_DAT"::DATE AS dsstdtc,  
null::TEXT AS dsgrpid,
null::TEXT AS dsrefid,
null::TEXT AS dsspid,
null::TEXT AS dsdecod,
null::TEXT AS visit,
null::NUMERIC AS visitnum,
null::INTEGER AS visitdy,
null::TEXT AS epoch,
null::TIMESTAMP WITHOUT TIME ZONE AS dsdtc,
null::INTEGER AS dsstdy
from tas120_201."EOS" eos
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
        ds.dsstdtc::DATE AS dsstdtc,
        ds.dsgrpid::TEXT AS dsgrpid,
        ds.dsrefid::TEXT AS dsrefid,
        ds.dsspid::TEXT AS dsspid,
        ds.dsdecod::TEXT AS dsdecod,
        ds.visit::TEXT AS visit,
        ds.visitnum::NUMERIC AS visitnum,
        ds.visitdy::INTEGER AS visitdy,
        ds.epoch::TEXT AS epoch,
        ds.dsdtc::TIMESTAMP WITHOUT TIME ZONE AS dsdtc,
        ds.dsstdy::INTEGER AS dsstdy
        /*KEY , (ds.studyid || '~' || ds.siteid || '~' || ds.usubjid || '~' || ds.dsseq)::TEXT  AS objectuniquekey KEY*/
        /*KEY , now()::TIMESTAMP WITH TIME ZONE AS comprehend_update_time KEY*/
FROM ds_data ds
JOIN included_subjects s ON (ds.studyid = s.studyid AND ds.siteid = s.siteid AND ds.usubjid = s.usubjid);
