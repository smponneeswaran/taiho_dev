WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject),

     ex_data AS (
                SELECT 
"SiteNumber"::text	AS	siteid,
"Subject"::text	AS	usubjid,
"RecordPosition"::INT   AS exseq,
"project"::text	AS	studyid,
"FolderName"::text	AS	visit,
'TAS120-201'::text	AS	extrt,
'Metastatic Breast Cancer'::text	AS	excat,
NULL::text	AS	exscat,
"EXOSDOSE"::numeric	AS	exdose,
NULL::text	AS	exdostxt,
"EXOSDOSE_Units"::text	AS	exdosu,
NULL::text	AS	exdosfrm,
NULL::text	AS	exdosfrq,
NULL::text	AS	exdostot,
"EXOSTDAT"::text	AS	exstdtc,
NULL::time without time zone	AS	exsttm,
NULL::text	AS	exstdy,
"EXOENDAT"::date	AS	exendtc,
NULL::time without time zone	AS	exendtm,
NULL::text	AS	exendy,
NULL::text	AS	exdur
from tas120_201."EXO"

UNION ALL

SELECT 
"SiteNumber"::text	AS	siteid,
"Subject"::text	AS	usubjid,
"RecordPosition"::INT  AS exseq,
'TAS120-201'::text	AS	studyid,
"FolderName"::text	AS	visit,
'TAS120-201'::text	AS	extrt,
'Metastatic Breast Cancer'::text	AS	excat,
NULL::text	AS	exscat,
"EXISDOSE"::numeric	AS	exdose,
NULL::text	AS	exdostxt,
"EXISDOSE_Units"::text	AS	exdosu,
NULL::text	AS	exdosfrm,
NULL::text	AS	exdosfrq,
NULL::text	AS	exdostot,
"EXISTDAT"::text	AS	exstdtc,
NULL::time without time zone	AS	exsttm,
NULL::text	AS	exstdy,
null::date AS exendtc,
NULL::time without time zone	AS	exendtm,
NULL::text	AS	exendy,
NULL::text	AS	exdur
from tas120_201."EXI" )

SELECT
        /*KEY (ex.studyid || '~' || ex.siteid || '~' || ex.usubjid)::text AS comprehendid, KEY*/
        ex.studyid::text AS studyid,
        ex.siteid::text AS siteid,
        ex.usubjid::text AS usubjid,
        ex.exseq::int AS exseq, 
        ex.visit::text AS visit,
        ex.extrt::text AS extrt,
        ex.excat::text AS excat,
        ex.exscat::text AS exscat,
        ex.exdose::numeric AS exdose,
        ex.exdostxt::text AS exdostxt,
        ex.exdosu::text AS exdosu,
        ex.exdosfrm::text AS exdosfrm,
        ex.exdosfrq::text AS exdosfrq,
        ex.exdostot::numeric AS exdostot,
        ex.exstdtc::date AS exstdtc,
        ex.exsttm::time AS exsttm,
        ex.exstdy::int AS exstdy,
        ex.exendtc::date AS exendtc,
        ex.exendtm::time AS exendtm,
        ex.exendy::int AS exendy,
        ex.exdur::text AS exdur
        /*KEY , (ex.studyid || '~' || ex.siteid || '~' || ex.usubjid || '~' || ex.exseq)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM ex_data ex
JOIN included_subjects s ON (ex.studyid = s.studyid AND ex.siteid = s.siteid AND ex.usubjid = s.usubjid);
