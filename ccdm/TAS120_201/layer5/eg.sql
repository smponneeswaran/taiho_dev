/*
CCDM EG mapping
Notes: Standard mapping to CCDM EG table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject ),

          eg_data AS (
     SELECT    eg.studyid,
                    eg.siteid,
                    eg.usubjid,
                    (row_number() over (partition by eg.studyid, eg.siteid, eg.usubjid order by eg.egseq, eg.egdtc))::int AS egseq,
                    eg.egtestcd,
                    eg.egtest,
                    eg.egcat,
                    eg.egscat,
                    eg.egpos,
                    eg.egorres,
                    eg.egorresu,
                    eg.egstresn,
                    eg.egstresu,
                    eg.egstat,
                    eg.egloc,
                    eg.egblfl,
                    eg.visit,
                    eg.egdtc,
                    eg.egtm 
                    from (
                SELECT
'TAS120-201'::text	AS	studyid	,
"SiteNumber"::text	AS	siteid	,
"Subject"::text	AS	usubjid	,
"RecordPosition"::int AS egseq,
'ECG'	::text	AS	egtestcd	,
'ECG'	::text	AS	egtest	,
'ECG'	::text	AS	egcat	,
'RR Interval'	::text	AS	egscat	,
'NA'::text	AS	egpos	,
"ECGRR"::text	AS	egorres	,
"ECGRR_Units"::text	AS	egorresu	,
"ECGRR"::numeric	AS	egstresn	,
"ECGRR_Units"::text	AS	egstresu	,
'NA'::text	AS	egstat	,
'NA'::text	AS	egloc	,
'NA'::text	AS	egblfl	,
"FolderName"::text	AS	visit	,
"ECGDAT"::timestamp without time zone	AS	egdtc,	
NULL::time without time zone	AS	egtm
from tas120_201."ECG"

union all

SELECT
"project"::text AS studyid,
"SiteNumber"::text AS siteid,
"Subject"::text AS usubjid,
"RecordPosition"::int AS egseq,
'ECG'::text AS egtestcd,
'ECG'::text AS egtest,
'ECG'::text AS egcat,
'Derived QTcF Interval'::text AS egscat,
'NA'::text AS egpos,
"ECGQTCF"::text AS egorres,
"ECGQTCF_Units"::text AS egorresu,
"ECGQTCF"::numeric AS egstresn,
"ECGQTCF_Units"::text AS egstresu,
'NA'::text AS egstat,
'NA'::text AS egloc,
'NA'::text AS egblfl,
"FolderName"::text AS visit,
"ECGDAT"::timestamp without time zone AS egdtc,
NULL::time without time zone AS egtm
from tas120_201."ECG"

UNION ALL

SELECT 
"project"::text AS studyid,
"SiteNumber"::text AS siteid,
"Subject"::text AS usubjid,
"RecordPosition"::int AS egseq,
'ECG'::text AS egtestcd,
'ECG'::text AS egtest,
'ECG'::text AS egcat,
'HR'::text AS egscat,
'NA'::text AS egpos,
"ECGHR"::text AS egorres,
"ECGHR_Units"::text AS egorresu,
"ECGHR"::numeric AS egstresn,
"ECGHR_Units"::text AS egstresu,
'NA'::text AS egstat,
'NA'::text AS egloc,
'NA'::text AS egblfl,
"FolderName"::text AS visit,
"ECGDAT"::timestamp without time zone AS egdtc,
NULL::time without time zone AS egtm
from tas120_201."ECG"

UNION ALL

SELECT
"project"::text AS studyid,
"SiteNumber"::text AS siteid,
"Subject"::text AS usubjid,
"RecordPosition"::int AS egseq,
'ECG'::text AS egtestcd,
'ECG'::text AS egtest,
'ECG'::text AS egcat,
'QT Interval'::text AS egscat,
'NA'::text AS egpos,
"ECGQT"::text AS egorres,
"ECGQT_Units"::text AS egorresu,
"ECGQT"::numeric AS egstresn,
"ECGQT_Units"::text AS egstresu,
'NA'::text AS egstat,
'NA'::text AS egloc,
'NA'::text AS egblfl,
"FolderName"::text AS visit,
"ECGDAT"::timestamp without time zone AS egdtc,
NULL::time without time zone AS egtm
from tas120_201."ECG") EG )

SELECT
        /*KEY (eg.studyid::text || '~' || eg.siteid::text || '~' || eg.usubjid::text) AS comprehendid, KEY*/
        eg.studyid::text AS studyid,
        eg.siteid::text AS siteid,
        eg.usubjid::text AS usubjid,
        eg.egseq::int AS egseq,
        eg.egtestcd::text AS egtestcd,
        eg.egtest::text AS egtest,
        eg.egcat::text AS egcat,
        eg.egscat::text AS egscat,
        eg.egpos::text AS egpos,
        eg.egorres::text AS egorres,
        eg.egorresu::text AS egorresu,
        eg.egstresn::numeric AS egstresn,
        eg.egstresu::text AS egstresu,
        eg.egstat::text AS egstat,
        eg.egloc::text AS egloc,
        eg.egblfl::text AS egblfl,
        eg.visit::text AS visit,
        eg.egdtc::timestamp without time zone AS egdtc,
        eg.egtm::time without time zone AS egtm
        /*KEY , (eg.studyid || '~' || eg.siteid || '~' || eg.usubjid || '~' || eg.egseq)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM eg_data eg
JOIN included_subjects s ON (eg.studyid = s.studyid AND eg.siteid = s.siteid AND eg.usubjid = s.usubjid);
