/*
CCDM SV mapping
Notes: Standard mapping to CCDM SV table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject ),

     sv_data AS (
                SELECT  'TAS120-201'::text AS studyid,
                        "SiteNumber"::text AS siteid,
                        "Subject"::text AS usubjid, 
                        "FolderSeq"::numeric AS visitnum,
                        "FolderName"::text AS visit,
                        1::int AS visitseq, /* defaulted to 1 - deprecated */
                        "VISITDAT"::date AS svstdtc,
                        "VISITDAT"::date AS svendtc
                        FROM tas120_201."VISIT" )

SELECT 
        /*KEY (sv.studyid || '~' || sv.siteid || '~' || sv.usubjid)::text AS comprehendid, KEY*/
        sv.studyid::text AS studyid,
        sv.siteid::text AS siteid,
        sv.usubjid::text AS usubjid, 
        sv.visitnum::numeric AS visitnum,
        sv.visit::text AS visit,
        sv.visitseq::int AS visitseq,
        sv.svstdtc::date AS svstdtc,
        sv.svendtc::date AS svendtc
        /*KEY , (sv.studyid || '~' || sv.siteid || '~' || sv.usubjid || '~' || sv.visitnum)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM sv_data sv
JOIN included_subjects s ON (sv.studyid = s.studyid AND sv.siteid = s.siteid AND sv.usubjid = s.usubjid);

