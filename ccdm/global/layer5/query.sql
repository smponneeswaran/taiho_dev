/*
CCDM Query mapping
Notes: Standard mapping to CCDM Query table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject )

SELECT 
        /*KEY (q.studyid || '~' || s.siteid || '~' || q.usubjid)::text AS comprehendid, KEY*/
        q.studyId::text AS studyId,
        s.siteId::text AS siteId,
        q.usubjId::text AS usubjId,
        q.queryId::text AS queryId,
        q.formId::text AS formId,
        q.fieldId::text AS fieldId,
        q.querytext::text AS querytext,
        q.querytype::text AS querytype,
        q.querystatus::text AS querystatus,
        q.queryopeneddate::date AS queryopeneddate,
        q.queryresponsedate::date AS queryresponsedate,
        q.querycloseddate::date AS querycloseddate,
        q.visit::text AS visit,
        q.formseq::int AS formseq,
        q.log_num::int AS log_num
        /*KEY , (q.studyid || '~' || s.siteid || '~' || q.usubjid || '~' || q.queryid)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp without time zone AS comprehend_update_time KEY*/
FROM stg_querydata_tmp q
JOIN included_subjects s ON (q.studyid = s.studyid AND q.siteid = s.siteid AND q.usubjid = s.usubjid);