/*
CCDM Query mapping
Notes: Standard mapping to CCDM Query table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject ),

     query_data AS (
                SELECT  "study"::text AS studyId,
                        "sitename"::text AS siteId,
                        "subjectname"::text AS usubjId,
                        "id_"::text AS queryId,
                        "folder"::text AS formId,
                        "field"::text AS fieldId,
                        "querytext"::text AS querytext,
                        "markinggroupname"::text AS querytype,
                        "name"::text AS querystatus,
                        "qryopendate"::timestamp without time zone AS queryopeneddate, 
                        nullif ("qryresponsedate", '')::timestamp without time zone AS queryresponsedate,
                        nullif ("qrycloseddate", '')::timestamp without time zone AS querycloseddate,
                        "form"::text AS visit,
                        1::int AS formseq,
                        "log"::int AS log_num,
                        null::text AS querycreator 
			from tas120_201."stream_query_detail")

SELECT 
        /*KEY (q.studyid || '~' || q.siteid || '~' || q.usubjid)::text AS comprehendid, KEY*/
        q.studyId::text AS studyId,
        q.siteId::text AS siteId,
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
        q.log_num::int AS log_num,
        q.querycreator::text AS querycreator
        /*KEY , (q.studyid || '~' || q.siteid || '~' || q.usubjid || '~' || q.queryid)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM query_data q
JOIN included_subjects s ON (q.studyid = s.studyid AND q.siteid = s.siteid AND q.usubjid = s.usubjid);

