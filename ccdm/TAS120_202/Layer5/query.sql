/*
CCDM Query mapping
Notes: Standard mapping to CCDM Query table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject ),

     query_data AS (
            SELECT left("study"::text, strpos("study", ' - ') - 1)::text AS studyId,
                        left("sitename"::text, strpos("sitename", '_') - 1)::text AS siteId,
                        "subjectname"::text AS usubjId,
                        "id_"::text AS queryId,
                        "folder"::text AS formId,
						"form"::text AS visit,
						1::int AS formseq,
						"log"::int AS log_num,
                        "field"::text AS fieldId,
                        "querytext"::text AS querytext,
                        "markinggroupname"::text AS querytype,
                        "name"::text AS querystatus,
                        "qryopendate"::text AS queryopeneddate,
                        "qryresponsedate"::text AS queryresponsedate,
                        "qrycloseddate"::text AS querycloseddate,
                        "qryopenby"::text AS querycreator
	FROM tas120_202."stream_query_detail" 
					)

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
JOIN included_subjects s ON (q.studyid = s.studyid AND q.siteid = s.siteid AND q.usubjid = s.usubjid)
;
