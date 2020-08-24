/* Query mapping 
Client: RGN 
Notes:  
*/ 
WITH included_subjects AS (SELECT studyid, siteid, usubjid FROM subject),  

    edc_query AS ( 
        SELECT "study" ::text   AS studyid, 
            "sitename"::text AS siteid,
            "subjectname"::text AS usubjid, 
            "id_"::text AS queryid,
            "form"::text    as visit,
            "folder"::text AS formid, 
            "field"::text AS fieldid, 
            "querytext"::text AS querytext, 
            "markinggroupname"::text AS querytype, 
            "name"::text AS querystatus, 
            "qryopendate"::date AS queryopeneddate, 
            "qryresponsedate"::date AS queryresponsedate,
            "qrycloseddate"::date AS querycloseddate,
            1::int as formseq,
            "log" as log_num
        FROM "tas120_202"."stream_query_detail"
        
        
)
 
SELECT 
    /*KEY (e.studyid || '~' || e.siteid || '~' || e.usubjid) AS comprehendId, KEY*/ 
    e.studyid :: text           AS studyid,
    e.siteid :: text            AS siteid, 
    e.usubjid :: text           AS usubjid, 
    e.queryid :: text           AS queryid, 
    e.visit :: text            AS visit, 
    e.formid :: text            AS formid, 
    e.fieldid :: text           AS fieldid, 
    e.querytext :: text         AS querytext, 
    e.querytype :: text         AS querytype, 
    e.querystatus :: text       AS querystatus, 
    e.queryopeneddate :: DATE   AS queryopeneddate, 
    e.queryresponsedate :: DATE AS queryresponsedate, 
    e.querycloseddate :: DATE   AS querycloseddate, 
    e.formseq::int              AS formseq,
    e.log_num::int              AS log_num 
    /*KEY , (e.studyid || '~' || e.siteid ||'~' || e.usubjid||'~' ||e.queryid) AS objectuniquekey  KEY*/
    /*KEY , now()::timestamp without time zone AS comprehend_update_time KEY*/ 
FROM   edc_query e 
JOIN included_subjects s ON (e.studyid = s.studyid AND e.siteid = s.siteid AND e.usubjid = s.usubjid);
