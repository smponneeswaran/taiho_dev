
/*
CCDM EX mapping
Notes: Standard mapping to CCDM EX table
*/

WITH included_subjects AS (
    SELECT DISTINCT studyid, siteid, usubjid FROM subject),

    ex_data AS (
             -- TAS3681-101
             SELECT
                ex."project"::				text      AS studyid,
                ex."SiteNumber"::	text      AS siteid,
                ex."Subject"::	text      AS usubjid,
                row_number() over (partition by ex."studyid", ex."siteid", ex."Subject" ORDER BY ex."EXOSTDAT")::int AS exseq,
                ex."FolderName"::			text      AS visit,
                'TAS3681'::					text      AS extrt,
                'Prostate Cancer'::					text      AS excat,
                NULL::					text      AS exscat,
                ex."EXOSDOSE"::			NUMERIC   AS exdose,
                NULL::					text      AS exdostxt,
                ex."EXOSDOSE_Units"::	text      AS exdosu,
                NULL::					text      AS exdosfrm,
                NULL::					text      AS exdosfrq,
                NULL::					NUMERIC   AS exdostot,
                ex."EXOSTDAT"::			DATE      AS exstdtc,
                ex."EXOENDAT"::time without time zone AS exsttm,
                NULL::					INT 	  AS exstdy,
                ex."EXOENDAT"::			DATE      AS exendtc,
                ex."EXOEMDAT"::time without time zone AS exendtm,
                NULL::					INT       AS exendy,
                NULL::					text      AS exdur
            FROM tas3681_101."EXO" ex
            
     
            
     )

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
      /*KEY , now()::timestamp without time zone AS comprehend_update_time KEY*/
FROM ex_data ex
JOIN included_subjects s ON (ex.studyid = s.studyid AND ex.siteid = s.siteid AND ex.usubjid = s.usubjid);
