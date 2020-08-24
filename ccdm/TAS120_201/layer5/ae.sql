/*
CCDM AE mapping
Notes: Standard mapping to CCDM AE table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject),

     ae_data AS (
                SELECT  null::text AS studyid,
                        null::text AS siteid,
                        null::text AS usubjid,
                        null::text AS aeterm,
                        null::text AS aeverbatim,
                        null::text AS aebodsys,
                        null::timestamp without time zone AS aestdtc,
                        null::timestamp without time zone AS aeendtc,
                        null::text AS aesev,
                        null::text AS aeser,
                        null::text AS aerelnst,
                        null::int AS aeseq,
                        null::time without time zone AS aesttm,
                        null::time without time zone AS aeentm,
                        null::text AS aellt,
                        null::int AS aelltcd,
                        null::int AS aeptcd,
                        null::text AS aehlt,
                        null::int AS aehltcd,
                        null::text AS aehlgt,
                        null::int AS aehlgtcd,
                        null::int AS aebdsycd,
                        null::text AS aesoc,
                        null::int AS aesoccd,
                        null::text AS aeacn )

SELECT 
        /*KEY (ae.studyid || '~' || ae.siteid || '~' || ae.usubjid)::text AS comprehendid, KEY*/
        ae.studyid::text AS studyid,
        ae.siteid::text AS siteid,
        ae.usubjid::text AS usubjid,
        ae.aeterm::text AS aeterm,
        ae.aeverbatim::text AS aeverbatim,
        ae.aebodsys::text AS aebodsys,
        ae.aestdtc::timestamp without time zone AS aestdtc,
        ae.aeendtc::timestamp without time zone AS aeendtc,
        ae.aesev::text AS aesev,
        ae.aeser::text AS aeser,
        ae.aerelnst::text AS aerelnst,
        ae.aeseq::int AS aeseq,
        ae.aesttm::time without time zone AS aesttm,
        ae.aeentm::time without time zone AS aeentm,
        ae.aellt::text AS aellt,
        ae.aelltcd::int AS aelltcd,
        ae.aeptcd::int AS aeptcd,
        ae.aehlt::text AS aehlt,
        ae.aehltcd::int AS aehltcd,
        ae.aehlgt::text AS aehlgt,
        ae.aehlgtcd::int AS aehlgtcd,
        ae.aebdsycd::int AS aebdsycd,
        ae.aesoc::text AS aesoc,
        ae.aesoccd::int AS aesoccd,
        ae.aeacn::text AS aeacn
        /*KEY , (ae.studyid || '~' || ae.siteid || '~' || ae.usubjid || '~' || ae.aeseq)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM ae_data ae
JOIN included_subjects s ON (ae.studyid = s.studyid AND ae.siteid = s.siteid AND ae.usubjid = s.usubjid)
WHERE 1=2;  

