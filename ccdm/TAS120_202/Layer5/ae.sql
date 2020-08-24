/*
CCDM AE mapping
Notes: Standard mapping to CCDM AE table
*/

WITH included_subjects AS (
    SELECT DISTINCT studyid, siteid, usubjid FROM subject),


    ae_data AS (
                SELECT "project"::text AS studyid,
                       "SiteNumber"::text AS siteid,
                       "Subject"::text AS usubjid,
                       "AETERM_PT"::text AS aeterm,
                       "AETERM"::text AS aeverbatim,
                       "AETERM_SOC"::text AS aebodsys,
                       "AESTDAT"::timestamp without time zone AS aestdtc,
                       "AEENDAT"::timestamp without time zone AS aeendtc,
                       "AEGR"::text AS aesev,
                       "AESER"::text as aeser,
                       "AEREL"::text as aerelnst,
                       null::int AS aeseq,
                       "AESTDAT"::text AS aesttm,
                       "AEENDAT"::text AS aeentm,
					   null::text AS aellt,
					   null::text AS aelltcd,
					   null::text AS aeptcd,
					   null::text AS aehlt,
					   null::text AS aehltcd,
					   null::text AS aehlgt,
					   null::text AS aehlgtcd,
					   null::text AS aebdsycd,
					   null::text AS aesoc,
					   null::text AS aesoccd,
					   null ::text  AS aeacn
				FROM "tas120_202"."AE")



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
        ae.aeentm::time without time zone AS aeentm
        /*KEY , (ae.studyid || '~' || ae.siteid || '~' || ae.usubjid || '~' || ae.aeseq)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp without time zone AS comprehend_update_time KEY*/
FROM ae_data ae
JOIN included_subjects s ON (ae.studyid = s.studyid AND ae.siteid = s.siteid AND ae.usubjid = s.usubjid);


