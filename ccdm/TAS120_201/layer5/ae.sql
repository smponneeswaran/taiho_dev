/*
CCDM AE mapping
Notes: Standard mapping to CCDM AE table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject),

     ae_data AS (
SELECT
'TAS120-201'::text	AS	studyid	,
"SiteNumber"::text	AS	siteid	,
"Subject"::text	AS	usubjid	,
"AETERM_PT"::text	AS	aeterm	,
"AETERM"::text	AS	aeverbatim	,
"AETERM_SOC"::text	AS	aebodsys	,
"AESTDAT"::timestamp without time zone	AS	aestdtc	,
"AEENDAT"::timestamp without time zone	AS	aeendtc	,
NULL::text	AS	aesev	,
"AESER"::text	AS	aeser	,
"AEREL"::text	AS	aerelnst,
"RecordPosition"::int AS aeseq	,
"AESTDAT"::timestamp without time zone	AS	aesttm	,
"AEENDAT"::timestamp without time zone	AS	aeentm	,
"AETERM_LLT"::text	AS	aellt	,
"AETERM_LLT_CD"::text	AS	aelltcd	,
"AETERM_PT_CD"::text	AS	aeptcd	,
"AETERM_HLT"::text	AS	aehlt	,
"AETERM_HLT_CD"::text	AS	aehltcd	,
"AETERM_HLGT"::text	AS	aehlgt	,
"AETERM_HLGT_CD"::text	AS	aehlgtcd	,
NULL::text	AS	aebdsycd	,
"AETERM_SOC"::text	AS	aesoc	,
"AETERM_SOC_CD"::text	AS	aesoccd	,
COALESCE("AEACTSN","AEACTSDR","AEACTSID","AEACTSDQ") as aeacn
from tas120_201."AE"

UNION ALL

Select 
'TAS120-201'::text	AS	studyid,
"SiteNumber"::text	AS	siteid,
"Subject"::text	AS	usubjid,
"AETERM_PT"::text	AS	aeterm,
"AETERM"::text	AS	aeverbatim,
"AETERM_SOC"::text	AS	aebodsys,
"AESTDAT"::timestamp without time zone	AS	aestdtc,
"AEENDAT"::timestamp without time zone	AS	aeendtc,
"AEGR"::text	AS	aesev,
"AESER"::text	AS	aeser,
"AEREL"::text	AS	aerelnst,
"RecordPosition"::int AS aeseq,
"AESTDAT"::timestamp without time zone	AS	aesttm,
"AEENDAT"::timestamp without time zone	AS	aeentm,
"AETERM_LLT"::text	AS	aellt,
"AETERM_LLT_CD"::text	AS	aelltcd,
"AETERM_PT_CD"::text	AS	aeptcd,
"AETERM_HLT"::text	AS	aehlt,
"AETERM_HLT_CD"::text	AS	aehltcd,
"AETERM_HLGT"::text	AS	aehlgt,
"AETERM_HLGT_CD"::text	AS	aehlgtcd,
NULL::text	AS	aebdsycd,
"AETERM_SOC"::text	AS	aesoc,
"AETERM_SOC_CD"::text	AS	aesoccd,
COALESCE("AEACTSN","AEACTSDR","AEACTSID","AEACTSDQ") as aeacn
from tas120_201."AE2" )

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
JOIN included_subjects s ON (ae.studyid = s.studyid AND ae.siteid = s.siteid AND ae.usubjid = s.usubjid);
