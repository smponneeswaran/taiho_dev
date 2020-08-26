/*
CCDM CM mapping
Notes: Standard mapping to CCDM CM table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject ),

     cm_data AS (

                SELECT  "project"::text AS studyid,
                        "SiteNumber"::text AS siteid,
                        "Subject"::text AS usubjid,
                        "RecordPosition"::integer AS cmseq,
                        "CMTRT"::text AS cmtrt,
                        "CMINDC"::text AS cmmodify,
                        null::text AS cmdecod,
                        null::text AS cmcat,
                        'Concomitant Medications'::text AS cmscat,
                        "CMINDC"::text AS cmindc,
                        "CMDU"::numeric AS cmdose,
						null::text AS cmdosu,
						null::text AS cmdosfrm,
						null::text AS cmdosfrq,
                        null::numeric AS cmdostot,
                        case when "CMROUTE"='Other' then concat("CMROUTE"||'-'||"CMROUTEOTH") else "CMROUTE" end ::text AS cmroute,
                        "CMSTDAT_RAW"::timestamp without time zone AS cmstdtc,
                        "CMSTDAT_RAW"::timestamp without time zone AS cmendtc,
                        null::time without time zone AS cmsttm,
                        null::time without time zone AS cmentm
		From "tas120_202"."CM"


     )

SELECT 
        /*KEY (cm.studyid || '~' || cm.siteid || '~' || cm.usubjid)::text AS comprehendid, KEY*/
        cm.studyid::text AS studyid,
        cm.siteid::text AS siteid,
        cm.usubjid::text AS usubjid,
        cm.cmseq::integer AS cmseq,
        cm.cmtrt::text AS cmtrt,
        cm.cmmodify::text AS cmmodify,
        cm.cmdecod::text AS cmdecod,
        cm.cmcat::text AS cmcat,
        cm.cmscat::text AS cmscat,
        cm.cmindc::text AS cmindc,
        cm.cmdose::numeric AS cmdose,
        cm.cmdosu::text AS cmdosu,
        cm.cmdosfrm::text AS cmdosfrm,
        cm.cmdosfrq::text AS cmdosfrq,
        cm.cmdostot::numeric AS cmdostot,
        cm.cmroute::text AS cmroute,
        cm.cmstdtc::timestamp without time zone AS cmstdtc,
        cm.cmendtc::timestamp without time zone AS cmendtc,
        cm.cmsttm::time without time zone AS cmsttm,
        cm.cmentm::time without time zone AS cmentm
        /*KEY , (cm.studyid || '~' || cm.siteid || '~' || cm.usubjid || '~' || cm.cmseq)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp without time zone AS comprehend_update_time KEY*/
FROM cm_data cm
JOIN included_subjects s ON (cm.studyid = s.studyid AND cm.siteid = s.siteid AND cm.usubjid = s.usubjid);

