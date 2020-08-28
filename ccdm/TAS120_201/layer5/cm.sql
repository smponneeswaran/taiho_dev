/*
CCDM CM mapping
Notes: Standard mapping to CCDM CM table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject ),

          cm_data AS (
                SELECT 
				'TAS120-201'	::text	AS	studyid	,
				"SiteNumber"	::text	AS	siteid	,
				"Subject"	::text	AS	usubjid	,
				"RecordPosition"::integer AS cmseq,
				"CMTRT"::text	AS	cmtrt	,
				"CMINDC"::text	AS	cmmodify	,
				"CMTRT_PT"::text	AS	cmdecod	,
				"CMTRT_ATC"::text	AS	cmcat	,
				'Concomitant Medications'::text	AS	cmscat	,
				"CMINDC"::text	AS	cmindc	,
				"CMDU"::text 	AS	cmdose	,
				NULL::text	AS	cmdosu	,
				NULL::text	AS	cmdosfrm	,
				NULL::text	AS	cmdosfrq	,
				NULL::text 	AS	cmdostot	,
				CASE WHEN "CMROUTE"='Other' then  "CMROUTE" || "CMROUTEOTH"   ELSE  "CMROUTE"::text END	AS	cmroute	,
				"CMSTDAT_RAW"::text	AS	cmstdtc	,
				"CMENDAT_RAW"::text	AS	cmendtc	,
				NULL::time without time zone	AS	cmsttm	,
				NULL::time without time zone	AS	cmentm
				FROM tas120_201."CM"  )

SELECT 
        (cm.studyid || '~' || cm.siteid || '~' || cm.usubjid)::text AS comprehendid,
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
        cm.cmdose::text AS cmdose,
        cm.cmdosu::text AS cmdosu,
        cm.cmdosfrm::text AS cmdosfrm,
        cm.cmdosfrq::text AS cmdosfrq,
        cm.cmdostot::text AS cmdostot,
        cm.cmroute::text AS cmroute,
        cm.cmstdtc::text cmstdtc,
        cm.cmendtc::text AS cmendtc,
        cm.cmsttm::time without time zone AS cmsttm,
        cm.cmentm::time without time zone AS cmentm,
       (cm.studyid || '~' || cm.siteid || '~' || cm.usubjid || '~' || cm.cmseq)::text  AS objectuniquekey,
        now()::timestamp with time zone AS comprehend_update_time
FROM cm_data cm
JOIN included_subjects s ON (cm.studyid = s.studyid AND cm.siteid = s.siteid AND cm.usubjid = s.usubjid);
