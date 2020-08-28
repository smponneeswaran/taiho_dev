/*
CCDM VS mapping
Notes: Standard mapping to CCDM VS table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject),

vs_data AS (
                SELECT
'TAS120-201'::text	AS	studyid	,
"SiteNumber"::text	AS	siteid	,
"Subject"::text	AS	usubjid	,
null::integer	AS	vsseq	,
null::text	AS	vstestcd	,
"VSDBP"::text	AS	vstest	,
'Diastolic Blood Pressure'::text	AS	vscat	,
'Vital Signs'::text	AS	vsscat	,
'Vital Signs'::text	AS	vspos	,
"VSDBP"::text	AS	vsorres	,
"VSDBP_Units"::text	AS	vsorresu	,
"VSDBP"::numeric	AS	vsstresn	,
"VSDBP_Units"::text	AS	vsstresu	,
null::text	AS	vsstat	,
null::text	AS	vsloc	,
null::text	AS	vsblfl	,
"FolderName"::text	AS	visit	,
"VSDAT"::timestamp without time zone	AS	vsdtc	,
null::time without time zone	AS	vstm
FROM tas120_201."VS"

UNION ALL

SELECT
'TAS120-201'::text	AS	studyid	,
"SiteNumber"::text	AS	siteid	,
"Subject"::text	AS	usubjid	,
null::integer	AS	vsseq	,
null::text	AS	vstestcd	,
"VSSBP"::text	AS	vstest	,
'Systolic Blood Pressure'::text	AS	vscat	,
'Vital Signs'::text	AS	vsscat	,
'Vital Signs'::text	AS	vspos	,
"VSSBP"::text	AS	vsorres	,
"VSSBP_Units"::text	AS	vsorresu	,
"VSSBP"::numeric	AS	vsstresn	,
"VSSBP_Units"::text	AS	vsstresu	,
null	::text	AS	vsstat	,
null	::text	AS	vsloc	,
null	::text	AS	vsblfl	,
"FolderName"::text	AS	visit	,
"VSDAT"::timestamp without time zone	AS	vsdtc	,
null	::time without time zone	AS	vstm
FROM tas120_201."VS"


UNION ALL

SELECT
'TAS120-201'::text	AS	studyid	,
"SiteNumber"::text	AS	siteid	,
"Subject"::text	AS	usubjid	,
null::integer	AS	vsseq	,
null::text	AS	vstestcd	,
"VSPR"::text	AS	vstest	,
'Pressure Rate'::text	AS	vscat	,
'Vital Signs'::text	AS	vsscat	,
'Vital Signs'::text	AS	vspos	,
"VSPR"::text	AS	vsorres	,
"VSPR_Units"::text	AS	vsorresu	,
"VSPR"::numeric	AS	vsstresn	,
"VSPR_Units"::text	AS	vsstresu	,
null::text	AS	vsstat	,
null::text	AS	vsloc	,
null::text	AS	vsblfl	,
"FolderName"::text	AS	visit	,
"VSDAT"::timestamp without time zone	AS	vsdtc	,
null::time without time zone	AS	vstm
FROM tas120_201."VS"

UNION ALL

SELECT	
'TAS120-201'::text	AS	studyid	,
"SiteNumber"::text	AS	siteid	,
"Subject"::text	AS	usubjid	,
null::integer	AS	vsseq	,
null::text	AS	vstestcd	,
"VSRESP"::text	AS	vstest	,
'Respiratory Rate'::text	AS	vscat	,
'Vital Signs'::text	AS	vsscat	,
'Vital Signs'::text	AS	vspos	,
"VSRESP"::text	AS	vsorres	,
"VSRESP_Units"::text	AS	vsorresu	,
"VSRESP"::numeric	AS	vsstresn	,
"VSRESP_Units"::text	AS	vsstresu	,
null::text	AS	vsstat	,
null::text	AS	vsloc	,
null::text	AS	vsblfl	,
"FolderName"::text	AS	visit	,
"VSDAT"::timestamp without time zone	AS	vsdtc	,
null::time without time zone	AS	vstm
FROM tas120_201."VS"

UNION ALL

SELECT
'TAS120-201'::text	AS	studyid	,
"SiteNumber"::text	AS	siteid	,
"Subject"::text	AS	usubjid	,
null::integer	AS	vsseq	,
null::text	AS	vstestcd	,
"VSTEMP"::text	AS	vstest	,
'Temperature'::text	AS	vscat	,
'Vital Signs'::text	AS	vsscat	,
'Vital Signs'::text	AS	vspos	,
"VSTEMP"::text	AS	vsorres	,
"VSTEMP_Units"::text	AS	vsorresu	,
"VSTEMP"::numeric	AS	vsstresn	,
"VSTEMP_Units"::text	AS	vsstresu	,
null::text	AS	vsstat	,
null::text	AS	vsloc	,
null::text	AS	vsblfl	,
"FolderName"::text	AS	visit	,
"VSDAT"::timestamp without time zone	AS	vsdtc	,
null::time without time zone	AS	vstm
FROM tas120_201."VS"

UNION ALL

SELECT
'TAS120-201'::text	AS	studyid	,
"SiteNumber"::text	AS	siteid	,
"Subject"::text	AS	usubjid	,
NULL	::integer	AS	vsseq	,
NULL::text	AS	vstestcd	,
"VSWT"::text	AS	vstest	,
'Weight'::text	AS	vscat	,
'Vital Signs'::text	AS	vsscat	,
'Vital Signs'::text	AS	vspos	,
"VSWT"::text	AS	vsorres	,
"VSWT_Units"::text	AS	vsorresu	,
"VSWT"::numeric	AS	vsstresn	,
"VSWT_Units"::text	AS	vsstresu	,
NULL::text	AS	vsstat	,
NULL::text	AS	vsloc	,
NULL::text	AS	vsblfl	,
"FolderName"::text	AS	visit	,
"VSDAT"::timestamp without time zone	AS	vsdtc	,
NULL::time without time zone	AS	vstm
FROM tas120_201."VS"

UNION ALL

SELECT
'TAS120-201'::text	AS	studyid	,
"SiteNumber"::text	AS	siteid	,
"Subject"::text	AS	usubjid	,
NULL::integer	AS	vsseq	,
NULL::text	AS	vstestcd	,
"VSHT"::text	AS	vstest	,
'Height'::text	AS	vscat	,
'Vital Signs'::text	AS	vsscat	,
'Vital Signs'::text	AS	vspos	,
"VSHT"::text	AS	vsorres	,
"VSHT_Units"::text	AS	vsorresu	,
"VSHT"::numeric	AS	vsstresn	,
"VSHT_Units"::text	AS	vsstresu	,
NULL::text	AS	vsstat	,
NULL::text	AS	vsloc	,
NULL::text	AS	vsblfl	,
"FolderName"::text	AS	visit	,
"VSDAT"::timestamp without time zone	AS	vsdtc	,
NULL::time without time zone	AS	vstm
FROM tas120_201."VS"

UNION ALL

select 
'TAS120-201'::text	AS	studyid	,
"SiteNumber"::text	AS	siteid	,
"Subject"::text	AS	usubjid	,
NULL::integer	AS	vsseq	,
NULL::text	AS	vstestcd	,
"VSDBP"::text	AS	vstest	,
'Diastolic Blood Pressure'::text	AS	vscat	,
'Vital Signs'::text	AS	vsscat	,
'Vital Signs'::text	AS	vspos	,
"VSDBP"::text	AS	vsorres	,
"VSDBP_Units"::text	AS	vsorresu	,
"VSDBP"::numeric	AS	vsstresn	,
"VSDBP_Units"::text	AS	vsstresu	,
NULL::text	AS	vsstat	,
NULL::text	AS	vsloc	,
NULL::text	AS	vsblfl	,
"FolderName"::text	AS	visit	,
"VSDAT"::timestamp without time zone	AS	vsdtc	,
NULL::time without time zone	AS	vstm
FROM tas120_201."VS2"
						
UNION ALL						

SELECT						
'TAS120-201'::text	AS	studyid	,
"SiteNumber"::text	AS	siteid	,
"Subject"::text	AS	usubjid	,
NULL::integer	AS	vsseq	,
NULL::text	AS	vstestcd	,
"VSSBP"::text	AS	vstest	,
'Systolic Blood Pressure'::text	AS	vscat	,
'Vital Signs'::text	AS	vsscat	,
'Vital Signs'::text	AS	vspos	,
"VSSBP"::text	AS	vsorres	,
"VSSBP_Units"::text	AS	vsorresu	,
"VSSBP"::numeric	AS	vsstresn	,
"VSSBP_Units"::text	AS	vsstresu	,
NULL::text	AS	vsstat	,
NULL::text	AS	vsloc	,
NULL::text	AS	vsblfl	,
"FolderName"::text	AS	visit	,
"VSDAT"::timestamp without time zone	AS	vsdtc	,
NULL::time without time zone	AS	vstm
FROM tas120_201."VS2"
						
UNION ALL						
	
SELECT	
'TAS120-201'::text	AS	studyid	,
"SiteNumber"::text	AS	siteid	,
"Subject"::text	AS	usubjid	,
NULL::integer	AS	vsseq	,
NULL::text	AS	vstestcd	,
"VSPR"::text	AS	vstest	,
'Pressure Rate'::text	AS	vscat	,
'Vital Signs'::text	AS	vsscat	,
'Vital Signs'::text	AS	vspos	,
"VSPR"::text	AS	vsorres	,
"VSPR_Units"::text	AS	vsorresu	,
"VSPR"::numeric	AS	vsstresn	,
"VSPR_Units"::text	AS	vsstresu	,
NULL::text	AS	vsstat	,
NULL::text	AS	vsloc	,
NULL::text	AS	vsblfl	,
"FolderName"::text	AS	visit	,
"VSDAT"::timestamp without time zone	AS	vsdtc	,
NULL::time without time zone	AS	vstm
FROM tas120_201."VS2"
						
UNION ALL	
SELECT					
						
'TAS120-201'::text	AS	studyid	,
"SiteNumber"::text	AS	siteid	,
"Subject"::text	AS	usubjid	,
NULL::integer	AS	vsseq	,
NULL::text	AS	vstestcd	,
"VSRESP"::text	AS	vstest	,
'Respiratory Rate'::text	AS	vscat	,
'Vital Signs'::text	AS	vsscat	,
'Vital Signs'::text	AS	vspos	,
"VSRESP"::text	AS	vsorres	,
"VSRESP_Units"::text	AS	vsorresu	,
"VSRESP"::numeric	AS	vsstresn	,
"VSRESP_Units"::text	AS	vsstresu	,
NULL::text	AS	vsstat	,
NULL::text	AS	vsloc	,
NULL::text	AS	vsblfl	,
"FolderName"::text	AS	visit	,
"VSDAT"::timestamp without time zone	AS	vsdtc	,
NULL::time without time zone	AS	vstm
FROM tas120_201."VS2"
						
UNION ALL		

SELECT						
'TAS120-201'::text	AS	studyid	,
"SiteNumber"::text	AS	siteid	,
"Subject"::text	AS	usubjid	,
NULL::integer	AS	vsseq	,
NULL::text	AS	vstestcd	,
"VSTEMP"::text	AS	vstest	,
'Temperature'::text	AS	vscat	,
'Vital Signs'::text	AS	vsscat	,
'Vital Signs'::text	AS	vspos	,
"VSTEMP"::text	AS	vsorres	,
"VSTEMP_Units"::text	AS	vsorresu	,
"VSTEMP"::numeric	AS	vsstresn	,
"VSTEMP_Units"::text	AS	vsstresu	,
NULL::text	AS	vsstat	,
NULL::text	AS	vsloc	,
NULL::text	AS	vsblfl	,
"FolderName"::text	AS	visit	,
"VSDAT"::timestamp without time zone	AS	vsdtc	,
NULL::time without time zone	AS	vstm
FROM tas120_201."VS2"
						
UNION ALL	
					
SELECT						
'TAS120-201'::text	AS	studyid	,
"SiteNumber"::text	AS	siteid	,
"Subject"::text	AS	usubjid	,
NULL::integer	AS	vsseq	,
NULL::text	AS	vstestcd	,
"VSWT"::text	AS	vstest	,
'Weight'::text	AS	vscat	,
'Vital Signs'::text	AS	vsscat	,
'Vital Signs'::text	AS	vspos	,
"VSWT"::text	AS	vsorres	,
"VSWT_Units"::text	AS	vsorresu	,
"VSWT"::numeric	AS	vsstresn	,
"VSWT_Units"::text	AS	vsstresu	,
NULL::text	AS	vsstat	,
NULL::text	AS	vsloc	,
NULL::text	AS	vsblfl	,
"FolderName"::text	AS	visit	,
"VSDAT"::timestamp without time zone	AS	vsdtc	,
NULL::time without time zone	AS	vstm
FROM tas120_201."VS2"
						
UNION ALL						

SELECT						
'TAS120-201'::text	AS	studyid	,
"SiteNumber"::text	AS	siteid	,
"Subject"::text	AS	usubjid	,
NULL::integer	AS	vsseq	,
NULL::text	AS	vstestcd	,
NULL::text	AS	vstest	,
'Height'::text	AS	vscat	,
'Vital Signs'::text	AS	vsscat	,
'Vital Signs'::text	AS	vspos	,
NULL::text	AS	vsorres	,
"VSHT_Units"::text	AS	vsorresu	,
NULL::numeric	AS	vsstresn	,
"VSHT_Units"::text	AS	vsstresu	,
NULL::text	AS	vsstat	,
NULL::text	AS	vsloc	,
NULL::text	AS	vsblfl	,
"FolderName"::text	AS	visit	,
"VSDAT"::timestamp without time zone	AS	vsdtc	,
NULL::time without time zone	AS	vstm
FROM tas120_201."VS2" )

SELECT
        /*KEY (vs.studyid || '~' || vs.siteid || '~' || vs.usubjid)::text AS comprehendid, KEY*/
        vs.studyid::text AS studyid,
        vs.siteid::text AS siteid,
        vs.usubjid::text AS usubjid,
        vs.vsseq::int AS vsseq, 
        vs.vstestcd::text AS vstestcd,
        vs.vstest::text AS vstest,
        vs.vscat::text AS vscat,
        vs.vsscat::text AS vsscat,
        vs.vspos::text AS vspos,
        vs.vsorres::text AS vsorres,
        vs.vsorresu::text AS vsorresu,
        vs.vsstresn::numeric AS vsstresn,
        vs.vsstresu::text AS vsstresu,
        vs.vsstat::text AS vsstat,
        vs.vsloc::text AS vsloc,
        vs.vsblfl::text AS vsblfl,
        vs.visit::text AS visit,
        vs.vsdtc::timestamp without time zone AS vsdtc,
        vs.vstm::time without time zone AS vstm
        /*KEY , (vs.studyid || '~' || vs.siteid || '~' || vs.usubjid || '~' || vs.vsseq)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM vs_data vs
JOIN included_subjects s ON (vs.studyid = s.studyid AND vs.siteid = s.siteid AND vs.usubjid = s.usubjid);

