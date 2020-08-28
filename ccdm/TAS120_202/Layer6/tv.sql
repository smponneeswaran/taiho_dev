/*
CCDM TV mapping
Notes: Standard mapping to CCDM TV table
*/

WITH included_studies AS (
	SELECT studyid FROM study
),

tv_scheduled AS (

	SELECT 'Screening'::text AS visit, 1::numeric AS visitnum, 0::int AS visitdy, 0::int AS visitwindowbefore, 0::int AS visitwindowafter
	UNION ALL
	SELECT 'Cycle 01'::text AS visit, 2::numeric AS visitnum, 99999::int AS visitdy, 0::int AS visitwindowbefore, 0::int AS visitwindowafter
	UNION ALL
	SELECT 'Day 1 of Cycle 1'::text AS visit, 3::numeric AS visitnum, 1::int AS visitdy, -1::int AS visitwindowbefore, 0::int AS visitwindowafter
	UNION ALL
	SELECT 'Day 8 of Cycle 1'::text AS visit, 4::numeric AS visitnum, 8::int AS visitdy, -1::int AS visitwindowbefore, 0::int AS visitwindowafter
	UNION ALL
	SELECT 'Day 15 of Cycle 1'::text AS visit, 5::numeric AS visitnum, 15::int AS visitdy, -1::int AS visitwindowbefore, 0::int AS visitwindowafter
	UNION ALL
	SELECT 'Day 22 of Cycle 1'::text AS visit, 6::numeric AS visitnum, 22::int AS visitdy, -1::int AS visitwindowbefore, 0::int AS visitwindowafter
	UNION ALL
	SELECT 'Cycle 02'::text AS visit, 7::numeric AS visitnum, 99999::int AS visitdy, 0::int AS visitwindowbefore, 0::int AS visitwindowafter
	UNION ALL
	SELECT 'Day 1 of Cycle 2'::text AS visit, 8::numeric AS visitnum, 1::int AS visitdy, -1::int AS visitwindowbefore, 0::int AS visitwindowafter
	UNION ALL
	SELECT 'Day 1 of Cycle'::text AS visit, 9::numeric AS visitnum, 99999::int AS visitdy, 0::int AS visitwindowbefore, 0::int AS visitwindowafter
	UNION ALL
	SELECT 'End of Treatment'::text AS visit, 11::numeric AS visitnum, 99999::int AS visitdy, 0::int AS visitwindowbefore, 0::int AS visitwindowafter
	UNION ALL
	SELECT 'Safety Follow-up 30 days after last dose'::text AS visit, 12::numeric AS visitnum, 99999::int AS visitdy, 0::int AS visitwindowbefore, 0::int AS visitwindowafter
	UNION ALL
	SELECT 'Tumor Assessments'::text AS visit, 13::numeric AS visitnum, 99999::int AS visitdy, 0::int AS visitwindowbefore, 0::int AS visitwindowafter
	UNION ALL
	SELECT 'Tumor Best Overall Response'::text AS visit, 14::numeric AS visitnum, 99999::int AS visitdy, 0::int AS visitwindowbefore, 0::int AS visitwindowafter
	UNION ALL
	SELECT 'Tumor Assessment (Baseline)'::text AS visit, 15::numeric AS visitnum, 99999::int AS visitdy, 0::int AS visitwindowbefore, 0::int AS visitwindowafter
	UNION ALL
	SELECT 'Tumor Assessment (Post Baseline)'::text AS visit, 16::numeric AS visitnum, 99999::int AS visitdy, 0::int AS visitwindowbefore, 0::int AS visitwindowafter
	UNION ALL
	SELECT 'Response Assessment - MLN'::text AS visit, 17::numeric AS visitnum, 99999::int AS visitdy, 0::int AS visitwindowbefore, 0::int AS visitwindowafter
	UNION ALL
	SELECT 'Adverse Events'::text AS visit, 18::numeric AS visitnum, 99999::int AS visitdy, 0::int AS visitwindowbefore, 0::int AS visitwindowafter
	UNION ALL
	SELECT 'Concomitant Medications and Therapies'::text AS visit, 19::numeric AS visitnum, 99999::int AS visitdy, 0::int AS visitwindowbefore, 0::int AS visitwindowafter
	UNION ALL
	SELECT 'Other Diagnostics and Procedures'::text AS visit, 20::numeric AS visitnum, 99999::int AS visitdy, 0::int AS visitwindowbefore, 0::int AS visitwindowafter
	UNION ALL
	SELECT 'Unscheduled Assessments'::text AS visit, 21::numeric AS visitnum, 99999::int AS visitdy, 0::int AS visitwindowbefore, 0::int AS visitwindowafter
	UNION ALL
	SELECT 'Death/Autopsy'::text AS visit, 22::numeric AS visitnum, 99999::int AS visitdy, 0::int AS visitwindowbefore, 0::int AS visitwindowafter
	UNION ALL
	SELECT 'Survival Follow-Up'::text AS visit, 23::numeric AS visitnum, 99999::int AS visitdy, 0::int AS visitwindowbefore, 0::int AS visitwindowafter
	UNION ALL
	SELECT 'End of Study'::text AS visit, 24::numeric AS visitnum, 99999::int AS visitdy, 0::int AS visitwindowbefore, 0::int AS visitwindowafter
	UNION ALL
	SELECT 'Bone Marrow Biopsy'::text AS visit, 25::numeric AS visitnum, 99999::int AS visitdy, 0::int AS visitwindowbefore, 0::int AS visitwindowafter
	UNION ALL

	SELECT 'Adjudication Response'::text AS visit, 26::numeric AS visitnum, 99999::int AS visitdy, 0::int AS visitwindowbefore, 0::int AS visitwindowafter
),

tv_data AS (
	SELECT
		'TAS120_202'::text AS studyid,
		visitnum::numeric AS visitnum,
		visit::text AS visit,
		visitdy::int AS visitdy,
		visitwindowbefore::int AS visitwindowbefore,
		visitwindowafter::int AS visitwindowafter
	FROM tv_scheduled tvs

	UNION ALL

	SELECT
		DISTINCT sv.studyid::text AS studyid,
		coalesce(sv."visitnum", 99)::numeric AS visitnum,
		sv."visit"::text AS visit,
		99999::int AS visitdy,
		0::int AS visitwindowbefore,
		0::int AS visitwindowafter
	FROM sv 
	WHERE (studyid, visit) NOT IN (SELECT DISTINCT studyid, visit FROM tv_scheduled)

	/*UNION ALL
	SELECT 
		DISTINCT studyid::text AS studyid,
		'99'::numeric AS visitnum,
		visit::text AS visit,
		'99999'::int AS visitdy,
		0::int AS visitwindowbefore,
		0::int AS visitwindowafter
	FROM formdata 
	WHERE (studyid, visit) NOT IN (SELECT DISTINCT studyid, visit FROM sv) 
	AND (studyid, visit) NOT IN (SELECT studyid, visit FROM tv_scheduled)*/
  
	/* UNION ALL

	SELECT 
		DISTINCT studyid::text AS studyid,
		'99'::int AS visitnum,
		visit::text AS visit,
		'99999'::int AS visitdy,
		0::int AS visitwindowbefore,
		0::int AS visitwindowafter
	FROM dv 
	WHERE (studyid, visit) NOT IN (SELECT DISTINCT studyid, visit FROM sv) 
	AND (studyid, visit) NOT IN (SELECT studyid, visit FROM tv_scheduled) */

)

SELECT
	/*KEY tv.studyid::text AS comprehendid, KEY*/
	tv.studyid::text AS studyid,
	tv.visitnum::numeric AS visitnum,
	tv.visit::text AS visit,
	tv.visitdy::int AS visitdy,
	tv.visitwindowbefore::int AS visitwindowbefore,
	tv.visitwindowafter::int AS visitwindowafter
	/*KEY , (tv.studyid || '~' || tv.visit)::text  AS objectuniquekey KEY*/
	/*KEY , now()::timestamp without time zone AS comprehend_update_time KEY*/
FROM tv_data tv
JOIN included_studies st ON (st.studyid = tv.studyid);