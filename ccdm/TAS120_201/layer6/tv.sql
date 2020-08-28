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
	SELECT 'Cycle 1 Day 1'::text AS visit, 2::numeric AS visitnum, 1::int AS visitdy, 1::int AS visitwindowbefore, 1::int AS visitwindowafter
	UNION ALL
	SELECT 'Cycle 1 Day 4'::text AS visit, 3::numeric AS visitnum, 4::int AS visitdy, 1::int AS visitwindowbefore, 1::int AS visitwindowafter
	UNION ALL
	SELECT 'Cycle 1 Day 8'::text AS visit, 4::numeric AS visitnum, 8::int AS visitdy, 1::int AS visitwindowbefore, 1::int AS visitwindowafter
	UNION ALL
	SELECT 'Cycle 1 Day 15'::text AS visit, 5::numeric AS visitnum, 15::int AS visitdy, 1::int AS visitwindowbefore, 1::int AS visitwindowafter
	UNION ALL
	SELECT 'Cycle 1 Day 22'::text AS visit, 6::numeric AS visitnum, 22::int AS visitdy, 1::int AS visitwindowbefore, 1::int AS visitwindowafter
	UNION ALL
	SELECT 'Cycle 1 Day 28'::text AS visit, 7::numeric AS visitnum, 28::int AS visitdy, 1::int AS visitwindowbefore, 1::int AS visitwindowafter
	UNION ALL
	SELECT 'Cycle 2 Day 1'::text AS visit, 8::numeric AS visitnum, 56::int AS visitdy, 3::int AS visitwindowbefore, 3::int AS visitwindowafter
	UNION ALL
	SELECT 'Cycle 3 Day 1'::text AS visit, 9::numeric AS visitnum, 84::int AS visitdy, 3::int AS visitwindowbefore, 3::int AS visitwindowafter
	UNION ALL
	SELECT 'Cycle 4 Day 1'::text AS visit, 10::numeric AS visitnum, 112::int AS visitdy, 3::int AS visitwindowbefore, 3::int AS visitwindowafter
	UNION ALL
	SELECT 'Cycle 5 Day 1'::text AS visit, 11::numeric AS visitnum, 140::int AS visitdy, 3::int AS visitwindowbefore, 3::int AS visitwindowafter
	UNION ALL
	SELECT 'Cycle 6 Day 1'::text AS visit, 12::numeric AS visitnum, 168::int AS visitdy, 3::int AS visitwindowbefore, 3::int AS visitwindowafter
	UNION ALL
	SELECT 'Cycle 7 Day 1'::text AS visit, 13::numeric AS visitnum, 196::int AS visitdy, 3::int AS visitwindowbefore, 3::int AS visitwindowafter
	UNION ALL
	SELECT 'Cycle 8 Day 1'::text AS visit, 14::numeric AS visitnum, 224::int AS visitdy, 3::int AS visitwindowbefore, 3::int AS visitwindowafter
	UNION ALL
	SELECT 'Cycle 9 Day 1'::text AS visit, 15::numeric AS visitnum, 252::int AS visitdy, 3::int AS visitwindowbefore, 3::int AS visitwindowafter
	UNION ALL
	SELECT 'Cycle 10 Day 1'::text AS visit, 16::numeric AS visitnum, 280::int AS visitdy, 3::int AS visitwindowbefore, 3::int AS visitwindowafter
	UNION ALL
	SELECT 'Cycle 11 Day 1'::text AS visit, 17::numeric AS visitnum, 308::int AS visitdy, 3::int AS visitwindowbefore, 3::int AS visitwindowafter
	UNION ALL
	SELECT 'Cycle 12 Day 1'::text AS visit, 18::numeric AS visitnum, 336::int AS visitdy, 3::int AS visitwindowbefore, 3::int AS visitwindowafter
	UNION ALL
	SELECT 'End of Treatment'::text AS visit, 19::numeric AS visitnum, 99999::int AS visitdy, 0::int AS visitwindowbefore, 0::int AS visitwindowafter
	UNION ALL
	SELECT '30 day After Last Dose'::text AS visit, 20::numeric AS visitnum, 99999::int AS visitdy, 0::int AS visitwindowbefore, 0::int AS visitwindowafter
	UNION ALL
	SELECT 'Adverse Events'::text AS visit, 21::numeric AS visitnum, 99999::int AS visitdy, 0::int AS visitwindowbefore, 0::int AS visitwindowafter
	UNION ALL
	SELECT 'Concomitant Medications'::text AS visit, 21::numeric AS visitnum, 99999::int AS visitdy, 0::int AS visitwindowbefore, 0::int AS visitwindowafter
	UNION ALL
	SELECT 'Hospitalization'::text AS visit, 21::numeric AS visitnum, 99999::int AS visitdy, 0::int AS visitwindowbefore, 0::int AS visitwindowafter
	UNION ALL
	SELECT 'Dose Limiting Toxicity'::text AS visit, 22::numeric AS visitnum, 99999::int AS visitdy, 0::int AS visitwindowbefore, 0::int AS visitwindowafter
	UNION ALL
	SELECT 'Lesions'::text AS visit, 23::numeric AS visitnum, 99999::int AS visitdy, 0::int AS visitwindowbefore, 0::int AS visitwindowafter
	UNION ALL
	SELECT 'End of Study'::text AS visit, 24::numeric AS visitnum, 99999::int AS visitdy, 0::int AS visitwindowbefore, 0::int AS visitwindowafter
	UNION ALL
	SELECT 'Follow-up Period'::text AS visit, 25::numeric AS visitnum, 99999::int AS visitdy, 0::int AS visitwindowbefore, 0::int AS visitwindowafter
	UNION ALL
	SELECT 'Death/Autopsy'::text AS visit, 26::numeric AS visitnum, 99999::int AS visitdy, 0::int AS visitwindowbefore, 0::int AS visitwindowafter
	UNION ALL
	SELECT 'Unscheduled'::text AS visit, 27::numeric AS visitnum, 99999::int AS visitdy, 0::int AS visitwindowbefore, 0::int AS visitwindowafter
),

tv_data AS (
	SELECT
		'TAS120_201'::text AS studyid,
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