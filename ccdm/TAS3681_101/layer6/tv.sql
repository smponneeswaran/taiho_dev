/*
CCDM TV mapping
Notes: Standard mapping to CCDM TV table
*/

WITH included_studies AS (
	SELECT studyid FROM study
),

tv_scheduled AS (

	SELECT 'Screening'::text AS visit, 1::numeric AS visitnum, 1::int AS visitdy, -14::int AS visitwindowbefore, 0::int AS visitwindowafter
	UNION ALL
	SELECT 'Baseline -3 to 0'::text AS visit, 2::numeric AS visitnum, -3::int AS visitdy, 0::int AS visitwindowbefore, 0::int AS visitwindowafter
	UNION ALL
	SELECT 'Escalation Cycle 01'::text AS visit, 3::numeric AS visitnum, -1::int AS visitdy, 0::int AS visitwindowbefore, 0::int AS visitwindowafter
	UNION ALL
	SELECT 'Escalation Cycle'::text AS visit, 4::numeric AS visitnum, 99999::int AS visitdy, 0::int AS visitwindowbefore, 0::int AS visitwindowafter
	UNION ALL
	SELECT 'Expansion Cycle 01'::text AS visit, 5::numeric AS visitnum, 99999::int AS visitdy, 0::int AS visitwindowbefore, 0::int AS visitwindowafter
	UNION ALL
	SELECT 'Expansion Cycle'::text AS visit, 6::numeric AS visitnum, 99999::int AS visitdy, 0::int AS visitwindowbefore, 0::int AS visitwindowafter
	UNION ALL
	SELECT 'Week 1 Day 1'::text AS visit, 7::numeric AS visitnum, 1::int AS visitdy, -1::int AS visitwindowbefore, 0::int AS visitwindowafter
	UNION ALL
	SELECT 'Week 2 Day 8'::text AS visit, 8::numeric AS visitnum, 8::int AS visitdy, 3::int AS visitwindowbefore, 3::int AS visitwindowafter
	UNION ALL
	SELECT 'Week 3 Day 15'::text AS visit, 9::numeric AS visitnum, 15::int AS visitdy, 3::int AS visitwindowbefore, 3::int AS visitwindowafter
	UNION ALL
	SELECT 'Week 4 Day 22'::text AS visit, 10::numeric AS visitnum, 22::int AS visitdy, 3::int AS visitwindowbefore, 3::int AS visitwindowafter
	UNION ALL
	SELECT 'Week 1 Day 1<W1DA1/>'::text AS visit, 11::numeric AS visitnum, 1::int AS visitdy, -1::int AS visitwindowbefore, 0::int AS visitwindowafter
	UNION ALL
	SELECT 'Week 2 Day 8<W2DA8/>'::text AS visit, 12::numeric AS visitnum, 8::int AS visitdy, 3::int AS visitwindowbefore, 3::int AS visitwindowafter
	UNION ALL
	SELECT 'Week 3 Day 15<W3DA15/>'::text AS visit, 13::numeric AS visitnum, 15::int AS visitdy, 3::int AS visitwindowbefore, 3::int AS visitwindowafter
	UNION ALL
	SELECT 'Week 4 Day 22<W4DA22/>'::text AS visit, 14::numeric AS visitnum, 22::int AS visitdy, 3::int AS visitwindowbefore, 3::int AS visitwindowafter
	UNION ALL
	SELECT 'Week 1 Day 1<WK1D1/>'::text AS visit, 15::numeric AS visitnum, 1::int AS visitdy, -1::int AS visitwindowbefore, 0::int AS visitwindowafter
	UNION ALL
	SELECT 'Week 3 Day 15<WK3D15/>'::text AS visit, 16::numeric AS visitnum, 15::int AS visitdy, 3::int AS visitwindowbefore, 3::int AS visitwindowafter
	UNION ALL
	SELECT 'Week 4 Day 28<WK4D28/>'::text AS visit, 17::numeric AS visitnum, 28::int AS visitdy, 3::int AS visitwindowbefore, 3::int AS visitwindowafter
	UNION ALL
	SELECT 'Week 1 Day 1<WK1DA1/>'::text AS visit, 18::numeric AS visitnum, 1::int AS visitdy, -1::int AS visitwindowbefore, 0::int AS visitwindowafter
	UNION ALL
	SELECT 'Week 3 Day 15<WK3DA15/>'::text AS visit, 19::numeric AS visitnum, 15::int AS visitdy, 3::int AS visitwindowbefore, 3::int AS visitwindowafter
	UNION ALL
	SELECT 'Week 4 Day 28<WK4DA28/>'::text AS visit, 20::numeric AS visitnum, 28::int AS visitdy, 3::int AS visitwindowbefore, 3::int AS visitwindowafter
	UNION ALL
	SELECT 'Tumor Follow-up'::text AS visit, 21::numeric AS visitnum, 99999::int AS visitdy, 0::int AS visitwindowbefore, 0::int AS visitwindowafter
	UNION ALL
	SELECT 'Subject Eligibility'::text AS visit, 22::numeric AS visitnum, 99999::int AS visitdy, 0::int AS visitwindowbefore, 0::int AS visitwindowafter
	UNION ALL
	SELECT 'Concomitant Medications and Therapies'::text AS visit, 23::numeric AS visitnum, 99999::int AS visitdy, 0::int AS visitwindowbefore, 0::int AS visitwindowafter
	UNION ALL
	SELECT 'CTC Biomarker Blood Sampling (AR)'::text AS visit, 24::numeric AS visitnum, 99999::int AS visitdy, 0::int AS visitwindowbefore, 0::int AS visitwindowafter
	UNION ALL
	SELECT 'CTC Enumeration'::text AS visit, 25::numeric AS visitnum, 99999::int AS visitdy, 0::int AS visitwindowbefore, 0::int AS visitwindowafter
	UNION ALL
	SELECT 'FDHT-PET Scan'::text AS visit, 26::numeric AS visitnum, 99999::int AS visitdy, 0::int AS visitwindowbefore, 0::int AS visitwindowafter
	UNION ALL
	SELECT 'ctDNA and Whole Blood mRNA Blood Sampling'::text AS visit, 27::numeric AS visitnum, 99999::int AS visitdy, 0::int AS visitwindowbefore, 0::int AS visitwindowafter
	UNION ALL
	SELECT 'CTC Signature Analysis Blood Sampling'::text AS visit, 28::numeric AS visitnum, 99999::int AS visitdy, 0::int AS visitwindowbefore, 0::int AS visitwindowafter
	UNION ALL
	SELECT 'Non protocol Defined Labs'::text AS visit, 29::numeric AS visitnum, 99999::int AS visitdy, 0::int AS visitwindowbefore, 0::int AS visitwindowafter
	UNION ALL
	SELECT 'Other Diagnostics and Procedures'::text AS visit, 30::numeric AS visitnum, 99999::int AS visitdy, 0::int AS visitwindowbefore, 0::int AS visitwindowafter
	UNION ALL
	SELECT 'On Treatment Death/Autopsy'::text AS visit, 31::numeric AS visitnum, 99999::int AS visitdy, 0::int AS visitwindowbefore, 0::int AS visitwindowafter
	UNION ALL
	SELECT 'On Treatment Death/Autopsy Prompt Form'::text AS visit, 32::numeric AS visitnum, 99999::int AS visitdy, 0::int AS visitwindowbefore, 0::int AS visitwindowafter
	UNION ALL
	SELECT 'Treatment Discontinuation'::text AS visit, 33::numeric AS visitnum, 99999::int AS visitdy, 0::int AS visitwindowbefore, 0::int AS visitwindowafter
	UNION ALL
	SELECT 'End of Therapy'::text AS visit, 34::numeric AS visitnum, 99999::int AS visitdy, 0::int AS visitwindowbefore, 0::int AS visitwindowafter
	UNION ALL
	SELECT '30-Day Follow-up'::text AS visit, 35::numeric AS visitnum, 99999::int AS visitdy, 0::int AS visitwindowbefore, 0::int AS visitwindowafter
	UNION ALL
	SELECT 'Adverse Event'::text AS visit, 51::numeric AS visitnum, 99999::int AS visitdy, 0::int AS visitwindowbefore, 0::int AS visitwindowafter
	UNION ALL
	SELECT 'SAE'::text AS visit, 52::numeric AS visitnum, 99999::int AS visitdy, 0::int AS visitwindowbefore, 0::int AS visitwindowafter
	UNION ALL
	SELECT 'ConMeds'::text AS visit, 53::numeric AS visitnum, 99999::int AS visitdy, 0::int AS visitwindowbefore, 0::int AS visitwindowafter
	UNION ALL
	SELECT 'Unscheduled Assessment'::text AS visit, 54::numeric AS visitnum, 99999::int AS visitdy, 0::int AS visitwindowbefore, 0::int AS visitwindowafter
	UNION ALL
	SELECT 'Tumor Core Biopsy'::text AS visit, 55::numeric AS visitnum, 99999::int AS visitdy, 0::int AS visitwindowbefore, 0::int AS visitwindowafter
	UNION ALL
	SELECT 'End of Study'::text AS visit, 56::numeric AS visitnum, 99999::int AS visitdy, 0::int AS visitwindowbefore, 0::int AS visitwindowafter
	UNION ALL
	SELECT 'COVID-19 IMPACT'::text AS visit, 57::numeric AS visitnum, 99999::int AS visitdy, 0::int AS visitwindowbefore, 0::int AS visitwindowafter
	
),

tv_data AS (
	SELECT
		'TAS3681_101'::text AS studyid,
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