/*
CCDM DM mapping
Notes: Standard mapping to CCDM DM table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject ),

     dm_data AS (
                SELECT
				"project"	::text	AS	studyid	,
				"SiteNumber"	::text	AS	siteid	,
				"Subject"	::text	AS	usubjid	,
				"FolderSeq"::text	AS	visitnum	,
				"FolderName"::text	AS	visit	,
				COALESCE("MinCreated", "RecordDate")::text	AS	dmdtc	,
				'NA'::text	AS	brthdtc	,
				"DMAGE"::text	AS	age	,
				"DMSEX"::text	AS	sex	,
				COALESCE("DMRACE", "DMOTH")::text	AS	race	,
				"DMETHNIC"::text	AS	ethnicity	,
				'NA'::text	AS armcd,
				'NA'::text	AS	arm
				FROM tas120_201."DM" )

SELECT 
        /*KEY (dm.studyid || '~' || dm.siteid || '~' || dm.usubjid)::text AS comprehendid, KEY*/
        dm.studyid::text AS studyid,
        dm.siteid::text AS siteid,
        dm.usubjid::text AS usubjid,
        dm.visitnum::numeric AS visitnum,
        dm.visit::text AS visit,
        dm.dmdtc::date AS dmdtc,
        dm.brthdtc::date AS brthdtc,
        dm.age::integer AS age,
        dm.sex::text AS sex,
        dm.race::text AS race,
        dm.ethnicity::text AS ethnicity,
        dm.armcd::text AS armcd,
        dm.arm::text AS arm
        /*KEY ,(dm.studyid || '~' || dm.siteid || '~' || dm.usubjid )::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM dm_data dm
JOIN included_subjects s ON (dm.studyid = s.studyid AND dm.siteid = s.siteid AND dm.usubjid = s.usubjid);
