/*
CCDM SV mapping
Notes: Standard mapping to CCDM SV table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject ),

sv_data AS (
				Select 
				sv.studyid,
				sv.siteid,
				sv.usubjid, 
				sv.visitnum,
				sv.visit,
				Row_number() OVER (partition BY sv.studyid, sv.siteid, sv.usubjid ORDER BY sv.visitseq, sv.svstdtc)AS visitseq,
				sv.svstdtc,
				sv.svendtc
				
                
				from(SELECT  "project"::text AS studyid,
                        "SiteNumber"::text AS siteid,
                        "Subject"::text AS usubjid, 
                        "FolderSeq"::numeric AS visitnum,
                        "FolderName"::text AS visit,
                       null::int AS visitseq, /* defaulted to 1 - deprecated */
                        "PEDAT"::date AS svstdtc,
                        "PEDAT"::date AS svendtc
				from	tas3681_101."PE"
				
				union all
				
				SELECT  "project"::text AS studyid,
                        "SiteNumber"::text AS siteid,
                        "Subject"::text AS usubjid, 
                        "FolderSeq"::numeric AS visitnum,
                        "FolderName"::text AS visit,
                       null::int AS visitseq, /* defaulted to 1 - deprecated */
                        "CYCLEDAT"::date AS svstdtc,
                        "CYCLEDAT"::date AS svendtc
				from	tas3681_101."CYCLE"
				
				union all
				SELECT  "project"::text AS studyid,
                        "SiteNumber"::text AS siteid,
                        "Subject"::text AS usubjid, 
                        "FolderSeq"::numeric AS visitnum,
                        "FolderName"::text AS visit,
                       null::int AS visitseq, /* defaulted to 1 - deprecated */
                        "VSDAT"::date AS svstdtc,
                        "VSDAT"::date AS svendtc
				from	tas3681_101."VS") sv
),

sv_data_min as
(

	select * from sv_data
	where (studyid,siteid,usubjid,visitnum,visit,svstdtc,visitseq) in
	(
		select studyid,siteid,usubjid,visitnum,visit,min(svstdtc) as svstdtc,min(visitseq)  as visitseq
		from sv_data
		group by studyid,siteid,usubjid,visitnum,visit
	)
)

SELECT 
        /*KEY (sv.studyid || '~' || sv.siteid || '~' || sv.usubjid)::text AS comprehendid, KEY*/
        sv.studyid::text AS studyid,
        sv.siteid::text AS siteid,
        sv.usubjid::text AS usubjid, 
        sv.visitnum::numeric AS visitnum,
        sv.visit::text AS visit,
        sv.visitseq::int AS visitseq,
        sv.svstdtc::date AS svstdtc,
        sv.svendtc::date AS svendtc
        /*KEY , (sv.studyid || '~' || sv.siteid || '~' || sv.usubjid || '~' || sv.visitnum)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM sv_data_min sv
JOIN included_subjects s ON (sv.studyid = s.studyid AND sv.siteid = s.siteid AND sv.usubjid = s.usubjid); 
