/*
CCDM SiteMilestone mapping
Notes: Standard mapping to CCDM SiteMilestone table
*/

WITH included_sites AS (
                SELECT DISTINCT studyid, siteid FROM site),

     sitemilestone_data AS (
                SELECT  null::text AS studyid,
                        null::text AS siteid,
                        null::int AS milestoneseq,
                        null::text AS milestonelabel,
                        null::text AS milestonetype,
                        null::date AS expecteddate,
                        null::boolean AS ismandatory )

SELECT 
        /*KEY (sm.studyid || '~' || sm.siteid)::text AS comprehendid, KEY*/
        sm.studyid::text AS studyid,
        sm.siteid::text AS siteid,
        sm.milestoneseq::int AS milestoneseq,
        sm.milestonelabel::text AS milestonelabel,
        sm.milestonetype::text AS milestonetype,
        sm.expecteddate::date AS expecteddate,
        sm.ismandatory::boolean AS ismandatory
        /*KEY , (sm.studyid || '~' || sm.siteid || '~' || sm.milestonetype || '~' || sm.milestoneseq)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM sitemilestone_data sm
JOIN included_sites si ON (sm.studyid = si.studyid AND sm.siteid = si.siteid)
WHERE 1=2; 
