/*
dimsite

Notes: PLO having site and site performance details together.

*/

CREATE TABLE dimsite AS

WITH study_subjects AS  
                    ( SELECT ds.studyid,
                          ds.siteid,
                          count(1) as site_subjects
                          FROM  rpt_subject_disposition ds
                          WHERE dsterm = study_start_disposition
                          GROUP BY
                          ds.studyid, ds.siteid),

    -- CTE to get SV count at site level
    sv_count AS (SELECT sv.studyid, sv.siteid, count(*) as sv_count
                    FROM sv
                    GROUP BY studyid, siteid)

SELECT sp.comprehendid::text as comprehendid,
       sp.siteid::text as siteid,
       sp.studyid::text as studyid,
       sp.sitename::text as sitename,
       s.croid::text as croid,
       sp.sitecro::text as sitecro,
       s.siteinvestigatorname::text as siteinvestigatorname,
       sp.sitecraname::text as sitecraname,
       sp.sitecountry::text as sitecountry,
       sp.siteregion::text as siteregion,
       s.sitecreationdate::date as sitecreationdate,
       s.siteactivationdate::date as siteactivationdate,
       s.sitedeactivationdate::date as sitedeactivationdate,
       s.siteaddress1::text as siteaddress1,
       s.siteaddress2::text as siteaddress2,
       s.sitecity::text as sitecity,
       s.sitestate::text as sitestate,
       s.sitepostal::text as sitepostal,
       ss.site_subjects::int as site_subjects,
       sp.subject_days_count::int as subject_days,
       sp.subjects_screened_count::int as subjects_screened_count,
       sp.failed_screen_count::int as failed_screen_count, 
       sp.site_active_days_count::int as site_active_days_count, 
       sp.enrolled_count::int as enrolled_count,
       sp.withdrawn_count::int as withdrawn_count, 
       sp.open_query_count::int as open_query_count, 
       sp.ae_count::int as ae_count, 
       sp.dv_count::int as dv_count,
       sp.subject_days_count::int as subject_days_count,
       sp.screen_failure_rate::numeric as screen_failure_rate, 
       sp.enrollment_rate::numeric as enrollment_rate, 
       sp.withdrawal_rate::numeric as withdrawal_rate, 
       sp.open_query_rate::numeric as open_query_rate, 
       sp.ae_rate::numeric as ae_rate, 
       sp.pd_rate::numeric as pd_rate,
       sv.sv_count::int as sv_count,
       now()::timestamp as comprehend_update_time
FROM rpt_site_performance sp
JOIN site s ON (sp.comprehendid = s.comprehendid)
LEFT JOIN study_subjects ss ON (ss.studyid = sp.studyid AND ss.siteid = sp.siteid)
LEFT JOIN sv_count sv ON (sp.studyid = sv.studyid AND sp.siteid = sv.siteid);

